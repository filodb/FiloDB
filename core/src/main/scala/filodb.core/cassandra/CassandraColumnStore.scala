package filodb.core.cassandra

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import spray.caching._

import filodb.core.messages._
import filodb.core.datastore2.{ColumnStore, Types}

/**
 * Implementation of a column store using Apache Cassandra tables.
 * This class must be thread-safe as it is intended to be used concurrently.
 *
 * Both the instances of the Segment* table classes above as well as ChunkRowMap entries
 * are cached for faster I/O.
 *
 * ==Configuration==
 * {{{
 *   cassandra {
 *     keyspace = "my_cass_keyspace"
 *   }
 *   columnstore {
 *     tablecache-size = 50    # Number of cache entries for the table cache
 *     rowmap-cache-size = 1000    # Number of ChunkRowMap entries to cache
 *   }
 * }}}
 *
 * ==Constructor Args==
 * @param config see the Configuration section above for the needed config
 * @param ec An ExecutionContext for futures.  See this for a way to do backpressure with futures:
 *        http://quantifind.com/blog/2015/06/throttling-instantiations-of-scala-futures-1/
 */
class CassandraColumnStore(config: Config)
                          (implicit val ec: ExecutionContext) extends ColumnStore with StrictLogging {
  import filodb.core.datastore2._
  import Types._

  val cassandraConfig = config.getConfig("cassandra")
  val tableCacheSize = config.getInt("columnstore.tablecache-size")
  val rowMapCacheSize = config.getInt("columnstore.rowmap-cache-size")

  val chunkTableCache = LruCache[ChunkTable](tableCacheSize)
  val rowMapTableCache = LruCache[ChunkRowMapTable](tableCacheSize)
  val rowMapCache = LruCache[UpdatableChunkRowMap[_]](rowMapCacheSize)

  def appendSegment[K : SortKeyHelper](segment: Segment[K], version: Int): Future[Response] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(segment.dataset)
          rowMap                 <- getChunkMap(segment, version, chunkTable, rowMapTable)
          writeChunkResp         <- writeChunks(chunkTable, segment, version)
          writeIndexResp         <- mergeAndWriteIndex(rowMapTable, rowMap, segment, version)
            if writeChunkResp == Success }
    yield { if (writeChunkResp == Success) writeIndexResp else writeChunkResp }
  }

  def readSegments[K : SortKeyHelper](columns: Set[String], keyRange: KeyRange[K], version: Int):
      Future[Iterator[Segment[K]]] = {
    // TODO: implement actual paging and the iterator over segments.  Or maybe that should be implemented
    // at a higher level.
    populateSegments(columns, keyRange, version).map(_.toIterator)
  }

  def clearRowMapCache(): Unit = { rowMapCache.clear() }

  // Retrieve handles to the tables for a particular dataset from the cache, creating the instances
  // if necessary
  def getSegmentTables(dataset: TableName): Future[(ChunkTable, ChunkRowMapTable)] = {
    val chunkTableFuture = chunkTableCache(dataset) {
      logger.debug(s"Creating a new ChunkTable for dataset $dataset with config $cassandraConfig")
      new ChunkTable(dataset, cassandraConfig)
    }
    val ChunkRowMapTableFuture = rowMapTableCache(dataset) {
      logger.debug(s"Creating a new ChunkRowMapTable for dataset $dataset with config $cassandraConfig")
      new ChunkRowMapTable(dataset, cassandraConfig)
    }
    for { chunkTable <- chunkTableFuture
          rowMapTable <- ChunkRowMapTableFuture }
    yield { (chunkTable, rowMapTable) }
  }

  // Retrieves the ChunkRowMap for a given segment from the cache.  If not present, tries to read it
  // from the ChunkRowMapTable, and if that's not there, then it's a new segment and we create a new
  // ChunkRowMap.
  private def getChunkMap[K : SortKeyHelper]
                            (segment: Segment[K],
                             version: Int,
                             chunkTable: ChunkTable,
                             rowMapTable: ChunkRowMapTable): Future[UpdatableChunkRowMap[K]] = {
    val idx = rowMapCache((segment.dataset, segment.partition, segment.segmentId, version)) {
      (rowMapTable.getChunkMaps(segment.partition, version, segment.segmentId, segment.segmentId) collect {
        case Nil =>
          logger.debug(s"No row index found for $segment, creating a new one")
          (new UpdatableChunkRowMap[K])
        case chunks: Any =>
          ???
      }).asInstanceOf[Future[UpdatableChunkRowMap[_]]]
    }
    idx.asInstanceOf[Future[UpdatableChunkRowMap[K]]]
  }

  // Writes all the chunks from the segment to the ChunkTable
  private def writeChunks[K](chunkTable: ChunkTable, segment: Segment[K], version: Int): Future[Response] =
    chunkTable.writeChunks(segment.partition, version, segment.segmentId, segment.getChunks)

  // Merges the rowMap, which represents the current (pre-write) segment row index, with the
  // row map from segment, which contains the updates and new inserts to rows, producing a new
  // combined ChunkRowMap which will be written to the rowMapTable.
  private def mergeAndWriteIndex[K](rowMapTable: ChunkRowMapTable,
                                    rowMap: UpdatableChunkRowMap[K],
                                    segment: Segment[K],
                                    version: Int): Future[Response] = {
    rowMap.update(segment.index.asInstanceOf[UpdatableChunkRowMap[K]])
    val (chunkIds, rowNums) = rowMap.serialize()
    rowMapTable.writeChunkMap(segment.partition, version, segment.segmentId,
                             chunkIds, rowNums)
  }

  // Populates segments by reading the ChunkRowMaps first, then reading the columnar chunk data.
  // Does most of the work of actually reading segments... but does not page results.
  // NOTE: ChunkRowMaps could be used to estimate how much data will be read in the chunks, and
  // thus limit the amount of memory used / terminate early
  private def populateSegments[K : SortKeyHelper](columns: Set[String], keyRange: KeyRange[K], version: Int):
      Future[Seq[Segment[K]]] = {
    val helper = implicitly[SortKeyHelper[K]]
    val fut =
      for { (chunkTable, rowMapTable) <- getSegmentTables(keyRange.dataset)
            cassRowMaps <- rowMapTable.getChunkMaps(keyRange.partition, version,
                                                    keyRange.binaryStart, keyRange.binaryEnd)
            segments     = createSegments(cassRowMaps, keyRange)
            chunks      <- getAllChunks(chunkTable, columns, keyRange, version) if cassRowMaps.nonEmpty
      } yield {
        chunks.foreach { case ChunkedData(columnName, chunkTriples) =>
          var segIndex = 0
          chunkTriples.foreach { case (segmentId, chunkId, chunkBytes) =>
            // Rely on the fact that chunks are sorted by segmentId, in the same order as the rowMaps
            val segmentKey = helper.fromBytes(segmentId)
            while (segmentKey != segments(segIndex).keyRange.start) segIndex += 1
            segments(segIndex).addChunk(chunkId, columnName, chunkBytes)
          }
        }
        segments
      }
    fut.recover {
      // No chunk maps found, so just return empty list of segments
      case e: java.util.NoSuchElementException => Nil
    }
  }

  private def createSegments[K : SortKeyHelper](cassRowMaps: Seq[ChunkRowMapRecord],
                                                origKeyRange: KeyRange[K]): Seq[Segment[K]] = {
    val helper = implicitly[SortKeyHelper[K]]
    cassRowMaps.map {
      case ChunkRowMapRecord(segmentId, chunkIds, rowNums, _) =>
        val rowMap = new BinaryChunkRowMap(chunkIds, rowNums)
        val (segStart, segEnd) = helper.getSegment(helper.fromBytes(segmentId))
        val segKeyRange = origKeyRange.copy(start = segStart, end = segEnd)
        new GenericSegment(segKeyRange, rowMap)
    }
  }

  // Reads columnar chunk data from each column in parallel.
  // Future.sequence creates a Future.failure() with the first exception from any of the futures.
  private def getAllChunks[K](chunkTable: ChunkTable,
                              columns: Set[String],
                              keyRange: KeyRange[K],
                              version: Int): Future[Seq[ChunkedData]] =
    Future.sequence(columns.toSeq.map(chunkTable.readChunks(keyRange.partition, version, _,
                                                            keyRange.binaryStart, keyRange.binaryEnd)))
}
