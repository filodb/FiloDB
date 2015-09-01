package filodb.core.cassandra

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import spray.caching._

import filodb.core.messages._
import filodb.core.datastore2.{CachedMergingColumnStore, Types}

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
 *     tablecache-size = 50    # Number of cache entries for C* for ChunkTable etc.
 *     segment-cache-size = 1000    # Number of segments to cache
 *   }
 * }}}
 *
 * ==Constructor Args==
 * @param config see the Configuration section above for the needed config
 * @param ec An ExecutionContext for futures.  See this for a way to do backpressure with futures:
 *        http://quantifind.com/blog/2015/06/throttling-instantiations-of-scala-futures-1/
 */
class CassandraColumnStore(config: Config)
                          (implicit val ec: ExecutionContext)
extends CachedMergingColumnStore with StrictLogging {
  import filodb.core.datastore2._
  import Types._

  val cassandraConfig = config.getConfig("cassandra")
  val tableCacheSize = config.getInt("columnstore.tablecache-size")
  val segmentCacheSize = config.getInt("columnstore.segment-cache-size")

  val chunkTableCache = LruCache[ChunkTable](tableCacheSize)
  val rowMapTableCache = LruCache[ChunkRowMapTable](tableCacheSize)
  val segmentCache = LruCache[Segment[_]](segmentCacheSize)

  // TODO: yes, we need to actually get a real sort key :)
  val mergingStrategy = new AppendingChunkMergingStrategy(this, { x => "Foo" })

  /**
   * Implementations of low-level storage primitives
   */
  def writeChunks(dataset: TableName,
                  partition: PartitionKey,
                  version: Int,
                  segmentId: ByteBuffer,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          resp <- chunkTable.writeChunks(partition, version, segmentId, chunks) }
    yield { resp }
  }

  def writeChunkRowMap(dataset: TableName,
                       partition: PartitionKey,
                       version: Int,
                       segmentId: ByteBuffer,
                       chunkRowMap: ChunkRowMap): Future[Response] = {
    val (chunkIds, rowNums) = chunkRowMap.serialize()
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          resp <- rowMapTable.writeChunkMap(partition, version, segmentId,
                                            chunkIds, rowNums, chunkRowMap.nextChunkId) }
    yield { resp }
  }

  def readChunks[K](columns: Set[ColumnId],
                    keyRange: KeyRange[K],
                    version: Int): Future[Seq[ChunkedData]] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(keyRange.dataset)
          data <- Future.sequence(columns.toSeq.map(
                    chunkTable.readChunks(keyRange.partition, version, _,
                                          keyRange.binaryStart, keyRange.binaryEnd))) }
    yield { data }
  }

  def readChunkRowMaps[K](keyRange: KeyRange[K],
                          version: Int): Future[Seq[(ByteBuffer, ChunkRowMap)]] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(keyRange.dataset)
          cassRowMaps <- rowMapTable.getChunkMaps(keyRange.partition, version,
                                                  keyRange.binaryStart, keyRange.binaryEnd) }
    yield {
      cassRowMaps.map {
        case ChunkRowMapRecord(segmentId, chunkIds, rowNums, nextChunkId) =>
          val rowMap = new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId)
          (segmentId, rowMap)
      }
    }
  }

  def scanChunkRowMaps(dataset: TableName,
                       partitionFilter: (PartitionKey => Boolean),
                       params: Map[String, String])
                      (processFunc: (ChunkRowMap => Unit)): Future[Response] = ???

  // Retrieve handles to the tables for a particular dataset from the cache, creating the instances
  // if necessary
  def getSegmentTables(dataset: TableName): Future[(ChunkTable, ChunkRowMapTable)] = {
    val chunkTableFuture = chunkTableCache(dataset) {
      logger.debug(s"Creating a new ChunkTable for dataset $dataset with config $cassandraConfig")
      new ChunkTable(dataset, cassandraConfig)
    }
    val chunkRowMapTableFuture = rowMapTableCache(dataset) {
      logger.debug(s"Creating a new ChunkRowMapTable for dataset $dataset with config $cassandraConfig")
      new ChunkRowMapTable(dataset, cassandraConfig)
    }
    for { chunkTable <- chunkTableFuture
          rowMapTable <- chunkRowMapTableFuture }
    yield { (chunkTable, rowMapTable) }
  }
}
