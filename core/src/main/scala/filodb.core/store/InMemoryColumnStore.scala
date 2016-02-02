package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
import javax.xml.bind.DatatypeConverter
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import spray.caching._

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}

/**
 * A ColumnStore implementation which is entirely in memory for speed.
 * Good for testing or performance.
 *
 * NOTE: This implementation effectively only works on a single node.
 * We would need, for example, a Spark-specific implementation which can
 * know how to distribute data, or at least keep track of different nodes,
 * TODO: use thread-safe structures
 */
class InMemoryColumnStore(val readEc: ExecutionContext)(implicit val ec: ExecutionContext)
extends CachedMergingColumnStore with InMemoryColumnStoreScanner with StrictLogging {
  import Types._
  import collection.JavaConversions._

  logger.info("Starting InMemoryColumnStore...")

  val segmentCache = LruCache[Segment](100)

  val mergingStrategy = new AppendingChunkMergingStrategy(this)

  val chunkDb = new HashMap[(TableName, BinaryPartition, Int), ChunkTree]
  val rowMaps = new HashMap[(TableName, BinaryPartition, Int), RowMapTree]

  def initializeProjection(projection: Projection): Future[Response] = Future.successful(Success)

  def clearProjectionDataInner(projection: Projection): Future[Response] = Future {
    chunkDb.keys.collect { case key @ (ds, _, _) if ds == projection.dataset => chunkDb remove key }
    rowMaps.keys.collect { case key @ (ds, _, _) if ds == projection.dataset => rowMaps remove key }
    Success
  }

  def writeChunks(dataset: TableName,
                  partition: BinaryPartition,
                  version: Int,
                  segmentId: SegmentId,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response] = Future {
    val chunkTree = chunkDb.synchronized {
      chunkDb.getOrElseUpdate((dataset, partition, version), new ChunkTree(Ordering[ChunkKey]))
    }
    chunks.foreach { case (colId, chunkId, bytes) =>
      chunkTree.put((colId, segmentId, chunkId), bytes)
    }
    Success
  }

  def writeChunkRowMap(dataset: TableName,
                       partition: BinaryPartition,
                       version: Int,
                       segmentId: SegmentId,
                       chunkRowMap: ChunkRowMap): Future[Response] = Future {
    val rowMapTree = rowMaps.synchronized {
      rowMaps.getOrElseUpdate((dataset, partition, version), new RowMapTree(Ordering[SegmentId]))
    }
    val (chunkIds, rowNums) = chunkRowMap.serialize()
    rowMapTree.put(segmentId, (chunkIds, rowNums, chunkRowMap.nextChunkId))
    Success
  }

  // Add an efficient scanSegments implementation here, which can avoid much of the async
  // cruft unnecessary for in-memory stuff
  override def scanSegments(projection: RichProjection,
                            columns: Seq[Column],
                            version: Int,
                            params: Map[String, String] = Map.empty)
                           (partitionFilter: (projection.PK => Boolean) = (x: projection.PK) => true):
                              Future[Iterator[Segment]] = {
    for { chunkmapsIt <- scanChunkRowMaps(projection.datasetName, version, params) }
    yield {
      chunkmapsIt.map(crm => toSegIndex(projection, crm))
                 .filter { case SegmentIndex(_, _, part, _, _) => partitionFilter(part) }
                 .map { case SegmentIndex(binPart, segId, part, segmentKey, binChunkRowMap) =>
        val segInfo = SegmentInfo(part, segmentKey)
        val segment = new RowReaderSegment(projection, segInfo, binChunkRowMap, columns)
        for { column <- columns } {
          val colName = column.name
          val chunkTree = chunkDb.getOrElse((projection.datasetName, binPart, version), EmptyChunkTree)
          chunkTree.subMap((colName, segId, 0), true, (colName, segId, Int.MaxValue), true)
                   .entrySet.iterator.foreach { entry =>
            val (_, _, chunkId) = entry.getKey
            segment.addChunk(chunkId, colName, entry.getValue)
          }
        }
        segment
      }
    }
  }

  def shutdown(): Unit = {}

  // InMemoryColumnStore is just on one node, so return no splits for now.
  // TODO: achieve parallelism by splitting on a range of partitions.
  def getScanSplits(dataset: TableName,
                    params: Map[String, String]): Seq[Map[String, String]] = Seq(Map.empty)

  def bbToHex(bb: ByteBuffer): String = DatatypeConverter.printHexBinary(bb.array)
}

trait InMemoryColumnStoreScanner extends ColumnStoreScanner {
  import Types._
  import collection.JavaConversions._

  type ChunkKey = (ColumnId, SegmentId, ChunkID)
  type ChunkTree = ConcurrentSkipListMap[ChunkKey, ByteBuffer]
  type RowMapTree = ConcurrentSkipListMap[SegmentId, (ByteBuffer, ByteBuffer, Int)]

  val EmptyChunkTree = new ChunkTree(Ordering[ChunkKey])
  val EmptyRowMapTree = new RowMapTree(Ordering[SegmentId])

  def chunkDb: HashMap[(TableName, BinaryPartition, Int), ChunkTree]
  def rowMaps: HashMap[(TableName, BinaryPartition, Int), RowMapTree]

  def readChunks(dataset: TableName,
                 columns: Set[ColumnId],
                 keyRange: BinaryKeyRange,
                 version: Int)(implicit ec: ExecutionContext): Future[Seq[ChunkedData]] = {
    val chunkTree = chunkDb.getOrElse((dataset, keyRange.partition, version),
                                      EmptyChunkTree)
    logger.debug(s"Reading chunks from columns $columns, keyRange $keyRange, version $version")
    val chunks = for { column <- columns.toSeq } yield {
      val startKey = (column, keyRange.start, 0)
      val endKey   = if (keyRange.endExclusive) { (column, keyRange.end, 0) }
                     else                       { (column, keyRange.end, Int.MaxValue) }
      val it = chunkTree.subMap(startKey, true, endKey, !keyRange.endExclusive).entrySet.iterator
      val chunkList = it.toSeq.map { entry =>
        val (colId, segmentId, chunkId) = entry.getKey
        (segmentId, chunkId, entry.getValue)
      }
      ChunkedData(column, chunkList)
    }
    Future.successful(chunks)
  }

  def readChunkRowMaps(dataset: TableName,
                       keyRange: BinaryKeyRange,
                       version: Int)
                      (implicit ec: ExecutionContext): Future[Iterator[ChunkMapInfo]] = Future {
    val rowMapTree = rowMaps.getOrElse((dataset, keyRange.partition, version), EmptyRowMapTree)
    val it = rowMapTree.subMap(keyRange.start, true,
                               keyRange.end, !keyRange.endExclusive).entrySet.iterator
    it.map { entry =>
      val (chunkIds, rowNums, nextChunkId) = entry.getValue
      (keyRange.partition, entry.getKey, new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId))
    }
  }

  def scanChunkRowMaps(dataset: TableName,
                       version: Int,
                       params: Map[String, String])
                      (implicit ec: ExecutionContext): Future[Iterator[ChunkMapInfo]] = Future {
    val parts = rowMaps.keysIterator.filter { case (ds, partition, ver) =>
      ds == dataset && ver == version }
    val maps = parts.flatMap { case key @ (_, partition, _) =>
                 rowMaps(key).entrySet.iterator.map { entry =>
                   val segId = entry.getKey
                   val (chunkIds, rowNums, nextChunkId) = entry.getValue
                   (partition, segId, new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId))
                 }
               }
    maps.toIterator
  }
}