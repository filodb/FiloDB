package filodb.core.columnstore

import java.nio.ByteBuffer
import java.util.TreeMap
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import spray.caching._

import filodb.core._
import filodb.core.metadata.Column

/**
 * A ColumnStore implementation which is entirely in memory for speed.
 * Good for testing or performance.
 * TODO: use thread-safe structures
 */
class InMemoryColumnStore(getSortColumn: Types.TableName => Column)
                         (implicit val ec: ExecutionContext) extends CachedMergingColumnStore {
  import Types._
  import filodb.core.columnstore._
  import collection.JavaConversions._

  val segmentCache = LruCache[Segment[_]](100)

  val mergingStrategy = new AppendingChunkMergingStrategy(this, getSortColumn)

  type ChunkTree = TreeMap[(ColumnId, ByteBuffer, ChunkID), ByteBuffer]
  type RowMapTree = TreeMap[ByteBuffer, (ByteBuffer, ByteBuffer, Int)]

  val chunkDb = new HashMap[(TableName, PartitionKey, Int), ChunkTree]
  val rowMaps = new HashMap[(TableName, PartitionKey, Int), RowMapTree]

  def writeChunks(dataset: TableName,
                  partition: PartitionKey,
                  version: Int,
                  segmentId: ByteBuffer,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response] = Future {
    val chunkTree = chunkDb.getOrElseUpdate((dataset, partition, version), new ChunkTree)
    chunks.foreach { case (colId, chunkId, bytes) => chunkTree.put((colId, segmentId, chunkId), bytes) }
    Success
  }

  def writeChunkRowMap(dataset: TableName,
                       partition: PartitionKey,
                       version: Int,
                       segmentId: ByteBuffer,
                       chunkRowMap: ChunkRowMap): Future[Response] = Future {
    val rowMapTree = rowMaps.getOrElseUpdate((dataset, partition, version), new RowMapTree)
    val (chunkIds, rowNums) = chunkRowMap.serialize()
    rowMapTree.put(segmentId, (chunkIds, rowNums, chunkRowMap.nextChunkId))
    Success
  }

  def readChunks[K](columns: Set[ColumnId],
                    keyRange: KeyRange[K],
                    version: Int): Future[Seq[ChunkedData]] = Future {
    val chunkTree = chunkDb.getOrElseUpdate((keyRange.dataset, keyRange.partition, version), new ChunkTree)
    for { column <- columns.toSeq } yield {
      val startKey = (column, keyRange.binaryStart, 0)
      val endKey   = (column, keyRange.binaryEnd,   0)  // exclusive end
      val it = chunkTree.subMap(startKey, endKey).entrySet.iterator
      val chunkList = it.toSeq.map { entry =>
        val (colId, segmentId, chunkId) = entry.getKey
        (segmentId, chunkId, entry.getValue)
      }
      ChunkedData(column, chunkList)
    }
  }

  def readChunkRowMaps[K](keyRange: KeyRange[K], version: Int):
      Future[Seq[(ByteBuffer, BinaryChunkRowMap)]] = Future {
    val rowMapTree = rowMaps.getOrElseUpdate((keyRange.dataset, keyRange.partition, version), new RowMapTree)
    val it = rowMapTree.subMap(keyRange.binaryStart, keyRange.binaryEnd).entrySet.iterator
    it.toSeq.map { entry =>
      val (chunkIds, rowNums, nextChunkId) = entry.getValue
      (entry.getKey, new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId))
    }
  }

  def scanChunkRowMaps(dataset: TableName,
                       partitionFilter: (PartitionKey => Boolean),
                       params: Map[String, String])
                      (processFunc: (ChunkRowMap => Unit)): Future[Response] = ???
}