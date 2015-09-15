package filodb.core.columnstore

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import java.util.TreeMap
import javax.xml.bind.DatatypeConverter
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import spray.caching._

import filodb.core._
import filodb.core.metadata.{Column, Projection}

/**
 * A ColumnStore implementation which is entirely in memory for speed.
 * Good for testing or performance.
 * TODO: use thread-safe structures
 */
class InMemoryColumnStore(implicit val ec: ExecutionContext)
extends CachedMergingColumnStore with StrictLogging {
  import Types._
  import collection.JavaConversions._

  val segmentCache = LruCache[Segment[_]](100)

  val mergingStrategy = new AppendingChunkMergingStrategy(this)

  type ChunkKey = (Types.ColumnId, ByteBuffer, Types.ChunkID)
  type ChunkTree = TreeMap[ChunkKey, Array[Byte]]
  type RowMapTree = TreeMap[ByteBuffer, (ByteBuffer, ByteBuffer, Int)]

  val chunkDb = new HashMap[(TableName, PartitionKey, Int), ChunkTree]
  val rowMaps = new HashMap[(TableName, PartitionKey, Int), RowMapTree]

  def initializeProjection(projection: Projection): Future[Response] = Future.successful(Success)

  def clearProjectionData(projection: Projection): Future[Response] = ???

  def writeChunks(dataset: TableName,
                  partition: PartitionKey,
                  version: Int,
                  segmentId: ByteBuffer,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response] = Future {
    val chunkTree = chunkDb.getOrElseUpdate((dataset, partition, version),
                                            new ChunkTree((Ordering[ChunkKey])))
    chunks.foreach { case (colId, chunkId, bytes) =>
      chunkTree.put((colId, segmentId, chunkId), minimalBytes(bytes))
    }
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
    val chunkTree = chunkDb.getOrElseUpdate((keyRange.dataset, keyRange.partition, version),
                                            new ChunkTree((Ordering[ChunkKey])))
    logger.debug(s"Reading chunks from columns $columns, keyRange $keyRange, version $version")
    for { column <- columns.toSeq } yield {
      val startKey = (column, keyRange.binaryStart, 0)
      val endKey   = (column, keyRange.binaryEnd,   0)  // exclusive end
      val it = chunkTree.subMap(startKey, endKey).entrySet.iterator
      val chunkList = it.toSeq.map { entry =>
        val (colId, segmentId, chunkId) = entry.getKey
        (segmentId, chunkId, ByteBuffer.wrap(entry.getValue))
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

  def bbToHex(bb: ByteBuffer): String = DatatypeConverter.printHexBinary(bb.array)

  def minimalBytes(bb: ByteBuffer): Array[Byte] = {
    if (bb.position == 0 && bb.arrayOffset == 0) return bb.array
    val aray = new Array[Byte](bb.remaining)
    System.arraycopy(bb.array, bb.arrayOffset + bb.position, aray, 0, bb.remaining)
    aray
  }
}