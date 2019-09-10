package filodb.core.memstore

import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging

import filodb.core.Types.ChunkID
import filodb.core.metadata.Schema
import filodb.core.store._
import filodb.memory.{BinaryRegion, BinaryRegionLarge, NativeMemoryManager}
import filodb.memory.format.UnsafeUtils

object PagedReadablePartition extends StrictLogging {
  val _log = logger
}

class PagedReadablePartition(override val schema: Schema,
                             override val shard: Int,
                             override val partID: Int,
                             pd: RawPartData,
                             mem: NativeMemoryManager) extends ReadablePartition {

  import PagedReadablePartition._
  val chunkInfos = ArrayBuffer[ChunkSetInfo]()

  var partitionKey: BinaryRegion.NativePointer = _

  loadToOffHeap()

  private def loadToOffHeap() = {
    val (_, pk, _) = BinaryRegionLarge.allocateAndCopy(partKeyBase, partKeyOffset, mem)
    partitionKey = pk

    pd.chunkSets.foreach { rawChunkSet =>

      val vectors = rawChunkSet.vectors.map { buf =>
        val (bytes, offset , len) = UnsafeUtils.BOLfromBuffer(buf)
        val vec = mem.allocateOffheap(len)
        UnsafeUtils.copy(bytes, offset, UnsafeUtils.ZeroPointer, vec, len)
        vec
      }
      val chunkInfoAddr = mem.allocateOffheap(schema.data.blockMetaSize)
      TimeSeriesShard.writeMetaWithoutPartId(chunkInfoAddr , rawChunkSet.infoBytes, vectors)
      val info = ChunkSetInfo(chunkInfoAddr)
      chunkInfos += info
    }
    _log.debug(s"Loaded ${chunkInfos.size} chunksets into partition with key ${stringPartition}")
  }

  override def numChunks: Int = chunkInfos.size

  override def appendingChunkLen: Int = 0

  override def infos(method: ChunkScanMethod): ChunkInfoIterator = {
    if (method == AllChunkScan) chunkInfoIteratorImpl
    else throw new UnsupportedOperationException("This partition supports AllChunkScan only")
  }

  override def infos(startTime: Long, endTime: Long): ChunkInfoIterator = ???

  override def hasChunks(method: ChunkScanMethod): Boolean = {
    method == AllChunkScan && chunkInfos.isEmpty
  }

  override def hasChunksAt(id: ChunkID): Boolean = chunkInfos.exists(_.id == id)

  override def earliestTime: Long = ???

  override def partKeyBase: Array[Byte] = pd.partitionKey

  override def partKeyOffset: Long = UnsafeUtils.arayOffset

  private def chunkInfoIteratorImpl = {
    new ChunkInfoIterator {
      private val iter = chunkInfos.iterator
      override def close(): Unit = {}
      override def hasNext: Boolean = iter.hasNext
      override def nextInfo: ChunkSetInfo = iter.next()
      override def lock(): Unit = {}
      override def unlock(): Unit = {}
    }
  }

  def free(): Unit = {
    chunkInfos.foreach { ci =>
      for { col <- schema.data.columns.indices } mem.freeMemory(ci.vectorPtr(col))
      mem.freeMemory(ci.infoAddr)
    }
    mem.freeMemory(partitionKey)
  }
}
