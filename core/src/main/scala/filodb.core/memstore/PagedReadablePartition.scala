package filodb.core.memstore

import com.typesafe.scalalogging.StrictLogging

import filodb.core.Types.ChunkID
import filodb.core.metadata.Schema
import filodb.core.store._
import filodb.memory.{BinaryRegion, BinaryRegionLarge, NativeMemoryManager}
import filodb.memory.format.UnsafeUtils

object PagedReadablePartition extends StrictLogging {
  val _log = logger
}

/**
  * Pages raw partition data read from Cassandra into off-heap memory.
  * This can now be used for various operations like downsampling, cross-DC repair,
  * or even be evolved (with more changes) into an ODP solution.
  *
  * Note that `mem` is the native memory manager used to allocate off-heap memory
  * to store the paged chunks.
  *
  * IMPORTANT: This partition needs to be freed by calling `free()` after the chunks
  * are read.
  */
class PagedReadablePartition(override val schema: Schema,
                             override val shard: Int,
                             override val partID: Int,
                             pd: RawPartData,
                             mem: NativeMemoryManager) extends ReadablePartition {

  import PagedReadablePartition._
  private val chunkInfos = debox.Buffer[BinaryRegion.NativePointer]()

  var partitionKey: BinaryRegion.NativePointer = _

  loadToOffHeap()

  private def loadToOffHeap() = {
    val (_, pk, _) = BinaryRegionLarge.allocateAndCopy(pd.partitionKey, UnsafeUtils.arayOffset, mem)
    partitionKey = pk

    pd.chunkSets.foreach { rawChunkSet =>

      val vectors = rawChunkSet.vectors.map { buf =>
        val (bytes, offset , len) = UnsafeUtils.BOLfromBuffer(buf)
        val vec = mem.allocateOffheap(len)
        UnsafeUtils.copy(bytes, offset, UnsafeUtils.ZeroPointer, vec, len)
        vec
      }
      val chunkInfoAddr = mem.allocateOffheap(schema.data.blockMetaSize)
      TimeSeriesShard.writeMetaWithoutPartId(chunkInfoAddr, rawChunkSet.infoBytes, vectors)
      val info = ChunkSetInfo(chunkInfoAddr)
      chunkInfos += info.infoAddr
    }
    _log.debug(s"Loaded ${chunkInfos.length} chunksets into partition with key ${stringPartition}")
  }

  override def numChunks: Int = chunkInfos.length

  override def appendingChunkLen: Int = 0

  override def infos(method: ChunkScanMethod): ChunkInfoIterator = {
    if (method == AllChunkScan) chunkInfoIteratorImpl
    else throw new UnsupportedOperationException("This partition supports AllChunkScan only")
  }

  override def infos(startTime: Long, endTime: Long): ChunkInfoIterator = ???

  override def hasChunks(method: ChunkScanMethod): Boolean = {
    method == AllChunkScan && chunkInfos.isEmpty
  }

  override def hasChunksAt(id: ChunkID): Boolean = chunkInfos.iterator().exists(ChunkSetInfo.getChunkID(_) == id)

  override def earliestTime: Long = ???

  def partKeyBase: Array[Byte] = UnsafeUtils.ZeroPointer.asInstanceOf[Array[Byte]]

  def partKeyOffset: Long = partitionKey

  private def chunkInfoIteratorImpl = {
    new ChunkInfoIterator {
      private val iter = chunkInfos.iterator
      override def close(): Unit = {}
      override def hasNext: Boolean = iter.hasNext
      override def nextInfo: ChunkSetInfo = ChunkSetInfo(iter.next())
      override def lock(): Unit = {}
      override def unlock(): Unit = {}
    }
  }

  def free(): Unit = {
    chunkInfos.foreach { ptr =>
      val ci = ChunkSetInfo(ptr)
      for { col <- schema.data.columns.indices } mem.freeMemory(ci.vectorPtr(col))
      mem.freeMemory(ci.infoAddr)
    }
    mem.freeMemory(partitionKey)
  }
}
