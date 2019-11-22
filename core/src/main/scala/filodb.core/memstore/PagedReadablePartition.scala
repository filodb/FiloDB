package filodb.core.memstore

import com.typesafe.scalalogging.StrictLogging

import filodb.core.Types.ChunkID
import filodb.core.metadata.Schema
import filodb.core.store._
import filodb.memory.format.UnsafeUtils

object PagedReadablePartition extends StrictLogging {
  val _log = logger
}

/**
  * Readable Partition constructed using data read from Cassandra.
  * This can now be used for various operations like downsampling, cross-DC repair etc.
  *
  * Note that this partition would not have ALL data available for the partition. Only
  * that which was read from Cassandra.
  *
  * Any ChunkScanMethod will return results from all available chunks. This optimization
  * is done since that check would already done and does not need to be repeated.
  */
class PagedReadablePartition(override val schema: Schema,
                             override val shard: Int,
                             override val partID: Int,
                             partData: RawPartData) extends ReadablePartition {

  override def numChunks: Int = partData.chunkSets.length

  override def appendingChunkLen: Int = 0

  override def infos(method: ChunkScanMethod): ChunkInfoIterator = chunkInfoIteratorImpl

  override def infos(startTime: Long, endTime: Long): ChunkInfoIterator = chunkInfoIteratorImpl

  override def hasChunks(method: ChunkScanMethod): Boolean = partData.chunkSets.nonEmpty

  override def hasChunksAt(id: ChunkID): Boolean =
    partData.chunkSets.iterator
      .map(c => ChunkSetInfoOnHeap(c.infoBytes, c.vectors))
      .exists(_.id == id)

  override def earliestTime: Long = ???

  def partKeyBase: Array[Byte] = partData.partitionKey

  def partKeyOffset: Long = UnsafeUtils.arayOffset

  override def partKeyBytes: Array[Byte] = partData.partitionKey

  private def chunkInfoIteratorImpl = {
    new ChunkInfoIterator {
      private val iter = partData.chunkSets.iterator
      override def close(): Unit = {}
      override def hasNext: Boolean = iter.hasNext
      override def nextInfo = ??? // intentionally not implemented since users dont bother with off-heap
      override def nextInfoReader: ChunkSetInfoReader = {
        val nxt = iter.next()
        ChunkSetInfoOnHeap(nxt.infoBytes, nxt.vectors)
      }
      override def lock(): Unit = {}
      override def unlock(): Unit = {}
    }
  }

}
