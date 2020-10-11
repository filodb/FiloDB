package filodb.core.memstore

import com.typesafe.scalalogging.StrictLogging
import org.jctools.queues.MpscUnboundedArrayQueue
import spire.syntax.cfor._

import filodb.core.metadata.DataSchema
import filodb.core.store.{ChunkSetInfo, StoreConfig}
import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.MemFactory

object WriteBufferPool {
  /**
   * Number of WriteBuffers to allocate at once.  Usually no reason to change it.
   * Higher number means higher latency during allocation, but more buffers can be individually allocated.
   */
  val AllocStepSize = 200
}

/**
 * A WriteBufferPool pre-allocates/creates a pool of WriteBuffers for sharing amongst many MemStore Partitions.
 * For efficiency it creates a whole set of BinaryAppendableVectors for all columns, so that
 * at flush time, the partitions can easily obtain a new one from the pool and rapidly swap out a new set of buffers.
 *
 * NOTE that the pool dynamically resizes.  It allocates allocationStepSize write buffers at a time as needed.
 * Also, if a large number of write buffers are returned, it might return some writebuffers and release its memory.
 *
 * The lifecycle is as follows:
 * 1. Partition gets data - obtains new set of initial buffers
 * 2. End of flush()     - original buffers, now encoded, are released, reset, and can be made available to others
 *
 * @param storeConf the StoreConfig containing parameters for configuring write buffers, etc.
 */
class WriteBufferPool(memFactory: MemFactory,
                      val schema: DataSchema,
                      storeConf: StoreConfig) extends StrictLogging {
  import TimeSeriesPartition._
  import WriteBufferPool._

  val queue = new MpscUnboundedArrayQueue[(NativePointer, AppenderArray)](storeConf.maxBufferPoolSize)

  private def allocateBuffers(): Unit = {
    logger.debug(s"Allocating ${AllocStepSize} WriteBuffers....")
    // Fill queue up
    (0 until AllocStepSize).foreach { n =>
      val builders = MemStore.getAppendables(memFactory, schema, storeConf)
      val info = ChunkSetInfo(memFactory, schema, 0, 0, 0, Long.MaxValue)
      // Point vectors in chunkset metadata to builders addresses
      cforRange { 0 until schema.columns.length } { colNo =>
        ChunkSetInfo.setVectorPtr(info.infoAddr, colNo, builders(colNo).addr)
      }
      queue.add((info.infoAddr, builders))
    }
  }

  /**
   * Returns the number of allocatable sets of buffers in the pool
   */
  def poolSize: Int = queue.size

  /**
   * Obtains a new set of AppendableVectors from the pool, creating additional buffers if there is memory available.
   *
   * @return Array of AppendableVectors
   * Throws NoSuchElementException if the Queue is empty and unable to create more buffers/out of memory.
   */
  def obtain(): (NativePointer, AppenderArray) = {
    // If queue is empty, try and allocate more buffers depending on if memFactory has more memory
    if (queue.isEmpty) allocateBuffers()
    queue.remove()
  }

  /**
   * Releases a set of AppendableVectors back to the pool, henceforth someone else can obtain it.
   * The state of the appenders are reset.
   */
  def release(metaAddr: NativePointer, appenders: AppenderArray): Unit = {
    if (poolSize >= storeConf.maxBufferPoolSize) {
      // pool is at max size, release extra so memory can be shared.  Be sure to release each vector's memory
      cforRange { 0 until schema.columns.length } { colNo =>
        memFactory.freeMemory(ChunkSetInfo.getVectorPtr(metaAddr, colNo))
      }
      memFactory.freeMemory(metaAddr)
    } else {
      // IMPORTANT: reset size in ChunkSetInfo metadata so there won't be an inconsistency
      // between appenders and metadata (in case some reader is still hanging on to this old info)
      ChunkSetInfo.resetNumRows(metaAddr)
      appenders.foreach(_.reset())
      queue.add((metaAddr, appenders))
    }
  }
}
