package filodb.core.memstore

import filodb.core.metadata.Dataset
import filodb.memory.MemFactory
import filodb.memory.format.BinaryAppendableVector

/**
 * A WriteBufferPool pre-allocates/creates a pool of WriteBuffers for sharing amongst many MemStore Partitions.
 * For efficiency it creates a whole set of BinaryAppendableVectors for all columns, so that
 * at flush time, the partitions can easily obtain a new one from the pool and rapidly swap out a new set of buffers.
 *
 * The lifecycle is as follows:
 * 1. Partition gets data - obtains new set of initial buffers
 * 2. End of flush()     - original buffers, now encoded, are released, reset, and can be made available to others
 *
 * @param maxChunkSize the max size of the write buffer in elements.
 *
 * TODO: Use MemoryManager etc. and allocate memory from a fixed block instead of specifying max # partitions
 */
class WriteBufferPool(memFactory: MemFactory,
                      dataset: Dataset,
                      maxChunkSize: Int,
                      numPartitions: Int = 100000) {
  val queue = new collection.mutable.Queue[Array[BinaryAppendableVector[_]]]

  // Fill queue up
  (0 until numPartitions).foreach { n =>
    val builders = MemStore.getAppendables(memFactory, dataset, maxChunkSize)
    queue.enqueue(builders)
  }

  /**
   * Returns the number of allocatable sets of buffers in the pool
   */
  def poolSize: Int = queue.length

  /**
   * Obtains a new set of AppendableVectors from the pool.
   *
   * @return Array of AppendableVectors
   * Throws NoSuchElementException if the Queue is empty
   */
  def obtain(): Array[BinaryAppendableVector[_]] = queue.dequeue

  /**
   * Releases a set of AppendableVectors back to the pool, henceforth someone else can obtain it.
   * The state of the appenders are reset.
   */
  def release(appenders: Array[BinaryAppendableVector[_]]): Unit = {
    appenders.foreach(_.reset())
    queue.enqueue(appenders)
  }
}