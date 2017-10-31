package filodb.core.memstore

import filodb.core.metadata.Dataset
import filodb.memory.MemFactory
import filodb.memory.format.{BinaryAppendableVector, RowReaderAppender}

/**
 * A WriteBufferPool pre-allocates/creates a pool of WriteBuffers for sharing amongst many MemStore Partitions.
 * For efficiency it creates a whole set of BinaryVectors for all columns as well as the RowReaderAppenders, so that
 * at flush time, the partitions can easily obtain a new one from the pool and rapidly swap out a new set of buffers.
 *
 * The lifecycle is as follows:
 * 1. Partition creation - obtains new set of initial buffers
 * 2. Start of flush()   - obtains new set of buffers so new ingest() calls write to new buffers, while old ones encode
 * 3. End of flush()     - original buffers, now encoded, are released, reset, and can be made available to others
 *
 * @param initChunkSize the initial size of the write buffer.  It is based on GrowableBuffer.
 *
 * TODO: Use MemoryManager etc. and allocate memory from a fixed block instead of specifying max # partitions
 */
class WriteBufferPool(memFactory: MemFactory,
                      dataset: Dataset,
                      initChunkSize: Int,
                      numPartitions: Int = 100000) {
  val queue = new collection.mutable.Queue[(Array[RowReaderAppender], Array[BinaryAppendableVector[_]])]

  // Fill queue up
  (0 until numPartitions).foreach { n =>
    val appenders = MemStore.getAppendables(memFactory, dataset, initChunkSize)
    val currentChunks = appenders.map(_.appender)
    queue.enqueue((appenders, currentChunks))
  }

  /**
   * Returns the number of allocatable sets of buffers in the pool
   */
  def poolSize: Int = queue.length

  /**
   * Obtains a new set of AppendableVectors/Appenders from the pool.
   * TODO: if we run out of buffers then what?  Evict some partition?
   *
   * @return (Array of RowReaderAppenders, and corresponding Array of AppendableVectors)
   */
  def obtain(): (Array[RowReaderAppender], Array[BinaryAppendableVector[_]]) = queue.dequeue

  /**
   * Releases a set of AppendableVectors back to the pool, henceforth someone else can obtain it.
   * The state of the appenders are reset.
   */
  def release(appenders: Array[RowReaderAppender]): Unit = {
    appenders.foreach(_.appender.reset())
    val currentChunks = appenders.map(_.appender)
    queue.enqueue((appenders, currentChunks))
  }
}