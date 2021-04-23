package filodb.core.memstore

import com.typesafe.scalalogging.StrictLogging

import filodb.memory.NativeMemoryManager

/**
 * This is a policy that determines when partitions should be evicted out of memory
 */
trait PartitionEvictionPolicy {
  /**
   * Returns if partitions _should_ be evicted right now based on criteria.
   * @param numPartitions the current number of partitions in the shard
   * @param memManager the MemFactory used to allocate write buffers and partition keys
   */
  def numPartitionsToEvict(numPartitions: Int, memManager: NativeMemoryManager): Int
}

/**
 * Evicts partitions when the offheap WriteBuffers free memory falls below a limit.
 * Right now, the WriteBuffers is likely to occupy much more memory per partition than the heap...
 *  (a couple KB per partition in WriteBuffers vs <300 bytes per partition on heap), so this policy works assuming
 *  that WriteBuffers are sized much more than the heap, which should be the normal case.
 * Also, determining heap free space is just really tricky.
 *
 * @param minBufferMemPercentage percent of capacity that should be free in the bufferMemManager
 */
class WriteBufferFreeEvictionPolicy(minBufferMemPercentage: Double) extends PartitionEvictionPolicy
                                                                              with StrictLogging {
  def numPartitionsToEvict(numPartitions: Int, memManager: NativeMemoryManager): Int = {
    val minBufferMem = memManager.upperBoundSizeInBytes * minBufferMemPercentage / 100
    if (memManager.numFreeBytes < minBufferMem) {
      logger.info(s"Recommending partition eviction; buffer free memory = ${memManager.numFreeBytes}")
      ((minBufferMem.toDouble / memManager.upperBoundSizeInBytes) * numPartitions).toInt
    } else {
      0
    }
  }
}

/**
 * A policy, used for testing, which evicts any partitions if the # of partitions is above a max.
 */
class FixedMaxPartitionsEvictionPolicy(maxPartitions: Int) extends PartitionEvictionPolicy {
  def numPartitionsToEvict(numPartitions: Int, memManager: NativeMemoryManager): Int =
    Math.max(numPartitions - maxPartitions + 1, 0)
}

class CompositeEvictionPolicy(maxPartPolicy: FixedMaxPartitionsEvictionPolicy,
                              freeBufferPolicy: WriteBufferFreeEvictionPolicy) extends PartitionEvictionPolicy {
  def numPartitionsToEvict(numPartitions: Int, memManager: NativeMemoryManager): Int = {
    Math.max(maxPartPolicy.numPartitionsToEvict(numPartitions, memManager),
             freeBufferPolicy.numPartitionsToEvict(numPartitions, memManager))
  }
}