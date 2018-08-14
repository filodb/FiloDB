package filodb.core.memstore

import com.typesafe.scalalogging.StrictLogging

import filodb.memory.MemFactory

/**
 * This is a policy that determines when partitions should be evicted out of memory
 */
trait PartitionEvictionPolicy {
  /**
   * Returns if partitions _should_ be evicted right now based on criteria.
   * @param numPartitions the current number of partitions in the shard
   * @param memManager the MemFactory used to allocate write buffers and partition keys
   */
  def shouldEvict(numPartitions: Int, memManager: MemFactory): Boolean
}

/**
 * Evicts partitions when the offheap WriteBuffers free memory falls below a limit.
 * Right now, the WriteBuffers is likely to occupy much more memory per partition than the heap...
 *  (a couple KB per partition in WriteBuffers vs <300 bytes per partition on heap), so this policy works assuming
 *  that WriteBuffers are sized much more than the heap, which should be the normal case.
 * Also, determining heap free space is just really tricky.
 *
 * @param minBufferMem the minimum number of bytes that should be free in the bufferMemManager
 */
class WriteBufferFreeEvictionPolicy(minBufferMem: Long = 1024*1024) extends PartitionEvictionPolicy with StrictLogging {
  def shouldEvict(numPartitions: Int, memManager: MemFactory): Boolean = {
    if (memManager.numFreeBytes < minBufferMem) {
      logger.info(s"Recommending partition eviction; buffer free memory = ${memManager.numFreeBytes}")
      true
    } else {
      false
    }
  }
}

/**
 * A policy, used for testing, which evicts any partitions if the # of partitions is above a max.
 */
class FixedMaxPartitionsEvictionPolicy(maxPartitions: Int) extends PartitionEvictionPolicy with StrictLogging {
  def shouldEvict(numPartitions: Int, memManager: MemFactory): Boolean = numPartitions > maxPartitions
}