package filodb.core.memstore

import com.typesafe.scalalogging.StrictLogging

/**
 * This is a policy that determines when, how many, and which partitions should be evicted
 */
trait PartitionEvictionPolicy {
  /**
   * Returns how many partitions _should_ be evicted right now based on criteria.  The actual number of partitions
   * evicted may not necessarily reach this number
   * @param numPartitions the current number of partitions in the shard
   */
  def howManyToEvict(numPartitions: Int): Int

  /**
   * Returns true if the partition in question should be evicted
   */
  def canEvict(partition: TimeSeriesPartition): Boolean
}

/**
 * Evicts partitions when the heap is less than a given percentage free.  Only evicts partitions with no
 * active WriteBuffers.  NOTE: "free" memory also includes the potential expansion heap room (max - current total)
 * @param minFreePercentage the minimum % of heap memory (out of max allowed) that must be free
 * @param partitionsToFree the number of partitions to try to free at a time
 */
class HeapPercentageEvictionPolicy(minFreePercentage: Int,
                                   partitionsToFree: Int = 100) extends PartitionEvictionPolicy with StrictLogging {
  require(minFreePercentage > 0 && minFreePercentage < 100)

  def howManyToEvict(numPartitions: Int): Int = {
    val freeMem = sys.runtime.freeMemory + (sys.runtime.maxMemory - sys.runtime.totalMemory)
    val freePct = freeMem * 100L / sys.runtime.maxMemory
    if (freePct < minFreePercentage) {
      logger.info(s"Recommending partition eviction: freeMem=$freeMem freePct=$freePct")
      partitionsToFree
    } else {
      0
    }
  }

  def canEvict(partition: TimeSeriesPartition): Boolean =
    partition.latestChunkLen == 0 && partition.notFlushing
}

/**
 * A policy, used for testing, which evicts any partitions unless the # of partitions is below a max.
 */
class FixedMaxPartitionsEvictionPolicy(maxPartitions: Int,
                                       partitionsToFree: Int = 2) extends PartitionEvictionPolicy with StrictLogging {
  def howManyToEvict(numPartitions: Int): Int =
    if (numPartitions > maxPartitions) partitionsToFree else 0

  def canEvict(partition: TimeSeriesPartition): Boolean = true
}