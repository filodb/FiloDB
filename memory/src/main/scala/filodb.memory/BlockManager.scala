package filodb.memory

import java.lang.{Long => jLong}
import java.util
import java.util.concurrent.locks.ReentrantLock

import com.kenai.jffi.{MemoryIO, PageManager}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.Counter

/**
  * Allows requesting blocks.
  */
trait BlockManager {

  /**
    * @return The size of the block in bytes which can be allocated by this BlockManager
    */
  def blockSizeInBytes: Long

  /**
    * @return The number of free blocks still available for consumption
    */
  def numFreeBlocks: Int

  /**
   * @return true if the time bucket has blocks allocated
   */
  def hasTimeBucket(bucket: Long): Boolean

  /**
    * @param memorySize The size of memory in bytes for which blocks are to be allocated
    * @param bucketTime the timebucket (timestamp) from which to allocate block(s), or None for the general list
    * @param owner the BlockMemFactory that will be owning this block, until reclaim.  Used for debugging.
    * @return A sequence of blocks totaling up in memory requested or empty if unable to allocate
    */
  def requestBlocks(memorySize: Long, bucketTime: Option[Long], owner: Option[BlockMemFactory] = None): Seq[Block]

  /**
    * @param bucketTime the timebucket from which to allocate block(s), or None for the general list
    * @param owner the BlockMemFactory that will be owning this block, until reclaim.  Used for debugging.
    * @return One block of memory
    */
  def requestBlock(bucketTime: Option[Long], owner: Option[BlockMemFactory] = None): Option[Block]

  /**
    * Releases all blocks allocated by this store.
    */
  def releaseBlocks(): Unit

  /**
   * Marks all time-bucketed blocks in buckets up to upTo as reclaimable
   */
  def markBucketedBlocksReclaimable(upTo: Long): Unit

  /**
    * @return Memory stats for recording
    */
  def stats(): MemoryStats
}

class MemoryStats(tags: Map[String, String]) {
  val usedBlocksMetric = Kamon.gauge("blockstore-used-blocks").refine(tags)
  val freeBlocksMetric = Kamon.gauge("blockstore-free-blocks").refine(tags)
  val requestedBlocksMetric = Kamon.counter("blockstore-blocks-requested").refine(tags)
  val usedBlocksTimeOrderedMetric = Kamon.gauge("blockstore-used-time-ordered-blocks").refine(tags)
  val timeOrderedBlocksReclaimedMetric = Kamon.counter("blockstore-time-ordered-blocks-reclaimed").refine(tags)
  val blocksReclaimedMetric = Kamon.counter("blockstore-blocks-reclaimed").refine(tags)
}

final case class ReclaimEvent(block: Block, reclaimTime: Long, oldOwner: Option[BlockMemFactory], remaining: Long)

object PageAlignedBlockManager {
  val MaxReclaimLogSize = 10000
}

/**
  * Pre Allocates blocks totalling to the passed memory size.
  * Each block size is the same as the OS page size.
  * This class is thread safe
  *
  * @param totalMemorySizeInBytes Control the number of pages to allocate. (totalling up to the totallMemorySizeInBytes)
  * @param stats                  Memory metrics which need to be recorded
  * @param reclaimer              ReclaimListener to use on block metadata when a block is freed
  * @param numPagesPerBlock       The number of pages a block spans
  */
class PageAlignedBlockManager(val totalMemorySizeInBytes: Long,
                              val stats: MemoryStats,
                              reclaimer: ReclaimListener,
                              numPagesPerBlock: Int)
  extends BlockManager with StrictLogging {
  import PageAlignedBlockManager._

  val mask = PageManager.PROT_READ | PageManager.PROT_EXEC | PageManager.PROT_WRITE

  import collection.JavaConverters._

  protected var firstPageAddress: Long = 0L

  protected val freeBlocks: util.LinkedList[Block] = allocate()
  protected[memory] val usedBlocks: util.LinkedList[Block] = new util.LinkedList[Block]()
  protected[memory] val usedBlocksTimeOrdered = new util.TreeMap[Long, util.LinkedList[Block]]
  val reclaimLog = new collection.mutable.Queue[ReclaimEvent]

  protected val lock = new ReentrantLock()

  override def blockSizeInBytes: Long = PageManager.getInstance().pageSize() * numPagesPerBlock

  def availablePreAllocated: Long = numFreeBlocks * blockSizeInBytes

  def usedMemory: Long = usedBlocks.size * blockSizeInBytes

  override def numFreeBlocks: Int = freeBlocks.size

  override def requestBlock(bucketTime: Option[Long], bmf: Option[BlockMemFactory] = None): Option[Block] = {
    val blocks = requestBlocks(blockSizeInBytes, bucketTime, bmf)
    blocks.size match {
      case 0 => None
      case 1 => Some(blocks.head)
      case _ => throw new IllegalStateException("Should not have gotten more than one block")
    }
  }

  /* Used in tests for assertion */
  def usedBlocksSize(bucketTime: Option[Long]): Int = {
    bucketTime match {
      case Some(t) => usedBlocksTimeOrdered.get(t).size()
      case None => usedBlocks.size()
    }
  }

  /**
    * Allocates requested number of blocks. If enough blocks are not available,
    * then uses the ReclaimPolicy to check if blocks can be reclaimed
    * Uses a lock to ensure that concurrent requests are safe.
    */
  override def requestBlocks(memorySize: Long,
                             bucketTime: Option[Long],
                             bmf: Option[BlockMemFactory] = None): Seq[Block] = {
    lock.lock()
    try {
      val num: Int = Math.ceil(memorySize / blockSizeInBytes).toInt
      stats.requestedBlocksMetric.increment(num)

      if (freeBlocks.size < num) tryReclaim(num)

      if (freeBlocks.size >= num) {
        val allocated = new Array[Block](num)
        (0 until num).foreach { i =>
          val block = freeBlocks.remove()
          if (bmf.nonEmpty) block.setOwner(bmf.get)
          use(block, bucketTime)
          allocated(i) = block
        }
        allocated
      } else {
        logger.warn(s"Out of blocks to allocate!  num_blocks=$num num_bytes=$memorySize freeBlocks=${freeBlocks.size}")
        Seq.empty[Block]
      }
    } finally {
      lock.unlock()
    }
  }

  protected def allocate(): util.LinkedList[Block] = {
    val numBlocks: Int = Math.floor(totalMemorySizeInBytes / blockSizeInBytes).toInt
    val blocks = new util.LinkedList[Block]()
    logger.info(s"Allocating $numBlocks blocks of $blockSizeInBytes bytes each, total $totalMemorySizeInBytes")
    firstPageAddress = MemoryIO.getCheckedInstance().allocateMemory(totalMemorySizeInBytes, false)
    for (i <- 0 until numBlocks) {
      val address = firstPageAddress + (i * blockSizeInBytes)
      blocks.add(new Block(address, blockSizeInBytes, reclaimer))
    }
    stats.freeBlocksMetric.set(blocks.size())
    blocks
  }

  protected def use(block: Block, bucketTime: Option[Long]) = {
    block.markInUse
    bucketTime match {
      case Some(bucket) => val blockList = Option(usedBlocksTimeOrdered.get(bucket)).getOrElse {
                                             val list = new util.LinkedList[Block]()
                                             usedBlocksTimeOrdered.put(bucket, list)
                                             list
                                           }
                           blockList.add(block)
                           stats.usedBlocksTimeOrderedMetric.set(numTimeOrderedBlocks)
      case None =>         usedBlocks.add(block)
                           stats.usedBlocksMetric.set(usedBlocks.size())
    }
    stats.freeBlocksMetric.set(freeBlocks.size())
  }

  private def addToReclaimLog(block: Block): Unit = {
    val event = ReclaimEvent(block, System.currentTimeMillis, block.owner, block.remaining)
    if (reclaimLog.size >= MaxReclaimLogSize) { reclaimLog.dequeue }
    reclaimLog += event
  }

  protected def tryReclaim(num: Int): Unit = {
    var reclaimed = 0
    var currList = 0
    val timeOrderedListIt = usedBlocksTimeOrdered.entrySet.iterator
    while ( reclaimed < num &&
            timeOrderedListIt.hasNext ) {
      val entry = timeOrderedListIt.next
      val prevReclaimed = reclaimed
      val removed = reclaimFrom(entry.getValue, stats.timeOrderedBlocksReclaimedMetric)
      if (removed.nonEmpty) {
        logger.info(s"timeBlockReclaim: Reclaimed ${removed.length} time ordered blocks " +
                    s"from list at t=${entry.getKey} (${(System.currentTimeMillis - entry.getKey)/3600000} hrs ago) " +
                    s"\nReclaimed blocks: ${removed.map(b => jLong.toHexString(b.address)).mkString(" ")}")
      }
      // If the block list is now empty, remove it from tree map
      if (entry.getValue.isEmpty) timeOrderedListIt.remove()
    }
    if (reclaimed < num) reclaimFrom(usedBlocks, stats.blocksReclaimedMetric)
    // if we do not get required blocks even after reclaim call
    if (reclaimed < num) {
      logger.warn(s"$num blocks to reclaim but only reclaimed $reclaimed.  usedblocks=${usedBlocks.size} " +
                  s"usedBlocksTimeOrdered=${usedBlocksTimeOrdered.asScala.toList.map{case(n, l) => (n, l.size)}}")
    }

    def reclaimFrom(list: util.LinkedList[Block], reclaimedCounter: Counter): Seq[Block] = {
      val entries = list.iterator
      val removed = new collection.mutable.ArrayBuffer[Block]
      while (entries.hasNext && reclaimed < num) {
        val block = entries.next
        if (block.canReclaim) {
          entries.remove()
          removed += block
          addToReclaimLog(block)
          block.reclaim()
          block.clearOwner()
          freeBlocks.add(block)
          stats.freeBlocksMetric.set(freeBlocks.size())
          reclaimedCounter.increment()
          reclaimed = reclaimed + 1
        }
      }
      removed
    }
  }

  def numTimeOrderedBlocks: Int = usedBlocksTimeOrdered.values.asScala.map(_.size).sum

  def timeBuckets: Seq[Long] = usedBlocksTimeOrdered.keySet.asScala.toSeq

  def markBucketedBlocksReclaimable(upTo: Long): Unit = {
    lock.lock()
    try {
      logger.info(s"timeBlockReclaim: Marking ($upTo) - this is -${(System.currentTimeMillis - upTo)/3600000}hrs")
      val keys = usedBlocksTimeOrdered.headMap(upTo).keySet.asScala
      logger.info(s"timeBlockReclaim: Marking lists $keys as reclaimable")
      usedBlocksTimeOrdered.headMap(upTo).values.asScala.foreach { list =>
        list.asScala.foreach(_.tryMarkReclaimable)
      }
    } finally {
      lock.unlock()
    }
  }

  def hasTimeBucket(bucket: Long): Boolean = {
    lock.lock()
    val result = usedBlocksTimeOrdered.containsKey(bucket)
    lock.unlock()
    result
  }

  /**
   * Used during testing only to try and reclaim all existing blocks
   */
  def reclaimAll(): Unit = {
    logger.warn(s"Reclaiming all used blocks -- THIS BETTER BE A TEST!!!")
    markBucketedBlocksReclaimable(Long.MaxValue)
    usedBlocks.asScala.foreach(_.markReclaimable)
    tryReclaim(usedBlocks.size + numTimeOrderedBlocks)
  }

  /**
   * Finds all reclaim events in the log whose Block contains the pointer passed in.
   * Useful for debugging.  O(n) - not performant.
   */
  def reclaimEventsForPtr(ptr: BinaryRegion.NativePointer): Seq[ReclaimEvent] =
    reclaimLog.filter { ev => ptr >= ev.block.address && ptr < (ev.block.address + ev.block.capacity) }

  def timeBlocksForPtr(ptr: BinaryRegion.NativePointer): Seq[Block] = {
    lock.lock()
    try {
      usedBlocksTimeOrdered.entrySet.iterator.asScala.flatMap { entry =>
        BlockDetective.containsPtr(ptr, entry.getValue)
      }.toBuffer
    } finally {
      lock.unlock()
    }
  }

  def releaseBlocks(): Unit = {
    lock.lock()
    try {
      if (firstPageAddress != 0) {
        MemoryIO.getCheckedInstance.freeMemory(firstPageAddress)
        firstPageAddress = 0
      }
    } catch {
      case e: Throwable => logger.warn(s"Could not release blocks at $firstPageAddress", e)
    } finally {
      lock.unlock()
    }
  }

  override def finalize(): Unit = releaseBlocks
}
