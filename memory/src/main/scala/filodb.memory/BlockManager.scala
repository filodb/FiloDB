package filodb.memory

import java.util.concurrent.locks.ReentrantLock

import com.kenai.jffi.{MemoryIO, PageManager}
import com.typesafe.scalalogging.StrictLogging
import java.util
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
    * Optimize data structures to reclaim and disable time ordered block allocation
    */
  def reclaimTimeOrderedBlocks(): Unit

  /**
    * @param memorySize The size of memory in bytes for which blocks are to be allocated
    * @param reclaimOrder The order in which blocks would be reclaimed - smaller order first.
    * @return A sequence of blocks totaling up in memory requested or empty if unable to allocate
    */
  def requestBlocks(memorySize: Long, reclaimOrder: Option[Int]): Seq[Block]

  /**
    * @param reclaimOrder The order in which blocks would be reclaimed - smaller order first.
    * @return One block of memory
    */
  def requestBlock(reclaimOrder: Option[Int]): Option[Block]

  /**
    * Releases all blocks allocated by this store.
    */
  def releaseBlocks(): Unit

  /**
    * @return Memory stats for recording
    */
  def stats(): MemoryStats
}

class MemoryStats(tags: Map[String, String]) {
  val usedBlocksMetric = Kamon.gauge("blockstore-used-blocks").refine(tags)
  val freeBlocksMetric = Kamon.gauge("blockstore-free-blocks").refine(tags)
  val usedBlocksTimeOrderedMetric = Kamon.gauge("blockstore-used-time-ordered-blocks").refine(tags)
  val timeOrderedBlocksReclaimedMetric = Kamon.counter("blockstore-time-ordered-blocks-reclaimed").refine(tags)
  val blocksReclaimedMetric = Kamon.counter("blockstore-blocks-reclaimed").refine(tags)
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
  * @param numTimeBuckets         Number of buckets for time ordered blocks
  */
class PageAlignedBlockManager(val totalMemorySizeInBytes: Long,
                              val stats: MemoryStats,
                              reclaimer: ReclaimListener,
                              numPagesPerBlock: Int,
                              numTimeBuckets: Int)
  extends BlockManager with StrictLogging {
  val mask = PageManager.PROT_READ | PageManager.PROT_EXEC | PageManager.PROT_WRITE

  import collection.JavaConverters._

  protected var firstPageAddress: Long = 0L

  protected val freeBlocks: util.LinkedList[Block] = allocate()
  protected[memory] val usedBlocks: util.LinkedList[Block] = new util.LinkedList[Block]()
  protected[memory] val usedBlocksTimeOrdered = Array.fill(numTimeBuckets)(new util.LinkedList[Block]())

  protected[memory] var timeOrderedBlocksEnabled = true

  protected val lock = new ReentrantLock()

  override def blockSizeInBytes: Long = PageManager.getInstance().pageSize() * numPagesPerBlock

  def availablePreAllocated: Long = numFreeBlocks * blockSizeInBytes

  def usedMemory: Long = usedBlocks.size * blockSizeInBytes

  override def numFreeBlocks: Int = freeBlocks.size

  override def requestBlock(reclaimOrder: Option[Int]): Option[Block] = {
    val blocks = requestBlocks(blockSizeInBytes, reclaimOrder)
    blocks.size match {
      case 0 => None
      case 1 => Some(blocks.head)
      case _ => throw new IllegalStateException("Should not have gotten more than one block")
    }
  }

  /* Used in tests for assertion */
  def usedBlocksSize(reclaimOrder: Option[Int]): Int = {
    reclaimOrder match {
      case Some(r) => usedBlocksTimeOrdered(r).size()
      case None => usedBlocks.size()
    }
  }

  /**
    * Allocates requested number of blocks. If enough blocks are not available,
    * then uses the ReclaimPolicy to check if blocks can be reclaimed
    */
  override def requestBlocks(memorySize: Long, reclaimOrder: Option[Int]): Seq[Block] = {
    lock.lock()
    try {
      val num: Int = Math.ceil(memorySize / blockSizeInBytes).toInt

      if (freeBlocks.size < num) tryReclaim(num)

      if (freeBlocks.size >= num) {
        val allocated = new Array[Block](num)
        (0 until num).foreach { i =>
          val block = freeBlocks.remove()
          use(block, reclaimOrder)
          allocated(i) = block
        }
        allocated
      } else {
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

  protected def use(block: Block, reclaimOrder: Option[Int]) = {
    block.markInUse
    reclaimOrder match {
      case Some(bucket) => usedBlocksTimeOrdered(bucket).add(block)
                           stats.usedBlocksTimeOrderedMetric.set(usedBlocksTimeOrdered.map(_.size).sum)
      case None =>         usedBlocks.add(block)
                           stats.usedBlocksMetric.set(usedBlocks.size())
    }
    stats.freeBlocksMetric.set(freeBlocks.size())
  }

  def reclaimTimeOrderedBlocks(): Unit = {
    lock.lock()
    try {
      usedBlocksTimeOrdered.foreach { list =>
        // reclaim all blocks
        val entries = list.iterator
        while (entries.hasNext) {
          val block = entries.next
          block.markReclaimable()
          entries.remove()
          block.reclaim()
          freeBlocks.add(block)
          stats.freeBlocksMetric.set(freeBlocks.size())
          stats.timeOrderedBlocksReclaimedMetric.increment()
        }
      }
      timeOrderedBlocksEnabled = false // so that tryReclaim(num) does not bother checking the time ordered lists
    } finally {
      lock.unlock()
    }
  }

  protected def tryReclaim(num: Int): Unit = {
    var reclaimed = 0
    var currList = 0
    while ( timeOrderedBlocksEnabled &&
            reclaimed < num &&
            currList < usedBlocksTimeOrdered.length) {
      reclaimFrom(usedBlocksTimeOrdered(currList), stats.timeOrderedBlocksReclaimedMetric)
      currList = currList + 1
    }
    if (reclaimed < num) reclaimFrom(usedBlocks, stats.blocksReclaimedMetric)

    def reclaimFrom(list: util.LinkedList[Block], reclaimedCounter: Counter): Unit = {
      val entries = list.iterator
      while (entries.hasNext && reclaimed < num) {
        val block = entries.next
        if (block.canReclaim) {
          entries.remove()
          block.reclaim()
          freeBlocks.add(block)
          stats.freeBlocksMetric.set(freeBlocks.size())
          reclaimedCounter.increment()
          reclaimed = reclaimed + 1
        }
      }
    }
  }

  /**
   * Used during testing only to try and reclaim all existing blocks
   */
  def reclaimAll(): Unit = {
    logger.warn(s"Reclaiming all used blocks -- THIS BETTER BE A TEST!!!")
    usedBlocks.asScala.foreach(_.markReclaimable)
    tryReclaim(usedBlocks.size)
    reclaimTimeOrderedBlocks()
  }

  def releaseBlocks(): Unit = {
    lock.lock()
    try {
      MemoryIO.getCheckedInstance.freeMemory(firstPageAddress)
    } catch {
      case e: Throwable => logger.warn(s"Could not release blocks at $firstPageAddress", e)
    } finally {
      lock.unlock()
    }
  }

}