package filodb.memory

import java.util
import java.util.concurrent.locks.ReentrantLock

import com.kenai.jffi.{MemoryIO, PageManager}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.tag.TagSet

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
    * Request one block which is immediately marked as reclaimable. Reclamation order is FIFO.
    *
    * @param owner the BlockMemFactory that will be owning this block, until reclaim.  Used for debugging.
    * @return One block of memory
    */
  def requestReclaimableBlock(owner: Option[BlockMemFactory] = None): Option[Block]

  /**
    * Request one block which must be manually marked as reclaimable. Reclamation order is
    * FIFO, based on when the block was originally requested. If a block is still marked as
    * non-reclaimable when it's in the oldest position, it gets moved to the newest position.
    *
    * @param owner the BlockMemFactory that will be owning this block, until reclaim.  Used for debugging.
    * @return One block of memory
    */
  def requestNonReclaimableBlock(owner: Option[BlockMemFactory] = None): Option[Block]

  /**
    * Request one or more blocs which are immediately marked as reclaimable. Reclamation order is FIFO.
    *
    * @param memorySize The size of memory in bytes for which blocks are to be allocated
    * @param owner the BlockMemFactory that will be owning this block, until reclaim.  Used for debugging.
    * @return A sequence of blocks totaling up in memory requested or empty if unable to allocate
    */
  def requestReclaimableBlocks(memorySize: Long, owner: Option[BlockMemFactory] = None): Seq[Block]

  /**
    * Request one or more blocks which must be manually marked as reclaimable. Reclamation order is
    * FIFO, based on when the block was originally requested. If a block is still marked as
    * non-reclaimable when it's in the oldest position, it gets moved to the newest position.
    *
    * @param memorySize The size of memory in bytes for which blocks are to be allocated
    * @param owner the BlockMemFactory that will be owning this block, until reclaim.  Used for debugging.
    * @return A sequence of blocks totaling up in memory requested or empty if unable to allocate
    */
  def requestNonReclaimableBlocks(memorySize: Long, owner: Option[BlockMemFactory] = None): Seq[Block]

  /**
    * Attempts to reclaim as many blocks as necessary to ensure that enough free blocks are
    * available.
    *
    * @return numFreeBlocks
    */
  def ensureFreeBlocks(num: Int): Int

  /**
    * Attempts to reclaim as many blocks as necessary to ensure that enough free bytes are
    * available. The actual amount reclaimed might be higher than requested.
    *
    * @return numFreeBlocks
    */
  def ensureFreeBytes(amt: Long): Int = {
    val blocks = (amt + blockSizeInBytes - 1) / blockSizeInBytes
    ensureFreeBlocks(Math.min(Integer.MAX_VALUE, blocks).toInt)
  }

  /**
    * Attempts to reclaim as many blocks as necessary to ensure that enough free bytes are
    * available as a percentage of total size. The actual amount reclaimed might be higher than
    * requested.
    *
    * @param pct percentage: 0.0 to 100.0
    * @return numFreeBlocks
    */
  def ensureFreePercent(pct: Double): Int = {
    ensureFreeBytes((totalMemorySizeInBytes * pct * 0.01).toLong)
  }

  def totalMemorySizeInBytes: Long

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
  val usedBlocksMetric = Kamon.gauge("blockstore-used-blocks").withTags(TagSet.from(tags))
  val freeBlocksMetric = Kamon.gauge("blockstore-free-blocks").withTags(TagSet.from(tags))
  val requestedBlocksMetric = Kamon.counter("blockstore-blocks-requested").withTags(TagSet.from(tags))
  val blocksReclaimedMetric = Kamon.counter("blockstore-blocks-reclaimed").withTags(TagSet.from(tags))
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
  protected[memory] val usedBlocks: util.Queue[Block] = new util.ArrayDeque[Block]()
  val reclaimLog = new collection.mutable.Queue[ReclaimEvent]

  protected val lock = new ReentrantLock()

  override def blockSizeInBytes: Long = PageManager.getInstance().pageSize() * numPagesPerBlock

  def availablePreAllocated: Long = numFreeBlocks * blockSizeInBytes

  def usedMemory: Long = usedBlocks.size * blockSizeInBytes

  override def numFreeBlocks: Int = freeBlocks.size

  override def requestReclaimableBlock(bmf: Option[BlockMemFactory] = None): Option[Block] = {
    oneBlockOnly(requestReclaimableBlocks(blockSizeInBytes, bmf))
  }

  override def requestNonReclaimableBlock(bmf: Option[BlockMemFactory] = None): Option[Block] = {
    oneBlockOnly(requestNonReclaimableBlocks(blockSizeInBytes, bmf))
  }

  private def oneBlockOnly(blocks: Seq[Block]): Option[Block] = {
    blocks.size match {
      case 0 => None
      case 1 => Some(blocks.head)
      case _ => throw new IllegalStateException("Should not have gotten more than one block")
    }
  }

  override def requestReclaimableBlocks(memorySize: Long, bmf: Option[BlockMemFactory] = None): Seq[Block] = {
    doRequestBlocks(memorySize, true, bmf);
  }

  override def requestNonReclaimableBlocks(memorySize: Long, bmf: Option[BlockMemFactory] = None): Seq[Block] = {
    doRequestBlocks(memorySize, false, bmf);
  }

  private def doRequestBlocks(memorySize: Long,
                              reclaimable: Boolean,
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
          block.markReclaimable(reclaimable);
          usedBlocks.add(block)
          stats.usedBlocksMetric.update(usedBlocks.size())
          stats.freeBlocksMetric.update(freeBlocks.size())
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

  /* Used in tests for assertion */
  def usedBlocksSize: Int = {
    usedBlocks.size()
  }

  override def ensureFreeBlocks(num: Int): Int = {
    lock.lock()
    try {
      val require = num - numFreeBlocks
      if (require > 0) tryReclaim(require)
      numFreeBlocks
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
    stats.freeBlocksMetric.update(blocks.size())
    blocks
  }

  private def addToReclaimLog(block: Block): Unit = {
    val event = ReclaimEvent(block, System.currentTimeMillis, block.owner, block.remaining)
    if (reclaimLog.size >= MaxReclaimLogSize) { reclaimLog.dequeue }
    reclaimLog += event
  }

  protected def tryReclaim(num: Int): Unit = {
    var reclaimed = 0

    for (i <- 0 until usedBlocks.size()) {
      if (reclaimed >= num) return
      val block = usedBlocks.remove()
      if (!block.canReclaim) {
        // Skip it the next time.
        usedBlocks.add(block)
      } else {
        addToReclaimLog(block)
        block.reclaim()
        block.clearOwner()
        freeBlocks.add(block)
        stats.freeBlocksMetric.update(freeBlocks.size())
        stats.blocksReclaimedMetric.increment()
        reclaimed += 1
      }
    }

    if (reclaimed < num) {
      logger.warn(s"$num blocks to reclaim but only reclaimed $reclaimed.  usedblocks=${usedBlocks.size}")
    }
  }

  /**
   * Used during testing only to try and reclaim all existing blocks
   */
  def reclaimAll(): Unit = {
    logger.warn(s"Reclaiming all used blocks -- THIS BETTER BE A TEST!!!")
    usedBlocks.asScala.foreach(_.markReclaimable)
  }

  /**
   * Finds all reclaim events in the log whose Block contains the pointer passed in.
   * Useful for debugging.  O(n) - not performant.
   */
  def reclaimEventsForPtr(ptr: BinaryRegion.NativePointer): Seq[ReclaimEvent] =
    reclaimLog.filter { ev => ptr >= ev.block.address && ptr < (ev.block.address + ev.block.capacity) }

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
