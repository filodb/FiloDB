package filodb.memory

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.kenai.jffi.{MemoryIO, PageManager}
import com.typesafe.scalalogging.StrictLogging
import java.util
import kamon.Kamon
import kamon.metric.{Counter, Gauge}
import kamon.tag.TagSet

final case class MemoryRequestException(msg: String) extends Exception(msg)

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
    * @param memorySize The size of memory in bytes for which blocks are to be allocated
    * @param odp true if this is for paging ODPed chunks
    * @param owner the BlockMemFactory that will be owning this block, until reclaim.  Used for debugging.
    * @return A sequence of blocks totaling up in memory requested or empty if unable to allocate
    */
  def requestBlocks(memorySize: Long, odp: Boolean, owner: Option[BlockMemFactory] = None): Seq[Block]

  /**
    * @param odp true if requesting for ODP
    * @param owner the BlockMemFactory that will be owning this block, until reclaim.  Used for debugging.
    * @return One block of memory
    */
  def requestBlock(odp: Boolean, owner: Option[BlockMemFactory] = None): Option[Block]

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

  def currentFreePercent: Double = {
    (((numFreeBlocks * blockSizeInBytes).toDouble) / totalMemorySizeInBytes) * 100.0
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
  val usedIngestionBlocksMetric = Kamon.gauge("blockstore-used-ingestion-blocks").withTags(TagSet.from(tags))
  val freeBlocksMetric = Kamon.gauge("blockstore-free-blocks").withTags(TagSet.from(tags))
  val requestedBlocksMetric = Kamon.counter("blockstore-blocks-requested").withTags(TagSet.from(tags))
  val usedOdpBlocksMetric = Kamon.gauge("blockstore-used-odp-blocks").withTags(TagSet.from(tags))
  val odpBlocksReclaimedMetric = Kamon.counter("blockstore-odp-blocks-reclaimed")
                                            .withTags(TagSet.from(tags))
  val ingestionBlocksReclaimedMetric = Kamon.counter("blockstore-ingestion-blocks-reclaimed")
                                            .withTags(TagSet.from(tags))

  /**
    * How much time a thread was potentially stalled while attempting to ensure
    * free space. Unit is nanoseconds.
    */
  val blockHeadroomStall = Kamon.counter("blockstore-headroom-stall-nanos").withTags(TagSet.from(tags))

  /**
    * How much time a thread was stalled while attempting to acquire the reclaim lock.
    * Unit is nanoseconds.
    */
  val blockReclaimStall = Kamon.counter("blockstore-reclaim-stall-nanos").withTags(TagSet.from(tags))
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

  protected val freeBlocks: util.ArrayDeque[Block] = allocate()
  protected[memory] val usedIngestionBlocks: util.ArrayDeque[Block] = new util.ArrayDeque[Block]()
  protected[memory] val usedOdpBlocks: util.ArrayDeque[Block] = new util.ArrayDeque[Block]()
  val reclaimLog = new collection.mutable.Queue[ReclaimEvent]

  protected val lock = new ReentrantLock()

  // Acquired when reclaiming on demand. Acquire shared lock to prevent block reclamation.
  final val reclaimLock = new Latch

  override def blockSizeInBytes: Long = PageManager.getInstance().pageSize() * numPagesPerBlock

  def usedMemory: Long = usedIngestionBlocks.size * blockSizeInBytes

  override def numFreeBlocks: Int = freeBlocks.size

  override def requestBlock(odp: Boolean, bmf: Option[BlockMemFactory] = None): Option[Block] = {
    val blocks = requestBlocks(blockSizeInBytes, odp, bmf)
    blocks.size match {
      case 0 => None
      case 1 => Some(blocks.head)
      case _ => throw new IllegalStateException("Should not have gotten more than one block")
    }
  }

  /**
    * Allocates requested number of blocks. If enough blocks are not available,
    * then uses the ReclaimPolicy to check if blocks can be reclaimed
    * Uses a lock to ensure that concurrent requests are safe.
    *
    * If odp is true, a MemoryRequestException is thrown when sufficient blocks are not
    * currently free. In other words, ODP block request doesn't attempt
    * reclamation. Instead, a background task must be running which calls ensureFreeBlocks.
    * ODP blocks are used for on-demand-paging only (ODP), initiated by a query, and
    * reclamation during ODP can end up causing the query results to have "holes". Throwing an
    * exception isn't a perfect solution, but it can suffice until a proper block pinning
    * mechanism is in place. Queries which fail with this exception can retry, perhaps after
    * calling ensureFreeBLocks explicitly.
    */
  override def requestBlocks(memorySize: Long,
                             odp: Boolean,
                             ownerBmf: Option[BlockMemFactory] = None): Seq[Block] = {
    val num: Int = Math.ceil(memorySize / blockSizeInBytes).toInt
    stats.requestedBlocksMetric.increment(num)

    lock.lock()
    try {
      if (freeBlocks.size < num) {
        if (!odp) {
          tryReclaimOnDemand(num)
        } else {
            val msg = s"Unable to allocate ODP block(s) without forcing a reclamation: " +
                      s"num_blocks=$num num_bytes=$memorySize freeBlocks=${freeBlocks.size}"
            throw new MemoryRequestException(msg)
        }
      }

      if (freeBlocks.size >= num) {
        val allocated = new Array[Block](num)
        (0 until num).foreach { i =>
          val block = freeBlocks.remove()
          if (ownerBmf.nonEmpty) block.setOwner(ownerBmf.get)
          use(block, odp)
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

  /**
    * Internal variant of the tryReclaim method which is called when blocks are requested, but
    * none are available. Instead of blindly reclaiming blocks, it attempts to exclusively
    * acquire the reclaim lock. By doing so, it avoids reclaiming blocks which are currently
    * being accessed. To work properly, all threads which require this protection must hold the
    * shared reclaimLock. To prevent indefinite stalls, this method times out lock acquisition,
    * logs an error, and then reclaims anyhow.
    *
    * This method must be called with the primary lock object held. To avoid deadlock, this
    * method releases and re-acquires the lock.
    */
  private def tryReclaimOnDemand(num: Int): Unit = {
    lock.unlock()
    var acquired: Boolean = false
    try {
      val start = System.nanoTime()
      // Give up after waiting (in total) a little over 16 seconds.
      acquired = tryExclusiveReclaimLock(8192)

      if (!acquired) {
        // Don't stall ingestion forever. Some queries might return invalid results because
        // the lock isn't held. If the lock state is broken, then ingestion is really stuck
        // and the node must be restarted. Queries should always release the lock.
        logger.error(s"Lock for BlockManager.tryReclaimOnDemand timed out: ${reclaimLock}")
      } else {
        logger.debug("Lock for BlockManager.tryReclaimOnDemand aquired")
      }

      val stall = System.nanoTime() - start
      stats.blockReclaimStall.increment(stall)
    } finally {
      lock.lock()
    }

    try {
      if (numFreeBlocks < num) { // double check since lock was released
        tryReclaim(num)
      }
    } finally {
      if (acquired) {
        reclaimLock.releaseExclusive()
      }
    }
  }

  private def tryExclusiveReclaimLock(finalTimeoutMillis: Int): Boolean = {
    // Attempting to acquire the exclusive lock must wait for concurrent queries to finish, but
    // waiting will also stall new queries from starting. To protect against this, attempt with
    // a timeout to let any stalled queries through. To prevent starvation of the exclusive
    // lock attempt, increase the timeout each time, but eventually give up. The reason why
    // waiting for an exclusive lock causes this problem is that the thread must enqueue itself
    // into the lock as a waiter, and all new shared requests must wait their turn. The risk
    // with timing out is that if there's a continuous stream of long running queries (more than
    // one second), then the exclusive lock will never be acqiured, and then ensureFreeBlocks
    // won't be able to do its job. The timeout settings might need to be adjusted in that case.
    // Perhaps the timeout should increase automatically if ensureFreeBlocks failed the last time?
    // This isn't safe to do until we gain higher confidence that the shared lock is always
    // released by queries.

    var timeout = 1;
    while (true) {
      val acquired = reclaimLock.tryAcquireExclusiveNanos(TimeUnit.MILLISECONDS.toNanos(timeout))
      if (acquired) {
        return true
      }
      if (timeout >= finalTimeoutMillis) {
        return false
      }
      Thread.`yield`()
      timeout = Math.min(finalTimeoutMillis, timeout << 1)
    }
    false // never reached, but scala compiler complains otherwise
  }

  /**
    * Expected to be called via a background task, to periodically ensure that enough blocks
    * are free for new allocations. This helps prevent ODP activity from reclaiming immediately
    * from itself.
    *
    * @param pct percentage: 0.0 to 100.0
    */
  def ensureHeadroom(pct: Double): Int = {
    // Ramp up the timeout as the current headroom shrinks. Max timeout per attempt is a little
    // over 2 seconds, and the total timeout can be double that, for a total of 4 seconds.
    val maxTimeoutMillis = 2048
    val timeoutMillis = ((1.0 - (currentFreePercent / pct)) * maxTimeoutMillis).toInt

    if (timeoutMillis <= 0) {
      // Headroom target is already met.
      return numFreeBlocks
    }

    var numFree: Int = 0
    val start = System.nanoTime()
    val acquired = tryExclusiveReclaimLock(timeoutMillis)
    if (!acquired) {
      if (timeoutMillis >= maxTimeoutMillis / 2) {
        // Start warning when the current headroom has dipped below the halfway point.
        // The lock state is logged in case it's stuck due to a runaway query somewhere.
        logger.warn(s"Lock for BlockManager.ensureHeadroom timed out: ${reclaimLock}")
      }
      numFree = numFreeBlocks
    } else {
      try {
        numFree = ensureFreePercent(pct)
      } finally {
        reclaimLock.releaseExclusive()
      }
      val numBytes = numFree * blockSizeInBytes
      logger.debug(s"BlockManager.ensureHeadroom numFree: $numFree ($numBytes bytes)")
    }
    val stall = System.nanoTime() - start
    stats.blockHeadroomStall.increment(stall)
    numFree
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

  protected def allocate(): util.ArrayDeque[Block] = {
    val numBlocks: Int = Math.floor(totalMemorySizeInBytes / blockSizeInBytes).toInt
    val blocks = new util.ArrayDeque[Block]()
    logger.info(s"Allocating $numBlocks blocks of $blockSizeInBytes bytes each, total $totalMemorySizeInBytes")
    firstPageAddress = MemoryIO.getCheckedInstance().allocateMemory(totalMemorySizeInBytes, false)
    for (i <- 0 until numBlocks) {
      val address = firstPageAddress + (i * blockSizeInBytes)
      blocks.add(new Block(address, blockSizeInBytes, reclaimer))
    }
    stats.freeBlocksMetric.update(blocks.size())
    blocks
  }

  protected def use(block: Block, odp: Boolean) = {
    block.markInUse
    if (odp) {
      usedOdpBlocks.add(block)
      stats.usedOdpBlocksMetric.update(usedOdpBlocks.size())
    } else {
      usedIngestionBlocks.add(block)
      stats.usedIngestionBlocksMetric.update(usedIngestionBlocks.size())
    }
    stats.freeBlocksMetric.update(freeBlocks.size())
  }

  private def addToReclaimLog(block: Block): Unit = {
    val event = ReclaimEvent(block, System.currentTimeMillis, block.owner, block.remaining)
    if (reclaimLog.size >= MaxReclaimLogSize) { reclaimLog.dequeue }
    reclaimLog += event
  }

  protected[memory] def tryReclaim(num: Int): Int = {
    var reclaimed = 0
    reclaimFrom(usedOdpBlocks, stats.odpBlocksReclaimedMetric, stats.usedOdpBlocksMetric)
    if (reclaimed < num)
      reclaimFrom(usedIngestionBlocks, stats.ingestionBlocksReclaimedMetric, stats.usedIngestionBlocksMetric)
    // if we do not get required blocks even after reclaim call
    if (reclaimed < num) {
      logger.warn(s"$num blocks to reclaim but only reclaimed $reclaimed. " +
        s"usedIngestionBlocks=${usedIngestionBlocks.size} usedOdpBlocks=${usedOdpBlocks.size()}")
    }

    def reclaimFrom(list: util.ArrayDeque[Block],
                    reclaimedCounter: Counter,
                    usedBlocksStats: Gauge): Seq[Block] = {
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
          stats.freeBlocksMetric.update(freeBlocks.size())
          reclaimedCounter.increment()
          reclaimed = reclaimed + 1
        }
      }
      usedBlocksStats.update(list.size())
      removed
    }
    reclaimed
  }

  /**
   * Used during testing only to try and reclaim all existing blocks
   */
  def reclaimAll(): Unit = {
    logger.warn(s"Reclaiming all used blocks -- THIS BETTER BE A TEST!!!")
    usedIngestionBlocks.asScala.foreach(_.markReclaimable())
    usedOdpBlocks.asScala.foreach(_.markReclaimable())
    tryReclaim(usedIngestionBlocks.size + usedOdpBlocks.size())
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
        BlockDetective.containsPtr(ptr, usedOdpBlocks)
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

  override def finalize(): Unit = releaseBlocks()
}
