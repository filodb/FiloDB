package filodb.memory

import java.util
import java.util.concurrent.locks.ReentrantLock

import com.kenai.jffi.{MemoryIO, PageManager}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

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
    * @param canReclaim Function which checks if a block can be reclaimed
    * @return A sequence of blocks totaling up in memory requested or empty if unable to allocate
    */
  def requestBlocks(memorySize: Long): Seq[Block]

  /**
    * @param canReclaim Function which checks if a block can be reclaimed
    * @return One block of memory
    */
  def requestBlock(): Option[Block]

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
  val usedBlocksMetric = Kamon.metrics.gauge("blockstore-used-blocks", tags)(0L)
  val freeBlocksMetric = Kamon.metrics.gauge("blockstore-free-blocks", tags)(0L)
  val blocksReclaimedMetric = Kamon.metrics.counter("blockstore-blocks-reclaimed", tags)
  val blockUtilizationMetric = Kamon.metrics.histogram("blockstore-block-utilized-bytes", tags)
}


/**
  * Pre Allocates blocks totalling to the passed memory size.
  * Each block size is the same as the OS page size.
  * This class is thread safe
  *
  * @param totalMemorySizeInBytes Control the number of pages to allocate. (totalling up to the totallMemorySizeInBytes)
  * @param stats                  Memory metrics which need to be recorded
  * @param numPagesPerBlock       The number of pages a block spans
  */
class PageAlignedBlockManager(val totalMemorySizeInBytes: Long,
                              val stats: MemoryStats,
                              numPagesPerBlock: Int = 1)
  extends BlockManager with StrictLogging {
  val mask = PageManager.PROT_READ | PageManager.PROT_EXEC | PageManager.PROT_WRITE

  protected var firstPageAddress: Long = 0L

  protected val freeBlocks: util.LinkedList[Block] = allocate()
  protected val usedBlocks: util.LinkedList[Block] = new util.LinkedList[Block]()

  protected val lock = new ReentrantLock()

  override def blockSizeInBytes: Long = PageManager.getInstance().pageSize() * numPagesPerBlock

  def availablePreAllocated: Long = numFreeBlocks * blockSizeInBytes

  def usedMemory: Long = usedBlocks.size * blockSizeInBytes

  override def numFreeBlocks: Int = freeBlocks.size

  override def requestBlock(): Option[Block] = {
    val blocks = requestBlocks(blockSizeInBytes)
    blocks.size match {
      case 0 => None
      case _ => Some(blocks.head)
    }
  }

  /**
    * Allocates requested number of blocks. If enough blocks are not available,
    * then uses the ReclaimPolicy to check if blocks can be reclaimed
    */
  override def requestBlocks(memorySize: Long): Seq[Block] = {
    lock.lock()
    try {
      val num: Int = Math.ceil(memorySize / blockSizeInBytes).toInt

      if (freeBlocks.size < num) tryReclaim(num)

      if (freeBlocks.size >= num) {
        val allocated = new Array[Block](num)
        (0 until num).foreach { i =>
          val block = freeBlocks.remove()
          use(block)
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
      blocks.add(new Block(address, blockSizeInBytes))
    }
    stats.freeBlocksMetric.record(blocks.size())
    blocks
  }

  protected def use(block: Block) = {
    block.markInUse
    usedBlocks.add(block)
    stats.usedBlocksMetric.record(usedBlocks.size())
  }

  protected def tryReclaim(num: Int): Unit = {
    val entries = usedBlocks.iterator
    var i = 0
    while (entries.hasNext) {
      val block = entries.next
      if (block.canReclaim) {
        entries.remove()
        block.reclaim()
        freeBlocks.add(block)
        stats.freeBlocksMetric.record(freeBlocks.size())
        stats.blocksReclaimedMetric.increment()
        i = i + 1
      }
      if (i >= num) {
        return
      }
    }
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