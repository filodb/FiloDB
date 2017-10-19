package filodb.memory.impl

import java.util
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._

import filodb.memory.{Block, BlockManager}

import com.kenai.jffi.PageManager

/**
  * Pre Allocates blocks totalling to the passed memory size.
  * Each block size is the same as the OS page size.
  * This class is thread safe
  *
  * @param totalMemorySizeInBytes Control the number of pages to allocate. (totalling up to the totallMemorySizeInBytes)
  */
class PageAlignedBlockManager(val totalMemorySizeInBytes: Long) extends BlockManager with CleanShutdown {

  private val pageSize = PageManager.getInstance().pageSize()

  protected val freeBlocks: util.LinkedList[Block] = allocateWithPageManager
  protected val usedBlocks: util.LinkedList[Block] = new util.LinkedList[Block]()

  protected val lock = new ReentrantLock()

  override def blockSizeInBytes: Long = pageSize

  def availablePreAllocated: Long = numFreeBlocks * blockSizeInBytes

  def usedMemory: Long = usedBlocks.size * blockSizeInBytes

  override def numFreeBlocks: Int = freeBlocks.size

  override def requestBlock(canReclaim: (Block) => Boolean): Option[Block] = {
    val blocks = requestBlocks(pageSize, canReclaim)
    blocks.size match {
      case 0 => None
      case _ => Some(blocks.head)
    }
  }

  /**
    * Allocates requested number of blocks. If enough blocks are not available,
    * then uses the ReclaimPolicy to check if blocks can be reclaimed
    */
  override def requestBlocks(memorySize: Long, canReclaim: (Block) => Boolean): Seq[Block] = {
    lock.lock()
    try {
      val num: Int = Math.ceil(memorySize / blockSizeInBytes).toInt

      if (freeBlocks.size < num) tryReclaim(num, canReclaim)

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

  protected def allocateWithPageManager = {
    val numBlocks: Int = Math.floor(totalMemorySizeInBytes / blockSizeInBytes).toInt
    val blocks = new util.LinkedList[Block]()
    val firstPageAddress: Long = PageManager.getInstance().allocatePages(numBlocks,
      PageManager.PROT_READ | PageManager.PROT_EXEC | PageManager.PROT_WRITE)
    for (i <- 0 until numBlocks) {
      val address = firstPageAddress + (i * pageSize)
      blocks.add(new Block(address, blockSizeInBytes))
    }
    blocks
  }


  protected def use(block: Block) = {
    block.markInUse
    usedBlocks.add(block)
  }

  protected def tryReclaim(num: Int, canReclaim: (Block) => Boolean): Unit = {
    val entries = usedBlocks.iterator
    var i = 0
    while (entries.hasNext) {
      val block = entries.next
      if (canReclaim(block)) {
        entries.remove()
        block.reclaim()
        freeBlocks.add(block)
        i = i + 1
      }
      if (i >= num) {
        return
      }
    }
  }

  override protected[memory] def releaseBlocks() = {
    lock.lock()
    try {
      releaseBlocksWithPM(freeBlocks)
      releaseBlocksWithPM(usedBlocks)
    } finally {
      lock.unlock()
    }
  }

  protected def releaseBlocksWithPM(blocks: java.lang.Iterable[Block]) = {
    blocks.asScala.foreach { block =>
      PageManager.getInstance().freePages(block.address, 1)
    }
  }

  override def shutdown(): Unit = {
    releaseBlocks
  }

}
