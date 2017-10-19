package filodb.memory

import java.util.concurrent.atomic.AtomicReference

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
  def requestBlocks(memorySize: Long, canReclaim: (Block) => Boolean): Seq[Block]

  /**
    * @param canReclaim Function which checks if a block can be reclaimed
    * @return One block of memory
    */
  def requestBlock(canReclaim: (Block) => Boolean): Option[Block]

  /**
    * Releases all blocks allocated by this store.
    */
  protected[memory] def releaseBlocks(): Unit

}

object BlockManager {

  val noReclaimPolicy = ((block: Block) => false)

  val reclaimAnyPolicy = ((block: Block) => true)

}

/**
  * A holder which maintains a reference to a currentBlock which is replaced when
  * it is full
  *
  * @param blockStore The BlockStore which is used to request more blocks when the current
  *                   block is full.
  */
class BlockHolder(blockStore: BlockManager, canReclaim: (Block) => Boolean) {

  protected val currentBlock = new AtomicReference[Block]()

  currentBlock.set(blockStore.requestBlock(canReclaim).get)

  def requestBlock(forSize: Long): Block = {
    if (!currentBlock.get().hasCapacity(forSize)) {
      currentBlock.set(blockStore.requestBlock(canReclaim).get)
    }
    currentBlock.get()
  }

}