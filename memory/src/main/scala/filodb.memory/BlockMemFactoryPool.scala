package filodb.memory

import com.typesafe.scalalogging.StrictLogging

/**
 * This class allows BlockMemFactory's to be reused so that the blocks can be fully utilized, instead of left stranded
 * and half empty.  It has a checkout and return semantics.  Multiple parallel tasks each do their own
 * checkout and return, thus there should be one blockholder outstanding per task.
 *
 * @param blockStore the underlying BlockManager to allocate blocks from for each BlockMemFactory
 * @param metadataAllocSize size of each metadata set per allocation
 * @param baseTags a set of tags to identify each BlockMemFactory, used only for debugging
 */
class BlockMemFactoryPool(blockStore: BlockManager,
                          metadataAllocSize: Int,
                          baseTags: Map[String, String]) extends StrictLogging {
  private val factoryPool = new collection.mutable.Queue[BlockMemFactory]()

  def poolSize: Int = factoryPool.length

  /**
   * Checks out a BlockMemFactory, optionally adding in extra tags for this particular BMF
   */
  def checkout(moreTags: Map[String, String] = Map.empty): BlockMemFactory = synchronized {
    val fact = if (factoryPool.nonEmpty) {
      logger.debug(s"Checking out BlockMemFactory from pool.  poolSize=$poolSize")
      factoryPool.dequeue
    } else {
      logger.debug(s"Nothing in BlockMemFactory pool.  Creating a new one")
      new BlockMemFactory(blockStore, false, metadataAllocSize, baseTags)
    }
    fact.tags = baseTags ++ moreTags
    fact
  }

  def release(factory: BlockMemFactory): Unit = synchronized {
    logger.debug(s"Returning factory $factory to the pool.  New size ${poolSize + 1}")
    factoryPool += factory
  }

  def blocksContainingPtr(ptr: BinaryRegion.NativePointer): Seq[Block] =
    factoryPool.flatMap { bmf =>
      val blocks = bmf.fullBlocks ++ Option(bmf.currentBlock).toList
      BlockDetective.containsPtr(ptr, blocks)
    }
}
