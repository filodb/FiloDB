package filodb.memory

import scala.collection.mutable

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
  private val checkedOut = new mutable.HashMap[Int, BlockMemFactory]()

  def poolSize: Int = factoryPool.length

  /**
   * Allocates (if needed) and returns new block mem factory from the pool for the given
   * flush group. Subsequent calls to fetch will return the same block mem factory
   * until the flush group is checked out for flush
   */
  def fetchForOverflow(flushGroup: Int): BlockMemFactory = synchronized {
    if (checkedOut.contains(flushGroup)) {
      checkedOut(flushGroup)
    } else {
      val fact = if (factoryPool.nonEmpty) {
        logger.debug(s"Checking out BlockMemFactory from pool for flushGroup=$flushGroup poolSize=$poolSize")
        factoryPool.dequeue
      } else {
        logger.debug(s"Nothing in BlockMemFactory pool.  Creating a new one for flushGroup=$flushGroup")
        new BlockMemFactory(blockStore, metadataAllocSize, baseTags)
      }
      fact.tags = baseTags + ("flushGroup" -> flushGroup.toString)
      checkedOut(flushGroup) = fact
      fact
    }
  }

  /**
   * Checks out a BlockMemFactory for flush. Subsequent call for fetch for this
   * flush group will return new BlockMemFactory
   */
  def checkoutForFlush(flushGroup: Int): BlockMemFactory = synchronized {
    checkedOut.remove(flushGroup).getOrElse(fetchForOverflow(flushGroup))
  }

  /**
   * Release factory back to pool. It should have been checked out for flush
   * @param factory
   */
  def release(factory: BlockMemFactory): Unit = synchronized {
    logger.debug(s"Returning factory $factory to the pool.  New size ${poolSize + 1}")
    factoryPool += factory
  }

  def blocksContainingPtr(ptr: BinaryRegion.NativePointer): Seq[Block] = synchronized {
    factoryPool.flatMap { bmf =>
      val blocks = bmf.fullBlocksToBeMarkedAsReclaimable ++ Option(bmf.currentBlock).toList
      BlockDetective.containsPtr(ptr, blocks)
    }
  }

}
