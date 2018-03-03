package filodb.memory

import com.typesafe.scalalogging.StrictLogging

/**
 * This class allows BlockMemFactory's to be reused so that the blocks can be fully utilized, instead of left stranded
 * and half empty.  It has a checkout and return semantics.  Multiple parallel tasks each do their own
 * checkout and return, thus there should be one blockholder outstanding per task.
 */
class BlockMemFactoryPool(blockStore: BlockManager) extends StrictLogging {
  private val factoryPool = new collection.mutable.Queue[BlockHolder]()

  def poolSize: Int = factoryPool.length

  def checkout(): BlockHolder = synchronized {
    if (factoryPool.nonEmpty) {
      logger.debug(s"Checking out BlockMemFactory from pool.  poolSize=$poolSize")
      factoryPool.dequeue
    } else {
      logger.debug(s"Nothing in BlockMemFactory pool.  Creating a new one")
      new BlockHolder(blockStore)
    }
  }

  def release(factory: BlockHolder): Unit = synchronized {
    logger.debug(s"Returning factory $factory to the pool.  New size ${poolSize + 1}")
    factoryPool += factory
  }
}