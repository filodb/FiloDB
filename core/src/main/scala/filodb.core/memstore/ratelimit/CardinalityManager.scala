package filodb.core.memstore.ratelimit

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._

import filodb.core.DatasetRef
import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.metadata.PartitionSchema

/**
 * Manages the calculation and storage of cardinality count in a shard of a cluster
 */
class CardinalityManager(datasetRef: DatasetRef,
                         shardNum: Int,
                         shardKeyLen: Int,
                         partKeyIndex: PartKeyLuceneIndex,
                         partSchema: PartitionSchema,
                         filodbConfig: Config,
                         meteringEnabled: Boolean,
                         quotaSource: QuotaSource) extends StrictLogging {

  // Initializing cardTracker to None initially. shouldTriggerCardinalityCount evaluates to true when cardTracker
  // is set to None
  var cardTracker: Option[CardinalityTracker] = None

  // number of shards per node - used to stagger/distribute the cardinality count calculation of different shards
  // to different times of day
  private val numShardsPerNode: Int = getNumShardsPerNodeFromConfig(datasetRef.dataset, filodbConfig)

  // This flag is used to avoid concurrent calculation of cardinality calculation. This is useful to avoid using of
  // physical resources for duplicate calculation
  private var isCardTriggered: Boolean = false


  /**
   * `dataset-configs` is an string array where each string is a file path for a dataset config. This function reads
   * those file paths and parses it to Config object
   * @param filoConfig
   * @return
   */
  private def getDataSetConfigs(filoConfig: Config): Seq[Config] = {
    val datasetConfPaths = filoConfig.as[Seq[String]]("dataset-configs")
    datasetConfPaths.map { d => ConfigFactory.parseFile(new java.io.File(d)) }
  }

  /**
   * Helper method to get the shards per node in the filodb cluster. We look for the two specific
   * config that is `min-num-nodes` ( the minimum number of nodes/pods which has to be in a cluster ) and
   * `num-shards` (the total number of shards). num-shards/min-num-nodes gives us the required value.
   *
   * NOTE: This configs can be part of `dataset-configs` or `inline-dataset-configs`. Also making sure, we look
   * for the given dataset
   *
   * @return shards present per node
   */
  def getNumShardsPerNodeFromConfig(datasetToSearch: String, filoConfig: Config): Int = {
    val datasetConfig: Option[Config] = if (filoConfig.hasPath("dataset-configs")) {
      getDataSetConfigs(filoConfig).find(x => x.getString("dataset") == datasetToSearch)
    } else None

    val configToUse = datasetConfig match {
      case Some(datasetConf) => Some(datasetConf)
      case None =>
        // use fallback to inline dataset config
        logger.info(s"Didn't find required config for dataset=${datasetToSearch} in config `dataset-configs`." +
          s"Checking in config `inline-dataset-configs`")

        if (filoConfig.hasPath("inline-dataset-configs")) {
          filoConfig.as[Seq[Config]]("inline-dataset-configs")
            .find(x => x.getString("dataset") == datasetToSearch)
        }
        else {
          None
        }
    }

    configToUse match {
      case Some(conf) =>
        val minNumNodes = conf.getInt("min-num-nodes")
        val numShards = conf.getInt("num-shards")
        val result = numShards / minNumNodes
        logger.info(s"Found config to estimate the shards per node. minNumNodes=$minNumNodes numShards=$numShards" +
          s" numShardsPerNode=$result")
        result
      case None =>
        // NOTE: This is an Extremely UNLIKELY case, because the configs we rely on, are required for startup
        val defaultNumShardsPerNode = 8
        logger.error(s"Could not find config for dataset=${datasetToSearch} in configs " +
          s"`dataset-configs` and `inline-dataset-configs`. Please check filodb config = ${filodbConfig.toString}" +
          s" default-value=$defaultNumShardsPerNode")
        defaultNumShardsPerNode
    }
  }

  /**
   * Determines if cardinality should be triggered at after the current indexRefresh using the indexRefreshCount.
   * For Example: With shardsPerNode = 8, the shard with number 1, should trigger cardinality when
   * indexRefreshCount is = 1,9, 17 etc
   *
   * NOTE: WHY are we using such criteria to trigger the cardinality count? -> This is done to stagger/distribute the
   * calculation cost of cardinality count of different shards residing in the same node at different times of day.
   * We don't want all the shards to be calculating this at the same time and using up all the resources of the node.
   *
   * @param currentShardNum Shard Number (0 - Num-Shards)
   * @param shardsPerNode   Number of Shards per node
   * @param indexRefreshCount The number of time the indexRefresh has already happened
   * @return True if cardinality count is to be triggered, false otherwise
   */
  def shouldTriggerCardinalityCount(currentShardNum: Int, shardsPerNode: Int, indexRefreshCount: Int): Boolean = {
    if (meteringEnabled) {
      if (isCardTriggered) {
        // Already calculating cardinality, avoid triggering of another one now
        logger.info(s"[CardinalityManager] isCardTriggered already set to true. Skipping run! " +
          s"shardNum=$shardNum indexRefreshCount=$indexRefreshCount")
        false
      }
      else {
        require(shardsPerNode > 0)
        cardTracker match {
          case Some(_) => (indexRefreshCount % shardsPerNode) == (currentShardNum % shardsPerNode)
          // if the tracker is not initialized, we should trigger the build of card store and card tracker
          case None => true
        }
      }
    }
    else {
      // metering is not enabled. Shouldn't trigger cardinality calculation
      false
    }
  }

  /**
   * Triggers cardinalityCount if metering is enabled and the required criteria matches.
   * It creates a new instance of CardinalityTracker and uses the PartKeyLuceneIndex to calculate cardinality count
   * and store data in a local CardinalityStore. We then close the previous instance of CardinalityTracker and switch
   * it with the new one we created in this call.
   * @param indexRefreshCount The number of time the indexRefresh has already happened. This is used in the logic of
   *                          shouldTriggerCardinalityCount
   */
  def triggerCardinalityCount(indexRefreshCount: Int): Unit = {
    if (meteringEnabled) {
      try {
        if (shouldTriggerCardinalityCount(shardNum, numShardsPerNode, indexRefreshCount)) {
          isCardTriggered = true
          val newCardTracker = getNewCardTracker()
          var cardCalculationComplete = false
          try {
            partKeyIndex.calculateCardinality(partSchema, newCardTracker)
            cardCalculationComplete = true
          } catch {
            case ex: Exception =>
              logger.error(s"[CardinalityManager]Error while calculating cardinality using" +
                s" PartKeyLuceneIndex! shardNum=$shardNum indexRefreshCount=$indexRefreshCount", ex)
              // cleanup resources used by the newCardTracker tracker to avoid leaking of resources
              newCardTracker.close()
          }
          if (cardCalculationComplete) {
            try {
              // close the cardinality store and release the physical resources of the current cardinality store
              close()
              cardTracker = Some(newCardTracker)
              logger.info(s"[CardinalityManager] Triggered cardinality count successfully for" +
                s" shardNum=$shardNum indexRefreshCount=$indexRefreshCount")
            } catch {
              case ex: Exception =>
                // Very unlikely scenario, but can happen if the disk call fails.
                logger.error(s"[CardinalityManager]Error closing card tracker! shardNum=$shardNum", ex)
                // setting cardTracker to None in this case, since the exception happened on the close. We
                // can't rely on the card store. The next trigger should re-build the card store and card tracker
                cardTracker = None
            }
          }
          isCardTriggered = false
        }
        else {
          logger.info(s"[CardinalityManager] shouldTriggerCardinalityCount returned false for shardNum=$shardNum" +
            s" numShardsPerNode=$numShardsPerNode indexRefreshCount=$indexRefreshCount " +
            s"isCardTriggered=$isCardTriggered")
        }
      }
      catch {
        case e: Exception =>
          logger.error(s"[CardinalityManager]Error while triggering cardinality count! shardNum=$shardNum " +
            s" indexRefreshCount=$indexRefreshCount", e)
          // making sure we are able to trigger the cardinality calculation for the next time
          isCardTriggered = false
      }
    }
  }

  /**
   * Helper method to create a CardinalityTracker instance
   *
   * @return instance of CardinalityTracker
   */
  private def getNewCardTracker(): CardinalityTracker = {
    val cardStore = new RocksDbCardinalityStore(datasetRef, shardNum)
    val defaultQuota = quotaSource.getDefaults(datasetRef)
    val tracker = new CardinalityTracker(datasetRef, shardNum, shardKeyLen, defaultQuota, cardStore)
    quotaSource.getQuotas(datasetRef).foreach { q =>
      tracker.setQuota(q.shardKeyPrefix, q.quota)
    }
    tracker
  }

  def scan(shardKeyPrefix: Seq[String], depth: Int): Seq[CardinalityRecord] = {
    if (meteringEnabled) {
      cardTracker match {
        case Some(tracker) => tracker.scan(shardKeyPrefix, depth)
        case None =>
          // Very unlikely scenario. This can happen, if we are not able to init a card tracker or if there was an
          // exception while closing the existing card tracker object while triggering cardinality count
          logger.error(s"[CardinalityManager] CardTracker is set to None." +
            s"Scan query returning empty result for shardNum=$shardNum")
          Seq()
      }
    } else {
      logger.info(s"[CardinalityManager]Cardinality Metering is not enabled for shardNum=$shardNum")
      Seq()
    }
  }

  /**
   * Closes the cardinality tracker and clean/release all the physical resources used
   */
  def close(): Unit = {
    if (meteringEnabled) {
      cardTracker match {
        case Some(tracker) => tracker.close()
        case None => logger.error(s"[CardinalityManager]CardTracker is set to None. Can't close for shardNum=$shardNum")
      }
    }
  }
}
