package filodb.core.downsample

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef
import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.memstore.ratelimit.{CardinalityTracker, QuotaSource, RocksDbCardinalityStore}
import filodb.core.metadata.PartitionSchema
import filodb.core.store.StoreConfig
import filodb.memory.format.UnsafeUtils

/**
 * Manages the calculation and storage of cardinality count in a shard of downsample cluster
 */
class DownsampleCardinalityManager(rawDatasetRef: DatasetRef,
                                   shardNum: Int,
                                   shardKeyLen: Int,
                                   partKeyIndex: PartKeyLuceneIndex,
                                   partSchema: PartitionSchema,
                                   filodbConfig: Config,
                                   downsampleStoreConfig: StoreConfig,
                                   quotaSource: QuotaSource) extends StrictLogging {

  var cardTracker: CardinalityTracker = initCardTracker()

  // number of shards per node - used to stagger/distribute the cardinality count calculation of different shards
  // to different times of day
  private val numShardsPerNode: Int = getNumShardsPerNodeFromConfig(rawDatasetRef.dataset, filodbConfig)

  /**
   * Helper method to get the shards per node in the downsample filodb cluster. We look for the two specific
   * config that is `min-num-nodes` ( the minimum number of nodes/pods which has to be in a cluster ) and
   * `num-shards` (the total number of shards). num-shards/min-num-nodes gives us the required value.
   *
   * NOTE: This configs can be part of `dataset-configs` or `inline-dataset-configs`. Also making sure, we look
   * for the dataset, which was used to create this instance of DownsampleTimeSeriesShard
   *
   * @return shards present per node
   */
  def getNumShardsPerNodeFromConfig(datasetToSearch: String, filoConfig: Config): Int = {
    // init
    var datasetConfig: Option[Config] = None

    if (filoConfig.hasPath("dataset-configs")) {
      datasetConfig = filoConfig.getConfigList("dataset-configs")
        .toArray().toList
        .asInstanceOf[List[Config]]
        .filter(x => x.getString("dataset") == datasetToSearch)
        .headOption
    }

    val configToUse = datasetConfig match {
      case Some(datasetConf) => Some(datasetConf)
      case None =>
        // use fallback to inline dataset config
        logger.info(s"Didn't find required config for dataset=${rawDatasetRef.dataset} in config `dataset-configs`." +
          s"Checking in config `inline-dataset-configs`")

        if (filoConfig.hasPath("inline-dataset-configs")) {
          filoConfig.getConfigList("inline-dataset-configs")
            .toArray().toList
            .asInstanceOf[List[Config]]
            .filter(x => x.getString("dataset") == datasetToSearch).headOption
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
        logger.info(s"Found the config to estimate the shards per node. minNumNodes=$minNumNodes numShards=$numShards" +
          s" numShardsPerNode=$result")
        result
      case None =>
        // NOTE: This is an Extremely UNLIKELY case, because the configs we rely on, are required for startup
        // WHY 8? This is the current configuration of our production downsample cluster as of 06/02/2023
        val defaultNumShardsPerNode = 8
        logger.error(s"Could not find config for dataset=${rawDatasetRef.dataset} in configs " +
          s"`dataset-configs` and `inline-dataset-configs`. Please check filodb config = ${filodbConfig.toString}" +
          s" default-value=$defaultNumShardsPerNode")
        defaultNumShardsPerNode
    }
  }

  /**
   * Determines if cardinality should be triggered at current hour of the day.
   * For Example: With shardsPerNode = 8, the shard with number 0, should trigger cardinality when currentHour = 0,8,16
   *
   * NOTE: WHY are we using such criteria to trigger the cardinality count? -> This is done to stagger/distribute the
   * calculation cost of cardinality count of different shards residing in the same node at different times of day.
   * since cardinality count in downsample cluster is essentially a rebuild from scratch, which takes few mins. We don't
   * want all the shards to be calculating this at the same time and using up all the resources of the node.
   *
   * @param currentShardNum Shard Number (0 - Num-Shards)
   * @param shardsPerNode   Number of Shards per node
   * @param currentHour     Current hour of the day (0-23)
   * @return True if cardinality count is to be triggered, false otherwise
   */
  def shouldTriggerCardinalityCount(currentShardNum: Int, shardsPerNode: Int, currentHour: Int): Boolean = {
    // check based on current time if this is the hour for current shard's cardinality count
    require(shardsPerNode > 0)
    (currentHour % shardsPerNode) == (currentShardNum % shardsPerNode)
  }

  /**
   * Triggers cardinalityCount if metering is enabled and the required criteria matches.
   * It creates a new instance of CardinalityTracker and uses the PartKeyLuceneIndex to calculate cardinality count
   * and store data in a local CardinalityStore. We then close the previous instance of CardinalityTracker and switch
   * it with the new one we created in this call.
   */
  def triggerCardinalityCount(): Unit = {
    if (downsampleStoreConfig.meteringEnabled) {
      try {
        val currentHour = ((System.currentTimeMillis() / 1000 / 60 / 60) % 24).toInt
        if (shouldTriggerCardinalityCount(shardNum, numShardsPerNode, currentHour)) {
          // get a new cardinality tracker object and re-calculate the cardinality using this
          val newCardTracker = getNewCardTracker()
          partKeyIndex.calculateCardinality(partSchema, newCardTracker)
          // close the cardinality store and release/delete the physical resources of the current cardinality store
          cardTracker.close()
          cardTracker = newCardTracker
          logger.info(s"Triggered cardinality count successfully for shardNum=$shardNum and hour=$currentHour")
        }
      }
      catch {
        case e: Exception =>
          logger.error(s"Error while triggering cardinality count! shardNum = $shardNum", e)
      }
    }
  }

  /**
   * Initializes the cardinality tracker for DownsampledTimeSeriesShard
   *
   * @return instance of CardinalityTracker or a ZeroPointer (if metering not enabled)
   */
  private def initCardTracker() = {
    logger.info(s"Downsample Cardinality enabled flag=${downsampleStoreConfig.meteringEnabled.toString}")
    if (downsampleStoreConfig.meteringEnabled) {
      getNewCardTracker()
    } else UnsafeUtils.ZeroPointer.asInstanceOf[CardinalityTracker]
  }

  /**
   * Helper method to create a CardinalityTracker instance
   *
   * @return instance of CardinalityTracker
   */
  private def getNewCardTracker(): CardinalityTracker = {
    val cardStore = new RocksDbCardinalityStore(rawDatasetRef, shardNum)
    val defaultQuota = quotaSource.getDefaults(rawDatasetRef)
    val tracker = new CardinalityTracker(rawDatasetRef, shardNum, shardKeyLen, defaultQuota, cardStore)
    quotaSource.getQuotas(rawDatasetRef).foreach { q =>
      tracker.setQuota(q.shardKeyPrefix, q.quota)
    }
    tracker
  }

  /**
   * Closes the cardinality tracker and clean/release all the physical resources used
   */
  def close(): Unit = {
    cardTracker.close()
  }
}
