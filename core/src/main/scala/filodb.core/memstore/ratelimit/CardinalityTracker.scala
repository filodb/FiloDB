package filodb.core.memstore.ratelimit

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef

case class QuotaReachedException(cannotSetShardKey: Seq[String], prefix: Seq[String], quota: Int)
  extends RuntimeException

/**
  * Tracks cardinality (number of time series) under each shard key prefix. The shard key
  * essentially comprises of the part key key/value pairs that determine which shard a time
  * series goes into.
  *
  * For example if shard Key is Seq("myWs", "myNs", "myMetric") then cardinality of each prefix
  * Seq(), Seq("myWs"), Seq("myWs, "myNs"), Seq("myWs", "myNs", "myMetric") would be tracked.
  *
  * Thus, the cardinality store can be represented as a Trie of shard key elements. For the above example,
  * the trie would have 4 levels. Cardinality is tracked at each node of the trie. Both count of number
  * of immediate children as well as number of time series under that level are tracked.
  *
  * This Cardinality Tracker can also enforce quotas. Quotas are set by invoking the setQuota method.
  * While this tracker tracks both immediate children as well as total number of time series under each node,
  * quota enforcement is for immediate children only.
  *
  * @param ref the dataset for which we track the cardinality
  * @param shard the shard number
  * @param shardKeyLen number of elements in the shard key
  * @param defaultChildrenQuota the default quota at each level if no explicit quota is set
  * @param store fast memory or disk based store where cardinality and quota can be read and written
  * @param quotaExceededProtocol action to be taken when quota is breached
  * @param flushCount threshold to flush the cardinality count records to rocksDB. This is also used to toggle between
  *    the aggregated (agg using in-memory map. caller has to call function `flushCardinalityCount` to ensure write) vs.
  *    non-aggregated way (calling rocksDB.store() after update) of storing cardinality count
  */
class CardinalityTracker(ref: DatasetRef,
                         shard: Int,
                         shardKeyLen: Int,
                         defaultChildrenQuota: Seq[Int],
                         val store: CardinalityStore,
                         quotaExceededProtocol: QuotaExceededProtocol = NoActionQuotaProtocol,
                         flushCount: Option[Int] = None) extends StrictLogging {

  require(defaultChildrenQuota.length == shardKeyLen + 1)
  require(defaultChildrenQuota.forall(q => q > 0))
  logger.info(s"Initializing Cardinality Tracker for shard $shard with $defaultChildrenQuota")

  // separates ws, ns, metric, etc. names
  val NAME_DELIMITER = ","

  /**
   * Map used to track cardinality count in downsample cluster.
   * WHY this is used for Downsample cardinality count only ?
   * This is because, in downsample cluster, we read through the entire index periodically and re-calculate
   * the cardinality count from scratch. Without an in-memory aggregation data-structure, we would be modifying
   * the records in RocksDB too frequently, causing a slowdown because of heavy disk writes.
   * Hence we are using this map to help us aggregate in memory and then flush to RocksDB periodically
   */
  private val cardinalityCountMap : Map[Seq[String], (Int, Int)] = Map()

  /**
   * Call when a new time series with the given shard key has been added to the system.
   * This will update the cardinality at each level within the trie. If quota is breached,
   * QuotaReachedException will be thrown and quotaExceededProtocol will be invoked
   *
   * @param shardKey elements in the shard key of time series. For example: (ws, ns, name). Full shard key needed.
   * @return current cardinality for each shard key prefix. There
   *         will be shardKeyLen + 1 items in the return value
   */
  def modifyCount(shardKey: Seq[String], totalDelta: Int, activeDelta: Int): Seq[CardinalityRecord] = synchronized {

    // note this method is synchronized since the read-modify-write pattern that happens here is not thread-safe
    // modifyCount and decrementCount methods are protected this way

    require(shardKey.length == shardKeyLen, "full shard key is needed")
    require(totalDelta == 1 && activeDelta == 0 || // new ts but inactive
            totalDelta == 1 && activeDelta == 1 || // new ts and active
            totalDelta == 0 && activeDelta == 1 ||   // // existing inactive ts that became active
            totalDelta == 0 && activeDelta == -1, // existing active ts that became inactive
            "invalid values for totalDelta / activeDelta")

    val cardinalityRecords = flushCount match {
      case Some(threshold) => modifyCountWithAggregation(shardKey, threshold, totalDelta)
      case None => {
        val toStore = ArrayBuffer[CardinalityRecord]()
        // first make sure there is no breach for any prefix
        (0 to shardKey.length).foreach { i =>
          val prefix = shardKey.take(i)
          val old = store.getOrZero(prefix,
            CardinalityRecord(shard, prefix, CardinalityValue(0, 0, 0, defaultChildrenQuota(i))))

          val neu = old.copy(value = old.value.copy(tsCount = old.value.tsCount + totalDelta,
            activeTsCount = old.value.activeTsCount + activeDelta,
            childrenCount = if (i == shardKeyLen) old.value.childrenCount + totalDelta else old.value.childrenCount))

          if (i == shardKeyLen && neu.value.tsCount > neu.value.childrenQuota) {
            quotaExceededProtocol.quotaExceeded(ref, shard, prefix, neu.value.childrenQuota)
            throw QuotaReachedException(shardKey, prefix, neu.value.childrenQuota)
          }

          // Updating children count of the parent prefix, when a new child is added
          if (i > 0 && neu.value.tsCount == 1 && totalDelta == 1) { // parent's new child
            val parent = toStore(i - 1)
            val neuParent = parent.copy(value = parent.value.copy(childrenCount = parent.value.childrenCount + 1))
            toStore(i - 1) = neuParent
            if (neuParent.value.childrenCount > neuParent.value.childrenQuota) {
              quotaExceededProtocol.quotaExceeded(ref, shard, parent.prefix, neuParent.value.childrenQuota)
              throw QuotaReachedException(shardKey, parent.prefix, neuParent.value.childrenQuota)
            }
          }
          toStore += neu
        }

        toStore.map { case neu =>
          store.store(neu)
          neu
        }
      }
    }
    cardinalityRecords
  }

  /**
   * Updates the DOWNSAMPLE CLUSTER's cardinality count in the cardinalityCountMap. Flushes the data to RocksDB
   * when `dsCardinalityMapFlushCount` threshold reached
   *
   *  NOTE: We are only cardinality count for total TS in aggregated fashion. We will add support for active TS if
   *  needed
   *
   * Cardinality count at each level of shardKey needs to be updated
   * For example: if shardKey = (my_ws, my_ns, my_metric), then we have to update
   * the cardinality count of 4 prefixes. They are -
   * 1. (total across all ws)
   * 2. (my_ws)
   * 3. (my_ws, my_ns)
   * 4. (my_ws, my_ns, my_metric)
   *
   * @param shardKey
   */
  private def modifyCountWithAggregation(shardKey: Seq[String], threshold: Int,
                                         totalDelta: Int): Seq[CardinalityRecord] = synchronized {
    (0 to shardKey.length).foreach { i =>
      // update current prefix's cardinality count
      val prefix = shardKey.take(i)
      val cardCountRecord = cardinalityCountMap.get(prefix)
        .map(x => (x._1 + totalDelta, x._2))
        .getOrElse((1, 0)) // child prefix update parent's childrenCount
      cardinalityCountMap.put(prefix, cardCountRecord)
      // update children count of parent and throw exception if quota reached
      if (i > 0) {
        val parentPrefix = shardKey.take(i - 1)

        // we always add parent before the child, hence it is okay to get the parent prefix's record directly
        // without the None check
        val updatedCountRecord = cardinalityCountMap.get(parentPrefix)
          .map(x => (x._1, x._2 + 1)).get

        // check if number of children is higher than the given quota. This allows us guard our physical resources
        // and avoid failures because of runaway cardinality
        val childrenQuota = defaultChildrenQuota(parentPrefix.length)
        if (updatedCountRecord._2 > childrenQuota) {
          quotaExceededProtocol.quotaExceeded(ref, shard, prefix, childrenQuota)
          throw QuotaReachedException(prefix, prefix, childrenQuota)
        }

        // store the updated parent's childrenCount
        cardinalityCountMap.put(parentPrefix, updatedCountRecord)
      }
    }
    if (cardinalityCountMap.size > threshold) {
      flushCardinalityCount()
    }
    // NOTE: We are not using the returned CardinalityRecord records when modifying count
    // using an aggregation map. We will update it when it is required but keeping things simple for now
    Seq()
  }

  /**
  * Flush the cardinality data to RocksDB before reading the counts. The downsample cardinality count is built from
  * scratch at a periodic interval and the caller of CardinalityTracker can also call this method to ensure all data
  * is flushed to RocksDB
  */
  def flushCardinalityCount(): Unit = {
    if (cardinalityCountMap.size > 0) {
      // iterate through map and store each prefix and count to the rocksDB
      cardinalityCountMap.foreach(kv => {
        storeCardinalityCountInRocksDB(kv._1, kv._2._1, kv._2._1, kv._2._2)
      })
      // clear the map
      cardinalityCountMap.clear()
    }
  }

  /**
   * Used to store the cardinality count for the given prefix in the downsample cluster.
   * NOTE:
   * 1. In downsample cluster, tsCount == activeTsCount. So totalDelta and activeDelta is same.
   * 2. The following function should only be called from `updateCardinalityCountsDS` and hence it is marked private.
   * @param prefix usually contains labels _ws_, _ns_, _metric_ and different combinations of it
   * @param totalDelta Increase in total timeseries
   * @param activeDelta Increase in active timeseries
   * @param childrenDelta Increase in children count
   */
  private def storeCardinalityCountInRocksDB(prefix: Seq[String],
                            totalDelta: Int, activeDelta: Int, childrenDelta: Int): Unit = {

    // get the current cardinality count from RocksDB for the given prefix. Also add a default if not present
    val old = store.getOrZero(prefix,
      CardinalityRecord(shard, prefix, CardinalityValue(0, 0, 0, defaultChildrenQuota(prefix.length))))

    // update the count with the provided delta values
    val neu = old.copy(value = old.value.copy(tsCount = old.value.tsCount + totalDelta,
      activeTsCount = old.value.activeTsCount + activeDelta,
      childrenCount = old.value.childrenCount + childrenDelta))

    store.store(neu)
  }

  /**
   * Fetch cardinality for given shard key or shard key prefix
   *
   * @param shardKeyPrefix zero or more elements that form a valid shard key prefix
   */
  private def getCardinality(shardKeyPrefix: Seq[String]): CardinalityRecord = {
    require(shardKeyPrefix.length <= shardKeyLen, s"Too many shard keys in $shardKeyPrefix - max $shardKeyLen")
    store.getOrZero(
      shardKeyPrefix,
      CardinalityRecord(shard, shardKeyPrefix, CardinalityValue(0, 0, 0, defaultChildrenQuota(shardKeyPrefix.length))))
  }

  /**
   * Set quota for given shard key or shard key prefix
   *
   * @param shardKeyPrefix zero or more elements that form a valid shard key prefix
   * @param childrenQuota maximum number of time series for this prefix
   * @return current CardinalityRecord for the prefix
   */
  def setQuota(shardKeyPrefix: Seq[String], childrenQuota: Int): CardinalityRecord = {
    require(shardKeyPrefix.length <= shardKeyLen, s"Too many shard keys in $shardKeyPrefix - max $shardKeyLen")
    require(childrenQuota > 0 && childrenQuota < 2000000, "Children quota invalid. Provide [1, 2000000)")

    logger.debug(s"Setting children quota for $shardKeyPrefix as $childrenQuota")
    val old = store.getOrZero(
      shardKeyPrefix,
      CardinalityRecord(shard, shardKeyPrefix, CardinalityValue(0, 0, 0, defaultChildrenQuota(shardKeyPrefix.length))))
    val neu = old.copy(value = old.value.copy(childrenQuota = childrenQuota))
    store.store(neu)
    neu
  }

  /**
   * Call when an existing time series with the given shard key has been removed from the system.
   * This will reduce the cardinality at each level within the trie.
   *
   * If cardinality reduces to 0, and the quota is the default quota, record will be removed from the store
   *
   * @param shardKey elements in the shard key of time series.
   *                 For example: (ws, ns, name). Full shard key is needed.
   * @return current cardinality for each shard key prefix. There
   *         will be shardKeyLen + 1 items in the return value
   */
  def decrementCount(shardKey: Seq[String]): Seq[CardinalityRecord] = synchronized {
    // note this method is synchronized since the read-modify-write pattern that happens here is not thread-safe
    // modifyCount and decrementCount methods are protected this way

    try {
      require(shardKey.length == shardKeyLen, "full shard key is needed")
      val toStore = (0 to shardKey.length).map { i =>
        val prefix = shardKey.take(i)
        val old = store.getOrZero(prefix,
          CardinalityRecord(shard, Nil, CardinalityValue(0, 0, 0, defaultChildrenQuota(i))))
        if (old.value.tsCount == 0)
          throw new IllegalArgumentException(s"$prefix count is already zero - cannot reduce " +
            s"further. A double delete likely happened.")
        val neu = old.copy(value = old.value.copy(tsCount = old.value.tsCount - 1,
                          childrenCount = if (i == shardKeyLen) old.value.childrenCount-1 else old.value.childrenCount))
        (prefix, neu)
      }
      toStore.map { case (prefix, neu) =>
        if (neu == CardinalityRecord(shard, prefix, CardinalityValue(0, 0, 0, defaultChildrenQuota(prefix.length)))) {
          // node can be removed
          store.remove(prefix)
        } else {
          store.store(neu)
        }
        neu
      }
    } catch { case e: Exception =>
      logger.error("Caught and swallowed this exception when reducing cardinality in tracker", e)
      Nil
    }
  }

  /**
   * Use this method to query cardinalities under a provided shard key prefix.
   * NOTE: All read calls must pass through this method
   *
   * @param depth cardinalities are returned for all prefixes of this size
   * @param shardKeyPrefix zero or more elements that form a valid shard key prefix
   */
  def scan(shardKeyPrefix: Seq[String], depth: Int): Seq[CardinalityRecord] = {
    require(shardKeyPrefix.length <= shardKeyLen, s"Too many shard keys in $shardKeyPrefix - max $shardKeyLen")
    require(depth >= shardKeyPrefix.size,
      s"depth $depth must be at least the size of the prefix ${shardKeyPrefix.size}")

    if (shardKeyPrefix.size == depth) {
      // directly fetch the single cardinality
      Seq(getCardinality(shardKeyPrefix))
    } else {
      store.scanChildren(shardKeyPrefix, depth)
    }
  }

  def close(): Unit = {
    store.close()

    // WHY are we not flushing before the close? This is because in our current implementation of
    // RocksDbCardinalityStore.close(), we delete the RocksDB itself. so to avoid any additional writes, we are just
    // clearing the map to clean the state
    cardinalityCountMap.clear()
  }

  /**
   * returns a clone of cardinalityCountMapDS map for testing purposes
   */
  def getCardinalityCountMapDSClone(): Map[Seq[String], (Int, Int)] = {
    cardinalityCountMap.clone()
  }
}
