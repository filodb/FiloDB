package filodb.core.memstore.ratelimit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
  */
class CardinalityTracker(ref: DatasetRef,
                         shard: Int,
                         shardKeyLen: Int,
                         defaultChildrenQuota: Seq[Int],
                         val store: CardinalityStore,
                         quotaExceededProtocol: QuotaExceededProtocol = NoActionQuotaProtocol) extends StrictLogging {

  require(defaultChildrenQuota.length == shardKeyLen + 1)
  require(defaultChildrenQuota.forall(q => q > 0))
  logger.info(s"Initializing Cardinality Tracker for shard $shard with $defaultChildrenQuota")

  // separates ws, ns, metric, etc. names
  val NAME_DELIMITER = ","

  /**
   * Call when a new time series with the given shard key has been added to the system.
   * This will update the cardinality at each level within the trie. If quota is breached,
   * QuotaReachedException will be thrown and quotaExceededProtocol will be invoked
   *
   * @param shardKey elements in the shard key of time series. For example: (ws, ns, name). Full shard key needed.
   * @return current cardinality for each shard key prefix. There
   *         will be shardKeyLen + 1 items in the return value
   */
  def modifyCount(shardKey: Seq[String], totalDelta: Int, activeDelta: Int): Seq[CardinalityRecord] = {
    require(shardKey.length == shardKeyLen, "full shard key is needed")
    require(totalDelta == 1 && activeDelta == 0 ||   // new ts but inactive
            totalDelta == 1 && activeDelta == 1 ||   // new ts and active
            totalDelta == 0 && activeDelta == 1 ||   // // existing inactive ts that became active
            totalDelta == 0 && activeDelta == -1,    // existing active ts that became inactive
            "invalid values for totalDelta / activeDelta")

    val toStore = ArrayBuffer[(Seq[String], CardinalityRecord)]()
    // first make sure there is no breach for any prefix
    (0 to shardKey.length).foreach { i =>
      val prefix = shardKey.take(i)
      val old = store.getOrZero(prefix, CardinalityRecord(shard, prefix, 0, 0, 0, defaultChildrenQuota(i)))
      val neu = old.copy(tsCount = old.tsCount + totalDelta,
                         activeTsCount = old.activeTsCount + activeDelta,
                         childrenCount = if (i == shardKeyLen) old.childrenCount + totalDelta else old.childrenCount)
      if (i == shardKeyLen && neu.tsCount > neu.childrenQuota) {
        quotaExceededProtocol.quotaExceeded(ref, shard, prefix, neu.childrenQuota)
        throw QuotaReachedException(shardKey, prefix, neu.childrenQuota)
      }
      if (i > 0 && neu.tsCount == 1 && totalDelta == 1) { // parent's new child
        val parent = toStore(i-1)
        val neuParent = parent._2.copy(childrenCount = parent._2.childrenCount + 1)
        toStore(i-1) = (parent._1, neuParent)
        if (neuParent.childrenCount > neuParent.childrenQuota) {
          quotaExceededProtocol.quotaExceeded(ref, shard, parent._1, neuParent.childrenQuota)
          throw QuotaReachedException(shardKey, parent._1, neuParent.childrenQuota)
        }
      }
      toStore += (prefix -> neu)
    }

    toStore.map { case (prefix, neu) =>
      store.store(neu)
      neu
    }
  }



  /**
   * Fetch cardinality for given shard key or shard key prefix
   *
   * @param shardKeyPrefix zero or more elements that form a valid shard key prefix
   */
  def getCardinality(shardKeyPrefix: Seq[String]): CardinalityRecord = {
    require(shardKeyPrefix.length <= shardKeyLen, s"Too many shard keys in $shardKeyPrefix - max $shardKeyLen")
    store.getOrZero(
      shardKeyPrefix,
      CardinalityRecord(shard, shardKeyPrefix, 0, 0, 0, defaultChildrenQuota(shardKeyPrefix.length)))
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
      CardinalityRecord(shard, shardKeyPrefix, 0, 0, 0, defaultChildrenQuota(shardKeyPrefix.length)))
    val neu = old.copy(childrenQuota = childrenQuota)
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
  def decrementCount(shardKey: Seq[String]): Seq[CardinalityRecord] = {
    try {
      require(shardKey.length == shardKeyLen, "full shard key is needed")
      val toStore = (0 to shardKey.length).map { i =>
        val prefix = shardKey.take(i)
        val old = store.getOrZero(prefix, CardinalityRecord(shard, Nil, 0, 0, 0, defaultChildrenQuota(i)))
        if (old.tsCount == 0)
          throw new IllegalArgumentException(s"$prefix count is already zero - cannot reduce " +
            s"further. A double delete likely happened.")
        val neu = old.copy(tsCount = old.tsCount - 1,
                          childrenCount = if (i == shardKeyLen) old.childrenCount-1 else old.childrenCount)
        (prefix, neu)
      }
      toStore.map { case (prefix, neu) =>
        if (neu == CardinalityRecord(shard, prefix, 0, 0, 0, defaultChildrenQuota(prefix.length))) {
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
   * Use this method to query the top-k cardinality consumers
   *   under a provided shard key prefix.
   *
   * @param depth cardinalities are returned for all prefixes of this size
   * @param shardKeyPrefix zero or more elements that form a valid shard key prefix
   * @return Top-K records, can the less than K if fewer children
   */
  def topk(k: Int, shardKeyPrefix: Seq[String], depth: Int, addInactive: Boolean): Seq[CardinalityRecord] = {
    require(shardKeyPrefix.length <= shardKeyLen, s"Too many shard keys in $shardKeyPrefix - max $shardKeyLen")
    require(depth >= shardKeyPrefix.size,
      s"depth $depth must be at least the size of the prefix ${shardKeyPrefix.size}")

    implicit val ord = new Ordering[CardinalityRecord]() {
      override def compare(x: CardinalityRecord, y: CardinalityRecord): Int = {
        if (addInactive) x.tsCount - y.tsCount
        else x.activeTsCount - y.activeTsCount
      }
    }.reverse

    if (shardKeyPrefix.size == depth) {
      // directly fetch the cardinality and return
      return Seq(getCardinality(shardKeyPrefix))
      // TODO(a_theimer): need to keep this?
//      return Seq(CardinalityRecord(
//        shard, card.prefix, card.tsCount, card.activeTsCount,
//        if (shardKeyPrefix.length == shardKeyLen - 1) card.tsCount else card.childrenCount,
//        card.childrenQuota))
    }

    val heap = mutable.PriorityQueue[CardinalityRecord]()
    val it = store.scanChildren(shardKeyPrefix, depth)
    try {
      it.foreach { card =>
        heap.enqueue(CardinalityRecord(
          shard, card.prefix, card.tsCount,
          card.activeTsCount,
          if (shardKeyPrefix.length == shardKeyLen - 1) card.tsCount else card.childrenCount,
          card.childrenQuota))
        if (heap.size > k) heap.dequeue()
      }
    } finally {
      it.close()
    }
    heap.toSeq
  }

  def close(): Unit = {
    store.close()
  }
}
