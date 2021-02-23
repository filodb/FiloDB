package filodb.core.memstore.ratelimit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef

case class QuotaReachedException(cannotSetShardKey: Seq[String], prefix: Seq[String], quota: Int)
  extends RuntimeException

case class CardinalityRecord(shard: Int, childName: String, timeSeriesCount: Int,
                             childrenCount: Int, childrenQuota: Int)

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

  /**
   * Call when a new time series with the given shard key has been added to the system.
   * This will update the cardinality at each level within the trie. If quota is breached,
   * QuotaReachedException will be thrown and quotaExceededProtocol will be invoked
   *
   * @param shardKey elements in the shard key of time series. For example: (ws, ns, name). Full shard key needed.
   * @return current cardinality for each shard key prefix. There
   *         will be shardKeyLen + 1 items in the return value
   */
  def incrementCount(shardKey: Seq[String]): Seq[Cardinality] = {
    require(shardKey.length == shardKeyLen, "full shard key is needed")

    val toStore = ArrayBuffer[(Seq[String], Cardinality)]()
    // first make sure there is no breach for any prefix
    (0 to shardKey.length).foreach { i =>
      val prefix = shardKey.take(i)
      val name = if (prefix.isEmpty) "" else prefix.last
      val old = store.getOrZero(prefix, Cardinality(name, 0, 0, defaultChildrenQuota(i)))
      val neu = old.copy(timeSeriesCount = old.timeSeriesCount + 1,
                         childrenCount = if (i == shardKeyLen) old.childrenCount + 1 else old.childrenCount)
      if (i == shardKeyLen && neu.timeSeriesCount > neu.childrenQuota) {
        quotaExceededProtocol.quotaExceeded(ref, shard, prefix, neu.childrenQuota)
        throw QuotaReachedException(shardKey, prefix, neu.childrenQuota)
      }
      if (i > 0 && neu.timeSeriesCount == 1) { // parent's new child
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
      store.store(prefix, neu)
      neu
    }
  }

  /**
   * Fetch cardinality for given shard key or shard key prefix
   *
   * @param shardKeyPrefix zero or more elements that form a valid shard key prefix
   */
  def getCardinality(shardKeyPrefix: Seq[String]): Cardinality = {
    require(shardKeyPrefix.length <= shardKeyLen, s"Too many shard keys in $shardKeyPrefix - max $shardKeyLen")
    val name = if (shardKeyPrefix.isEmpty) "" else shardKeyPrefix.last
    store.getOrZero(shardKeyPrefix, Cardinality(name, 0, 0, defaultChildrenQuota(shardKeyPrefix.length)))
  }

  /**
   * Set quota for given shard key or shard key prefix
   *
   * @param shardKeyPrefix zero or more elements that form a valid shard key prefix
   * @param childrenQuota maximum number of time series for this prefix
   * @return current Cardinality for the prefix
   */
  def setQuota(shardKeyPrefix: Seq[String], childrenQuota: Int): Cardinality = {
    require(shardKeyPrefix.length <= shardKeyLen, s"Too many shard keys in $shardKeyPrefix - max $shardKeyLen")
    require(childrenQuota > 0 && childrenQuota < 2000000, "Children quota invalid. Provide [1, 2000000)")

    logger.debug(s"Setting children quota for $shardKeyPrefix as $childrenQuota")
    val name = if (shardKeyPrefix.isEmpty) "" else shardKeyPrefix.last
    val old = store.getOrZero(shardKeyPrefix, Cardinality(name, 0, 0, defaultChildrenQuota(shardKeyPrefix.length)))
    val neu = old.copy(childrenQuota = childrenQuota)
    store.store(shardKeyPrefix, neu)
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
  def decrementCount(shardKey: Seq[String]): Seq[Cardinality] = {
    require(shardKey.length == shardKeyLen, "full shard key is needed")
    val toStore = (0 to shardKey.length).map { i =>
      val prefix = shardKey.take(i)
      val old = store.getOrZero(prefix, Cardinality("", 0, 0, defaultChildrenQuota(i)))
      if (old.timeSeriesCount == 0)
        throw new IllegalArgumentException(s"$prefix count is already zero - cannot reduce further")
      val neu = old.copy(timeSeriesCount = old.timeSeriesCount - 1)
      (prefix, neu)
    }
    toStore.map { case (prefix, neu) =>
      val name = if (prefix.isEmpty) "" else prefix.last
      if (neu == Cardinality(name, 0, 0, defaultChildrenQuota(prefix.length))) {
        // node can be removed
        store.remove(prefix)
      } else {
        store.store(prefix, neu)
      }
      neu
    }
  }

  /**
   * Use this method to query the top-k cardinality consumers immediately
   * under a provided shard key prefix
   *
   * @param k
   * @param shardKeyPrefix zero or more elements that form a valid shard key prefix
   * @return Top-K records, can the less than K if fewer children
   */
  def topk(k: Int, shardKeyPrefix: Seq[String]): Seq[CardinalityRecord] = {
    require(shardKeyPrefix.length <= shardKeyLen, s"Too many shard keys in $shardKeyPrefix - max $shardKeyLen")
    implicit val ord = new Ordering[CardinalityRecord]() {
      override def compare(x: CardinalityRecord, y: CardinalityRecord): Int = {
        x.timeSeriesCount - y.timeSeriesCount
      }
    }.reverse
    val heap = mutable.PriorityQueue[CardinalityRecord]()
    store.scanChildren(shardKeyPrefix).foreach { card =>
      heap.enqueue(
        CardinalityRecord(shard, card.name,
                 card.timeSeriesCount,
                 if (shardKeyPrefix.length == shardKeyLen - 1) card.timeSeriesCount else card.childrenCount,
                 card.childrenQuota))
      if (heap.size > k) heap.dequeue()
    }
    heap.toSeq
  }

  def close(): Unit = {
    store.close()
  }
}
