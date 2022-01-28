package filodb.core.memstore.ratelimit

/**
 * The data stored in each node of the Cardinality Store trie
 *
 * @param shard the shard ID for which this CardinalityStore contains data
 * @param prefix the shard key prefix that corresponds with the cardinality data
 * @param tsCount total number of timeSeries under this shardKeyPrefix (example, number of timeseries under ws,ns)
 * @param activeTsCount number of actively ingesting timeSeries under this shardKeyPrefix
 *                      (example, number of timeseries under ws,ns)
 * @param childrenCount number of immediate children for this shardKey (example, number of ns under ws)
 * @param childrenQuota quota for number of immediate children
 */
case class CardinalityRecord(shard: Int, prefix: Seq[String], tsCount: Int,
                             activeTsCount: Int, childrenCount: Int, childrenQuota: Int)

/**
 *
 * Abstracts storage of cardinality for each shard prefix.
 *
 * Data model needs to represent a trie like data structure.
 * For the Ws/Ns/Name shard key example, here is the trie structure:
 *
 * <pre>
 * Root
 * * myWs1
 * ** myNs11
 * *** myMetric111
 * *** myMetric112
 * ** myNs12
 * *** myMetric121
 * *** myMetric122
 * * myWs2
 * ** myNs21
 * *** myMetric211
 * *** myMetric212
 * ** myNs22
 * *** myMetric221
 * *** myMetric222
 * </pre>
 *
 * The root to leaf path forms a full shard key. At each level in that path, we store
 * the cardinality under that shard key prefix.
 *
 * The store also needs to be able to fetch immediate children of any node quickly.
 * There can potentially be numerous nodes in the trie, so exhaustive tree-wide searches would
 * be inefficient. So it is important that the store chosen is an implementation of some kind of tree
 * like data structure and provides prefix search capability.
 *
 * Amount of memory used should be configurable. So it does not affect rest of the system.
 *
 * Implementations need to be local, fast and should not involve network I/O.
 */
trait CardinalityStore {

  /**
   * This method will be called for each shard key prefix when a new time series is added
   * to the index.
   */
  def store(card: CardinalityRecord): Unit

  /**
   * Read existing cardinality value, if one does not exist return the zero value
   * indicated
   */
  def getOrZero(shardKeyPrefix: Seq[String], zero: CardinalityRecord): CardinalityRecord

  /**
   * Remove entry from store. Need to call for each shard key prefix to fully remove shard key.
   * Called when a time series is purged from the index.
   */
  def remove(shardKeyPrefix: Seq[String]): Unit

  /**
   * Fetch children of the node for the given shard key prefix.
   * Result size is limited to MAX_RESULT_SIZE. If more children exist,
   *   their counts summed into one additional CardinalityRecord with
   *   prefix OVERFLOW_PREFIX.
   *
   * @param depth: only children of this size will be scanned.
   */
  def scanChildren(shardKeyPrefix: Seq[String], depth: Int): Seq[CardinalityRecord]

  /**
   * Close store. Data will be thrown away
   */
  def close(): Unit
}

object CardinalityStore {
  // See scanChildren doc for details.
  val MAX_RESULT_SIZE = 5000
  val OVERFLOW_PREFIX = Seq("_overflow_")
}