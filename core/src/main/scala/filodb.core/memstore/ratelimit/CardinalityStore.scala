package filodb.core.memstore.ratelimit

case class Cardinality(name: String, timeSeriesCount: Int, childrenCount: Int, childrenQuota: Int)

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
  def store(shardKeyPrefix: Seq[String], card: Cardinality): Unit

  /**
   * Read existing cardinality value, if one does not exist return the zero value
   * indicated
   */
  def getOrZero(shardKeyPrefix: Seq[String], zero: Cardinality): Cardinality

  /**
   * Remove entry from store. Need to call for each shard key prefix to fully remove shard key.
   * Called when a time series is purged from the index.
   */
  def remove(shardKeyPrefix: Seq[String]): Unit

  /**
   * Fetch immediate children of the node for the given shard key prefix
   */
  def scanChildren(shardKeyPrefix: Seq[String]): Seq[Cardinality]

  /**
   * Close store. Data will be thrown away
   */
  def close(): Unit
}
