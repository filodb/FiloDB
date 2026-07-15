package filodb.core.memstore

import filodb.core.memstore.ratelimit.CardinalityRecord

/**
 * Sink for periodic cardinality snapshots from a TimeSeriesShard.
 *
 * The shard owns the schedule, the CardinalityTracker scans, and the state
 * that tracks which (ws, ns) pairs were written last cycle. The sink is
 * stateless with respect to the shard: every `publish` is a full overwrite
 * intent for the passed records, and every `evict` is a full removal intent.
 *
 * Implementations MAY assume single-threaded invocation per instance
 * (the driver constructs one sink per shard) and MUST NOT throw except
 * for programmer-error guards like invalid input — exceptions
 * are caught and logged by the caller, but should not occur in steady state.
 */
trait CardinalitySnapshotSink {

  /**
   * Publish this shard's current cardinality view.
   *
   * @param partition FiloDB deployment-partition name (e.g. "tsdb3")
   * @param shardNum  shard number within this partition
   * @param ns        depth-2 records: one per (ws, ns) this shard has data for
   * @param perMetric retained for interface stability but unused by the namespace-only sink
   *                  (the driver passes `Map.empty`)
   */
  def publish(partition: String, shardNum: Int,
              ns: Seq[CardinalityRecord],
              perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit

  /**
   * Remove this shard's contribution for namespaces that were written in
   * a prior cycle but are no longer present in this shard's tracker.
   */
  def evict(partition: String, shardNum: Int, stale: Set[(String, String)]): Unit

  def close(): Unit
}

object NoOpCardinalitySnapshotSink extends CardinalitySnapshotSink {
  override def publish(partition: String, shardNum: Int,
                       ns: Seq[CardinalityRecord],
                       perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit = ()
  override def evict(partition: String, shardNum: Int, stale: Set[(String, String)]): Unit = ()
  override def close(): Unit = ()
}
