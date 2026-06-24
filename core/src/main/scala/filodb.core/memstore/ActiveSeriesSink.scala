package filodb.core.memstore

/**
 * Sink for active-series state transitions in a TimeSeriesShard.
 *
 * Called from the four sites where activeTsCount changes:
 *   - new series admitted at ingest
 *   - reactivation of an evicted/idle series on next ingest
 *   - retirement of an idle series at flush time
 *   - recovery of an actively-ingesting series at shard restart
 *
 * Implementations must be thread-safe and must not throw — exceptions thrown
 * from these methods are caught and logged by callers, but should not occur
 * in steady state.
 *
 * shardKeyValues is the same Seq[String] already computed for cardinality
 * tracking; by convention index 0 is workspace and index 1 is namespace.
 */
trait ActiveSeriesSink {
  def onActivate(shardKeyValues: Seq[String], partKeyBytes: Array[Byte]): Unit
  def onDeactivate(shardKeyValues: Seq[String], partKeyBytes: Array[Byte]): Unit
  def close(): Unit
}

object NoOpActiveSeriesSink extends ActiveSeriesSink {
  override def onActivate(shardKeyValues: Seq[String], partKeyBytes: Array[Byte]): Unit = ()
  override def onDeactivate(shardKeyValues: Seq[String], partKeyBytes: Array[Byte]): Unit = ()
  override def close(): Unit = ()
}
