package filodb.core.memstore.aggregation

object TimeBucket {
  /**
   * Ceils a timestamp to the next bucket boundary.
   * This is the core time bucketing logic that determines which bucket a sample belongs to.
   *
   * Example with 30-second buckets:
   * - ceilToBucket(12:00:05, 30000) => 12:00:30
   * - ceilToBucket(12:00:25, 30000) => 12:00:30
   * - ceilToBucket(12:00:31, 30000) => 12:01:00
   *
   * @param ts the timestamp to bucket
   * @param intervalMs the bucket interval in milliseconds
   * @return the bucket timestamp (ceiling boundary)
   */
  def ceilToBucket(ts: Long, intervalMs: Long): Long = {
    ((ts + intervalMs - 1) / intervalMs) * intervalMs
  }
}
