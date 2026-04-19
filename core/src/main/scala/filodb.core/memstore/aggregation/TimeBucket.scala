package filodb.core.memstore.aggregation

/**
 * Represents a time bucket that aggregates samples within a specific time interval.
 * Each bucket has a timestamp representing the bucket boundary (ceiling of the interval).
 *
 * Note: This case class is test infrastructure — it is not used in the production ingestion path.
 * Production code uses only TimeBucket.ceilToBucket() from the companion object below.
 * The case class and its methods (aggregate, emit, canAccept, etc.) are exercised only in TimeBucketSpec.
 *
 * @param bucketTimestamp the timestamp representing the bucket boundary (end of interval)
 * @param aggregators array of aggregators, one per column being aggregated
 * @param sampleCount number of samples added to this bucket
 */
case class TimeBucket(
  bucketTimestamp: Long,
  aggregators: Array[Aggregator],
  var sampleCount: Int = 0
) {
  /**
   * Aggregates column values into this bucket.
   * @param columnValues array of values to aggregate (must match aggregators length)
   */
  def aggregate(columnValues: Array[Any]): Unit = {
    require(columnValues.length == aggregators.length,
      s"Column count mismatch: ${columnValues.length} values vs ${aggregators.length} aggregators")

    var i = 0
    while (i < aggregators.length) {
      aggregators(i).add(columnValues(i))
      i += 1
    }
    sampleCount += 1
  }

  /**
   * Aggregates column values with timestamp for time-sensitive aggregations.
   * @param columnValues array of values to aggregate
   * @param timestamp timestamp of the sample
   */
  def aggregateWithTimestamp(columnValues: Array[Any], timestamp: Long): Unit = {
    require(columnValues.length == aggregators.length,
      s"Column count mismatch: ${columnValues.length} values vs ${aggregators.length} aggregators")

    var i = 0
    while (i < aggregators.length) {
      aggregators(i).addWithTimestamp(columnValues(i), timestamp)
      i += 1
    }
    sampleCount += 1
  }

  /**
   * Returns the aggregated results from all aggregators.
   * @return array of aggregated values, one per column
   */
  def emit(): Array[Any] = {
    val results = new Array[Any](aggregators.length)
    var i = 0
    while (i < aggregators.length) {
      results(i) = aggregators(i).result()
      i += 1
    }
    results
  }

  /**
   * Checks if this bucket can accept a sample with the given timestamp.
   * A sample is acceptable if it falls within the bucket's acceptance window:
   * [bucketStart - tolerance, bucketEnd + tolerance]
   *
   * @param ts the timestamp to check
   * @param intervalMs the bucket interval in milliseconds
   * @param toleranceMs the out-of-order tolerance in milliseconds
   * @return true if the sample can be accepted, false otherwise
   */
  def canAccept(ts: Long, intervalMs: Long, toleranceMs: Long): Boolean = {
    val bucketStart = bucketTimestamp - intervalMs
    val bucketEnd = bucketTimestamp
    val windowStart = bucketStart - toleranceMs
    val windowEnd = bucketEnd + toleranceMs

    ts >= windowStart && ts <= windowEnd
  }

  /**
   * Resets all aggregators in this bucket to initial state.
   */
  def reset(): Unit = {
    aggregators.foreach(_.reset())
    sampleCount = 0
  }

  /**
   * Checks if this bucket is empty (no samples added).
   */
  def isEmpty: Boolean = sampleCount == 0

  override def toString: String =
    s"TimeBucket(ts=$bucketTimestamp, samples=$sampleCount, aggregators=${aggregators.length})"
}

object TimeBucket {
  // --- Production method (used by BucketAggregationState, AggregatingTimeSeriesPartition) ---
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

  // --- Test-only utilities below ---
  /**
   * Floors a timestamp to the previous bucket boundary.
   * Useful for calculating bucket start times.
   *
   * @param ts the timestamp to bucket
   * @param intervalMs the bucket interval in milliseconds
   * @return the bucket start timestamp (floor boundary)
   */
  def floorToBucket(ts: Long, intervalMs: Long): Long = {
    (ts / intervalMs) * intervalMs
  }

  /**
   * Creates a new TimeBucket with fresh aggregators copied from templates.
   * @param bucketTs the bucket timestamp
   * @param aggregatorTemplates array of aggregator templates to copy
   * @return a new TimeBucket instance
   */
  def create(bucketTs: Long, aggregatorTemplates: Array[Aggregator]): TimeBucket = {
    val aggregators = aggregatorTemplates.map(_.copy())
    TimeBucket(bucketTs, aggregators, 0)
  }

  /**
   * Calculates the bucket start time given a bucket timestamp and interval.
   * @param bucketTimestamp the bucket end timestamp
   * @param intervalMs the bucket interval
   * @return the bucket start timestamp
   */
  def bucketStart(bucketTimestamp: Long, intervalMs: Long): Long = {
    bucketTimestamp - intervalMs
  }

  /**
   * Determines if two timestamps belong to the same bucket.
   * @param ts1 first timestamp
   * @param ts2 second timestamp
   * @param intervalMs bucket interval
   * @return true if both timestamps ceil to the same bucket
   */
  def sameBucket(ts1: Long, ts2: Long, intervalMs: Long): Boolean = {
    ceilToBucket(ts1, intervalMs) == ceilToBucket(ts2, intervalMs)
  }
}
