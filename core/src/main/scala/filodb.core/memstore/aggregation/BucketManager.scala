package filodb.core.memstore.aggregation

import scala.collection.mutable

/**
 * Manages the lifecycle of active time buckets for a single aggregated column.
 * Implements bounded bucket retention to prevent unbounded memory growth.
 *
 * The BucketManager maintains a limited number of active buckets (typically 3):
 * - Current bucket: receiving new samples
 * - Previous bucket: accepting late-arriving samples within tolerance
 * - Transition bucket: handling edge cases during bucket boundaries
 *
 * When the limit is exceeded, the oldest bucket is automatically evicted.
 *
 * @param maxBuckets maximum number of active buckets to maintain
 * @param aggregatorTemplates array of aggregator templates used to create new buckets
 */
class BucketManager(
  val maxBuckets: Int = 3,
  aggregatorTemplates: Array[Aggregator]
) {
  require(maxBuckets > 0, s"maxBuckets must be positive, got $maxBuckets")
  require(aggregatorTemplates != null && aggregatorTemplates.nonEmpty,
    "aggregatorTemplates must be non-empty")

  private val activeBuckets = mutable.Map[Long, TimeBucket]()

  /**
   * Gets an existing bucket or creates a new one for the given bucket timestamp.
   * If creating a new bucket would exceed maxBuckets, the oldest bucket is evicted first.
   *
   * @param bucketTs the bucket timestamp
   * @return the TimeBucket for the given timestamp
   */
  def getOrCreateBucket(bucketTs: Long): TimeBucket = {
    activeBuckets.getOrElseUpdate(bucketTs, {
      evictIfNeeded()
      TimeBucket.create(bucketTs, aggregatorTemplates)
    })
  }

  /**
   * Gets an existing bucket if it exists.
   * @param bucketTs the bucket timestamp
   * @return Some(bucket) if exists, None otherwise
   */
  def getBucket(bucketTs: Long): Option[TimeBucket] = {
    activeBuckets.get(bucketTs)
  }

  /**
   * Checks if a bucket exists for the given timestamp.
   * @param bucketTs the bucket timestamp
   * @return true if the bucket exists
   */
  def hasBucket(bucketTs: Long): Boolean = activeBuckets.contains(bucketTs)

  /**
   * Evicts the oldest bucket if we've reached the maximum bucket count.
   * This ensures bounded memory usage regardless of out-of-order patterns.
   *
   * @return Some((timestamp, bucket)) if a bucket was evicted, None otherwise
   */
  private def evictIfNeeded(): Option[(Long, TimeBucket)] = {
    if (activeBuckets.size >= maxBuckets) {
      val oldestTs = activeBuckets.keys.min
      val bucket = activeBuckets.remove(oldestTs)
      bucket.map(b => (oldestTs, b))
    } else {
      None
    }
  }

  /**
   * Evicts all buckets older than the given threshold timestamp.
   * This is used to flush buckets that are beyond the tolerance window.
   *
   * @param thresholdTs buckets older than this timestamp will be evicted
   * @return sequence of (timestamp, bucket) pairs that were evicted, sorted by timestamp
   */
  def evictBucketsOlderThan(thresholdTs: Long): Seq[(Long, TimeBucket)] = {
    val toEvict = activeBuckets.filter { case (ts, _) => ts < thresholdTs }.toSeq.sortBy(_._1)
    toEvict.foreach { case (ts, _) => activeBuckets.remove(ts) }
    toEvict
  }

  /**
   * Evicts all buckets up to and including the given timestamp.
   * @param upToTs buckets with timestamp <= upToTs will be evicted
   * @return sequence of evicted (timestamp, bucket) pairs, sorted by timestamp
   */
  def evictBucketsUpTo(upToTs: Long): Seq[(Long, TimeBucket)] = {
    val toEvict = activeBuckets.filter { case (ts, _) => ts <= upToTs }.toSeq.sortBy(_._1)
    toEvict.foreach { case (ts, _) => activeBuckets.remove(ts) }
    toEvict
  }

  /**
   * Returns all active buckets sorted by timestamp.
   * @return sequence of (timestamp, bucket) pairs
   */
  def allBuckets: Seq[(Long, TimeBucket)] = activeBuckets.toSeq.sortBy(_._1)

  /**
   * Returns the oldest bucket timestamp if any buckets exist.
   * @return Some(timestamp) if buckets exist, None otherwise
   */
  def oldestBucketTimestamp: Option[Long] = {
    if (activeBuckets.nonEmpty) Some(activeBuckets.keys.min) else None
  }

  /**
   * Returns the newest bucket timestamp if any buckets exist.
   * @return Some(timestamp) if buckets exist, None otherwise
   */
  def newestBucketTimestamp: Option[Long] = {
    if (activeBuckets.nonEmpty) Some(activeBuckets.keys.max) else None
  }

  /**
   * Returns the number of active buckets.
   */
  def size: Int = activeBuckets.size

  /**
   * Checks if there are any active buckets.
   */
  def isEmpty: Boolean = activeBuckets.isEmpty

  /**
   * Clears all active buckets.
   * Used for testing or partition reset scenarios.
   */
  def clear(): Unit = activeBuckets.clear()

  /**
   * Returns a specific bucket by timestamp without creating it.
   * @param bucketTs the bucket timestamp
   * @return the bucket if it exists
   */
  def apply(bucketTs: Long): TimeBucket = {
    activeBuckets.getOrElse(bucketTs,
      throw new NoSuchElementException(s"No bucket found for timestamp $bucketTs"))
  }

  /**
   * Returns statistics about the bucket manager state.
   * Useful for metrics and debugging.
   */
  def stats: BucketManagerStats = {
    BucketManagerStats(
      activeBucketCount = activeBuckets.size,
      maxBuckets = maxBuckets,
      oldestBucket = oldestBucketTimestamp,
      newestBucket = newestBucketTimestamp,
      totalSamples = activeBuckets.values.map(_.sampleCount).sum
    )
  }

  override def toString: String = {
    val bucketInfo = if (activeBuckets.isEmpty) {
      "empty"
    } else {
      s"${activeBuckets.size} buckets (${oldestBucketTimestamp.get} to ${newestBucketTimestamp.get})"
    }
    s"BucketManager(max=$maxBuckets, $bucketInfo)"
  }
}

/**
 * Statistics snapshot for a BucketManager.
 *
 * @param activeBucketCount number of currently active buckets
 * @param maxBuckets maximum allowed buckets
 * @param oldestBucket timestamp of oldest bucket (if any)
 * @param newestBucket timestamp of newest bucket (if any)
 * @param totalSamples total number of samples across all buckets
 */
case class BucketManagerStats(
  activeBucketCount: Int,
  maxBuckets: Int,
  oldestBucket: Option[Long],
  newestBucket: Option[Long],
  totalSamples: Int
) {
  def utilizationPercent: Double = (activeBucketCount.toDouble / maxBuckets) * 100

  override def toString: String = {
    val range = (oldestBucket, newestBucket) match {
      case (Some(oldest), Some(newest)) => s"range: $oldest-$newest"
      case _ => "range: none"
    }
    s"BucketManagerStats($activeBucketCount/$maxBuckets buckets, $totalSamples samples, $range)"
  }
}

object BucketManager {
  /**
   * Default maximum number of active buckets per partition.
   * This provides:
   * - 1 current bucket
   * - 1 previous bucket for late arrivals
   * - 1 transition bucket for edge cases
   */
  val DefaultMaxBuckets = 3

  /**
   * Creates a BucketManager with a single aggregator.
   * Convenience method for single-column aggregation.
   *
   * @param maxBuckets maximum number of buckets
   * @param aggregator the aggregator template
   * @return a new BucketManager
   */
  def forSingleAggregator(maxBuckets: Int, aggregator: Aggregator): BucketManager = {
    new BucketManager(maxBuckets, Array(aggregator))
  }
}
