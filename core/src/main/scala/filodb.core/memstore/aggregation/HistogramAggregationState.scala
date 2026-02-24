package filodb.core.memstore.aggregation

import scala.collection.mutable

import org.agrona.DirectBuffer

import filodb.memory.format.vectors.{BinaryHistogram, MutableHistogram}

/**
 * Manages in-memory aggregation state for histogram columns.
 * Histograms are aggregated in-memory until their bucket exits the tolerance window,
 * at which point they're finalized and written to the vector.
 *
 * This hybrid approach keeps histograms immediately queryable (from memory) while
 * avoiding the complexity of variable-length overwrites in histogram vectors.
 *
 * @param aggType the aggregation type (HistogramSum or HistogramLast)
 * @param intervalMs the bucket interval in milliseconds
 * @param toleranceMs the out-of-order tolerance in milliseconds
 */
class HistogramAggregationState(
  val aggType: AggregationType,
  val intervalMs: Long,
  val toleranceMs: Long
) {
  // Active buckets being aggregated (within tolerance window)
  private val activeBuckets = mutable.HashMap.empty[Long, MutableHistogram]

  // Track which bucket timestamps have been finalized to vector
  // This prevents accepting samples for already-finalized buckets
  private val finalizedBuckets = mutable.HashSet.empty[Long]

  /**
   * Aggregates a histogram value into the appropriate bucket.
   * Returns Some(histogram) if this is a new or updated bucket that should be queryable.
   * Returns None if the bucket has already been finalized.
   *
   * @param bucketTs the bucket timestamp
   * @param value the histogram as DirectBuffer (BinaryHistogram format)
   * @param sampleTs the original sample timestamp (used for HistogramLast)
   * @return Some(aggregated histogram) if successful, None if bucket already finalized
   */
  def aggregate(bucketTs: Long, value: DirectBuffer, sampleTs: Long): Option[MutableHistogram] = {
    if (finalizedBuckets.contains(bucketTs)) {
      None // Already finalized, cannot accept more samples
    } else {
      val binHist = BinaryHistogram.BinHistogram(value)
      val hist = binHist.toHistogram

      aggType match {
        case AggregationType.HistogramSum =>
          // Sum aggregation: add histogram to existing accumulator
          activeBuckets.get(bucketTs) match {
            case Some(existing) =>
              existing.add(hist)
              Some(existing)
            case None =>
              val newHist = MutableHistogram(hist)
              activeBuckets.put(bucketTs, newHist)
              Some(newHist)
          }

        case AggregationType.HistogramLast =>
          // Last aggregation: replace with histogram having latest timestamp
          // Note: The bucket manager already handles timestamp comparison
          // so we can just replace here
          val newHist = MutableHistogram(hist)
          activeBuckets.put(bucketTs, newHist)
          Some(newHist)

        case _ =>
          None // Unsupported aggregation type for histograms
      }
    }
  }

  /**
   * Gets the current aggregated histogram for a bucket, if it exists.
   * Used by query path to read in-memory histograms.
   *
   * @param bucketTs the bucket timestamp
   * @return Some(histogram) if active, None if not found or finalized
   */
  def get(bucketTs: Long): Option[MutableHistogram] = activeBuckets.get(bucketTs)

  /**
   * Returns all active buckets that should be finalized (older than threshold).
   * Buckets are returned in sorted order by timestamp.
   *
   * @param thresholdTs the threshold timestamp (buckets older than this should be finalized)
   * @return sequence of (bucketTimestamp, histogram) pairs to finalize
   */
  def getBucketsToFinalize(thresholdTs: Long): Seq[(Long, MutableHistogram)] = {
    activeBuckets.filter(_._1 < thresholdTs).toSeq.sortBy(_._1)
  }

  /**
   * Marks a bucket as finalized and removes it from active state.
   * After this, the bucket will not accept new samples.
   *
   * @param bucketTs the bucket timestamp to mark as finalized
   */
  def markFinalized(bucketTs: Long): Unit = {
    activeBuckets.remove(bucketTs)
    finalizedBuckets.add(bucketTs)
  }

  /**
   * Checks if a bucket timestamp is still active (not finalized).
   */
  def isActive(bucketTs: Long): Boolean = activeBuckets.contains(bucketTs)

  /**
   * Checks if a bucket has been finalized.
   */
  def isFinalized(bucketTs: Long): Boolean = finalizedBuckets.contains(bucketTs)

  /**
   * Returns all active bucket timestamps.
   */
  def activeBucketTimestamps: Set[Long] = activeBuckets.keySet.toSet

  /**
   * Cleans up finalized bucket tracking for buckets that are very old.
   * Called periodically to prevent unbounded growth of finalizedBuckets set.
   * Only removes tracking for buckets significantly older than the threshold.
   *
   * @param thresholdTs buckets older than this can have their tracking removed
   */
  def cleanupOldFinalizedTracking(thresholdTs: Long): Unit = {
    // Keep finalizedBuckets for at least 2x tolerance to handle edge cases
    val cleanupThreshold = thresholdTs - (2 * toleranceMs)
    // filterInPlace is not available in Scala 2.12, so we retain and filter manually
    finalizedBuckets.retain(_ >= cleanupThreshold)
  }

  /**
   * Clears all state. Used for testing or partition shutdown.
   */
  def clear(): Unit = {
    activeBuckets.clear()
    finalizedBuckets.clear()
  }

  /**
   * Returns statistics about the current state.
   */
  def stats: HistogramAggregationStats = HistogramAggregationStats(
    activeBucketCount = activeBuckets.size,
    finalizedBucketCount = finalizedBuckets.size
  )
}

/**
 * Statistics about histogram aggregation state.
 */
case class HistogramAggregationStats(
  activeBucketCount: Int,
  finalizedBucketCount: Int
) {
  override def toString: String =
    s"HistogramAggregationStats(active=$activeBucketCount, finalized=$finalizedBucketCount)"
}
