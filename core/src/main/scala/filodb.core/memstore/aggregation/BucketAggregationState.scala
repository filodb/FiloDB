package filodb.core.memstore.aggregation

import scala.collection.mutable

import org.agrona.DirectBuffer

import filodb.memory.format.vectors.{BinaryHistogram, MutableHistogram}

/**
 * Manages in-memory aggregation state for ALL columns across time buckets.
 * This is the central state management for out-of-order sample aggregation.
 *
 * Design:
 * - Each bucket timestamp maps to aggregated values for all columns
 * - Buckets are kept in memory until they exit the tolerance window
 * - When finalized, complete rows with all columns are written at once
 * - This avoids issues with partial column writes and chunk lifecycle
 *
 * @param columnConfigs array of optional aggregation configs per column
 * @param numColumns total number of data columns
 */
class BucketAggregationState(
  columnConfigs: Array[Option[AggregationConfig]],
  numColumns: Int
) {
  // Map from bucket timestamp -> column index -> aggregated value
  // Using nested maps for efficient per-bucket access
  private val activeBuckets = mutable.TreeMap.empty[Long, BucketState]

  // Track finalized bucket timestamps to reject late samples
  private val finalizedBuckets = mutable.HashSet.empty[Long]

  // Track the latest sample timestamp seen for OOO detection
  private var latestSampleTimestamp: Long = Long.MinValue

  /**
   * Aggregates a sample into the appropriate bucket for each aggregating column.
   *
   * @param sampleTimestamp the original sample timestamp
   * @param ingestionTime the current ingestion time (for tolerance checking)
   * @param columnValues array of column values from the row (indexed by column)
   * @return true if sample was aggregated, false if it was dropped (outside tolerance or finalized)
   */
  // scalastyle:off method.length
  def aggregate(
    sampleTimestamp: Long,
    ingestionTime: Long,
    columnValues: Array[Any]
  ): Boolean = {
    // Calculate bucket timestamps for each column (they might differ if configs differ)
    // For simplicity, we use the first aggregating column's interval
    val bucketTs = getBucketTimestamp(sampleTimestamp)

    if (bucketTs.isEmpty) {
      // No aggregation configured
      return false
    }

    val ts = bucketTs.get

    // Check if bucket is already finalized
    if (finalizedBuckets.contains(ts)) {
      return false
    }

    // Check if sample is within tolerance window
    if (!isWithinTolerance(sampleTimestamp, ingestionTime)) {
      return false
    }

    // Get or create bucket state
    val bucketState = activeBuckets.getOrElseUpdate(ts, new BucketState(numColumns))

    // Aggregate each column
    var aggregated = false
    var i = 0
    while (i < numColumns) {
      columnConfigs(i) match {
        case Some(config) =>
          val value = columnValues(i)
          if (value != null) {
            bucketState.aggregate(i, config, value, sampleTimestamp)
            aggregated = true
          }
        case None =>
          // Non-aggregating column - keep the latest value (or first, depending on needs)
          // For now, we'll store the value from the first sample
          if (!bucketState.hasValue(i) && columnValues(i) != null) {
            bucketState.setValue(i, columnValues(i))
          }
      }
      i += 1
    }

    if (sampleTimestamp > latestSampleTimestamp) {
      latestSampleTimestamp = sampleTimestamp
    }

    aggregated
  }
  // scalastyle:on method.length

  /**
   * Gets buckets that should be finalized (older than threshold).
   * Returns bucket timestamps in sorted order.
   */
  def getBucketsToFinalize(thresholdTs: Long): Seq[Long] = {
    activeBuckets.keys.filter(_ < thresholdTs).toSeq
  }

  /**
   * Gets the complete aggregated row for a bucket.
   * Returns column values array suitable for creating a RowReader.
   *
   * @param bucketTs the bucket timestamp
   * @return Some(array of column values) if bucket exists, None otherwise
   */
  def getBucketValues(bucketTs: Long): Option[Array[Any]] = {
    activeBuckets.get(bucketTs).map { state =>
      val values = new Array[Any](numColumns)
      var i = 0
      while (i < numColumns) {
        columnConfigs(i) match {
          case Some(config) =>
            values(i) = state.getAggregatedValue(i, config)
          case None =>
            values(i) = state.getValue(i)
        }
        i += 1
      }
      values
    }
  }

  /**
   * Marks a bucket as finalized and removes it from active state.
   */
  def markFinalized(bucketTs: Long): Unit = {
    activeBuckets.remove(bucketTs)
    finalizedBuckets.add(bucketTs)
  }

  /**
   * Gets all active bucket timestamps.
   */
  def activeBucketTimestamps: Set[Long] = activeBuckets.keySet.toSet

  /**
   * Checks if a bucket is active (not finalized).
   */
  def isActive(bucketTs: Long): Boolean = activeBuckets.contains(bucketTs)

  /**
   * Cleans up old finalized tracking to prevent unbounded growth.
   */
  def cleanupOldFinalizedTracking(thresholdTs: Long): Unit = {
    val minConfig = columnConfigs.flatten.headOption
    minConfig.foreach { config =>
      val cleanupThreshold = thresholdTs - (2 * config.oooToleranceMs)
      finalizedBuckets.retain(_ >= cleanupThreshold)
    }
  }

  /**
   * Returns statistics about the current state.
   */
  def stats: BucketAggregationStats = BucketAggregationStats(
    activeBucketCount = activeBuckets.size,
    finalizedBucketCount = finalizedBuckets.size,
    latestSampleTimestamp = latestSampleTimestamp
  )

  /**
   * Clears all state. Used for testing or partition shutdown.
   */
  def clear(): Unit = {
    activeBuckets.clear()
    finalizedBuckets.clear()
    latestSampleTimestamp = Long.MinValue
  }

  // Helper to calculate bucket timestamp based on first aggregation config
  private def getBucketTimestamp(sampleTs: Long): Option[Long] = {
    columnConfigs.flatten.headOption.map { config =>
      TimeBucket.ceilToBucket(sampleTs, config.intervalMs)
    }
  }

  // Helper to check if sample is within tolerance
  private def isWithinTolerance(sampleTs: Long, ingestionTime: Long): Boolean = {
    columnConfigs.flatten.headOption.exists { config =>
      ingestionTime - sampleTs <= config.oooToleranceMs
    }
  }

  /**
   * Gets the aggregated histogram for a specific column and bucket.
   * Used for histogram queries.
   */
  def getAggregatedHistogram(colIdx: Int, bucketTs: Long): Option[MutableHistogram] = {
    activeBuckets.get(bucketTs).flatMap(_.getHistogram(colIdx))
  }
}

/**
 * State for a single time bucket, holding aggregated values for all columns.
 */
private class BucketState(numColumns: Int) {
  // Aggregators for aggregating columns
  private val aggregators = new Array[Aggregator](numColumns)

  // Raw values for non-aggregating columns
  private val rawValues = new Array[Any](numColumns)

  // Histogram accumulators for histogram columns
  private val histogramAccumulators = new Array[MutableHistogram](numColumns)

  // Track latest timestamp per column for Last aggregation
  private val latestTimestamps = new Array[Long](numColumns)
  java.util.Arrays.fill(latestTimestamps, Long.MinValue)

  def aggregate(colIdx: Int, config: AggregationConfig, value: Any, sampleTimestamp: Long): Unit = {
    config.aggType match {
      case AggregationType.HistogramSum =>
        aggregateHistogramSum(colIdx, value)

      case AggregationType.HistogramLast =>
        aggregateHistogramLast(colIdx, value, sampleTimestamp)

      case scalarType =>
        // Scalar aggregation
        if (aggregators(colIdx) == null) {
          aggregators(colIdx) = Aggregator.create(scalarType)
        }
        aggregators(colIdx).addWithTimestamp(value, sampleTimestamp)
    }
  }

  private def aggregateHistogramSum(colIdx: Int, value: Any): Unit = {
    val hist = valueToHistogram(value)
    if (histogramAccumulators(colIdx) == null) {
      histogramAccumulators(colIdx) = MutableHistogram(hist)
    } else {
      histogramAccumulators(colIdx).add(hist)
    }
  }

  private def aggregateHistogramLast(colIdx: Int, value: Any, sampleTimestamp: Long): Unit = {
    if (sampleTimestamp >= latestTimestamps(colIdx)) {
      val hist = valueToHistogram(value)
      histogramAccumulators(colIdx) = MutableHistogram(hist)
      latestTimestamps(colIdx) = sampleTimestamp
    }
  }

  private def valueToHistogram(value: Any): filodb.memory.format.vectors.HistogramWithBuckets = {
    value match {
      case buf: DirectBuffer =>
        BinaryHistogram.BinHistogram(buf).toHistogram
      case h: filodb.memory.format.vectors.HistogramWithBuckets =>
        h
      case _ =>
        throw new IllegalArgumentException(s"Cannot convert $value to histogram")
    }
  }

  def getAggregatedValue(colIdx: Int, config: AggregationConfig): Any = {
    config.aggType match {
      case AggregationType.HistogramSum | AggregationType.HistogramLast =>
        Option(histogramAccumulators(colIdx)).map(_.serialize()).orNull

      case _ =>
        Option(aggregators(colIdx)).map(_.result()).orNull
    }
  }

  def getHistogram(colIdx: Int): Option[MutableHistogram] = {
    Option(histogramAccumulators(colIdx))
  }

  def hasValue(colIdx: Int): Boolean = rawValues(colIdx) != null

  def setValue(colIdx: Int, value: Any): Unit = {
    rawValues(colIdx) = value
  }

  def getValue(colIdx: Int): Any = rawValues(colIdx)
}

/**
 * Statistics about bucket aggregation state.
 */
case class BucketAggregationStats(
  activeBucketCount: Int,
  finalizedBucketCount: Int,
  latestSampleTimestamp: Long
) {
  override def toString: String = {
    s"BucketAggregationStats(active=$activeBucketCount, " +
      s"finalized=$finalizedBucketCount, latestTs=$latestSampleTimestamp)"
  }
}
