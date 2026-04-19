package filodb.core.memstore.aggregation

import scala.collection.mutable

import filodb.core.metadata.Column
import filodb.memory.format.RowReader
import filodb.memory.format.vectors.MutableHistogram

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
  numColumns: Int,
  columnTypes: Array[Column.ColumnType] = Array.empty
) {
  // Map from bucket timestamp -> column index -> aggregated value
  // Using nested maps for efficient per-bucket access
  private val activeBuckets = mutable.TreeMap.empty[Long, BucketState]

  // Track finalized bucket timestamps to reject late samples
  private val finalizedBuckets = mutable.HashSet.empty[Long]

  // Track the latest sample timestamp seen for OOO detection
  private var latestSampleTimestamp: Long = Long.MinValue

  // Cached primary config values (computed once at construction to avoid per-sample collection ops)
  private val primaryIntervalMs: Long = columnConfigs.flatten.headOption.map(_.intervalMs).getOrElse(0L)
  private val primaryOooToleranceMs: Long = columnConfigs.flatten.headOption.map(_.oooToleranceMs).getOrElse(0L)
  private val hasPrimaryConfig: Boolean = columnConfigs.flatten.nonEmpty

  // Pre-computed arrays for fast inner-loop access (avoids Option matching per column per sample)
  private val isAggregating: Array[Boolean] = columnConfigs.map(_.isDefined)
  // scalastyle:off null
  private val aggConfigsFlat: Array[AggregationConfig] = columnConfigs.map(_.orNull)
  // scalastyle:on null

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
    if (!hasPrimaryConfig) return false

    val ts = getBucketTimestamp(sampleTimestamp)

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
      if (isAggregating(i)) {
        val value = columnValues(i)
        if (value != null) {
          bucketState.aggregate(i, aggConfigsFlat(i), value, sampleTimestamp)
          aggregated = true
        }
      } else {
        // Non-aggregating column - keep first value
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
   * Aggregates a sample from a RowReader directly, using type-specialized paths
   * to avoid boxing for Double and Long columns.
   * Requires columnTypes to be provided at construction.
   */
  // scalastyle:off method.length cyclomatic.complexity
  def aggregate(
    sampleTimestamp: Long,
    ingestionTime: Long,
    row: RowReader
  ): Boolean = {
    if (!hasPrimaryConfig) return false

    val ts = getBucketTimestamp(sampleTimestamp)

    if (finalizedBuckets.contains(ts)) return false
    if (!isWithinTolerance(sampleTimestamp, ingestionTime)) return false

    val bucketState = activeBuckets.getOrElseUpdate(ts, new BucketState(numColumns))

    var aggregated = false
    var i = 0
    while (i < numColumns) {
      if (isAggregating(i)) {
        if (columnTypes.length > i) {
          columnTypes(i) match {
            case Column.ColumnType.DoubleColumn =>
              val value = row.getDouble(i)
              if (!value.isNaN) {
                bucketState.aggregateDoubleWithTimestamp(i, aggConfigsFlat(i), value, sampleTimestamp)
                aggregated = true
              }
            case Column.ColumnType.LongColumn | Column.ColumnType.TimestampColumn =>
              bucketState.aggregateLongWithTimestamp(i, aggConfigsFlat(i), row.getLong(i), sampleTimestamp)
              aggregated = true
            case Column.ColumnType.HistogramColumn =>
              val value = row.getAny(i)
              // scalastyle:off null
              if (value != null) {
                bucketState.aggregate(i, aggConfigsFlat(i), value, sampleTimestamp)
                aggregated = true
              }
              // scalastyle:on null
            case _ =>
              val value = row.getAny(i)
              // scalastyle:off null
              if (value != null) {
                bucketState.aggregate(i, aggConfigsFlat(i), value, sampleTimestamp)
                aggregated = true
              }
              // scalastyle:on null
          }
        } else {
          // Fallback if columnTypes not provided
          val value = row.getAny(i)
          // scalastyle:off null
          if (value != null) {
            bucketState.aggregate(i, aggConfigsFlat(i), value, sampleTimestamp)
            aggregated = true
          }
          // scalastyle:on null
        }
      } else {
        // scalastyle:off null
        if (!bucketState.hasValue(i)) {
          val value = row.getAny(i)
          if (value != null) bucketState.setValue(i, value)
        }
        // scalastyle:on null
      }
      i += 1
    }

    if (sampleTimestamp > latestSampleTimestamp) {
      latestSampleTimestamp = sampleTimestamp
    }

    aggregated
  }
  // scalastyle:on method.length cyclomatic.complexity

  /**
   * Gets buckets that should be finalized (older than threshold).
   * Uses TreeMap's sorted nature for efficient range iteration.
   */
  def getBucketsToFinalize(thresholdTs: Long): Seq[Long] = {
    if (activeBuckets.isEmpty) return Seq.empty
    activeBuckets.until(thresholdTs).keys.toSeq
  }

  /**
   * Returns the earliest active bucket timestamp, or Long.MaxValue if no active buckets.
   * Used for fast guard checks before attempting finalization.
   */
  def earliestBucketTimestamp: Long =
    if (activeBuckets.isEmpty) Long.MaxValue else activeBuckets.firstKey

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
        if (isAggregating(i)) {
          values(i) = state.getAggregatedValue(i, aggConfigsFlat(i))
        } else {
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
   * Returns an iterator over active buckets in the given time range [startTime, endTime].
   * Each entry is (bucketTimestamp, columnValues) where histogram columns return MutableHistogram
   * objects directly (not serialized DirectBuffers), suitable for the query path.
   *
   * Uses TreeMap's range view — no intermediate Set, Seq, or sort is allocated.
   */
  def bucketValuesIteratorInRange(startTime: Long, endTime: Long): Iterator[(Long, Array[Any])] = {
    // TreeMap.range(from, until) is [from, until). Use rangeFrom to avoid Long.MaxValue+1 overflow.
    val rangeView = if (endTime == Long.MaxValue) {
      activeBuckets.rangeFrom(startTime)
    } else {
      activeBuckets.range(startTime, endTime + 1)
    }
    rangeView.iterator.map { case (ts, state) =>
      val values = new Array[Any](numColumns)
      var i = 0
      while (i < numColumns) {
        columnConfigs(i) match {
          case Some(_) =>
            values(i) = state.getValueForQuery(i)
          case None =>
            values(i) = state.getValue(i)
        }
        i += 1
      }
      (ts, values)
    }
  }

  /**
   * Returns true if there are any active (non-finalized) buckets.
   */
  def hasActiveBuckets: Boolean = activeBuckets.nonEmpty

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
    if (hasPrimaryConfig) {
      val cleanupThreshold = thresholdTs - (2 * primaryOooToleranceMs)
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

  // Helper to calculate bucket timestamp using cached primary interval
  private def getBucketTimestamp(sampleTs: Long): Long =
    TimeBucket.ceilToBucket(sampleTs, primaryIntervalMs)

  // Helper to check if sample is within tolerance using cached tolerance
  private def isWithinTolerance(sampleTs: Long, ingestionTime: Long): Boolean =
    ingestionTime - sampleTs <= primaryOooToleranceMs

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
 * Uses the Aggregator interface uniformly for all column types (scalar and histogram).
 */
private class BucketState(numColumns: Int) {
  // Aggregators for all aggregating columns (scalar and histogram)
  private val aggregators = new Array[Aggregator](numColumns)

  // Raw values for non-aggregating columns
  private val rawValues = new Array[Any](numColumns)

  def aggregate(colIdx: Int, config: AggregationConfig, value: Any, sampleTimestamp: Long): Unit = {
    if (aggregators(colIdx) == null) {
      aggregators(colIdx) = Aggregator.create(config.aggType)
    }
    aggregators(colIdx).addWithTimestamp(value, sampleTimestamp)
  }

  def aggregateDoubleWithTimestamp(colIdx: Int, config: AggregationConfig,
                                   value: Double, sampleTimestamp: Long): Unit = {
    if (aggregators(colIdx) == null) {
      aggregators(colIdx) = Aggregator.create(config.aggType)
    }
    aggregators(colIdx).addDoubleWithTimestamp(value, sampleTimestamp)
  }

  def aggregateLongWithTimestamp(colIdx: Int, config: AggregationConfig,
                                 value: Long, sampleTimestamp: Long): Unit = {
    if (aggregators(colIdx) == null) {
      aggregators(colIdx) = Aggregator.create(config.aggType)
    }
    aggregators(colIdx).addLongWithTimestamp(value, sampleTimestamp)
  }

  def getAggregatedValue(colIdx: Int, config: AggregationConfig): Any = {
    // scalastyle:off null
    val agg = aggregators(colIdx)
    if (agg != null) agg.result() else null
    // scalastyle:on null
  }

  /**
   * Returns the aggregated value for the query path. For histogram columns, returns
   * MutableHistogram directly (not serialized DirectBuffer). For scalar columns,
   * returns the aggregator result (Double/Long). Returns null if no aggregator exists.
   */
  // scalastyle:off null
  def getValueForQuery(colIdx: Int): Any = {
    val agg = aggregators(colIdx)
    if (agg == null) return null
    agg match {
      case ha: HistogramAggregator => ha.getAccumulator.orNull
      case hla: HistogramLastAggregator => hla.getCurrentHistogram.orNull
      case _ => agg.result()
    }
  }
  // scalastyle:on null

  def getHistogram(colIdx: Int): Option[MutableHistogram] = {
    Option(aggregators(colIdx)).flatMap {
      case ha: HistogramAggregator => ha.getAccumulator
      case hla: HistogramLastAggregator => hla.getCurrentHistogram
      case _ => None
    }
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
