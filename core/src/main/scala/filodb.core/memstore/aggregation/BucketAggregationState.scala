package filodb.core.memstore.aggregation

import java.util.{TreeMap => JTreeMap}

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
  // Map from bucket timestamp -> aggregated values for all columns.
  // Java TreeMap avoids Scala collection wrapper overhead while
  // preserving sorted iteration needed by getBucketsToFinalize
  // (headMap), bucketValuesIteratorInRange (subMap/tailMap),
  // and earliestBucketTimestamp (firstKey).
  private val activeBuckets = new JTreeMap[java.lang.Long, BucketState]()

  // Track finalized bucket timestamps to reject late samples.
  // Java HashSet avoids Scala boxing overhead on Long keys.
  private val finalizedBuckets = new java.util.HashSet[java.lang.Long]()

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

  // Small pool of BucketState objects to reduce allocation pressure.
  // When a bucket is finalized, its BucketState is reset and returned to the pool.
  // When a new bucket is needed, a pooled BucketState is reused if available.
  private val bucketPool = new java.util.ArrayDeque[BucketState](8)

  private def acquireBucketState(): BucketState = {
    val pooled = bucketPool.pollFirst()
    if (pooled != null) {
      pooled.reset()
      pooled
    } else {
      new BucketState(numColumns, aggConfigsFlat)
    }
  }

  private def releaseBucketState(state: BucketState): Unit = {
    if (bucketPool.size() < 8) {
      bucketPool.offerLast(state)
    }
  }

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

    // Get or create bucket state (Java TreeMap has no getOrElseUpdate)
    // scalastyle:off null
    var bucketState = activeBuckets.get(ts)
    if (bucketState == null) {
      bucketState = acquireBucketState()
      activeBuckets.put(ts, bucketState)
    }
    // scalastyle:on null

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

    // scalastyle:off null
    var bucketState = activeBuckets.get(ts)
    if (bucketState == null) {
      bucketState = acquireBucketState()
      activeBuckets.put(ts, bucketState)
    }
    // scalastyle:on null

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
   * Uses Java TreeMap's headMap view for efficient range iteration with no intermediate allocations.
   */
  def getBucketsToFinalize(thresholdTs: Long): Seq[Long] = {
    if (activeBuckets.isEmpty) return Seq.empty
    val headView = activeBuckets.headMap(thresholdTs) // exclusive upper bound, returns view
    if (headView.isEmpty) return Seq.empty
    // Copy keys to a Seq since headView is a live view and we may mutate activeBuckets during finalization
    val result = new Array[Long](headView.size())
    val it = headView.keySet().iterator()
    var i = 0
    while (it.hasNext) {
      result(i) = it.next()
      i += 1
    }
    result.toSeq
  }

  /**
   * Returns the earliest active bucket timestamp, or Long.MaxValue if no active buckets.
   * Used for fast guard checks before attempting finalization.
   */
  def earliestBucketTimestamp: Long =
    if (activeBuckets.isEmpty) Long.MaxValue else activeBuckets.firstKey()

  /**
   * Gets the complete aggregated row for a bucket.
   * Returns column values array suitable for creating a RowReader.
   *
   * @param bucketTs the bucket timestamp
   * @return Some(array of column values) if bucket exists, None otherwise
   */
  def getBucketValues(bucketTs: Long): Option[Array[Any]] = {
    // scalastyle:off null
    val state = activeBuckets.get(bucketTs)
    if (state == null) return None
    // scalastyle:on null
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
    Some(values)
  }

  /**
   * Marks a bucket as finalized and removes it from active state.
   */
  def markFinalized(bucketTs: Long): Unit = {
    val state = activeBuckets.remove(bucketTs)
    // scalastyle:off null
    if (state != null) releaseBucketState(state)
    // scalastyle:on null
    finalizedBuckets.add(bucketTs)
  }

  /**
   * Returns an iterator over active buckets in the given time range [startTime, endTime].
   * Each entry is (bucketTimestamp, columnValues) where histogram columns return MutableHistogram
   * objects directly (not serialized DirectBuffers), suitable for the query path.
   *
   * Uses Java TreeMap's subMap/tailMap views — no intermediate Set, Seq, or sort is allocated.
   */
  def bucketValuesIteratorInRange(startTime: Long, endTime: Long): Iterator[(Long, Array[Any])] = {
    val rangeView = if (endTime == Long.MaxValue) {
      activeBuckets.tailMap(startTime, true) // inclusive lower bound
    } else {
      activeBuckets.subMap(startTime, true, endTime, true) // inclusive both bounds
    }
    // Wrap Java iterator as Scala iterator
    val javaIt = rangeView.entrySet().iterator()
    new Iterator[(Long, Array[Any])] {
      def hasNext: Boolean = javaIt.hasNext
      def next(): (Long, Array[Any]) = {
        val entry = javaIt.next()
        val ts = entry.getKey.longValue()
        val state = entry.getValue
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
  }

  /**
   * Returns true if there are any active (non-finalized) buckets.
   */
  def hasActiveBuckets: Boolean = !activeBuckets.isEmpty

  /**
   * Gets all active bucket timestamps.
   */
  def activeBucketTimestamps: Set[Long] = {
    val result = scala.collection.mutable.Set.empty[Long]
    val it = activeBuckets.keySet().iterator()
    while (it.hasNext) {
      result += it.next()
    }
    result.toSet
  }

  /**
   * Checks if a bucket is active (not finalized).
   */
  def isActive(bucketTs: Long): Boolean = activeBuckets.containsKey(bucketTs)

  /**
   * Cleans up old finalized tracking to prevent unbounded growth.
   */
  def cleanupOldFinalizedTracking(thresholdTs: Long): Unit = {
    if (hasPrimaryConfig) {
      val cleanupThreshold = thresholdTs - (2 * primaryOooToleranceMs)
      val it = finalizedBuckets.iterator()
      while (it.hasNext) {
        if (it.next() < cleanupThreshold) it.remove()
      }
    }
  }

  /**
   * Returns statistics about the current state.
   */
  def stats: BucketAggregationStats = BucketAggregationStats(
    activeBucketCount = activeBuckets.size(),
    finalizedBucketCount = finalizedBuckets.size(),
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
    // scalastyle:off null
    val state = activeBuckets.get(bucketTs)
    if (state == null) None else state.getHistogram(colIdx)
    // scalastyle:on null
  }
}

/**
 * State for a single time bucket, holding aggregated values for all columns.
 * Uses the Aggregator interface uniformly for all column types (scalar and histogram).
 *
 * Aggregators are pre-allocated at construction for all aggregating columns.
 * This improves memory locality since objects allocated together in time are likely
 * contiguous in memory due to TLAB allocation, and avoids per-sample null checks
 * and lazy creation overhead.
 *
 * @param numColumns total number of data columns
 * @param aggConfigs flat array of aggregation configs (null for non-aggregating columns)
 */
// scalastyle:off null
private class BucketState(numColumns: Int, aggConfigs: Array[AggregationConfig]) {
  // Aggregators for all aggregating columns — pre-allocated at construction
  private val aggregators: Array[Aggregator] = {
    val arr = new Array[Aggregator](numColumns)
    var i = 0
    while (i < numColumns) {
      if (aggConfigs(i) != null) {
        arr(i) = Aggregator.create(aggConfigs(i).aggType)
      }
      i += 1
    }
    arr
  }

  // Raw values for non-aggregating columns
  private val rawValues = new Array[Any](numColumns)

  /** Resets this BucketState for reuse from the pool. */
  def reset(): Unit = {
    var i = 0
    while (i < numColumns) {
      if (aggregators(i) != null) {
        aggregators(i).reset()
      }
      rawValues(i) = null
      i += 1
    }
  }

  def aggregate(colIdx: Int, config: AggregationConfig, value: Any, sampleTimestamp: Long): Unit = {
    aggregators(colIdx).addWithTimestamp(value, sampleTimestamp)
  }

  def aggregateDoubleWithTimestamp(colIdx: Int, config: AggregationConfig,
                                   value: Double, sampleTimestamp: Long): Unit = {
    aggregators(colIdx).addDoubleWithTimestamp(value, sampleTimestamp)
  }

  def aggregateLongWithTimestamp(colIdx: Int, config: AggregationConfig,
                                 value: Long, sampleTimestamp: Long): Unit = {
    aggregators(colIdx).addLongWithTimestamp(value, sampleTimestamp)
  }

  def getAggregatedValue(colIdx: Int, config: AggregationConfig): Any = {
    val agg = aggregators(colIdx)
    if (agg != null) agg.result() else null
  }

  /**
   * Returns the aggregated value for the query path. For histogram columns, returns
   * MutableHistogram directly (not serialized DirectBuffer). For scalar columns,
   * returns the aggregator result (Double/Long). Returns null if no aggregator exists.
   */
  def getValueForQuery(colIdx: Int): Any = {
    val agg = aggregators(colIdx)
    if (agg == null) return null
    agg match {
      case ha: HistogramAggregator => ha.getAccumulator.orNull
      case hla: HistogramLastAggregator => hla.getCurrentHistogram.orNull
      case _ => agg.result()
    }
  }

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
// scalastyle:on null

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
