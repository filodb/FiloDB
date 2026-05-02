package filodb.core.memstore.aggregation

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
 * Active bucket count per partition is bounded by ceil(tolerance/interval)+1
 * (typically ~3 for default config). At this size, sorted parallel Long/Object
 * arrays beat java.util.TreeMap on every dimension: no boxing, no Entry allocation,
 * cache-friendly linear scan.
 *
 * @param columnConfigs array of optional aggregation configs per column
 * @param numColumns total number of data columns
 */
class BucketAggregationState(
  columnConfigs: Array[Option[AggregationConfig]],
  numColumns: Int,
  columnTypes: Array[Column.ColumnType] = Array.empty
) {
  // Cached primary config values (computed once at construction to avoid per-sample collection ops)
  private val primaryIntervalMs: Long = columnConfigs.flatten.headOption.map(_.intervalMs).getOrElse(0L)
  private val primaryOooToleranceMs: Long = columnConfigs.flatten.headOption.map(_.oooToleranceMs).getOrElse(0L)
  private val hasPrimaryConfig: Boolean = columnConfigs.flatten.nonEmpty

  // Pre-computed arrays for fast inner-loop access (avoids Option matching per column per sample)
  private val isAggregating: Array[Boolean] = columnConfigs.map(_.isDefined)
  // scalastyle:off null
  private val aggConfigsFlat: Array[AggregationConfig] = columnConfigs.map(_.orNull)
  // scalastyle:on null

  // Capacity based on tolerance/interval + headroom
  private val maxActive: Int = {
    val theoretical =
      if (primaryIntervalMs > 0)
        ((primaryOooToleranceMs + primaryIntervalMs - 1) / primaryIntervalMs).toInt + 1
      else 4
    math.max(theoretical * 2, 4)
  }

  // Parallel arrays, kept sorted ascending by bucketTsArr(i).
  // Invariant: bucketTsArr(0..numActive-1) is strictly increasing.
  private var bucketTsArr        = new Array[Long](maxActive)
  private var bucketStatesArr    = new Array[BucketState](maxActive)
  private var bucketMinOffsetArr = new Array[Long](maxActive)
  private var bucketLastIngest   = new Array[Long](maxActive)
  private var numActive: Int = 0
  private var currentCapacity: Int = maxActive

  // Primitive set for finalized tracking (debox avoids boxing Long)
  private val finalizedBuckets = debox.Set.empty[Long]

  // Track the latest sample timestamp seen for OOO detection
  private var latestSampleTimestamp: Long = Long.MinValue

  // Overflow counter (incremented when growArrays is triggered)
  private var overflowEvents: Int = 0

  // Small pool of BucketState objects to reduce allocation pressure.
  private val bucketPool = new java.util.ArrayDeque[BucketState](8)

  // scalastyle:off null
  private def acquireBucketState(): BucketState = {
    val pooled = bucketPool.pollFirst()
    if (pooled != null) {
      pooled.reset()
      pooled
    } else {
      new BucketState(numColumns, aggConfigsFlat)
    }
  }
  // scalastyle:on null

  private def releaseBucketState(state: BucketState): Unit = {
    if (bucketPool.size() < 8) {
      bucketPool.offerLast(state)
    }
  }

  /** Linear scan for the index of bucketTsArr == ts. Returns -1 if not found. */
  private def findActiveIndex(ts: Long): Int = {
    var i = 0
    while (i < numActive) {
      val cur = bucketTsArr(i)
      if (cur == ts) return i
      if (cur > ts) return -1
      i += 1
    }
    -1
  }

  /** Insert at sorted position. Grows arrays if needed. Returns inserted index. */
  private def insertActive(ts: Long, state: BucketState): Int = {
    if (numActive >= currentCapacity) growArrays()
    // Insertion sort: shift elements right to make room
    var i = numActive
    while (i > 0 && bucketTsArr(i - 1) > ts) {
      bucketTsArr(i) = bucketTsArr(i - 1)
      bucketStatesArr(i) = bucketStatesArr(i - 1)
      bucketMinOffsetArr(i) = bucketMinOffsetArr(i - 1)
      bucketLastIngest(i) = bucketLastIngest(i - 1)
      i -= 1
    }
    bucketTsArr(i) = ts
    bucketStatesArr(i) = state
    bucketMinOffsetArr(i) = Long.MaxValue
    bucketLastIngest(i) = Long.MinValue
    numActive += 1
    i
  }

  // scalastyle:off null
  private def removeActive(idx: Int): Unit = {
    val last = numActive - 1
    // Shift elements left
    var i = idx
    while (i < last) {
      bucketTsArr(i) = bucketTsArr(i + 1)
      bucketStatesArr(i) = bucketStatesArr(i + 1)
      bucketMinOffsetArr(i) = bucketMinOffsetArr(i + 1)
      bucketLastIngest(i) = bucketLastIngest(i + 1)
      i += 1
    }
    numActive = last
    bucketStatesArr(numActive) = null
  }
  // scalastyle:on null

  private def growArrays(): Unit = {
    val newCap = currentCapacity * 2
    val newTs = new Array[Long](newCap)
    val newStates = new Array[BucketState](newCap)
    val newOffsets = new Array[Long](newCap)
    val newIngest = new Array[Long](newCap)
    System.arraycopy(bucketTsArr, 0, newTs, 0, numActive)
    System.arraycopy(bucketStatesArr, 0, newStates, 0, numActive)
    System.arraycopy(bucketMinOffsetArr, 0, newOffsets, 0, numActive)
    System.arraycopy(bucketLastIngest, 0, newIngest, 0, numActive)
    bucketTsArr = newTs
    bucketStatesArr = newStates
    bucketMinOffsetArr = newOffsets
    bucketLastIngest = newIngest
    currentCapacity = newCap
    overflowEvents += 1
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
    columnValues: Array[Any],
    offset: Long = Long.MaxValue
  ): Boolean = {
    if (!hasPrimaryConfig) return false

    val ts = getBucketTimestamp(sampleTimestamp)

    if (finalizedBuckets(ts)) return false
    if (!isWithinTolerance(sampleTimestamp, ingestionTime)) return false

    var idx = findActiveIndex(ts)
    if (idx < 0) {
      val bucketState = acquireBucketState()
      idx = insertActive(ts, bucketState)
    }
    val bucketState = bucketStatesArr(idx)

    // Aggregate each column
    // scalastyle:off null
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
        if (!bucketState.hasValue(i) && columnValues(i) != null) {
          bucketState.setValue(i, columnValues(i))
        }
      }
      i += 1
    }
    // scalastyle:on null

    if (sampleTimestamp > latestSampleTimestamp) {
      latestSampleTimestamp = sampleTimestamp
    }

    // Update parallel-array metadata for this bucket
    if (offset != Long.MaxValue) {
      if (offset < bucketMinOffsetArr(idx)) bucketMinOffsetArr(idx) = offset
    }
    if (ingestionTime > bucketLastIngest(idx)) bucketLastIngest(idx) = ingestionTime

    aggregated
  }
  // scalastyle:on method.length

  /**
   * Aggregates a sample from a RowReader directly, using type-specialized paths
   * to avoid boxing for Double and Long columns.
   * Requires columnTypes to be provided at construction.
   */
  // scalastyle:off method.length cyclomatic.complexity
  def aggregateRow(
    sampleTimestamp: Long,
    ingestionTime: Long,
    row: RowReader,
    offset: Long = Long.MaxValue
  ): Boolean = {
    if (!hasPrimaryConfig) return false

    val ts = getBucketTimestamp(sampleTimestamp)

    if (finalizedBuckets(ts)) return false
    if (!isWithinTolerance(sampleTimestamp, ingestionTime)) return false

    var idx = findActiveIndex(ts)
    if (idx < 0) {
      val bucketState = acquireBucketState()
      idx = insertActive(ts, bucketState)
    }
    val bucketState = bucketStatesArr(idx)

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

    // Update parallel-array metadata for this bucket
    if (offset != Long.MaxValue) {
      if (offset < bucketMinOffsetArr(idx)) bucketMinOffsetArr(idx) = offset
    }
    if (ingestionTime > bucketLastIngest(idx)) bucketLastIngest(idx) = ingestionTime

    aggregated
  }
  // scalastyle:on method.length cyclomatic.complexity

  /**
   * Gets buckets that should be finalized (older than threshold).
   * Walks the sorted array from the beginning — O(k) where k is the number of
   * buckets below threshold (typically 0 or 1).
   */
  def getBucketsToFinalize(thresholdTs: Long): Seq[Long] = {
    if (numActive == 0) return Seq.empty
    var count = 0
    while (count < numActive && bucketTsArr(count) < thresholdTs) count += 1
    if (count == 0) return Seq.empty
    val result = new Array[Long](count)
    System.arraycopy(bucketTsArr, 0, result, 0, count)
    result.toSeq
  }

  /**
   * Returns the earliest active bucket timestamp, or Long.MaxValue if no active buckets.
   */
  def earliestBucketTimestamp: Long =
    if (numActive == 0) Long.MaxValue else bucketTsArr(0)

  /**
   * Returns the smallest Kafka offset referenced by any active bucket.
   * Long.MaxValue if no offsets have been tracked (no active buckets or no offsets supplied).
   */
  def earliestActiveOffset: Long = {
    if (numActive == 0) return Long.MaxValue
    var min = bucketMinOffsetArr(0)
    var i = 1
    while (i < numActive) {
      if (bucketMinOffsetArr(i) < min) min = bucketMinOffsetArr(i)
      i += 1
    }
    min
  }

  /**
   * Returns bucket timestamps whose last ingestion wall-clock time is older than
   * (nowMs - toleranceMs). These are "stale" buckets that should be reaped so they
   * don't hold back the Kafka commit watermark indefinitely.
   */
  def getStaleBuckets(nowMs: Long, toleranceMs: Long): Seq[Long] = {
    if (numActive == 0) return Seq.empty
    val cutoff = nowMs - toleranceMs
    val buf = new Array[Long](numActive)
    var count = 0
    var i = 0
    while (i < numActive) {
      if (bucketLastIngest(i) < cutoff) { buf(count) = bucketTsArr(i); count += 1 }
      i += 1
    }
    if (count == 0) Seq.empty
    else {
      val result = new Array[Long](count)
      System.arraycopy(buf, 0, result, 0, count)
      result.toSeq
    }
  }

  /**
   * Gets the complete aggregated row for a bucket.
   * Returns column values array suitable for creating a RowReader.
   *
   * @param bucketTs the bucket timestamp
   * @return Some(array of column values) if bucket exists, None otherwise
   */
  def getBucketValues(bucketTs: Long): Option[Array[Any]] = {
    val idx = findActiveIndex(bucketTs)
    if (idx < 0) return None
    val state = bucketStatesArr(idx)
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
    val idx = findActiveIndex(bucketTs)
    if (idx >= 0) {
      val state = bucketStatesArr(idx)
      releaseBucketState(state)
      removeActive(idx)
    }
    finalizedBuckets += bucketTs
  }

  /**
   * Returns an iterator over active buckets in the given time range [startTime, endTime].
   * Each entry is (bucketTimestamp, columnValues) where histogram columns return MutableHistogram
   * objects directly (not serialized DirectBuffers), suitable for the query path.
   *
   * Safety: relies on single-ingestion-thread-per-partition guarantee. The iterator reads
   * live array references; concurrent insertActive/removeActive would corrupt iteration.
   */
  def bucketValuesIteratorInRange(startTime: Long, endTime: Long): Iterator[(Long, Array[Any])] = {
    // Find starting index: first i where bucketTsArr(i) >= startTime
    var start = 0
    while (start < numActive && bucketTsArr(start) < startTime) start += 1

    val capturedStart = start
    val capturedNumActive = numActive

    new Iterator[(Long, Array[Any])] {
      private var pos = capturedStart
      def hasNext: Boolean = pos < capturedNumActive && bucketTsArr(pos) <= endTime
      def next(): (Long, Array[Any]) = {
        val ts = bucketTsArr(pos)
        val state = bucketStatesArr(pos)
        pos += 1
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
  def hasActiveBuckets: Boolean = numActive > 0

  /**
   * Gets all active bucket timestamps.
   */
  def activeBucketTimestamps: Set[Long] = {
    val result = scala.collection.mutable.Set.empty[Long]
    var i = 0
    while (i < numActive) { result += bucketTsArr(i); i += 1 }
    result.toSet
  }

  /**
   * Checks if a bucket is active (not finalized).
   */
  def isActive(bucketTs: Long): Boolean = findActiveIndex(bucketTs) >= 0

  /**
   * Cleans up old finalized tracking to prevent unbounded growth.
   */
  def cleanupOldFinalizedTracking(thresholdTs: Long): Unit = {
    if (hasPrimaryConfig) {
      val cleanupThreshold = thresholdTs - (2 * primaryOooToleranceMs)
      val toRemove = debox.Buffer.empty[Long]
      finalizedBuckets.foreach { ts =>
        if (ts < cleanupThreshold) toRemove += ts
      }
      var i = 0
      while (i < toRemove.length) {
        finalizedBuckets.remove(toRemove(i))
        i += 1
      }
    }
  }

  /**
   * Returns statistics about the current state.
   */
  def stats: BucketAggregationStats = BucketAggregationStats(
    activeBucketCount = numActive,
    finalizedBucketCount = finalizedBuckets.size,
    latestSampleTimestamp = latestSampleTimestamp,
    overflowCount = overflowEvents
  )

  /**
   * Clears all state. Used for testing or partition shutdown.
   */
  // scalastyle:off null
  def clear(): Unit = {
    var i = 0
    while (i < numActive) {
      bucketStatesArr(i) = null
      i += 1
    }
    numActive = 0
    finalizedBuckets.clear()
    bucketPool.clear()
    overflowEvents = 0
    latestSampleTimestamp = Long.MinValue
  }
  // scalastyle:on null

  private def getBucketTimestamp(sampleTs: Long): Long =
    TimeBucket.ceilToBucket(sampleTs, primaryIntervalMs)

  private def isWithinTolerance(sampleTs: Long, ingestionTime: Long): Boolean =
    ingestionTime - sampleTs <= primaryOooToleranceMs

  /**
   * Gets the aggregated histogram for a specific column and bucket.
   */
  def getAggregatedHistogram(colIdx: Int, bucketTs: Long): Option[MutableHistogram] = {
    val idx = findActiveIndex(bucketTs)
    if (idx < 0) None else bucketStatesArr(idx).getHistogram(colIdx)
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
  latestSampleTimestamp: Long,
  overflowCount: Int = 0
) {
  override def toString: String = {
    s"BucketAggregationStats(active=$activeBucketCount, " +
      s"finalized=$finalizedBucketCount, latestTs=$latestSampleTimestamp, " +
      s"overflow=$overflowCount)"
  }
}
