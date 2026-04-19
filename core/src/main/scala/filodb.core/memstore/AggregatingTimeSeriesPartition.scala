package filodb.core.memstore

import com.typesafe.scalalogging.StrictLogging

import filodb.core.memstore.aggregation._
import filodb.core.metadata.{Column, Schema}
import filodb.memory.BlockMemFactory
import filodb.memory.format.RowReader
import filodb.memory.format.vectors.MutableHistogram

/**
 * A TimeSeriesPartition that supports time-bucketed aggregation for handling out-of-order samples.
 *
 * This partition extends the standard TimeSeriesPartition with aggregation buffer capabilities.
 * Samples are aggregated within time buckets before being ingested into chunks. This allows
 * handling out-of-order samples within a configurable tolerance window.
 *
 * Design (Bucket-First Approach):
 * - ALL aggregated values (both scalar and histogram) are kept in memory until finalization
 * - Each sample is aggregated into the appropriate bucket for all columns
 * - When buckets exit the tolerance window, complete rows with all columns are written at once
 * - This avoids issues with partial column writes and chunk lifecycle transitions
 * - Aggregated data is queryable from memory for in-progress buckets
 *
 * @param partID partition ID
 * @param schema the schema defining columns and aggregation configs
 * @param partitionKey pointer to partition key
 * @param shardInfo shard information
 * @param initMapSize initial size of the chunk map
 */
class AggregatingTimeSeriesPartition(
  partID: Int,
  schema: Schema,
  partitionKey: Long,
  shardInfo: TimeSeriesShardInfo,
  initMapSize: Int
) extends TimeSeriesPartition(partID, schema, partitionKey, shardInfo, initMapSize)
  with StrictLogging {

  import AggregatingTimeSeriesPartition._

  // Build aggregation configs from schema-level aggregators
  private val aggConfigs: Array[Option[AggregationConfig]] = {
    val configMap = schema.data.aggregators.map(a => a.columnId -> a.aggType).toMap
    schema.data.columns.indices.map { idx =>
      configMap.get(idx).map(aggType =>
        AggregationConfig(idx, aggType,
          schema.data.aggregationIntervalMs,
          schema.data.aggregationOooToleranceMs))
    }.toArray
  }

  // Check if any column has aggregation configured
  private val hasAnyAggregation: Boolean = aggConfigs.exists(_.isDefined)

  // Guard to prevent re-entrant switchBuffers calls (flushAllBuckets -> super.ingest -> switchBuffers)
  private var isSwitchingBuffers: Boolean = false

  // Determine which columns are histogram columns
  private val isHistogramColumn: Array[Boolean] = {
    schema.data.columns.map { col =>
      col.columnType == Column.ColumnType.HistogramColumn
    }.toArray
  }

  // Pre-compute column types for type-specialized aggregation
  private val columnTypes: Array[Column.ColumnType] = schema.data.columns.map(_.columnType).toArray

  // If no aggregation, behave as a normal partition
  if (!hasAnyAggregation) {
    logger.info(s"AggregatingTimeSeriesPartition created but no aggregation configured for partition $partID")
  }

  // Central bucket aggregation state managing ALL columns
  private lazy val bucketState: BucketAggregationState = {
    new BucketAggregationState(aggConfigs, schema.numDataColumns, columnTypes)
  }

  // Get the primary aggregation config (used for interval/tolerance calculations)
  private lazy val primaryConfig: Option[AggregationConfig] = aggConfigs.flatten.headOption

  /**
   * Overrides the standard ingest method to add aggregation support.
   *
   * Flow:
   * 1. If no aggregation configured, delegate to super.ingest()
   * 2. Extract column values from row
   * 3. Aggregate all values into the appropriate bucket
   * 4. Finalize and write buckets that have exited the tolerance window
   *
   * Note: All aggregated values are kept in memory until finalization.
   * Complete rows with all columns are written at once during finalization.
   */
  override def ingest(
    ingestionTime: Long,
    row: RowReader,
    overflowBlockHolder: BlockMemFactory,
    createChunkAtFlushBoundary: Boolean,
    flushIntervalMillis: Option[Long],
    acceptDuplicateSamples: Boolean,
    maxChunkTime: Long = Long.MaxValue
  ): Unit = {

    // If no aggregation configured, use standard ingestion
    if (!hasAnyAggregation) {
      super.ingest(ingestionTime, row, overflowBlockHolder, createChunkAtFlushBoundary,
        flushIntervalMillis, acceptDuplicateSamples, maxChunkTime)
      return
    }

    val timestamp = schema.timestamp(row)

    // Pass RowReader directly to avoid Array[Any] boxing
    val aggregated = bucketState.aggregate(timestamp, ingestionTime, row)

    if (!aggregated) {
      // Sample was outside tolerance or bucket was finalized
      logger.debug(s"Sample at $timestamp dropped for partition $partID (outside tolerance or finalized)")
      shardInfo.stats.outOfOrderDropped.increment()
    }

    // Finalize buckets that have exited the tolerance window
    finalizeOldBuckets(ingestionTime, overflowBlockHolder, createChunkAtFlushBoundary,
      flushIntervalMillis, acceptDuplicateSamples, maxChunkTime)
  }

  /**
   * Finalizes buckets that have exited the tolerance window.
   * Writes complete rows with all columns to the vectors.
   */
  private def finalizeOldBuckets(
    ingestionTime: Long,
    overflowBlockHolder: BlockMemFactory,
    createChunkAtFlushBoundary: Boolean,
    flushIntervalMillis: Option[Long],
    acceptDuplicateSamples: Boolean,
    maxChunkTime: Long
  ): Unit = {
    primaryConfig.foreach { config =>
      val thresholdTs = TimeBucket.ceilToBucket(
        ingestionTime - config.oooToleranceMs,
        config.intervalMs
      )

      // Fast guard: skip if no active buckets are old enough
      if (bucketState.earliestBucketTimestamp < thresholdTs) {
        val bucketsToFinalize = bucketState.getBucketsToFinalize(thresholdTs)

        if (bucketsToFinalize.nonEmpty) {
          logger.debug(s"Finalizing ${bucketsToFinalize.size} buckets for partition $partID")

          bucketsToFinalize.foreach { bucketTs =>
            bucketState.getBucketValues(bucketTs).foreach { columnValues =>
              // Create a complete row with all columns
              val aggregatedRow = new CompleteAggregatedRow(bucketTs, columnValues, isHistogramColumn)

              logger.trace(s"Writing finalized bucket $bucketTs to vectors for partition $partID")

              // Write the complete row using super.ingest
              // Use acceptDuplicateSamples=true since buckets are written in order
              super.ingest(ingestionTime, aggregatedRow, overflowBlockHolder,
                createChunkAtFlushBoundary, flushIntervalMillis,
                acceptDuplicateSamples = true, maxChunkTime)
            }

            bucketState.markFinalized(bucketTs)
          }

          // Periodically clean up old finalized tracking
          bucketState.cleanupOldFinalizedTracking(thresholdTs)
        }
      }
    }
  }

  /**
   * Gets the current aggregated histogram for a bucket timestamp.
   * Used by query path to read in-memory (not yet finalized) histograms.
   *
   * @param colIdx the column index
   * @param bucketTs the bucket timestamp
   * @return Some(histogram) if found in active state, None if finalized or not found
   */
  def getAggregatedHistogram(colIdx: Int, bucketTs: Long): Option[MutableHistogram] = {
    bucketState.getAggregatedHistogram(colIdx, bucketTs)
  }

  /**
   * Gets the current aggregated scalar value for a bucket timestamp.
   * Used by query path to read in-memory (not yet finalized) values.
   *
   * @param colIdx the column index
   * @param bucketTs the bucket timestamp
   * @return Some(value) if found in active state, None if finalized or not found
   */
  def getAggregatedValue(colIdx: Int, bucketTs: Long): Option[Any] = {
    bucketState.getBucketValues(bucketTs).map(_(colIdx))
  }

  /**
   * Returns statistics about the bucket aggregation state.
   */
  def bucketAggregationStats: BucketAggregationStats = bucketState.stats

  /**
   * Returns the set of active bucket timestamps.
   * Used by the query layer to iterate over in-memory bucket data.
   */
  def activeBucketTimestamps: Set[Long] = bucketState.activeBucketTimestamps

  /**
   * Returns true if there are any active (non-finalized) buckets.
   * Cheap O(1) check to short-circuit query path when no buckets exist.
   */
  def hasActiveBuckets: Boolean = bucketState.hasActiveBuckets

  /**
   * Returns an iterator over active bucket values in [startTime, endTime].
   * For histogram columns, returns MutableHistogram objects (not serialized).
   * Uses TreeMap range view — no intermediate collections are allocated.
   */
  def bucketValuesIteratorInRange(startTime: Long, endTime: Long): Iterator[(Long, Array[Any])] = {
    bucketState.bucketValuesIteratorInRange(startTime, endTime)
  }

  /**
   * Gets all column values for a specific bucket.
   * Returns an array where index 0 is the bucket timestamp.
   *
   * @param bucketTs the bucket timestamp
   * @return Some(array of values) if bucket exists, None otherwise
   */
  def getBucketColumnValues(bucketTs: Long): Option[Array[Any]] = {
    bucketState.getBucketValues(bucketTs)
  }

  /**
   * Returns the primary aggregation config (used for interval/tolerance calculations).
   * Returns None if no aggregation is configured.
   */
  def aggregationConfig: Option[AggregationConfig] = primaryConfig

  /**
   * Forces emission of all active buckets.
   * Used during partition shutdown or for testing.
   */
  def flushAllBuckets(
    ingestionTime: Long,
    overflowBlockHolder: BlockMemFactory,
    createChunkAtFlushBoundary: Boolean,
    flushIntervalMillis: Option[Long],
    acceptDuplicateSamples: Boolean,
    maxChunkTime: Long = Long.MaxValue
  ): Unit = {
    // Finalize all active buckets regardless of tolerance
    val allBuckets = bucketState.activeBucketTimestamps.toSeq.sorted

    if (allBuckets.nonEmpty) {
      logger.debug(s"Force flushing ${allBuckets.size} buckets for partition $partID")

      allBuckets.foreach { bucketTs =>
        bucketState.getBucketValues(bucketTs).foreach { columnValues =>
          val aggregatedRow = new CompleteAggregatedRow(bucketTs, columnValues, isHistogramColumn)

          super.ingest(ingestionTime, aggregatedRow, overflowBlockHolder,
            createChunkAtFlushBoundary, flushIntervalMillis,
            acceptDuplicateSamples = true, maxChunkTime)
        }

        bucketState.markFinalized(bucketTs)
      }
    }
  }

  /**
   * Override switchBuffers to flush all active buckets before switching.
   * This ensures no data is lost when chunks are sealed.
   */
  override def switchBuffers(blockHolder: BlockMemFactory, encode: Boolean = false): Boolean = {
    if (hasAnyAggregation && !isSwitchingBuffers) {
      isSwitchingBuffers = true
      try {
        // Use the latest sample timestamp from bucket state instead of wall clock
        val latestTs = bucketState.stats.latestSampleTimestamp
        val flushTime = if (latestTs == Long.MinValue) 0L else latestTs
        flushAllBuckets(flushTime, blockHolder, createChunkAtFlushBoundary = false,
          flushIntervalMillis = None, acceptDuplicateSamples = true)
      } finally {
        isSwitchingBuffers = false
      }
    }

    super.switchBuffers(blockHolder, encode)
  }
}

object AggregatingTimeSeriesPartition {

  /**
   * A RowReader implementation for complete aggregated rows with all columns.
   * Used when writing finalized buckets to vectors.
   */
  private class CompleteAggregatedRow(
    timestamp: Long,
    columnValues: Array[Any],
    isHistogramColumn: Array[Boolean]
  ) extends RowReader {

    // scalastyle:off null
    def notNull(columnNo: Int): Boolean = {
      if (columnNo == 0) true
      else if (columnNo < columnValues.length) columnValues(columnNo) != null
      else false
    }

    def getBoolean(columnNo: Int): Boolean =
      throw new UnsupportedOperationException("Boolean not supported in aggregated row")

    def getInt(columnNo: Int): Int = {
      val value = if (columnNo == 0) timestamp else columnValues(columnNo)
      value match {
        case i: Int => i
        case l: Long => l.toInt
        case d: Double => d.toInt
        case _ => throw new IllegalArgumentException(s"Cannot convert $value to Int")
      }
    }

    def getLong(columnNo: Int): Long = {
      if (columnNo == 0) timestamp
      else {
        val value = columnValues(columnNo)
        value match {
          case l: Long => l
          case i: Int => i.toLong
          case d: Double => d.toLong
          case _ => throw new IllegalArgumentException(s"Cannot convert $value to Long")
        }
      }
    }

    def getDouble(columnNo: Int): Double = {
      val value = if (columnNo == 0) timestamp.toDouble else columnValues(columnNo)
      value match {
        case d: Double => d
        case l: Long => l.toDouble
        case i: Int => i.toDouble
        case _ => throw new IllegalArgumentException(s"Cannot convert $value to Double")
      }
    }

    def getFloat(columnNo: Int): Float = getDouble(columnNo).toFloat

    def getString(columnNo: Int): String = {
      val value = if (columnNo == 0) timestamp.toString else columnValues(columnNo)
      if (value == null) "" else value.toString
    }

    def getAny(columnNo: Int): Any = {
      if (columnNo == 0) timestamp
      else if (columnNo < columnValues.length) columnValues(columnNo)
      else null
    }

    def getBlobBase(columnNo: Int): Any = {
      if (columnNo == 0 || columnNo >= columnValues.length) null
      else {
        columnValues(columnNo) match {
          case buf: org.agrona.MutableDirectBuffer => buf.byteArray()
          case buf: org.agrona.DirectBuffer => buf.byteArray()
          case _ => null
        }
      }
    }

    def getBlobOffset(columnNo: Int): Long = {
      if (columnNo == 0 || columnNo >= columnValues.length) 0L
      else {
        columnValues(columnNo) match {
          case buf: org.agrona.MutableDirectBuffer => buf.addressOffset()
          case buf: org.agrona.DirectBuffer => buf.addressOffset()
          case _ => 0L
        }
      }
    }

    def getBlobNumBytes(columnNo: Int): Int = {
      if (columnNo == 0 || columnNo >= columnValues.length) 0
      else {
        columnValues(columnNo) match {
          case buf: org.agrona.MutableDirectBuffer =>
            // BinaryHistogram format: 2-byte length + data
            (buf.getShort(0) & 0xFFFF) + 2
          case buf: org.agrona.DirectBuffer =>
            (buf.getShort(0) & 0xFFFF) + 2
          case _ => 0
        }
      }
    }

    override def filoUTF8String(i: Int): filodb.memory.format.ZeroCopyUTF8String = null
    // scalastyle:on null
  }
}
