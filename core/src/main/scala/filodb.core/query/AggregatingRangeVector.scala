package filodb.core.query

import java.util.concurrent.atomic.AtomicLong

import filodb.core.memstore.AggregatingTimeSeriesPartition
import filodb.core.store.{ChunkScanMethod, CountingChunkInfoIterator}
import filodb.memory.format.RowReader
import filodb.memory.format.vectors.MutableHistogram

/**
 * A RangeVector that includes both finalized vector data and in-memory aggregated bucket data.
 * This is used for AggregatingTimeSeriesPartition to make unfinished buckets queryable.
 *
 * @param key the range vector key
 * @param partition the aggregating partition (must be AggregatingTimeSeriesPartition)
 * @param chunkMethod the chunk scan method
 * @param columnIDs the columns to query
 * @param dataBytesScannedCtr counter for data bytes scanned
 * @param samplesScannedCtr counter for samples scanned
 * @param maxBytesScanned maximum bytes allowed to scan
 * @param queryId the query ID for error messages
 */
final case class AggregatingRangeVector(
  key: RangeVectorKey,
  partition: AggregatingTimeSeriesPartition,
  chunkMethod: ChunkScanMethod,
  columnIDs: Array[Int],
  dataBytesScannedCtr: AtomicLong,
  samplesScannedCtr: AtomicLong,
  maxBytesScanned: Long,
  queryId: String
) extends RangeVector {

  /**
   * Returns a cursor that merges finalized vector data with active bucket data.
   * Active buckets are returned after finalized data, sorted by timestamp.
   */
  def rows(): RangeVectorCursor = {
    // Get the base cursor for finalized vector data
    val baseCursor = partition.timeRangeRows(
      chunkMethod,
      columnIDs,
      new CountingChunkInfoIterator(
        partition.infos(chunkMethod), columnIDs, dataBytesScannedCtr,
        samplesScannedCtr, maxBytesScanned, queryId
      )
    )

    // Fast path: skip bucket iteration entirely when no active buckets exist
    if (!partition.hasActiveBuckets) {
      return baseCursor
    }

    // Always create MergingRangeVectorCursor when active buckets exist.
    // It handles the case where no buckets are in the query range gracefully.
    val bucketRowsIter = getActiveBucketRows(chunkMethod.startTime, chunkMethod.endTime)
    new MergingRangeVectorCursor(baseCursor, bucketRowsIter, columnIDs)
  }

  /**
   * Gets rows from active buckets that fall within the query time range.
   * Returns a lazy iterator of BucketRowData, sorted by timestamp (TreeMap order).
   * No intermediate collections (Set, Seq) are materialized; no redundant sort is performed.
   */
  // scalastyle:off null
  private def getActiveBucketRows(startTime: Long, endTime: Long): Iterator[BucketRowData] = {
    partition.bucketValuesIteratorInRange(startTime, endTime).map { case (bucketTs, allColumnValues) =>
      // Map column values to the requested column IDs
      val values = new Array[Any](columnIDs.length)
      var i = 0
      while (i < columnIDs.length) {
        val colIdx = columnIDs(i)
        if (colIdx == 0) {
          // Timestamp column - use the bucket timestamp
          values(i) = bucketTs
        } else if (colIdx < allColumnValues.length) {
          values(i) = allColumnValues(colIdx)
        } else {
          values(i) = null
        }
        i += 1
      }
      BucketRowData(bucketTs, values)
    }
  }
  // scalastyle:on null

  def publishInterval: Option[Long] = partition.publishInterval

  override def outputRange: Option[RvRange] = None

  def minResolutionMs: Int = partition.minResolutionMs
}

/**
 * Holds row data from an active bucket.
 */
private[query] case class BucketRowData(timestamp: Long, values: Array[Any])

/**
 * A cursor that merges finalized vector data with active bucket data.
 * Returns rows in timestamp order, with bucket data after vector data for the same timestamp.
 *
 * The merging strategy:
 * - First exhaust the base cursor (finalized vector data)
 * - Then return bucket rows that have timestamps > lastFinalizedTimestamp
 * - This avoids duplicates where buckets might overlap with recently finalized data
 */
private[query] class MergingRangeVectorCursor(
  baseCursor: RangeVectorCursor,
  bucketRowsIter: Iterator[BucketRowData],
  columnIDs: Array[Int]
) extends RangeVectorCursor {

  private var lastFinalizedTimestamp: Long = Long.MinValue
  private var exhaustedBase = false
  // scalastyle:off null
  private var currentBucketRow: BucketRowData = null  // null means no row staged
  private var lastBaseRow: RowReader = null  // reference to last row from base cursor
  // Prefetched next bucket row — avoids BufferedIterator wrapper overhead
  private var nextBucketRow: BucketRowData = if (bucketRowsIter.hasNext) bucketRowsIter.next() else null
  // scalastyle:on null

  // Pre-compute: does column 0 contain the timestamp?
  private val hasTimestampCol: Boolean = columnIDs.nonEmpty && columnIDs(0) == 0

  // The current row reader for bucket data
  private val bucketRowReader = new BucketDataRowReader(columnIDs)

  override def hasNext: Boolean = {
    if (!exhaustedBase && baseCursor.hasNext) {
      true
    } else {
      if (!exhaustedBase) {
        // Base cursor just became exhausted — capture last finalized timestamp once
        exhaustedBase = true
        // scalastyle:off null
        if (lastBaseRow != null && hasTimestampCol) {
          lastFinalizedTimestamp = lastBaseRow.getLong(0)
          lastBaseRow = null  // release reference
        }
        // scalastyle:on null
      }
      // Only find next bucket row if we don't already have one staged
      if (currentBucketRow != null) {
        true
      } else {
        currentBucketRow = findNextBucketRow()
        currentBucketRow != null
      }
    }
  }

  override def next(): RowReader = {
    if (!exhaustedBase) {
      val row = baseCursor.next()
      // Store reference to last row — timestamp is read lazily when base is exhausted
      lastBaseRow = row
      row
    } else {
      // scalastyle:off null
      if (currentBucketRow == null) throw new NoSuchElementException("No more rows")
      bucketRowReader.setData(currentBucketRow)
      currentBucketRow = null
      // scalastyle:on null
      bucketRowReader
    }
  }

  // Advance past bucket rows that are <= lastFinalizedTimestamp, then return the next one.
  // Since bucket rows are sorted by timestamp, we only need to skip forward (O(1) amortized).
  // Returns null if no more bucket rows are available.
  // scalastyle:off null
  private def findNextBucketRow(): BucketRowData = {
    while (nextBucketRow != null && nextBucketRow.timestamp <= lastFinalizedTimestamp) {
      nextBucketRow = if (bucketRowsIter.hasNext) bucketRowsIter.next() else null
    }
    if (nextBucketRow != null) {
      val result = nextBucketRow
      nextBucketRow = if (bucketRowsIter.hasNext) bucketRowsIter.next() else null
      result
    } else {
      null
    }
  }
  // scalastyle:on null

  override def close(): Unit = {
    baseCursor.close()
  }
}

/**
 * A RowReader that reads from bucket data (in-memory aggregated values).
 */
private[query] class BucketDataRowReader(columnIDs: Array[Int]) extends RowReader {
  // scalastyle:off null
  private var data: BucketRowData = _

  // Cache for serialized histogram buffers to avoid re-serializing per accessor call
  private var cachedBlobColumn: Int = -1
  private var cachedBlobBuffer: org.agrona.DirectBuffer = _

  def setData(bucketData: BucketRowData): Unit = {
    data = bucketData
    cachedBlobColumn = -1
    cachedBlobBuffer = null
  }

  override def notNull(columnNo: Int): Boolean = {
    columnNo < data.values.length && data.values(columnNo) != null
  }

  override def getBoolean(columnNo: Int): Boolean = {
    throw new UnsupportedOperationException("Boolean not supported in bucket data")
  }

  override def getInt(columnNo: Int): Int = {
    if (columnIDs(columnNo) == 0) data.timestamp.toInt
    else data.values(columnNo) match {
      case i: Int => i
      case l: Long => l.toInt
      case d: Double => d.toInt
      case _ => throw new IllegalArgumentException(s"Cannot convert ${data.values(columnNo)} to Int")
    }
  }

  override def getLong(columnNo: Int): Long = {
    if (columnIDs(columnNo) == 0) {
      data.timestamp
    } else {
      data.values(columnNo) match {
        case l: Long => l
        case i: Int => i.toLong
        case d: Double => d.toLong
        case _ => throw new IllegalArgumentException(s"Cannot convert ${data.values(columnNo)} to Long")
      }
    }
  }

  override def getDouble(columnNo: Int): Double = {
    if (columnIDs(columnNo) == 0) data.timestamp.toDouble
    else data.values(columnNo) match {
      case d: Double => d
      case l: Long => l.toDouble
      case i: Int => i.toDouble
      case f: Float => f.toDouble
      case _ => throw new IllegalArgumentException(s"Cannot convert ${data.values(columnNo)} to Double")
    }
  }

  override def getFloat(columnNo: Int): Float = getDouble(columnNo).toFloat

  override def getString(columnNo: Int): String = {
    if (data.values(columnNo) == null) "" else data.values(columnNo).toString
  }

  override def getAny(columnNo: Int): Any = {
    if (columnIDs(columnNo) == 0) data.timestamp else data.values(columnNo)
  }

  override def getHistogram(columnNo: Int): filodb.memory.format.vectors.Histogram = {
    data.values(columnNo) match {
      case h: MutableHistogram => h
      case h: filodb.memory.format.vectors.Histogram => h
      case null => filodb.memory.format.vectors.Histogram.empty
      case _ => throw new IllegalArgumentException(s"Cannot get histogram from ${data.values(columnNo)}")
    }
  }

  // Lazily serialize and cache the blob buffer for a given column
  private def getOrCacheBlobBuffer(columnNo: Int): org.agrona.DirectBuffer = {
    if (cachedBlobColumn == columnNo && cachedBlobBuffer != null) {
      return cachedBlobBuffer
    }
    val buf = data.values(columnNo) match {
      case h: MutableHistogram => h.serialize()
      case buf: org.agrona.DirectBuffer => buf
      case _ => null
    }
    cachedBlobColumn = columnNo
    cachedBlobBuffer = buf
    buf
  }

  override def getBlobBase(columnNo: Int): Any = {
    val buf = getOrCacheBlobBuffer(columnNo)
    if (buf != null) buf.byteArray() else null
  }

  override def getBlobOffset(columnNo: Int): Long = {
    val buf = getOrCacheBlobBuffer(columnNo)
    if (buf != null) buf.addressOffset() else 0L
  }

  override def getBlobNumBytes(columnNo: Int): Int = {
    val buf = getOrCacheBlobBuffer(columnNo)
    if (buf != null) (buf.getShort(0) & 0xFFFF) + 2 else 0
  }

  override def filoUTF8String(i: Int): filodb.memory.format.ZeroCopyUTF8String = null
  // scalastyle:on null
}
