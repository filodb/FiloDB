package filodb.core.query

import java.util.concurrent.atomic.AtomicLong

import filodb.core.memstore.AggregatingTimeSeriesPartition
import filodb.core.metadata.Column
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

    // Get active bucket data within the query time range
    val bucketRows = getActiveBucketRows(chunkMethod.startTime, chunkMethod.endTime)

    if (bucketRows.isEmpty) {
      baseCursor
    } else {
      new MergingRangeVectorCursor(baseCursor, bucketRows.iterator, columnIDs)
    }
  }

  /**
   * Gets rows from active buckets that fall within the query time range.
   * Returns a sequence of BucketRowData, sorted by timestamp.
   */
  private def getActiveBucketRows(startTime: Long, endTime: Long): Seq[BucketRowData] = {
    // Get all active bucket timestamps
    val allBuckets = partition.activeBucketTimestamps

    if (allBuckets.isEmpty) {
      return Seq.empty
    }

    // Filter buckets within the query time range
    val bucketsInRange = allBuckets.filter(ts => ts >= startTime && ts <= endTime).toSeq.sorted

    // Determine which columns are histogram columns for proper value extraction
    val isHistogramCol = columnIDs.map { colIdx =>
      if (colIdx == 0) false // timestamp column
      else if (colIdx < partition.schema.data.columns.size) {
        partition.schema.data.columns(colIdx).columnType == Column.ColumnType.HistogramColumn
      } else false
    }

    // Collect rows from active buckets
    bucketsInRange.flatMap { bucketTs =>
      partition.getBucketColumnValues(bucketTs).map { allColumnValues =>
        // Map column values to the requested column IDs
        val values = new Array[Any](columnIDs.length)
        var i = 0
        while (i < columnIDs.length) {
          val colIdx = columnIDs(i)
          if (colIdx == 0) {
            // Timestamp column - use the bucket timestamp
            values(i) = bucketTs
          } else if (colIdx < allColumnValues.length) {
            // For histogram columns, get the MutableHistogram directly
            if (isHistogramCol(i)) {
              values(i) = partition.getAggregatedHistogram(colIdx, bucketTs).orNull
            } else {
              values(i) = allColumnValues(colIdx)
            }
          } else {
            values(i) = None.orNull
          }
          i += 1
        }
        BucketRowData(bucketTs, values)
      }
    }
  }

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
  private var currentBucketRow: Option[BucketRowData] = None

  // Buffer to hold bucket rows that we haven't returned yet
  private val pendingBucketRows = bucketRowsIter.toBuffer

  // The current row reader for bucket data
  private val bucketRowReader = new BucketDataRowReader(columnIDs)

  override def hasNext: Boolean = {
    if (!exhaustedBase && baseCursor.hasNext) {
      true
    } else {
      exhaustedBase = true
      // Find bucket rows that are after the last finalized timestamp
      currentBucketRow = findNextBucketRow()
      currentBucketRow.isDefined
    }
  }

  override def next(): RowReader = {
    if (!exhaustedBase) {
      val row = baseCursor.next()
      // Track the last finalized timestamp (column 0 is always timestamp)
      if (columnIDs.nonEmpty && columnIDs(0) == 0) {
        lastFinalizedTimestamp = row.getLong(0)
      }
      row
    } else {
      currentBucketRow match {
        case Some(bucketData) =>
          bucketRowReader.setData(bucketData)
          currentBucketRow = None
          bucketRowReader
        case None =>
          throw new NoSuchElementException("No more rows")
      }
    }
  }

  private def findNextBucketRow(): Option[BucketRowData] = {
    val idx = pendingBucketRows.indexWhere(_.timestamp > lastFinalizedTimestamp)
    if (idx >= 0) {
      val row = pendingBucketRows(idx)
      pendingBucketRows.remove(idx)
      Some(row)
    } else {
      None
    }
  }

  override def close(): Unit = {
    baseCursor.close()
  }
}

/**
 * A RowReader that reads from bucket data (in-memory aggregated values).
 */
private[query] class BucketDataRowReader(columnIDs: Array[Int]) extends RowReader {
  private var data: BucketRowData = _

  def setData(bucketData: BucketRowData): Unit = {
    data = bucketData
  }

  override def notNull(columnNo: Int): Boolean = {
    columnNo < data.values.length && data.values(columnNo) != null
  }

  override def getBoolean(columnNo: Int): Boolean = {
    throw new UnsupportedOperationException("Boolean not supported in bucket data")
  }

  override def getInt(columnNo: Int): Int = {
    data.values(columnNo) match {
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
    data.values(columnNo) match {
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
    // scalastyle:off null
    data.values(columnNo) match {
      case h: MutableHistogram => h
      case h: filodb.memory.format.vectors.Histogram => h
      case null => filodb.memory.format.vectors.Histogram.empty
      case _ => throw new IllegalArgumentException(s"Cannot get histogram from ${data.values(columnNo)}")
    }
    // scalastyle:on null
  }

  override def getBlobBase(columnNo: Int): Any = {
    data.values(columnNo) match {
      case h: MutableHistogram =>
        val buf = h.serialize()
        buf.byteArray()
      case buf: org.agrona.DirectBuffer =>
        buf.byteArray()
      case _ => None.orNull
    }
  }

  override def getBlobOffset(columnNo: Int): Long = {
    data.values(columnNo) match {
      case h: MutableHistogram =>
        val buf = h.serialize()
        buf.addressOffset()
      case buf: org.agrona.DirectBuffer =>
        buf.addressOffset()
      case _ => 0L
    }
  }

  override def getBlobNumBytes(columnNo: Int): Int = {
    data.values(columnNo) match {
      case h: MutableHistogram =>
        val buf = h.serialize()
        (buf.getShort(0) & 0xFFFF) + 2
      case buf: org.agrona.DirectBuffer =>
        (buf.getShort(0) & 0xFFFF) + 2
      case _ => 0
    }
  }

  override def filoUTF8String(i: Int): filodb.memory.format.ZeroCopyUTF8String = None.orNull
}
