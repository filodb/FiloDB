package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.memory.format.RowReader
import filodb.memory.format.vectors.{CustomBuckets, LongHistogram, MutableHistogram}

class AggregatingRangeVectorSpec extends AnyFunSpec with Matchers {

  // scalastyle:off null

  // Simple RowReader for testing base cursor rows
  private class SimpleRowReader(values: Array[Any]) extends RowReader {
    def notNull(columnNo: Int): Boolean = columnNo < values.length && values(columnNo) != null
    def getBoolean(columnNo: Int): Boolean = values(columnNo).asInstanceOf[Boolean]
    def getInt(columnNo: Int): Int = values(columnNo).asInstanceOf[Number].intValue()
    def getLong(columnNo: Int): Long = values(columnNo).asInstanceOf[Number].longValue()
    def getDouble(columnNo: Int): Double = values(columnNo).asInstanceOf[Number].doubleValue()
    def getFloat(columnNo: Int): Float = values(columnNo).asInstanceOf[Number].floatValue()
    def getString(columnNo: Int): String = if (values(columnNo) == null) "" else values(columnNo).toString
    def getAny(columnNo: Int): Any = values(columnNo)
    def getBlobBase(columnNo: Int): Any = null
    def getBlobOffset(columnNo: Int): Long = 0L
    def getBlobNumBytes(columnNo: Int): Int = 0
    override def filoUTF8String(i: Int): filodb.memory.format.ZeroCopyUTF8String = null
  }

  // Simple RangeVectorCursor backed by a list of rows, tracks close() calls
  private class TestCursor(rows: Seq[RowReader]) extends RangeVectorCursor {
    private val iter = rows.iterator
    var closed = false
    override def hasNext: Boolean = iter.hasNext
    override def next(): RowReader = iter.next()
    override def close(): Unit = { closed = true }
  }

  private def emptyCursor(): TestCursor = new TestCursor(Seq.empty)

  private def cursorWithRows(timestampValues: (Long, Double)*): TestCursor = {
    new TestCursor(timestampValues.map { case (ts, v) =>
      new SimpleRowReader(Array[Any](ts, v))
    })
  }

  private def bucketRow(ts: Long, value: Double): BucketRowData = {
    BucketRowData(ts, Array[Any](ts, value))
  }

  // Standard columnIDs: [0=timestamp, 1=value]
  private val tsValueColumnIDs = Array(0, 1)

  // Helper to collect all rows from a cursor as (timestamp, value) pairs
  private def collectRows(cursor: RangeVectorCursor): Seq[(Long, Double)] = {
    val result = scala.collection.mutable.ArrayBuffer.empty[(Long, Double)]
    while (cursor.hasNext) {
      val row = cursor.next()
      result += ((row.getLong(0), row.getDouble(1)))
    }
    result.toSeq
  }

  // ======================== MergingRangeVectorCursor ========================

  describe("MergingRangeVectorCursor") {

    describe("basic iteration") {
      it("should return only base cursor rows when no bucket rows exist") {
        val base = cursorWithRows((1000L, 10.0), (2000L, 20.0))
        val cursor = new MergingRangeVectorCursor(base, Iterator.empty, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows.map(_._1) shouldEqual Seq(1000L, 2000L)
        rows.map(_._2) shouldEqual Seq(10.0, 20.0)
      }

      it("should return only bucket rows when base cursor is empty") {
        val base = emptyCursor()
        val buckets = Seq(bucketRow(3000L, 30.0), bucketRow(4000L, 40.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows.map(_._1) shouldEqual Seq(3000L, 4000L)
        rows.map(_._2) shouldEqual Seq(30.0, 40.0)
      }

      it("should return empty when both base cursor and bucket rows are empty") {
        val base = emptyCursor()
        val cursor = new MergingRangeVectorCursor(base, Iterator.empty, tsValueColumnIDs)

        cursor.hasNext shouldBe false
      }

      it("should return base rows followed by bucket rows in order") {
        val base = cursorWithRows((1000L, 10.0), (2000L, 20.0))
        val buckets = Seq(bucketRow(3000L, 30.0), bucketRow(4000L, 40.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows shouldEqual Seq((1000L, 10.0), (2000L, 20.0), (3000L, 30.0), (4000L, 40.0))
      }

      it("should handle single base row and single bucket row") {
        val base = cursorWithRows((1000L, 10.0))
        val buckets = Seq(bucketRow(2000L, 20.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows shouldEqual Seq((1000L, 10.0), (2000L, 20.0))
      }
    }

    describe("deduplication - skipping bucket rows overlapping with finalized data") {
      it("should skip bucket rows with timestamps <= last finalized timestamp") {
        // Base rows end at ts=3000, so lastFinalizedTimestamp=3000
        val base = cursorWithRows((1000L, 10.0), (2000L, 20.0), (3000L, 30.0))
        // Bucket rows: 2500 and 3000 are <= 3000, only 4000 should pass
        val buckets = Seq(bucketRow(2500L, 25.0), bucketRow(3000L, 30.0), bucketRow(4000L, 40.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows.map(_._1) shouldEqual Seq(1000L, 2000L, 3000L, 4000L)
        rows.map(_._2) shouldEqual Seq(10.0, 20.0, 30.0, 40.0)
      }

      it("should skip all bucket rows when all timestamps <= last finalized") {
        val base = cursorWithRows((1000L, 10.0), (5000L, 50.0))
        val buckets = Seq(bucketRow(2000L, 20.0), bucketRow(3000L, 30.0), bucketRow(4000L, 40.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        // All bucket rows (2000, 3000, 4000) are <= 5000, so none pass
        rows.map(_._1) shouldEqual Seq(1000L, 5000L)
      }

      it("should return all bucket rows when base cursor is empty (lastFinalizedTs = Long.MinValue)") {
        val base = emptyCursor()
        val buckets = Seq(bucketRow(1000L, 10.0), bucketRow(2000L, 20.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        // lastFinalizedTimestamp = Long.MinValue, so all bucket rows pass
        rows.map(_._1) shouldEqual Seq(1000L, 2000L)
      }

      it("should skip bucket row at exactly the last finalized timestamp") {
        val base = cursorWithRows((2000L, 20.0))
        // Bucket at exactly 2000 should be skipped (<= last finalized)
        val buckets = Seq(bucketRow(2000L, 99.0), bucketRow(3000L, 30.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows.map(_._1) shouldEqual Seq(2000L, 3000L)
        // Value 99.0 from bucket at ts=2000 is skipped, 30.0 from ts=3000 is included
        rows.map(_._2) shouldEqual Seq(20.0, 30.0)
      }

      it("should handle gap between last finalized and first bucket row") {
        val base = cursorWithRows((1000L, 10.0))
        // First bucket row at 5000 is well after last finalized at 1000
        val buckets = Seq(bucketRow(5000L, 50.0), bucketRow(6000L, 60.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows shouldEqual Seq((1000L, 10.0), (5000L, 50.0), (6000L, 60.0))
      }

      it("should handle multiple bucket rows where some overlap and some do not") {
        // Base covers up to ts=3000
        val base = cursorWithRows((1000L, 10.0), (2000L, 20.0), (3000L, 30.0))
        // Buckets: 1500, 2500, 3000 overlap, 3500 and 4000 do not
        val buckets = Seq(
          bucketRow(1500L, 15.0), bucketRow(2500L, 25.0), bucketRow(3000L, 30.5),
          bucketRow(3500L, 35.0), bucketRow(4000L, 40.0)
        )
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows.map(_._1) shouldEqual Seq(1000L, 2000L, 3000L, 3500L, 4000L)
      }
    }

    describe("timestamp tracking") {
      it("should track last finalized timestamp from column 0 when columnIDs(0) == 0") {
        val base = cursorWithRows((1000L, 10.0), (3000L, 30.0))
        // Bucket at 2000 is <= 3000 (last finalized), 4000 is after
        val buckets = Seq(bucketRow(2000L, 20.0), bucketRow(4000L, 40.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows.map(_._1) shouldEqual Seq(1000L, 3000L, 4000L)
      }

      it("should not track timestamp when columnIDs(0) != 0") {
        // If first column is not timestamp (e.g., columnIDs = [1, 2])
        val nonTsColumnIDs = Array(1, 2)
        val base = cursorWithRows((1000L, 10.0), (3000L, 30.0))
        // Without timestamp tracking, lastFinalizedTimestamp stays at Long.MinValue
        // so all bucket rows should pass through
        val buckets = Seq(bucketRow(2000L, 20.0), bucketRow(4000L, 40.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, nonTsColumnIDs)

        val rows = collectRows(cursor)
        rows.length shouldEqual 4 // all 4 rows returned
      }

      it("should track progressively increasing timestamps from base cursor") {
        // Timestamps increase: 1000, 2000, 5000
        val base = cursorWithRows((1000L, 10.0), (2000L, 20.0), (5000L, 50.0))
        // Only bucket rows after 5000 (the last) should pass
        val buckets = Seq(bucketRow(3000L, 30.0), bucketRow(4000L, 40.0), bucketRow(6000L, 60.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        val rows = collectRows(cursor)
        rows.map(_._1) shouldEqual Seq(1000L, 2000L, 5000L, 6000L)
      }
    }

    describe("resource management") {
      it("should delegate close to base cursor") {
        val base = cursorWithRows((1000L, 10.0))
        val cursor = new MergingRangeVectorCursor(base, Iterator.empty, tsValueColumnIDs)

        base.closed shouldBe false
        cursor.close()
        base.closed shouldBe true
      }

      it("should throw NoSuchElementException when next() called after exhaustion") {
        val base = emptyCursor()
        val cursor = new MergingRangeVectorCursor(base, Iterator.empty, tsValueColumnIDs)

        cursor.hasNext shouldBe false
        assertThrows[NoSuchElementException] {
          cursor.next()
        }
      }

      it("should close base cursor even if bucket rows remain unconsumed") {
        val base = cursorWithRows((1000L, 10.0))
        val buckets = Seq(bucketRow(2000L, 20.0), bucketRow(3000L, 30.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        // Consume only the base row
        cursor.hasNext shouldBe true
        cursor.next()

        // Close without consuming bucket rows
        cursor.close()
        base.closed shouldBe true
      }
    }

    describe("hasNext idempotency") {
      it("should be idempotent - calling hasNext twice should not lose data") {
        val base = cursorWithRows((1000L, 10.0))
        val buckets = Seq(bucketRow(2000L, 20.0), bucketRow(3000L, 30.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        // Consume base row
        cursor.hasNext shouldBe true
        cursor.next().getLong(0) shouldEqual 1000L

        // Call hasNext twice in a row without next() — should be idempotent
        cursor.hasNext shouldBe true
        cursor.hasNext shouldBe true  // must still be true, not advance the iterator

        // The next row should be the FIRST bucket row (2000), not the second (3000)
        val row1 = cursor.next()
        row1.getLong(0) shouldEqual 2000L
        row1.getDouble(1) shouldEqual 20.0

        // Continue to the next bucket row
        cursor.hasNext shouldBe true
        val row2 = cursor.next()
        row2.getLong(0) shouldEqual 3000L
        row2.getDouble(1) shouldEqual 30.0

        cursor.hasNext shouldBe false
      }

      it("should be idempotent with empty base cursor") {
        val base = emptyCursor()
        val buckets = Seq(bucketRow(1000L, 10.0), bucketRow(2000L, 20.0))
        val cursor = new MergingRangeVectorCursor(base, buckets.iterator, tsValueColumnIDs)

        // Call hasNext multiple times before first next()
        cursor.hasNext shouldBe true
        cursor.hasNext shouldBe true
        cursor.hasNext shouldBe true

        // Should still get the first bucket row
        cursor.next().getLong(0) shouldEqual 1000L

        cursor.hasNext shouldBe true
        cursor.next().getLong(0) shouldEqual 2000L

        cursor.hasNext shouldBe false
      }

      it("should be idempotent when no more rows after exhaustion") {
        val base = cursorWithRows((1000L, 10.0))
        val cursor = new MergingRangeVectorCursor(base, Iterator.empty, tsValueColumnIDs)

        cursor.hasNext shouldBe true
        cursor.next()

        // Multiple hasNext calls at end — all should return false
        cursor.hasNext shouldBe false
        cursor.hasNext shouldBe false
        cursor.hasNext shouldBe false
      }
    }
  }

  // ======================== BucketDataRowReader ========================

  describe("BucketDataRowReader") {

    describe("timestamp handling") {
      it("should return bucket timestamp from getLong when columnIDs(columnNo) == 0") {
        val reader = new BucketDataRowReader(tsValueColumnIDs)
        reader.setData(BucketRowData(5000L, Array[Any](5000L, 42.0)))

        reader.getLong(0) shouldEqual 5000L
      }

      it("should return bucket timestamp from getAny when columnIDs(columnNo) == 0") {
        val reader = new BucketDataRowReader(tsValueColumnIDs)
        reader.setData(BucketRowData(5000L, Array[Any](5000L, 42.0)))

        reader.getAny(0) shouldEqual 5000L
      }

      it("should use bucket timestamp regardless of values array content for column 0") {
        val reader = new BucketDataRowReader(tsValueColumnIDs)
        // values(0) says 9999 but bucket timestamp is 5000
        reader.setData(BucketRowData(5000L, Array[Any](9999L, 42.0)))

        reader.getLong(0) shouldEqual 5000L
        reader.getAny(0) shouldEqual 5000L
      }
    }

    describe("getDouble type conversions") {
      it("should return Double directly") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 42.5)))

        reader.getDouble(1) shouldEqual 42.5
      }

      it("should convert Long to Double") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 100L)))

        reader.getDouble(1) shouldEqual 100.0
      }

      it("should convert Int to Double") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 50)))

        reader.getDouble(1) shouldEqual 50.0
      }

      it("should convert Float to Double") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 3.14f)))

        reader.getDouble(1) shouldEqual (3.14f.toDouble +- 0.001)
      }

      it("should throw IllegalArgumentException for unconvertible type") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, "not-a-number")))

        assertThrows[IllegalArgumentException] {
          reader.getDouble(1)
        }
      }
    }

    describe("getLong type conversions") {
      it("should return Long directly for non-timestamp column") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 999L)))

        reader.getLong(1) shouldEqual 999L
      }

      it("should convert Int to Long") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 42)))

        reader.getLong(1) shouldEqual 42L
      }

      it("should convert Double to Long (truncating)") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 99.9)))

        reader.getLong(1) shouldEqual 99L
      }

      it("should throw IllegalArgumentException for unconvertible type") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, "not-a-number")))

        assertThrows[IllegalArgumentException] {
          reader.getLong(1)
        }
      }
    }

    describe("getInt type conversions") {
      it("should return Int directly") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 7)))

        reader.getInt(1) shouldEqual 7
      }

      it("should convert Long to Int") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 123L)))

        reader.getInt(1) shouldEqual 123
      }

      it("should convert Double to Int (truncating)") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 45.7)))

        reader.getInt(1) shouldEqual 45
      }

      it("should throw IllegalArgumentException for unconvertible type") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, "not-a-number")))

        assertThrows[IllegalArgumentException] {
          reader.getInt(1)
        }
      }
    }

    describe("getFloat") {
      it("should delegate to getDouble and convert") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 2.5)))

        reader.getFloat(1) shouldEqual 2.5f
      }
    }

    describe("getString") {
      it("should return string representation of value") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 42.0)))

        reader.getString(1) shouldEqual "42.0"
      }

      it("should return empty string for null value") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, null)))

        reader.getString(1) shouldEqual ""
      }
    }

    describe("notNull") {
      it("should return true for non-null values") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 42.0)))

        reader.notNull(0) shouldBe true
        reader.notNull(1) shouldBe true
      }

      it("should return false for null values") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, null)))

        reader.notNull(1) shouldBe false
      }

      it("should return false for out-of-bounds column index") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 42.0)))

        reader.notNull(5) shouldBe false
      }
    }

    describe("getBoolean") {
      it("should throw UnsupportedOperationException") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, true)))

        assertThrows[UnsupportedOperationException] {
          reader.getBoolean(1)
        }
      }
    }

    describe("getAny") {
      it("should return bucket timestamp for timestamp column") {
        val reader = new BucketDataRowReader(tsValueColumnIDs)
        reader.setData(BucketRowData(5000L, Array[Any](5000L, 42.0)))

        reader.getAny(0) shouldEqual 5000L
      }

      it("should return value for non-timestamp column") {
        val reader = new BucketDataRowReader(tsValueColumnIDs)
        reader.setData(BucketRowData(5000L, Array[Any](5000L, 42.0)))

        reader.getAny(1) shouldEqual 42.0
      }
    }

    describe("histogram support") {
      it("should return MutableHistogram from getHistogram") {
        val hist = MutableHistogram(
          LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), Array(5L, 10L, 10L))
        )
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, hist)))

        val result = reader.getHistogram(1)
        result shouldBe a[MutableHistogram]
        result.numBuckets shouldEqual 3
        result.bucketValue(0) shouldEqual 5.0
        result.bucketValue(1) shouldEqual 10.0
      }

      it("should return Histogram.empty for null value") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, null)))

        val result = reader.getHistogram(1)
        result shouldEqual filodb.memory.format.vectors.Histogram.empty
      }

      it("should throw IllegalArgumentException for non-histogram value") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, "not-a-histogram")))

        assertThrows[IllegalArgumentException] {
          reader.getHistogram(1)
        }
      }
    }

    describe("blob serialization and caching") {
      it("should serialize MutableHistogram for blob access") {
        val hist = MutableHistogram(
          LongHistogram(CustomBuckets(Array(1.0, Double.PositiveInfinity)), Array(5L, 5L))
        )
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, hist)))

        reader.getBlobBase(1).asInstanceOf[AnyRef] should not be null
        reader.getBlobBase(1) shouldBe a[Array[_]]
        reader.getBlobNumBytes(1) should be > 0
        // getBlobOffset should be valid (non-negative for on-heap arrays)
        reader.getBlobOffset(1) should be >= 0L
      }

      it("should cache blob buffer for repeated access to same column") {
        val hist = MutableHistogram(
          LongHistogram(CustomBuckets(Array(1.0, Double.PositiveInfinity)), Array(5L, 5L))
        )
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, hist)))

        // First and second calls should return same byte array instance (cached)
        val base1 = reader.getBlobBase(1).asInstanceOf[AnyRef]
        val base2 = reader.getBlobBase(1).asInstanceOf[AnyRef]
        base1 shouldBe theSameInstanceAs(base2)

        // NumBytes should be consistent
        val bytes1 = reader.getBlobNumBytes(1)
        val bytes2 = reader.getBlobNumBytes(1)
        bytes1 shouldEqual bytes2
      }

      it("should invalidate cache when setData is called with new data") {
        val hist1 = MutableHistogram(
          LongHistogram(CustomBuckets(Array(1.0, Double.PositiveInfinity)), Array(5L, 5L))
        )
        val hist2 = MutableHistogram(
          LongHistogram(
            CustomBuckets(Array(1.0, 2.0, 5.0, Double.PositiveInfinity)),
            Array(3L, 7L, 12L, 12L)
          )
        )
        val reader = new BucketDataRowReader(Array(0, 1))

        reader.setData(BucketRowData(1000L, Array[Any](1000L, hist1)))
        val bytes1 = reader.getBlobNumBytes(1)

        // Set new data with a larger histogram - cache should be invalidated
        reader.setData(BucketRowData(2000L, Array[Any](2000L, hist2)))
        val bytes2 = reader.getBlobNumBytes(1)

        // Different histograms with different bucket counts produce different sizes
        bytes2 should not equal bytes1
      }

      it("should return null/0 for non-blob scalar values") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 42.0)))

        reader.getBlobBase(1).asInstanceOf[AnyRef] shouldBe null
        reader.getBlobOffset(1) shouldEqual 0L
        reader.getBlobNumBytes(1) shouldEqual 0
      }

      it("should pass through DirectBuffer values without re-serialization") {
        val hist = LongHistogram(CustomBuckets(Array(1.0, Double.PositiveInfinity)), Array(5L, 5L))
        val directBuf = hist.serialize(
          Some(new org.agrona.concurrent.UnsafeBuffer(new Array[Byte](4096)))
        )

        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, directBuf)))

        reader.getBlobBase(1).asInstanceOf[AnyRef] should not be null
        reader.getBlobNumBytes(1) should be > 0
      }

      it("should cache correctly across different column accesses") {
        val hist1 = MutableHistogram(
          LongHistogram(CustomBuckets(Array(1.0, Double.PositiveInfinity)), Array(5L, 5L))
        )
        val hist2 = MutableHistogram(
          LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), Array(3L, 7L, 7L))
        )
        // Two histogram columns
        val reader = new BucketDataRowReader(Array(0, 1, 2))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, hist1, hist2)))

        // Access column 1 first
        val base1_col1 = reader.getBlobBase(1)
        base1_col1.asInstanceOf[AnyRef] should not be null

        // Access column 2 - should produce different data (different histogram)
        val base1_col2 = reader.getBlobBase(2)
        base1_col2.asInstanceOf[AnyRef] should not be null

        // Access column 1 again - cache was invalidated when column 2 was accessed
        val base2_col1 = reader.getBlobBase(1)
        base2_col1.asInstanceOf[AnyRef] should not be null
      }
    }

    describe("setData reuse") {
      it("should allow reader to be reused with different data") {
        val reader = new BucketDataRowReader(Array(0, 1))

        reader.setData(BucketRowData(1000L, Array[Any](1000L, 10.0)))
        reader.getLong(0) shouldEqual 1000L
        reader.getDouble(1) shouldEqual 10.0

        reader.setData(BucketRowData(2000L, Array[Any](2000L, 20.0)))
        reader.getLong(0) shouldEqual 2000L
        reader.getDouble(1) shouldEqual 20.0
      }
    }

    describe("filoUTF8String") {
      it("should return null for any column") {
        val reader = new BucketDataRowReader(Array(0, 1))
        reader.setData(BucketRowData(1000L, Array[Any](1000L, 42.0)))

        reader.filoUTF8String(0) shouldBe null
        reader.filoUTF8String(1) shouldBe null
      }
    }
  }

  // ======================== BucketRowData ========================

  describe("BucketRowData") {
    it("should store timestamp and values") {
      val data = BucketRowData(5000L, Array[Any](5000L, 42.0, "label"))

      data.timestamp shouldEqual 5000L
      data.values(0) shouldEqual 5000L
      data.values(1) shouldEqual 42.0
      data.values(2) shouldEqual "label"
    }
  }

  // ======================== snapshotBucketsPooled (formerly snapshotBuckets) ========================

  describe("snapshotBucketsPooled (basic correctness)") {

    // Helper to allocate fresh per-cursor scratch — mirrors what `getActiveBucketRows` does
    def freshScratch(columnIDs: Array[Int])
      : (Array[Array[Double]], Array[MutableHistogram]) =
      (new Array[Array[Double]](columnIDs.length), new Array[MutableHistogram](columnIDs.length))

    describe("histogram snapshot and monotonic correction") {
      it("should apply makeMonotonic to non-monotonic histograms") {
        val bucketTs = 10000L

        // Non-monotonic: values [10, 5, 15] — bucket 1 < bucket 0
        val bucketBounds = CustomBuckets(Array(1.0, 10.0, Double.PositiveInfinity))
        val nonMonotonicHist = MutableHistogram(bucketBounds, Array(10.0, 5.0, 15.0))

        val columnIDs = Array(0, 1)
        val rawIterator = Iterator(
          (bucketTs, Array[Any](0L, nonMonotonicHist))
        )

        val (sv, ss) = freshScratch(columnIDs)
        val result = AggregatingRangeVector.snapshotBucketsPooled(rawIterator, columnIDs, sv, ss).toSeq

        result should have length 1
        val outputHist = result.head.values(1).asInstanceOf[MutableHistogram]
        // After makeMonotonic: [10, 10, 15]
        outputHist.values shouldEqual Array(10.0, 10.0, 15.0)
      }

      it("should snapshot histogram to isolate from concurrent ingest mutation") {
        val bucketTs = 10000L

        val bucketBounds = CustomBuckets(Array(1.0, 10.0, Double.PositiveInfinity))
        val originalHist = MutableHistogram(bucketBounds, Array(5.0, 10.0, 15.0))

        val columnIDs = Array(0, 1)
        val rawIterator = Iterator(
          (bucketTs, Array[Any](0L, originalHist))
        )

        val (sv, ss) = freshScratch(columnIDs)
        val result = AggregatingRangeVector.snapshotBucketsPooled(rawIterator, columnIDs, sv, ss).toSeq

        result should have length 1
        val snapshotHist = result.head.values(1).asInstanceOf[MutableHistogram]

        // Must be a different instance (snapshot, not a live reference to the source)
        snapshotHist should not be theSameInstanceAs(originalHist)
        snapshotHist.values should not be theSameInstanceAs(originalHist.values)

        // Simulate concurrent ingest mutating the original
        originalHist.values(0) = 999.0
        originalHist.values(1) = 999.0
        originalHist.values(2) = 999.0

        // Snapshot must be unaffected
        snapshotHist.values shouldEqual Array(5.0, 10.0, 15.0)
      }

      it("should not alter non-histogram scalar values") {
        val bucketTs = 10000L

        val rawIterator = Iterator(
          (bucketTs, Array[Any](0L, 42.0))
        )

        val (sv, ss) = freshScratch(tsValueColumnIDs)
        val result = AggregatingRangeVector
          .snapshotBucketsPooled(rawIterator, tsValueColumnIDs, sv, ss).toSeq

        result should have length 1
        result.head.values(1) shouldEqual 42.0
      }

      it("should handle already-monotonic histograms without corruption") {
        val bucketTs = 10000L

        val bucketBounds = CustomBuckets(Array(1.0, 10.0, Double.PositiveInfinity))
        val monotonicHist = MutableHistogram(bucketBounds, Array(5.0, 10.0, 15.0))

        val columnIDs = Array(0, 1)
        val rawIterator = Iterator(
          (bucketTs, Array[Any](0L, monotonicHist))
        )

        val (sv, ss) = freshScratch(columnIDs)
        val result = AggregatingRangeVector.snapshotBucketsPooled(rawIterator, columnIDs, sv, ss).toSeq

        val outputHist = result.head.values(1).asInstanceOf[MutableHistogram]
        outputHist.values shouldEqual Array(5.0, 10.0, 15.0)
      }

      it("should snapshot histograms and pass through scalars in mixed-column layout") {
        val bucketTs = 10000L

        val bucketBounds = CustomBuckets(Array(1.0, 10.0, Double.PositiveInfinity))
        val hist = MutableHistogram(bucketBounds, Array(10.0, 5.0, 15.0))

        // columnIDs: [0=timestamp, 1=histogram, 2=scalar]
        val columnIDs = Array(0, 1, 2)
        val rawIterator = Iterator(
          (bucketTs, Array[Any](0L, hist, 42.0))
        )

        val (sv, ss) = freshScratch(columnIDs)
        val result = AggregatingRangeVector.snapshotBucketsPooled(rawIterator, columnIDs, sv, ss).toSeq

        result should have length 1
        val outputHist = result.head.values(1).asInstanceOf[MutableHistogram]
        // Histogram snapshotted and corrected: [10, 10, 15]
        outputHist.values shouldEqual Array(10.0, 10.0, 15.0)
        outputHist should not be theSameInstanceAs(hist)
        // Scalar passed through unchanged
        result.head.values(2) shouldEqual 42.0
      }

      it("should handle empty raw iterator gracefully") {
        val (sv, ss) = freshScratch(tsValueColumnIDs)
        val result = AggregatingRangeVector
          .snapshotBucketsPooled(Iterator.empty, tsValueColumnIDs, sv, ss).toSeq
        result shouldBe empty
      }
    }
  }

  // ======================== snapshotBucketsPooled ========================

  describe("snapshotBucketsPooled") {

    describe("pooling correctness") {
      it("should produce correct histogram values within each next() call across many buckets") {
        // Build 5 buckets with progressively larger values
        val bounds = CustomBuckets(Array(1.0, 10.0, Double.PositiveInfinity))
        val raw = (0 until 5).iterator.map { i =>
          val h = MutableHistogram(bounds, Array(i * 1.0, i * 2.0, i * 3.0))
          ((1000L + i * 100).toLong, Array[Any](0L, h))
        }

        val columnIDs = Array(0, 1)
        val scratchValues = new Array[Array[Double]](columnIDs.length)
        val scratchShells = new Array[MutableHistogram](columnIDs.length)

        val iter = AggregatingRangeVector.snapshotBucketsPooled(
          raw, columnIDs, scratchValues, scratchShells
        )

        // Read each row's histogram values IMMEDIATELY after next() (live-ref contract)
        val snapshotted = scala.collection.mutable.ArrayBuffer.empty[Array[Double]]
        while (iter.hasNext) {
          val row = iter.next()
          val h = row.values(1).asInstanceOf[MutableHistogram]
          snapshotted += h.values.clone()
        }

        snapshotted should have length 5
        snapshotted(0) shouldEqual Array(0.0, 0.0, 0.0)
        snapshotted(1) shouldEqual Array(1.0, 2.0, 3.0)
        snapshotted(2) shouldEqual Array(2.0, 4.0, 6.0)
        snapshotted(3) shouldEqual Array(3.0, 6.0, 9.0)
        snapshotted(4) shouldEqual Array(4.0, 8.0, 12.0)
      }

      it("should return the same MutableHistogram shell instance across iterations (intentional pooling)") {
        val bounds = CustomBuckets(Array(1.0, Double.PositiveInfinity))
        val h1 = MutableHistogram(bounds, Array(1.0, 2.0))
        val h2 = MutableHistogram(bounds, Array(3.0, 4.0))

        val columnIDs = Array(0, 1)
        val scratchValues = new Array[Array[Double]](columnIDs.length)
        val scratchShells = new Array[MutableHistogram](columnIDs.length)

        val iter = AggregatingRangeVector.snapshotBucketsPooled(
          Iterator((100L, Array[Any](0L, h1)), (200L, Array[Any](0L, h2))),
          columnIDs, scratchValues, scratchShells
        )

        val row1 = iter.next()
        val ref1 = row1.values(1)
        val row2 = iter.next()
        val ref2 = row2.values(1)

        // The pool reuses one shell per column — same identity across rows is the contract
        ref1.asInstanceOf[AnyRef] should be theSameInstanceAs ref2.asInstanceOf[AnyRef]
      }

      it("should isolate the snapshot from concurrent mutation of the source histogram within one row") {
        val bounds = CustomBuckets(Array(1.0, 10.0, Double.PositiveInfinity))
        val source = MutableHistogram(bounds, Array(5.0, 10.0, 15.0))

        val columnIDs = Array(0, 1)
        val scratchValues = new Array[Array[Double]](columnIDs.length)
        val scratchShells = new Array[MutableHistogram](columnIDs.length)

        val iter = AggregatingRangeVector.snapshotBucketsPooled(
          Iterator((100L, Array[Any](0L, source))),
          columnIDs, scratchValues, scratchShells
        )

        val row = iter.next()
        val snap = row.values(1).asInstanceOf[MutableHistogram]
        // Different shell from the source
        snap should not be theSameInstanceAs(source)
        snap.values should not be theSameInstanceAs(source.values)

        // Mutate source — snapshot must be unaffected
        source.values(0) = 999.0
        source.values(1) = 999.0
        source.values(2) = 999.0

        snap.values shouldEqual Array(5.0, 10.0, 15.0)
      }
    }

    describe("monotonicity") {
      it("should apply makeMonotonic per bucket independently") {
        val bounds = CustomBuckets(Array(1.0, 10.0, Double.PositiveInfinity))
        val h1 = MutableHistogram(bounds, Array(10.0, 5.0, 15.0))  // non-monotonic
        val h2 = MutableHistogram(bounds, Array(3.0, 1.0, 4.0))    // non-monotonic

        val columnIDs = Array(0, 1)
        val scratchValues = new Array[Array[Double]](columnIDs.length)
        val scratchShells = new Array[MutableHistogram](columnIDs.length)

        val iter = AggregatingRangeVector.snapshotBucketsPooled(
          Iterator((100L, Array[Any](0L, h1)), (200L, Array[Any](0L, h2))),
          columnIDs, scratchValues, scratchShells
        )

        val snap1 = iter.next().values(1).asInstanceOf[MutableHistogram].values.clone()
        val snap2 = iter.next().values(1).asInstanceOf[MutableHistogram].values.clone()

        // makeMonotonic carries forward the running max
        snap1 shouldEqual Array(10.0, 10.0, 15.0)
        snap2 shouldEqual Array(3.0, 3.0, 4.0)
      }
    }

    describe("multiple histogram columns") {
      it("should use separate scratch shells per histogram column") {
        val bounds = CustomBuckets(Array(1.0, Double.PositiveInfinity))
        val h1 = MutableHistogram(bounds, Array(1.0, 2.0))
        val h2 = MutableHistogram(bounds, Array(3.0, 4.0))

        val columnIDs = Array(0, 1, 2)
        val scratchValues = new Array[Array[Double]](columnIDs.length)
        val scratchShells = new Array[MutableHistogram](columnIDs.length)

        val iter = AggregatingRangeVector.snapshotBucketsPooled(
          Iterator((100L, Array[Any](0L, h1, h2))),
          columnIDs, scratchValues, scratchShells
        )

        val row = iter.next()
        val v1 = row.values(1).asInstanceOf[MutableHistogram]
        val v2 = row.values(2).asInstanceOf[MutableHistogram]

        // Per-column scratches must not alias
        v1.asInstanceOf[AnyRef] should not be theSameInstanceAs(v2.asInstanceOf[AnyRef])
        v1.values.asInstanceOf[AnyRef] should not be theSameInstanceAs(v2.values.asInstanceOf[AnyRef])
        v1.values shouldEqual Array(1.0, 2.0)
        v2.values shouldEqual Array(3.0, 4.0)
      }
    }

    describe("schema changes") {
      it("should reallocate scratch when bucket count changes between rows") {
        val bounds16 = CustomBuckets((1 to 15).map(_.toDouble).toArray :+ Double.PositiveInfinity)
        val bounds64 = CustomBuckets((1 to 63).map(_.toDouble).toArray :+ Double.PositiveInfinity)

        val small = MutableHistogram(bounds16, (0 until 16).map(_.toDouble).toArray)
        val large = MutableHistogram(bounds64, (0 until 64).map(_.toDouble).toArray)

        val columnIDs = Array(0, 1)
        val scratchValues = new Array[Array[Double]](columnIDs.length)
        val scratchShells = new Array[MutableHistogram](columnIDs.length)

        val iter = AggregatingRangeVector.snapshotBucketsPooled(
          Iterator((100L, Array[Any](0L, small)), (200L, Array[Any](0L, large))),
          columnIDs, scratchValues, scratchShells
        )

        val snap1 = iter.next().values(1).asInstanceOf[MutableHistogram].values.clone()
        val snap2 = iter.next().values(1).asInstanceOf[MutableHistogram].values.clone()

        snap1.length shouldEqual 16
        snap2.length shouldEqual 64
        snap1 shouldEqual (0 until 16).map(_.toDouble).toArray
        snap2 shouldEqual (0 until 64).map(_.toDouble).toArray
      }
    }

    describe("non-histogram passthrough") {
      it("should pass through scalar values without modification") {
        val columnIDs = Array(0, 1)
        val scratchValues = new Array[Array[Double]](columnIDs.length)
        val scratchShells = new Array[MutableHistogram](columnIDs.length)

        val iter = AggregatingRangeVector.snapshotBucketsPooled(
          Iterator((100L, Array[Any](0L, 42.0))),
          columnIDs, scratchValues, scratchShells
        )

        val row = iter.next()
        row.values(1) shouldEqual 42.0
      }

      it("should handle empty raw iterator gracefully") {
        val columnIDs = tsValueColumnIDs
        val scratchValues = new Array[Array[Double]](columnIDs.length)
        val scratchShells = new Array[MutableHistogram](columnIDs.length)

        val iter = AggregatingRangeVector.snapshotBucketsPooled(
          Iterator.empty, columnIDs, scratchValues, scratchShells
        )
        iter.hasNext shouldBe false
      }
    }
  }

  // scalastyle:on null
}
