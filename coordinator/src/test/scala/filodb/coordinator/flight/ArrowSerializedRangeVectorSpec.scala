package filodb.coordinator.flight

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.arrow.memory.RootAllocator

import filodb.core.binaryrecord2.RecordContainer.BRIterator
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordSchema}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}
import filodb.memory.format.vectors.{CustomBuckets, Histogram, HistogramWithBuckets, LongHistogram}

class ArrowSerializedRangeVectorSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  System.setProperty("arrow.memory.debug.allocator", "true")
  private val allocator = new RootAllocator(10000000)
  private val rb = SerializedRangeVector.newBuilder()

  val resSchema = new ResultSchema(Seq(
    ColumnInfo("time", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn)
  ), 1)
  val recSchema = resSchema.toRecordSchema

  override def afterAll(): Unit = {
    allocator.close()
  }

  // Helper to create mock RangeVector with double values
  private def toRv(samples: Seq[(Long, Double)],
                   rangeVectorKey: RangeVectorKey,
                   rvPeriod: RvRange): RangeVector = {
    new RangeVector {
      import NoCloseCursor._
      override def key: RangeVectorKey = rangeVectorKey
      override def rows(): RangeVectorCursor = samples.map(r => new TransientRow(r._1, r._2)).iterator
      override def outputRange: Option[RvRange] = Some(rvPeriod)
    }
  }

  // Helper to create mock RangeVector with histogram values
  private def toHistRv(samples: Seq[(Long, HistogramWithBuckets)],
                       rangeVectorKey: RangeVectorKey,
                       rvPeriod: RvRange): RangeVector = {
    new RangeVector {
      import NoCloseCursor._
      override def key: RangeVectorKey = rangeVectorKey
      override def rows(): RangeVectorCursor = samples.map(r => new TransientHistRow(r._1, r._2)).iterator
      override def outputRange: Option[RvRange] = Some(rvPeriod)
    }
  }

  describe("ArrowSerializedRangeVector2") {

    it("should deserialize ArrowSerializedRangeVector & ScalarFixedDouble RV from single VSR with double values") {

      val keysMap = Map(UTF8Str("metric") -> UTF8Str("temperature"),
                        UTF8Str("host") -> UTF8Str("server1"))
      val key = CustomRangeVectorKey(keysMap)

      val outputRange = Some(RvRange(1000, 1000, 5000))
      val rv = toRv(
        Seq((1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0), (5000, 50.0)),
        key,
        outputRange.get
      )

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      // Populate VSR
      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      val srv2 = ScalarFixedDouble(RangeParams(1, 1, 5), 100.0)
      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        srv2, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      // Convert to ArrowSerializedRangeVector2 instances
      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)
      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)

      srvs.length shouldEqual 2
      val srv = srvs.head

      // Verify key
      srv.key shouldEqual key
      srv.numRowsSerialized shouldEqual 5

      // Verify data
      srv.rows().map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual Seq(
        (1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0), (5000, 50.0)
      )

      srvs(1) shouldEqual srv2
      // Cleanup
      allVsrs.foreach(_.close())
    }

    it("should handle NaN values and reconstruct them during deserialization") {

      val key = CustomRangeVectorKey(Map(UTF8Str("metric") -> UTF8Str("cpu")))

      val outputRange = Some(RvRange(0, 1000, 4000))

      val rv = toRv(
        Seq((0, Double.NaN), (1000, 1.0), (2000, Double.NaN),
            (3000, 3.0), (4000, Double.NaN)),
        key,
        outputRange.get
      )

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)
      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)

      srvs.foreach(s => VSRDebug.debug(s.asInstanceOf[ArrowSerializedRangeVector]))

      srvs.length shouldEqual 1
      val srv = srvs.head

      // Should only serialize non-NaN values
      srv.numRowsSerialized shouldEqual 5

      // But should reconstruct all 5 rows with NaN filled in
      val rows = srv.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
      rows.length shouldEqual 5
      rows.map(_._1) shouldEqual Seq(0L, 1000L, 2000L, 3000L, 4000L)
      rows.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0)

      allVsrs.foreach(_.close())
    }

    it("should handle histogram values and empty histograms") {
      val resSchema = new ResultSchema(Seq(
        ColumnInfo("time", ColumnType.TimestampColumn),
        ColumnInfo("value", ColumnType.HistogramColumn)
      ), 1)
      val recSchema = resSchema.toRecordSchema

      val key = CustomRangeVectorKey(Map(UTF8Str("metric") -> UTF8Str("latency")))
      val h1 = LongHistogram(CustomBuckets(Array(1.0, 5.0, 10.0, Double.PositiveInfinity)),
                             Array(10L, 20L, 30L, 40L))

      val outputRange = Some(RvRange(0, 1000, 3000))
      val rv = toHistRv(
        Seq((0, Histogram.empty), (1000, h1), (2000, Histogram.empty), (3000, h1)),
        key,
        outputRange.get
      )

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)
      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)

      srvs.length shouldEqual 1
      val srv = srvs.head

      // Should only serialize non-empty histograms
      srv.numRowsSerialized shouldEqual 4

      // But should reconstruct all 4 rows
      val rows = srv.rows().map(r => (r.getLong(0), r.getHistogram(1))).toList
      rows.length shouldEqual 4
      rows.map(_._1) shouldEqual Seq(0L, 1000L, 2000L, 3000L)
      rows.map(_._2).filterNot(_.isEmpty) shouldEqual Seq(h1, h1)

      allVsrs.foreach(_.close())
    }

    it("should handle multiple RangeVectors in multiple VSRs") {
      val key1 = CustomRangeVectorKey(Map(UTF8Str("host") -> UTF8Str("server1")))
      val key2 = CustomRangeVectorKey(Map(UTF8Str("host") -> UTF8Str("server2")))
      val key3 = CustomRangeVectorKey(Map(UTF8Str("host") -> UTF8Str("server3")))

      val outputRange = Some(RvRange(1000, 1000, 2000))
      val rv1 = toRv(Seq((1000, 1.0), (2000, 2.0)), key1, outputRange.get)
      val rv2 = toRv(Seq((1000, 10.0), (2000, 20.0)), key2, outputRange.get)
      val rv3 = toRv(Seq((1000, 100.0), (2000, 200.0)), key3, outputRange.get)

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      // Populate multiple RVs
      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv1, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )
      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv2, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )
      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv3, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)
      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)

      srvs.length shouldEqual 3

      // Verify first RV
      srvs(0).key shouldEqual key1
      srvs(0).rows().map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual Seq((1000, 1.0), (2000, 2.0))

      // Verify second RV
      srvs(1).key shouldEqual key2
      srvs(1).rows().map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual Seq((1000, 10.0), (2000, 20.0))

      // Verify third RV
      srvs(2).key shouldEqual key3
      srvs(2).rows().map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual Seq((1000, 100.0), (2000, 200.0))

      allVsrs.foreach(_.close())
    }

    it("should handle RangeVector spanning multiple VSRs") {

      val key = CustomRangeVectorKey(Map(UTF8Str("metric") -> UTF8Str("counter")))

      // Create a large dataset that will span multiple VSRs
      val largeDataset = (1 to ArrowSerializedRangeVectorOps.maxNumRows + 100).map { i =>
        (i.toLong * 1000, i.toDouble)
      }

      val outputRange = Some(RvRange(largeDataset.head._1, 1000, largeDataset.last._1))
      val rv = toRv(largeDataset, key, outputRange.get)

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)

      // Should have created multiple VSRs
      allVsrs.length shouldEqual 2

      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)
      srvs.length shouldEqual 1

      val srv = srvs.head
      srv.key shouldEqual key
      srv.numRowsSerialized shouldEqual largeDataset.length

      // Verify all data is intact across VSRs
      srv.rows().map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual largeDataset

      allVsrs.foreach(_.close())
    }

    it("should handle RVK at last row of VSR with data rows in next VSR") {
      val key = CustomRangeVectorKey(Map(UTF8Str("metric") -> UTF8Str("counter")))

      // Create a large dataset that will span multiple VSRs
      // we choose 52408 because next RVK is written at row 52409, and new VSR is created for 52410
      // This tests the edge case where RVK is at the last row of a VSR, and next data row goes into new VSR.
      // If content of vector is modified, we may need to adjust this number to ensure RVK is at last row of VSR
      val largeDataset = (1 to 52408).map { i =>
        (i.toLong * 1000, i.toDouble)
      }

      val outputRange = Some(RvRange(largeDataset.head._1, 1000, largeDataset.last._1))
      val rv = toRv(largeDataset, key, outputRange.get)

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )
      println(s"Done with first RV, now adding one more row to trigger RVK at last row of VSR")
      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)

      // Should have created multiple VSRs
      allVsrs.length shouldEqual 2

      val isRvkVec1 = allVsrs.head.getVector(0).asInstanceOf[org.apache.arrow.vector.BitVector]
      isRvkVec1.get(isRvkVec1.getValueCount - 1) shouldEqual 1
      val rvkBrVec2 = allVsrs.last.getVector(1).asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
      rvkBrVec2.get(0) should not be null

      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)
      srvs.length shouldEqual 2

      val srv = srvs.head
      srv.key shouldEqual key
      srv.numRowsSerialized shouldEqual largeDataset.length

      // Verify all data is intact across VSRs
      srv.rows().map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual largeDataset
      srvs(1).rows().map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual largeDataset

      allVsrs.foreach(_.close())
    }

    it("should handle empty RangeVector") {

      val outputRange = Some(RvRange(0, 1000, 0))
      val key = CustomRangeVectorKey(Map(UTF8Str("metric") -> UTF8Str("empty")))
      val rv = toRv(Seq.empty, key, outputRange.get)

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)
      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)

      srvs.length shouldEqual 1
      srvs.head.key shouldEqual key
      srvs.head.numRowsSerialized shouldEqual 0
      srvs.head.rows().toSeq.length shouldEqual 0

      allVsrs.foreach(_.close())
    }

    it("should properly close cursor without closing shared VSRs") {
      val recSchema = new RecordSchema(Seq(
        ColumnInfo("time", ColumnType.TimestampColumn),
        ColumnInfo("value", ColumnType.DoubleColumn)
      ))

      val outputRange = Some(RvRange(1000, 1000, 2000))
      val key = CustomRangeVectorKey(Map(UTF8Str("metric") -> UTF8Str("test")))
      val rv = toRv(Seq((1000, 1.0), (2000, 2.0)), key, outputRange.get)

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)
      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)

      val srv = srvs.head
      val cursor = srv.rows()

      // Consume some rows
      cursor.hasNext shouldEqual true
      cursor.next()

      // Close cursor
      cursor.close()

      // VSRs should still be valid (not closed by cursor.close())
      allVsrs.foreach { vsr =>
        vsr.getRowCount should be >= 0  // Should not throw if still valid
      }

      // Manual cleanup
      allVsrs.foreach(_.close())
    }

    it("should throw NoSuchElementException when iterating beyond available rows") {

      val outputRange = Some(RvRange(1000, 1000, 1000))
      val key = CustomRangeVectorKey(Map(UTF8Str("metric") -> UTF8Str("test")))
      val rv = toRv(Seq((1000, 1.0)), key, outputRange.get)

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)
      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)

      val cursor = srvs.head.rows()
      cursor.hasNext shouldEqual true
      cursor.next()
      cursor.hasNext shouldEqual false

      an[NoSuchElementException] should be thrownBy cursor.next()

      allVsrs.foreach(_.close())
    }
  }
}
