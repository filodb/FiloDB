package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}
import filodb.memory.format.vectors.{CustomBuckets, Histogram, HistogramWithBuckets, LongHistogram}

class SerializedRangeVectorSpec  extends AnyFunSpec with Matchers {

  private def toRv(samples: Seq[(Long, Double)],
                   rangeVectorKey: RangeVectorKey,
                   rvPeriod: RvRange): RangeVector = {
    new RangeVector {
      import NoCloseCursor._
      override def key: RangeVectorKey = rangeVectorKey
      override def rows(): RangeVectorCursor = samples.map(r => new TransientRow(r._1, r._2)).iterator

      override def period: Option[RvRange] = Some(rvPeriod)
    }
  }

  private def toHistRv(samples: Seq[(Long, HistogramWithBuckets)],
                       rangeVectorKey: RangeVectorKey,
                       rvPeriod: RvRange): RangeVector = {
    new RangeVector {
      import NoCloseCursor._
      override def key: RangeVectorKey = rangeVectorKey
      override def rows(): RangeVectorCursor = samples.map(r => new TransientHistRow(r._1, r._2)).iterator

      override def period: Option[RvRange] = Some(rvPeriod)
    }
  }

  it("should remove NaNs at encoding and add NaNs on decoding") {
    val builder = SerializedRangeVector.newBuilder()
    val recSchema = new RecordSchema(Seq(ColumnInfo("time", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)))
    val keysMap = Map(UTF8Str("key1") -> UTF8Str("val1"),
                      UTF8Str("key2") -> UTF8Str("val2"))
    val key = CustomRangeVectorKey(keysMap)

    val rv = toRv(Seq((0, Double.NaN), (100, 1.0), (200, Double.NaN),
                      (300, 3.0), (400, Double.NaN),
                      (500, 5.0), (600, 6.0),
                      (700, Double.NaN), (800, Double.NaN),
                      (900, Double.NaN), (1000, Double.NaN)), key,
      RvRange(0, 100, 1000))
    val srv = SerializedRangeVector.apply(rv, builder, recSchema, "someExecPlan")
    srv.numRows shouldEqual Some(4)
    val res = srv.rows.map(r => (r.getLong(0), r.getDouble(1))).toList
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)
  }

  it("should NOT remove NaNs at encoding and add NaNs on decoding for instant queries where start == end") {
    val builder = SerializedRangeVector.newBuilder()
    val recSchema = new RecordSchema(Seq(ColumnInfo("time", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)))
    val keysMap = Map(UTF8Str("key1") -> UTF8Str("val1"),
      UTF8Str("key2") -> UTF8Str("val2"))
    val key = CustomRangeVectorKey(keysMap)

    val rv = toRv(Seq((0, Double.NaN), (100, 1.0), (200, Double.NaN),
      (300, 3.0), (400, Double.NaN),
      (500, 5.0), (600, 6.0),
      (700, Double.NaN), (800, Double.NaN),
      (900, Double.NaN), (1000, Double.NaN)), key,
      RvRange(1000, 100, 1000))
    val srv = SerializedRangeVector.apply(rv, builder, recSchema, "someExecPlan")
    srv.numRows shouldEqual Some(11)
    val res = srv.rows.map(r => (r.getLong(0), r.getDouble(1))).toList
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)
  }

  it("should remove Hist.empty at encoding and add Hist.empty on decoding") {
    val builder = SerializedRangeVector.newBuilder()
    val recSchema = new RecordSchema(Seq(ColumnInfo("time", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.HistogramColumn)))
    val keysMap = Map(UTF8Str("key1") -> UTF8Str("val1"),
      UTF8Str("key2") -> UTF8Str("val2"))
    val key = CustomRangeVectorKey(keysMap)

    val h1 = LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), ( 0L to 10L by 5).toArray)

    val rv = toHistRv(Seq((0, Histogram.empty), (100, h1), (200, Histogram.empty),
                          (300, h1), (400, Histogram.empty), (500, h1),
                          (600, h1), (700, Histogram.empty), (800, Histogram.empty),
                          (900, Histogram.empty), (1000, Histogram.empty)), key,
                      RvRange(0, 100, 1000))

    val srv = SerializedRangeVector.apply(rv, builder, recSchema, "someExecPlan")

    val res = srv.rows.map(r => (r.getLong(0), r.getHistogram(1))).toList
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isEmpty) shouldEqual Seq(h1, h1, h1, h1)
  }

}
