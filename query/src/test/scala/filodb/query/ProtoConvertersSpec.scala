package filodb.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.core.query._
import ProtoConverters._
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType
import filodb.grpc.{GrpcMultiPartitionQueryService, ProtoRangeVector}
import filodb.memory.format.ZeroCopyUTF8String._


class ProtoConvertersSpec extends AnyFunSpec with Matchers {

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

  it("should convert the RvRange to proto and back") {
    val originalRvRange = RvRange(0, 10, 100)
    originalRvRange.toProto.fromProto shouldEqual originalRvRange
  }

  it("should convert the RangeVectorKey to proto and back") {

      val key = CustomRangeVectorKey(labelValues = Map(
        "label1".utf8 -> "labelValue1".utf8,
        "label2".utf8 -> "labelValue2".utf8
    ,
      ))
    key.toProto.fromProto shouldEqual key
  }

  it("should convert column into to proto and back to same column info") {
    val ci1 = ColumnInfo("intColumn", ColumnType.IntColumn)
    ci1.toProto.fromProto shouldEqual ci1

    val ci2 = ColumnInfo("HistColumn", ColumnType.HistogramColumn)
    ci2.toProto.fromProto shouldEqual ci2

    val ci3 = ColumnInfo("longColumn", ColumnType.LongColumn)
    ci3.toProto.fromProto shouldEqual ci3

    val ci4 = ColumnInfo("tsColumn", ColumnType.TimestampColumn)
    ci4.toProto.fromProto shouldEqual ci4

    val ci5 = ColumnInfo("brColumn", ColumnType.BinaryRecordColumn)
    ci5.toProto.fromProto shouldEqual ci5

    val ci6 = ColumnInfo("doubleColumn", ColumnType.DoubleColumn)
    ci6.toProto.fromProto shouldEqual ci6

    val ci7 = ColumnInfo("stringColumn", ColumnType.StringColumn)
    ci7.toProto.fromProto shouldEqual ci7

    val ci8 = ColumnInfo("mapColumn", ColumnType.MapColumn)
    ci8.toProto.fromProto shouldEqual ci8
  }

  it("should convert QueryParams to proto and back") {
    UnavailablePromQlQueryParams.toProto.fromProto shouldEqual UnavailablePromQlQueryParams

    val params = PromQlQueryParams(promQl = "cpu_utilization{host=\"foo\"}", startSecs = 0, stepSecs = 10,
      endSecs = 100 , verbose = true)
    params.toProto.fromProto shouldEqual params

    val params1 = PromQlQueryParams(promQl = "cpu_utilization{host=\"foo\"}", startSecs = 0, stepSecs = 10,
      endSecs = 100 , verbose = true, remoteQueryPath = Some("/api/v1/query_range"))
    params1.toProto.fromProto shouldEqual params1
  }

  it("should convert PlannerParams to proto and back") {
    val pp = PlannerParams( applicationId= "fdb",
                            queryTimeoutMillis = 10,
                            sampleLimit= 123,
                            groupByCardLimit = 123,
                            joinQueryCardLimit = 123,
                            resultByteLimit = 123,
                            timeSplitEnabled = true,
                            minTimeRangeForSplitMs = 10,
                            splitSizeMs = 10,
                            skipAggregatePresent = true,
                            processMultiPartition = true,
                            allowPartialResults = true)
    pp.toProto.fromProto shouldEqual pp
    PlannerParams().toProto.fromProto shouldEqual PlannerParams()
    // proto with none of the values set should build the default PlannerParam instance
    GrpcMultiPartitionQueryService.PlannerParams.newBuilder().build().fromProto shouldEqual PlannerParams()
  }

  it("should convert from Stat to proto and back") {
    val stat = Stat()
    stat.resultBytes.addAndGet(100)
    stat.dataBytesScanned.addAndGet(1000)
    stat.timeSeriesScanned.addAndGet(5)
    stat.cpuNanos.addAndGet(100)
    stat.toProto.fromProto shouldEqual stat
  }

  it("should convert QueryStatistics to proto and back") {
    val stat = Stat()
    stat.resultBytes.addAndGet(100)
    stat.dataBytesScanned.addAndGet(1000)
    stat.timeSeriesScanned.addAndGet(5)


    val stat1 = Stat()
    stat1.resultBytes.addAndGet(10)
    stat1.dataBytesScanned.addAndGet(100)
    stat1.timeSeriesScanned.addAndGet(10)

    val stat2 = Stat()
    stat2.resultBytes.addAndGet(1000)
    stat2.dataBytesScanned.addAndGet(10000)
    stat2.timeSeriesScanned.addAndGet(156)

    val qStats = QueryStats()
    qStats.stat.put(List(), stat2)
    qStats.stat.put(List("L1", "L2", "L3"), stat1)
    qStats.stat.put(List("L1", "L2", "L4"), stat)

    qStats.toProto.fromProto shouldEqual qStats
  }

  it("should convert RecordSchema to proto and back") {
    val binRc1 = new RecordSchema( columns = List(
      ColumnInfo("time", Column.ColumnType.LongColumn),
      ColumnInfo("value", Column.ColumnType.DoubleColumn)),
      partitionFieldStart = None, predefinedKeys = Nil, brSchema = Map.empty)

    val rs = new RecordSchema( columns = List(
                        ColumnInfo("time", Column.ColumnType.LongColumn),
                        ColumnInfo("value", Column.ColumnType.DoubleColumn),
                        ColumnInfo("map", Column.ColumnType.MapColumn)),
                      partitionFieldStart = Some(1),
                      predefinedKeys = List("_ws_", "_ns_"),
                      brSchema = Map(2 -> binRc1),
                      schemaVersion = 2)
      rs.toProto.fromProto shouldEqual rs
  }

  it("should convert ResultSchema to proto and back") {
    val binRs = new RecordSchema( columns = List(
      ColumnInfo("time", Column.ColumnType.LongColumn),
      ColumnInfo("value", Column.ColumnType.DoubleColumn),
      ColumnInfo("map", Column.ColumnType.MapColumn)),
      partitionFieldStart = Some(1),
      predefinedKeys = List("_ws_", "_ns_"),
      brSchema = Map.empty,
      schemaVersion = 1)

    val resSchema = ResultSchema(List(ColumnInfo("ts", ColumnType.DoubleColumn),
      ColumnInfo("val", ColumnType.DoubleColumn),
      ColumnInfo("brColumn", ColumnType.BinaryRecordColumn)),
      1,
      Map( 2->  binRs), Some(1), List(0, 1, 2))

    resSchema.toProto.fromProto shouldEqual resSchema

  }

  it ("should convert a success query response to and back from proto") {

    val resultSchema = ResultSchema(List(ColumnInfo("ts", ColumnType.DoubleColumn),
      ColumnInfo("val", ColumnType.DoubleColumn)), 1, Map.empty)

    val builder = SerializedRangeVector.newBuilder()
    val recSchema = new RecordSchema(Seq(ColumnInfo("time", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)))
    val keysMap = Map("key1".utf8 -> "val1".utf8,
      "key2".utf8 -> "val2".utf8)
    val key = CustomRangeVectorKey(keysMap)

    val rv = toRv(Seq((0, Double.NaN), (100, 1.0), (200, Double.NaN),
      (300, 3.0), (400, Double.NaN),
      (500, 5.0), (600, 6.0),
      (700, Double.NaN), (800, Double.NaN),
      (900, Double.NaN), (1000, Double.NaN)), key,
      RvRange(0, 100, 1000))


    val stat = Stat()
    stat.resultBytes.addAndGet(100)
    stat.dataBytesScanned.addAndGet(1000)
    stat.timeSeriesScanned.addAndGet(5)

    val qStats = QueryStats()
    qStats.stat.put(List(), stat)

    val srv = SerializedRangeVector.apply(rv, builder, recSchema, "someExecPlan", qStats)

    val origQueryResult = QueryResult("someId", resultSchema, List(srv), qStats, true, Some("Some shards timed out"))
    val successResp = origQueryResult.toProto.fromProto.asInstanceOf[QueryResult]
    successResp.id shouldEqual origQueryResult.id
    successResp.resultSchema shouldEqual origQueryResult.resultSchema
    successResp.result.size shouldEqual 1
    successResp.result.head.isInstanceOf[SerializedRangeVector] shouldEqual true
    successResp.queryStats shouldEqual origQueryResult.queryStats
    successResp.mayBePartial shouldEqual true
    successResp.partialResultReason shouldEqual Some("Some shards timed out")
    val deserializedSrv = successResp.result.head.asInstanceOf[SerializedRangeVector]
    deserializedSrv.numRows shouldEqual Some(11)
    deserializedSrv.numRowsSerialized shouldEqual 4
    val res = deserializedSrv.rows.map(r => (r.getLong(0), r.getDouble(1))).toList
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)
  }

  it ("should convert a failure query response to and back from proto") {
      // TODO
  }

  it ("should convert StreamingQueryResultHeader to and back from proto") {
    val resultSchema = ResultSchema(List(ColumnInfo("ts", ColumnType.DoubleColumn),
      ColumnInfo("val", ColumnType.DoubleColumn)), 1, Map.empty)

    val header = StreamQueryResultHeader("someId", resultSchema)
    header.toProto.fromProto shouldEqual header
  }


  it ("should convert StreamingQueryResultFooter to and back from proto") {
    val stat = Stat()
    stat.resultBytes.addAndGet(100)
    stat.dataBytesScanned.addAndGet(1000)
    stat.timeSeriesScanned.addAndGet(5)

    val qStats = QueryStats()
    qStats.stat.put(List(), stat)

    val footer1 = StreamQueryResultFooter("id", qStats, true, Some("Reason"))
    footer1.toProto.fromProto shouldEqual footer1

    val footer2 = StreamQueryResultFooter("id", qStats, false, None)
    footer2.toProto.fromProto shouldEqual footer2
  }

  it ("should convert StreamingQueryResultBody to and back from proto") {
    val builder = SerializedRangeVector.newBuilder()
    val recSchema = new RecordSchema(Seq(ColumnInfo("time", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)))
    val keysMap = Map("key1".utf8 -> "val1".utf8,
      "key2".utf8 -> "val2".utf8)
    val key = CustomRangeVectorKey(keysMap)

    val rv = toRv(Seq((0, Double.NaN), (100, 1.0), (200, Double.NaN),
      (300, 3.0), (400, Double.NaN),
      (500, 5.0), (600, 6.0),
      (700, Double.NaN), (800, Double.NaN),
      (900, Double.NaN), (1000, Double.NaN)), key,
      RvRange(0, 100, 1000))


    val qStats = QueryStats()

    val srv = SerializedRangeVector.apply(rv, builder, recSchema, "someExecPlan", qStats)
    val origQueryResult = StreamQueryResult("id", srv)

    val successResp = origQueryResult.toProto.fromProto.asInstanceOf[StreamQueryResult]
    successResp.id shouldEqual origQueryResult.id
    successResp.result.isInstanceOf[SerializedRangeVector] shouldEqual true
    val deserializedSrv = successResp.result.asInstanceOf[SerializedRangeVector]
    deserializedSrv.numRows shouldEqual Some(11)
    deserializedSrv.numRowsSerialized shouldEqual 4
    val res = deserializedSrv.rows.map(r => (r.getLong(0), r.getDouble(1))).toList
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)
  }

  it ("should convert StreamingQueryThrowableBody to and back from proto") {
    val stat = Stat()
    stat.resultBytes.addAndGet(100)
    stat.dataBytesScanned.addAndGet(1000)
    stat.timeSeriesScanned.addAndGet(5)

    val qStats = QueryStats()
    qStats.stat.put(List(), stat)

    val err = StreamQueryError("id", qStats, new IllegalArgumentException("Args"))
    val deser = err.toProto.fromProto.asInstanceOf[StreamQueryError]
    deser.id shouldEqual err.id
    deser.queryStats shouldEqual err.queryStats
    // Throwable is not constructed to the same type as original
    deser.t.getMessage shouldEqual err.t.getMessage

  }

  it ("should stream Iterator[StreamingResponse] to a QueryResponse in multiple cases") {
    // Case 1 Iter[StreamingResponse] to QueryResp Happy response with partial reason
    val resultSchema = ResultSchema(List(ColumnInfo("ts", ColumnType.DoubleColumn),
      ColumnInfo("val", ColumnType.DoubleColumn)), 1, Map.empty)

    val header = StreamQueryResultHeader("someId", resultSchema)

    val builder = SerializedRangeVector.newBuilder()
    val recSchema = new RecordSchema(Seq(ColumnInfo("time", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)))
    val keysMap = Map("key1".utf8 -> "val1".utf8,
      "key2".utf8 -> "val2".utf8)
    val key = CustomRangeVectorKey(keysMap)

    val rv = toRv(Seq((0, Double.NaN), (100, 1.0), (200, Double.NaN),
      (300, 3.0), (400, Double.NaN),
      (500, 5.0), (600, 6.0),
      (700, Double.NaN), (800, Double.NaN),
      (900, Double.NaN), (1000, Double.NaN)), key,
      RvRange(0, 100, 1000))
    val stats = QueryStats()
    val srv = SerializedRangeVector.apply(rv, builder, recSchema, "someExecPlan", stats)
    val streamingQueryBody = StreamQueryResult("someId", srv)


    val stat = Stat()
    stat.resultBytes.addAndGet(100)
    stat.dataBytesScanned.addAndGet(1000)
    stat.timeSeriesScanned.addAndGet(5)

    val qStats = QueryStats()
    qStats.stat.put(List(), stat)

    val footer = StreamQueryResultFooter("someId", qStats, true, Some("Reason"))


    val response = Seq(header.toProto, streamingQueryBody.toProto, footer.toProto)
      .toIterator.toQueryResponse.asInstanceOf[QueryResult]

    response.id shouldEqual "someId"
    response.resultSchema shouldEqual resultSchema
    response.result.size shouldEqual 1
    response.queryStats shouldEqual qStats
    response.mayBePartial shouldEqual true
    response.partialResultReason shouldEqual Some("Reason")
    val deserializedSrv = response.result.head.asInstanceOf[SerializedRangeVector]
    deserializedSrv.numRows shouldEqual Some(11)
    deserializedSrv.numRowsSerialized shouldEqual 4
    val res = deserializedSrv.rows.map(r => (r.getLong(0), r.getDouble(1))).toList
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)


    // Case 2 Iter[StreamingResponse] to QueryResp Happy response w/o partial reason


    val footer1 = StreamQueryResultFooter("someId", qStats, false, None)


    val response1 = Seq(header.toProto, streamingQueryBody.toProto, footer1.toProto)
      .toIterator.toQueryResponse.asInstanceOf[QueryResult]
    response1.mayBePartial shouldEqual false
    response1.partialResultReason shouldEqual None

    // Case 3 Iter[StreamingResponse] to Missing header
    val response2 = Seq(streamingQueryBody.toProto, footer.toProto)
        .toIterator.toQueryResponse.asInstanceOf[QueryResult]

    response2.id shouldEqual ""
    response2.resultSchema shouldEqual ResultSchema.empty
    response2.result.size shouldEqual 1
    response2.queryStats shouldEqual qStats
    response2.mayBePartial shouldEqual true
    response.partialResultReason shouldEqual Some("Reason")
    val deserializedSrv1 = response.result.head.asInstanceOf[SerializedRangeVector]
    deserializedSrv1.numRows shouldEqual Some(11)
    deserializedSrv1.numRowsSerialized shouldEqual 4
    val res1 = deserializedSrv1.rows.map(r => (r.getLong(0), r.getDouble(1))).toList
    res1.length shouldEqual 11
    res1.map(_._1) shouldEqual (0 to 1000 by 100)
    res1.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)

    // Case 4 Iter[StreamingResponse] to Missing footer

    val response3 = Seq(header.toProto, streamingQueryBody.toProto)
      .toIterator.toQueryResponse.asInstanceOf[QueryResult]

    response3.id shouldEqual "someId"
    response3.resultSchema shouldEqual resultSchema
    response3.result.size shouldEqual 1
    response3.queryStats shouldEqual QueryStats()
    response3.mayBePartial shouldEqual false
    response3.partialResultReason shouldEqual None
    val deserializedSrv2 = response.result.head.asInstanceOf[SerializedRangeVector]
    deserializedSrv2.numRows shouldEqual Some(11)
    deserializedSrv2.numRowsSerialized shouldEqual 4
    val res2 = deserializedSrv1.rows.map(r => (r.getLong(0), r.getDouble(1))).toList
    res2.length shouldEqual 11
    res2.map(_._1) shouldEqual (0 to 1000 by 100)
    res2.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)

    // Case 5 Iter[StreamingResponse] Error with throwable

    val errorResponse = Seq(StreamQueryError("errorId", qStats, new IllegalArgumentException("Exception")).toProto)
      .toIterator.toQueryResponse.asInstanceOf[QueryError]

    errorResponse.id shouldEqual "errorId"
    errorResponse.queryStats shouldEqual qStats
    errorResponse.t.getMessage shouldEqual "Exception"

  }

  it ("should convert RangeParams to proto and back") {
    val rp = RangeParams(10, 50, 100)
    rp.toProto.fromProto shouldEqual rp
  }

  it ("It should convert ScalarFixedDouble to proto and back") {
    val sfd = ScalarFixedDouble(RangeParams(10, 50, 100), 10)
    sfd.toProto.fromProto shouldEqual sfd
  }

  it ("It should convert TimeScalar to proto and back") {
    val ts = TimeScalar(RangeParams(10, 50, 100))
    ts.toProto.fromProto shouldEqual ts
  }

  it ("It should convert HourScalar to proto and back") {
    val hs = HourScalar(RangeParams(10, 50, 100))
    hs.toProto.fromProto shouldEqual hs
  }

  it ("It should convert MinuteScalar to proto and back") {
    val ms = MinuteScalar(RangeParams(10, 50, 100))
    ms.toProto.fromProto shouldEqual ms
  }

  it ("It should convert MonthScalar to proto and back") {
    val ms = MonthScalar(RangeParams(10, 50, 100))
    ms.toProto.fromProto shouldEqual ms
  }

  it ("It should convert YearScalar to proto and back") {
    val ys = YearScalar(RangeParams(10, 50, 100))
    ys.toProto.fromProto shouldEqual ys
  }

  it ("It should convert DayOfMonthScalar to proto and back") {
    val doms = DayOfMonthScalar(RangeParams(10, 50, 100))
    doms.toProto.fromProto shouldEqual doms
  }

  it ("It should convert DayOfWeekScalar to proto and back") {
    val dows = DayOfWeekScalar(RangeParams(10, 50, 100))
    dows.toProto.fromProto shouldEqual dows
  }

  it ("It should convert DaysInMonthScalar to proto and back") {
    val doms = DaysInMonthScalar(RangeParams(10, 50, 100))
    doms.toProto.fromProto shouldEqual doms
  }

  it ("It should convert ScalarVaryingDouble to proto and back") {
    val svd1 = ScalarVaryingDouble(Map( 1L -> 1.0, 2L -> 2.0), None)
    svd1.toProto.fromProto shouldEqual svd1

    val svd2 = ScalarVaryingDouble(Map( 1L -> 1.0, 2L -> 2.0), Some(RvRange(10, 50, 1000)))
    svd2.toProto.fromProto shouldEqual svd2
  }

  it("should be able to convert all SerializableRangeVector to proto and back") {

    val builder = ProtoRangeVector.SerializableRangeVector.newBuilder()
    val svd = ScalarVaryingDouble(Map(1L -> 1.0, 2L -> 2.0), Some(RvRange(10, 20, 100)))
    builder.setScalarVaryingDouble(svd.toProto)
    builder.build().fromProto shouldEqual svd

    val rp = RangeParams(10, 20, 100)
    val ts = TimeScalar(rp)
    builder.clear()
    builder.setTimeScalar(ts.toProto)
    builder.build().fromProto shouldEqual ts

    val hs = HourScalar(rp)
    builder.clear()
    builder.setHourScalar(hs.toProto)
    builder.build().fromProto shouldEqual hs

    val ms = MinuteScalar(rp)
    builder.clear()
    builder.setMinuteScalar(ms.toProto)
    builder.build().fromProto shouldEqual ms

    val mos = MonthScalar(rp)
    builder.clear()
    builder.setMonthScalar(mos.toProto)
    builder.build().fromProto shouldEqual mos

    val ys = YearScalar(rp)
    builder.clear()
    builder.setYearScalar(ys.toProto)
    builder.build().fromProto shouldEqual ys

    val doms = DayOfMonthScalar(rp)
    builder.clear()
    builder.setDayOfMonthScalar(doms.toProto)
    builder.build().fromProto shouldEqual doms

    val dows = DayOfWeekScalar(rp)
    builder.clear()
    builder.setDayOfWeekScalar(dows.toProto)
    builder.build().fromProto shouldEqual dows

    val dims = DaysInMonthScalar(rp)
    builder.clear()
    builder.setDaysInMonthScalar(dims.toProto)
    builder.build().fromProto shouldEqual dims

    val sfd = ScalarFixedDouble(rp, 1.0)
    builder.clear()
    builder.setScalarFixedDouble(sfd.toProto)
    builder.build().fromProto shouldEqual sfd

    val resultSchema = ResultSchema(List(ColumnInfo("ts", ColumnType.DoubleColumn),
      ColumnInfo("val", ColumnType.DoubleColumn)), 1, Map.empty)

    val recBuilder = SerializedRangeVector.newBuilder()
    val recSchema = new RecordSchema(Seq(ColumnInfo("time", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)))
    val keysMap = Map("key1".utf8 -> "val1".utf8,
      "key2".utf8 -> "val2".utf8)
    val key = CustomRangeVectorKey(keysMap)

    val rv = toRv(Seq((0, Double.NaN), (100, 1.0), (200, Double.NaN),
      (300, 3.0), (400, Double.NaN),
      (500, 5.0), (600, 6.0),
      (700, Double.NaN), (800, Double.NaN),
      (900, Double.NaN), (1000, Double.NaN)), key,
      RvRange(0, 100, 1000))
    val stats = QueryStats()
    val srv = SerializedRangeVector.apply(rv, recBuilder, recSchema, "someExecPlan", stats)

    builder.clear()
    builder.setSerializedRangeVector(srv.toProto)
    val deserializedSrv = builder.build().fromProto.asInstanceOf[SerializedRangeVector]
    deserializedSrv.numRows shouldEqual Some(11)
    deserializedSrv.numRowsSerialized shouldEqual 4
    val res = deserializedSrv.rows.map(r => (r.getLong(0), r.getDouble(1))).toList
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)
  }


    // This currently converts exception to Throwable and tests fail.
//  it("should convert exception to proto and back") {
//    val exception = new IllegalStateException("Exception message")
//    val t: Throwable  = exception.fillInStackTrace()
//    t.toProto.fromProto shouldEqual t
//
//  }

}
