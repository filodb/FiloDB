package filodb.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.core.query._
import ProtoConverters._
import QueryResponseConverter._
import akka.pattern.AskTimeoutException
import filodb.core.QueryTimeoutException
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.SchemaMismatch
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
                            enforcedLimits = PerQueryLimits(
                              groupByCardinality = 123,
                              joinQueryCardinality = 124,
                              execPlanResultBytes = 125,
                              execPlanSamples = 126,
                              timeSeriesSamplesScannedBytes = 127,
                              timeSeriesScanned = 200,
                              rawScannedBytes = 201),
                            warnLimits = PerQueryLimits(
                              groupByCardinality = 128,
                              joinQueryCardinality = 129,
                              execPlanResultBytes = 130,
                              execPlanSamples = 131,
                              timeSeriesSamplesScannedBytes = 132,
                              rawScannedBytes = 201),
                            queryOrigin = Option("rr"),
                            queryOriginId = Option("rr_id"),
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

    val warnings = QueryWarnings()
    warnings.updateTimeSeriesScanned(1)
    warnings.updateExecPlanResultBytes(2)
    warnings.updateGroupByCardinality(3)
    warnings.updateExecPlanSamples(4)
    warnings.updateJoinQueryCardinality(5)

    val srv = SerializedRangeVector.apply(rv, builder, recSchema, "someExecPlan", qStats)

    val origQueryResult = QueryResult(
      "someId", resultSchema, List(srv), qStats, warnings, true, Some("Some shards timed out")
    )
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
    val res = deserializedSrv.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
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

    val header = StreamQueryResultHeader("someId", "planId", resultSchema)
    header.toProto.fromProto shouldEqual header
  }


  it ("should convert StreamingQueryResultFooter to and back from proto") {
    val stat = Stat()
    stat.resultBytes.addAndGet(100)
    stat.dataBytesScanned.addAndGet(1000)
    stat.timeSeriesScanned.addAndGet(5)

    val qStats = QueryStats()
    qStats.stat.put(List(), stat)

    val warnings = QueryWarnings()
    warnings.updateJoinQueryCardinality(1)
    warnings.updateTimeSeriesScanned(1)
    warnings.updateExecPlanResultBytes(2)
    warnings.updateGroupByCardinality(3)
    warnings.updateExecPlanSamples(4)
    warnings.updateJoinQueryCardinality(5)

    val footer1 = StreamQueryResultFooter("id", "planId", qStats, warnings, true, Some("Reason"))
    footer1.toProto.fromProto shouldEqual footer1

    val footer2 = StreamQueryResultFooter("id", "planId", qStats, warnings, false, None)
    footer2.toProto.fromProto shouldEqual footer2
  }

  it("QueryWarnings should have proper hashes and equals methods") {

    val warnings = QueryWarnings()
    warnings.updateJoinQueryCardinality(1)
    warnings.updateTimeSeriesScanned(1)
    warnings.updateExecPlanResultBytes(2)
    warnings.updateGroupByCardinality(3)
    warnings.updateExecPlanSamples(4)
    warnings.updateJoinQueryCardinality(5)

    val warnings2 = QueryWarnings()
    warnings2.updateJoinQueryCardinality(1)
    warnings2.updateTimeSeriesScanned(1)
    warnings2.updateExecPlanResultBytes(2)
    warnings2.updateGroupByCardinality(3)
    warnings2.updateExecPlanSamples(4)
    warnings2.updateJoinQueryCardinality(5)

    warnings shouldEqual warnings2
    warnings.hashCode() shouldEqual warnings2.hashCode()
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
    val origQueryResult = StreamQueryResult("id", "planId", Seq(srv))

    val successResp = origQueryResult.toProto.fromProto.asInstanceOf[StreamQueryResult]
    successResp.queryId shouldEqual origQueryResult.queryId
    successResp.result.head.isInstanceOf[SerializedRangeVector] shouldEqual true
    val deserializedSrv = successResp.result.head.asInstanceOf[SerializedRangeVector]
    deserializedSrv.numRows shouldEqual Some(11)
    deserializedSrv.numRowsSerialized shouldEqual 4
    val res = deserializedSrv.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
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

    val err = StreamQueryError("id", "planId", qStats, new IllegalArgumentException("Args"))
    val deser = err.toProto.fromProto.asInstanceOf[StreamQueryError]
    deser.queryId shouldEqual err.queryId
    deser.queryStats shouldEqual err.queryStats
    // Throwable is not constructed to the same type as original
    deser.t.getMessage shouldEqual err.t.getMessage

  }

  it("should preserve the order of rvs when deserializing it to big response") {

    val resultSchema = ResultSchema(List(ColumnInfo("ts", ColumnType.DoubleColumn),
      ColumnInfo("val", ColumnType.DoubleColumn)), 1, Map.empty)

    val keysMap = Map("key1".utf8 -> "val1".utf8,
      "key2".utf8 -> "val2".utf8)
    val key = CustomRangeVectorKey(keysMap)
    val rvs = (0 to 10).map(
      x => toRv(Seq((100, x)), key, RvRange(100, 1, 100))
    )

    val builder = SerializedRangeVector.newBuilder()
    val recSchema = new RecordSchema(Seq(ColumnInfo("time", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)))

    val streamingResponse = QueryResult("test", resultSchema,
      rvs.map(rv => SerializedRangeVector.apply(rv, builder, recSchema, "someExecPlan", QueryStats()))
    ).toStreamingResponse(
      QueryConfig.unitTestingQueryConfig.copy(numRvsPerResultMessage = 3)).map(_.toProto)
    val deserializedQueryResponse = streamingResponse.iterator.toQueryResponse.asInstanceOf[QueryResult]
    // This deserializedQueryResponse should have the same order for the Rvs as the original

    deserializedQueryResponse.result.map(
      rv => rv.asInstanceOf[SerializedRangeVector].rows().next().getDouble(1).toInt) shouldEqual (0 to 10).toList
  }

  it ("should stream Iterator[StreamingResponse] to a QueryResponse in multiple cases") {
    // Case 1 Iter[StreamingResponse] to QueryResp Happy response with partial reason
    val resultSchema = ResultSchema(List(ColumnInfo("ts", ColumnType.DoubleColumn),
      ColumnInfo("val", ColumnType.DoubleColumn)), 1, Map.empty)

    val header = StreamQueryResultHeader("someId", "planId", resultSchema)

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
    val streamingQueryBody = StreamQueryResult("someId", "planId", Seq(srv))


    val stat = Stat()
    stat.resultBytes.addAndGet(100)
    stat.dataBytesScanned.addAndGet(1000)
    stat.timeSeriesScanned.addAndGet(5)

    val qStats = QueryStats()
    qStats.stat.put(List(), stat)

    val warnings = QueryWarnings()
    warnings.updateTimeSeriesScanned(1)
    warnings.updateExecPlanResultBytes(2)
    warnings.updateGroupByCardinality(3)
    warnings.updateExecPlanSamples(4)
    warnings.updateJoinQueryCardinality(5)
    warnings.updateTimeSeriesSampleScannedBytes(6)

    val footer = StreamQueryResultFooter("someId", "planId", qStats, warnings, true, Some("Reason"))

    val response = Seq(header.toProto, streamingQueryBody.toProto, footer.toProto)
      .iterator.toQueryResponse.asInstanceOf[QueryResult]

    response.id shouldEqual "someId"
    response.resultSchema shouldEqual resultSchema
    response.result.size shouldEqual 1
    response.queryStats shouldEqual qStats
    response.mayBePartial shouldEqual true
    response.partialResultReason shouldEqual Some("Reason")
    val deserializedSrv = response.result.head.asInstanceOf[SerializedRangeVector]
    deserializedSrv.numRows shouldEqual Some(11)
    deserializedSrv.numRowsSerialized shouldEqual 4
    val res = deserializedSrv.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)


    // Case 2 Iter[StreamingResponse] to QueryResp Happy response w/o partial reason


    val footer1 = StreamQueryResultFooter("someId", "planId", qStats, warnings, false, None)


    val response1 = Seq(header.toProto, streamingQueryBody.toProto, footer1.toProto)
      .iterator.toQueryResponse.asInstanceOf[QueryResult]
    response1.mayBePartial shouldEqual false
    response1.partialResultReason shouldEqual None

    // Case 3 Iter[StreamingResponse] to Missing header
    val response2 = Seq(streamingQueryBody.toProto, footer.toProto)
        .iterator.toQueryResponse.asInstanceOf[QueryResult]

    response2.id shouldEqual ""
    response2.resultSchema shouldEqual ResultSchema.empty
    response2.result.size shouldEqual 1
    response2.queryStats shouldEqual qStats
    response2.mayBePartial shouldEqual true
    response.partialResultReason shouldEqual Some("Reason")
    val deserializedSrv1 = response.result.head.asInstanceOf[SerializedRangeVector]
    deserializedSrv1.numRows shouldEqual Some(11)
    deserializedSrv1.numRowsSerialized shouldEqual 4
    val res1 = deserializedSrv1.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
    res1.length shouldEqual 11
    res1.map(_._1) shouldEqual (0 to 1000 by 100)
    res1.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)

    // Case 4 Iter[StreamingResponse] to Missing footer

    val response3 = Seq(header.toProto, streamingQueryBody.toProto)
      .iterator.toQueryResponse.asInstanceOf[QueryResult]

    response3.id shouldEqual "someId"
    response3.resultSchema shouldEqual resultSchema
    response3.result.size shouldEqual 1
    response3.queryStats shouldEqual QueryStats()
    response3.mayBePartial shouldEqual false
    response3.partialResultReason shouldEqual None
    val deserializedSrv2 = response.result.head.asInstanceOf[SerializedRangeVector]
    deserializedSrv2.numRows shouldEqual Some(11)
    deserializedSrv2.numRowsSerialized shouldEqual 4
    val res2 = deserializedSrv1.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
    res2.length shouldEqual 11
    res2.map(_._1) shouldEqual (0 to 1000 by 100)
    res2.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)

    // Case 5 Iter[StreamingResponse] Error with throwable

    val errorResponse = Seq(StreamQueryError("errorId", "planId", qStats,
                   new IllegalArgumentException("Exception")).toProto)
      .iterator.toQueryResponse.asInstanceOf[QueryError]

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
    val res = deserializedSrv.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)
  }

  it("should convert exception to proto and back") {
    // Limited exceptions, typically those that might be used by client application to decide the response codes are
    // marshalled and their types retained. IllegalArgumentException for instance indicates a bad input and should be
    // propagated as is.
    //   IMPORTANT: Tests do not assert stacktraces yet

    // Case 1. IllegalArgumentException
    val iae = new IllegalArgumentException("Exception message").fillInStackTrace()
    val deserialized = iae.toProto.fromProto
    deserialized.isInstanceOf[IllegalArgumentException] shouldBe true
    deserialized.getMessage shouldEqual iae.getMessage
    deserialized.getCause shouldBe null


    val iae1 = new IllegalArgumentException().fillInStackTrace()
    val deserialized1 = iae1.toProto.fromProto
    deserialized1.isInstanceOf[IllegalArgumentException] shouldBe true
    deserialized1.getMessage shouldEqual ""
    deserialized1.getCause shouldBe null

    val iae2 = new IllegalArgumentException(new IllegalArgumentException("Cause")).fillInStackTrace()
    val deserialized2 = iae2.toProto.fromProto
    deserialized2.isInstanceOf[IllegalArgumentException] shouldBe true
    deserialized2.getMessage shouldEqual "java.lang.IllegalArgumentException: Cause"
    deserialized2.getCause.getMessage shouldBe iae2.getCause.getMessage
    iae2.getCause.isInstanceOf[IllegalArgumentException] shouldBe true

    // Case 2. QueryTimeoutException
    val qte = QueryTimeoutException(10000L, "timedoutAt")
    qte.toProto.fromProto shouldEqual qte

    // Case 3: QueryTimeoutException
    val bqe = new BadQueryException("Bad Query")
    val deserializedbqe  = bqe.toProto.fromProto
    deserializedbqe.isInstanceOf[BadQueryException] shouldBe true
    deserializedbqe.getMessage shouldEqual bqe.getMessage

    // Case 4: java.util.concurrent.TimeoutException
    val te1 = new java.util.concurrent.TimeoutException("Bad Query")
    val deserializedte1  = te1.toProto.fromProto
    deserializedte1.isInstanceOf[java.util.concurrent.TimeoutException] shouldBe true
    deserializedte1.getMessage shouldBe te1.getMessage
    deserializedte1.getCause shouldBe null


    val te2 = new java.util.concurrent.TimeoutException()
    val deserializedte2  = te2.toProto.fromProto
    deserializedte2.isInstanceOf[java.util.concurrent.TimeoutException] shouldBe true
    deserializedte2.getMessage shouldBe null
    deserializedte2.getCause shouldBe null

    // Case 5: NotImplementedError
    val nee = new NotImplementedError("not implemented")
    val deserializednee = nee.toProto.fromProto
    deserializednee.isInstanceOf[NotImplementedError] shouldBe true
    deserializednee.getMessage shouldBe nee.getMessage
    deserializednee.getCause shouldBe null

    // Case 6: SchemaMismatch
    val sme = SchemaMismatch(expected = "expectedSchema", found = "foundSchema", clazz = "SomeClass")
    val deserializedsme = sme.toProto.fromProto
    deserializedsme shouldEqual sme

    // Case 7: UnsupportedOperationException
    val uoe = new UnsupportedOperationException("Exception message").fillInStackTrace()
    val deserializeduoe = uoe.toProto.fromProto
    deserializeduoe.isInstanceOf[UnsupportedOperationException] shouldBe true
    deserializeduoe.getMessage shouldEqual uoe.getMessage
    deserializeduoe.getCause shouldBe null


    val uoe1 = new UnsupportedOperationException().fillInStackTrace()
    val deserializeduoe1 = uoe1.toProto.fromProto
    deserializeduoe1.isInstanceOf[UnsupportedOperationException] shouldBe true
    deserializeduoe1.getMessage shouldEqual ""
    deserializeduoe1.getCause shouldBe null

    val uoe2 = new UnsupportedOperationException(new IllegalArgumentException("Cause")).fillInStackTrace()
    val deserializeduoe2 = uoe2.toProto.fromProto
    deserializeduoe2.isInstanceOf[UnsupportedOperationException] shouldBe true
    deserializeduoe2.getMessage shouldEqual "java.lang.IllegalArgumentException: Cause"
    deserializeduoe2.getCause.getMessage shouldBe uoe2.getCause.getMessage
    uoe2.getCause.isInstanceOf[IllegalArgumentException] shouldBe true

    // Case 8: ServiceUnavailableException
    val sue = new ServiceUnavailableException("serviceUnavailable")
    val deseruializedsue = sue.toProto.fromProto
    deseruializedsue.isInstanceOf[ServiceUnavailableException] shouldBe true
    deseruializedsue.getMessage shouldBe sue.getMessage

    // Case 9: RemoteQueryFailureException

    val rqfe = RemoteQueryFailureException(200, "OK", "none", "no error")
    rqfe.toProto.fromProto shouldEqual rqfe

    // case 10: Should deserialize AskTimeoutException
    val ate = new AskTimeoutException("message")
    val deserAte = ate.toProto.fromProto
    deserAte.isInstanceOf[AskTimeoutException] shouldBe true
    deserAte.getMessage shouldBe ate.getMessage

    val ate1 = new AskTimeoutException("message", new IllegalArgumentException("root"))
    val deserAte1 = ate1.toProto.fromProto
    deserAte1.isInstanceOf[AskTimeoutException] shouldBe true
    deserAte1.getMessage shouldBe ate.getMessage
    deserAte1.getCause.isInstanceOf[IllegalArgumentException] shouldBe true
    deserAte1.getCause.getMessage shouldBe "root"

    // case 11: Should deserialize QueryLimitException

    val qle = QueryLimitException("message", "queryId")
    qle.toProto.fromProto shouldEqual qle

    // Case 12: Anything else should throw Throwable
    val isecause = SchemaMismatch(expected = "expectedSchema", found = "foundSchema", clazz = "SomeClass")
    val ise = new IllegalStateException("Illegal state", isecause)
    val deserializedise = ise.toProto.fromProto
    deserializedise.isInstanceOf[Throwable] shouldBe true
    deserializedise.getMessage shouldBe ise.getMessage
    deserializedise.getCause shouldEqual isecause
  }
}
