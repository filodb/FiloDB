package filodb.query.exec

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.{MetricsTestData, TestData}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.memory.format.ZeroCopyUTF8String
import filodb.query._
import filodb.query.exec.InternalRangeFunction.Increase
import filodb.query.exec.rangefn.RawDataWindowingSpec

class PeriodicSamplesMapperSpec extends FunSpec with Matchers with ScalaFutures with RawDataWindowingSpec {

  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)

  val samples = Seq(
    100000L -> 100d,
    153000L -> 160d,
    200000L -> 200d
  )

  val rv = timeValueRVPk(samples)

  it("should return value present at time - staleSampleAfterMs") {

    val expectedResults = List(100000L -> 100d,
      200000L -> 200d,
      300000L -> 200d,
      400000L -> 200d,
      500000L -> 200d
    )
    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(100000L, 100000, 600000L, None, None, QueryContext())
    val resultObs = periodicSamplesVectorFnMapper(Observable.fromIterable(Seq(rv)),
      querySession, 1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map
    (r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN))

    resultRows.foreach(_.toList shouldEqual expectedResults)

    val outSchema = periodicSamplesVectorFnMapper.schema(resultSchema)
    outSchema.columns shouldEqual resultSchema.columns
    outSchema.fixedVectorLen shouldEqual Some(6)
  }

  it("should work with offset") {

    val expectedResults = List(100100L -> 100d,
      200100L -> 200d,
      300100L -> 200d,
      400100L -> 200d,
      500100L -> 200d
    )

    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(100100L, 100000, 600100L, None, None, QueryContext(),
      false, Nil, Some(100))
    val resultObs = periodicSamplesVectorFnMapper(Observable.fromIterable(Seq(rv)), querySession,
      1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map
    (r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN))

    resultRows.foreach(_.toList shouldEqual expectedResults)

    val outSchema = periodicSamplesVectorFnMapper.schema(resultSchema)
    outSchema.columns shouldEqual resultSchema.columns
    outSchema.fixedVectorLen shouldEqual Some(6)
  }

  it("should add publish interval to lookback when step factor notation and counter function is used") {
    val samples = Seq(
      100000L -> 100d,
      200000L -> 170d,
      300000L -> 180d,
      400000L -> 190d,
      500000L -> 200d,
      600000L -> 220d,
      700000L -> 240d,
      800000L -> 260d,
      900000L -> 280d,
      1000000L -> 300d,
      1100000L -> 400d,
      1200000L -> 500d,
      1300000L -> 600d
    )
    import ZeroCopyUTF8String._

    // step tag with 100s publish interval is important here
    val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8, "_step_".utf8 -> "100".utf8)
    val partBuilder = new RecordBuilder(TestData.nativeMem)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, "counterName".utf8, seriesTags)

    val rv = timeValueRVPk(samples, partKey)

    val expectedResults = List(
      500000L -> 125d,
      900000L ->100d,
      1300000 -> 400d
    )

    // step == lookback here
    // stepMultipleNotationUsed = true when step factor notation is used.
    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(500000L, 400000L, 1300000L,
      Some(400000L), Some(Increase), QueryContext(),
      stepMultipleNotationUsed = true, Nil, None)
    val resultObs = periodicSamplesVectorFnMapper.apply(Observable.fromIterable(Seq(rv)), querySession,
      1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map
    (r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN))

    resultRows.foreach(_.toList shouldEqual expectedResults)
  }

}
