package filodb.query.exec

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.MetricsTestData
import filodb.core.query._
import filodb.query._
import filodb.query.exec.rangefn.RawDataWindowingSpec

class PeriodicSamplesMapperSpec extends FunSpec with Matchers with ScalaFutures with RawDataWindowingSpec {

  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)

  val samples = Seq(
    100000L -> 100d,
    153000L -> 160d,
    200000L -> 200d
  )

  val rv = timeValueRV(samples)

  it("should return value present at time - staleSampleAfterMs") {

    val expectedResults = List(100000L -> 100d,
      200000L -> 200d,
      300000L -> 200d,
      400000L -> 200d,
      500000L -> 200d,
      600000L -> 200d
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
      500100L -> 200d,
      600100L -> 200d
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
}
