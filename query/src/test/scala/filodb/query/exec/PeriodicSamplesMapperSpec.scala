package filodb.query.exec

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import filodb.core.{MachineMetricsData, MetricsTestData, TestData}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query._
import filodb.query.exec.InternalRangeFunction.{Increase, Resets}
import filodb.query.exec.rangefn.RawDataWindowingSpec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class PeriodicSamplesMapperSpec extends AnyFunSpec with Matchers with ScalaFutures with RawDataWindowingSpec {

  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)

  val samples = Seq(
    100000L -> 100d,
    153000L -> 160d,
    200000L -> 200d
  )

  val rv = timeValueRVPk(samples)

  val histMaxMinSchema = ResultSchema(
    MachineMetricsData.histMaxMinDS.schema.infosFromIDs(Seq(0, 3, 5, 4)), 1, colIDs = Seq(0, 3, 5, 4))

  val rvHistMaxMin = MachineMetricsData.histMaxMinRV(100000L, numSamples = 3, numBuckets = 4)._2

  it("should return value present at time - staleSampleAfterMs") {

    val expectedResults = List(100000L -> 100d,
      200000L -> 200d,
      300000L -> 200d,
      400000L -> 200d,
      500000L -> 200d
    )
    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(100000L, 100000, 600000L, None, None)
    val resultObs = periodicSamplesVectorFnMapper(Observable.fromIterable(Seq(rv)),
      querySession, 1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runToFuture.futureValue.map(_.rows().map
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

    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(100100L, 100000, 600100L, None, None,
      false, Nil, Some(100))
    val resultObs = periodicSamplesVectorFnMapper(Observable.fromIterable(Seq(rv)), querySession,
      1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runToFuture.futureValue.map(_.rows().map
    (r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN))

    resultRows.foreach(_.toList shouldEqual expectedResults)

    val outSchema = periodicSamplesVectorFnMapper.schema(resultSchema)
    outSchema.columns shouldEqual resultSchema.columns
    outSchema.fixedVectorLen shouldEqual Some(6)
  }

  it("should work with offset for HistogramMaxMin") {
    val expected: List[(Long, Map[Double, Double])] = List(
      100100L -> Map(2.0 -> 1.0, 4.0 -> 2.0, 8.0 -> 3.0, 16.0 -> 3.0),
      200100L -> Map(2.0 -> 1.0, 4.0 -> 2.0, 8.0 -> 3.0, 16.0 -> 3.0),
      300100L -> Map(2.0 -> 1.0, 4.0 -> 2.0, 8.0 -> 3.0, 16.0 -> 3.0),
      400100L -> Map(2.0 -> 1.0, 4.0 -> 2.0, 8.0 -> 3.0, 16.0 -> 3.0),
      500100L -> Map(),
      600100L -> Map()
    )

    val expectedResults = expected.map { case (key, value) =>
      s"($key,buckets[${value.keys.map {k => s"$k"}.mkString(", ")}]:" +
        s" {${value.map { case (k, v) => s"$k=$v" }.mkString(", ")}})"
    }.mkString("List(", ", ", ")")

    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(100100L, 100000, 600100L, None, None,
      false, Nil, Some(100))
    val resultObs = periodicSamplesVectorFnMapper(Observable.fromIterable(Seq(rvHistMaxMin)), querySession,
      1000, histMaxMinSchema, Nil)

    val resultRows = resultObs.toListL.runToFuture.futureValue.map(_.rows().map
    (r => (r.getLong(0), r.getHistogram(1))))

    resultRows.map(_.toList).mkString shouldEqual expectedResults

    val outSchema = periodicSamplesVectorFnMapper.schema(histMaxMinSchema)
    outSchema.columns shouldEqual histMaxMinSchema.columns
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
      500000L -> 100d,
      900000L -> 80d,
      1300000 -> 320d
    )

    // step == lookback here
    // stepMultipleNotationUsed = true when step factor notation is used.
    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(500000L, 400000L, 1300000L,
      Some(400000L), Some(Increase),
      stepMultipleNotationUsed = true, Nil, None)
    val resultObs = periodicSamplesVectorFnMapper.apply(Observable.fromIterable(Seq(rv)), querySession,
      1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runToFuture.futureValue.map(_.rows().map
    (r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN))

    resultRows.foreach(_.toList shouldEqual expectedResults)
  }

  it("should not increase resets for NaN") {

    val samples = Seq(
      100000L -> Double.NaN,
      120000L -> 100d,
      153000L -> 20d,
      253000L -> Double.NaN,
      600000L -> 100d
    )

    val rv = timeValueRVPk(samples)

    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(600000L, 100000, 600000L, Some(600000), Some(Resets))
    val resultObs = periodicSamplesVectorFnMapper(Observable.fromIterable(Seq(rv)),
      querySession, 1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runToFuture.futureValue.map(_.rows().map
    (r => (r.getLong(0), r.getDouble(1))).toList)

    resultRows.head.head._2 shouldEqual(1) // 20d to NaN should not increase for NaN
  }

  it("should not increase resets consecutive NaN's") {

    val samples = Seq(
      100000L -> Double.NaN,
      120000L -> 100d,
      153000L -> 20d,
      253000L -> Double.NaN,
      600000L -> Double.NaN
    )

    val rv = timeValueRVPk(samples)

    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(600000L, 100000, 600000L, Some(600000), Some(Resets))
    val resultObs = periodicSamplesVectorFnMapper(Observable.fromIterable(Seq(rv)),
      querySession, 1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runToFuture.futureValue.map(_.rows().map
    (r => (r.getLong(0), r.getDouble(1))).toList)

    // 1 for 100 -> 20.  Not for 1 for 20 -> Double.NaN. Should not increase for Double.NaN -> Double.NaN
    resultRows.head.head._2 shouldEqual(1)
  }

  it("should handle non-RawDataRangeVector via sliding window fallback") {
    // Simulates the AggregatingRangeVector case from OOO ingestion.
    // IteratorBackedRangeVector extends RangeVector but NOT RawDataRangeVector,
    // so PeriodicSamplesMapper must fall back to the SlidingWindowIterator path.
    val testSamples = Seq(
      100000L -> 100d,
      153000L -> 160d,
      200000L -> 200d
    )

    import NoCloseCursor._
    val rows = testSamples.iterator.map { case (ts, v) =>
      val tr = new TransientRow()
      tr.setLong(0, ts)
      tr.setDouble(1, v)
      tr.asInstanceOf[RowReader]
    }
    val nonRawRv: RangeVector = IteratorBackedRangeVector(
      CustomRangeVectorKey(Map.empty), rows, None
    )

    val expectedResults = List(100000L -> 100d,
      200000L -> 200d,
      300000L -> 200d,
      400000L -> 200d,
      500000L -> 200d
    )
    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(100000L, 100000, 600000L, None, None)
    val resultObs = periodicSamplesVectorFnMapper(Observable.fromIterable(Seq(nonRawRv)),
      querySession, 1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runToFuture.futureValue.map(_.rows().map
    (r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN))

    resultRows.foreach(_.toList shouldEqual expectedResults)
  }

  it("should handle non-RawDataRangeVector with sum_over_time via sliding window fallback") {
    import NoCloseCursor._
    val testSamples = Seq(
      100000L -> 10d,
      200000L -> 20d,
      300000L -> 30d,
      400000L -> 40d
    )
    val rows = testSamples.iterator.map { case (ts, v) =>
      val tr = new TransientRow()
      tr.setLong(0, ts)
      tr.setDouble(1, v)
      tr.asInstanceOf[RowReader]
    }
    val nonRawRv: RangeVector = IteratorBackedRangeVector(
      CustomRangeVectorKey(Map.empty), rows, None
    )

    val periodicSamplesVectorFnMapper = exec.PeriodicSamplesMapper(
      200000L, 100000, 400000L, Some(200000L), Some(InternalRangeFunction.SumOverTime))
    val resultObs = periodicSamplesVectorFnMapper(Observable.fromIterable(Seq(nonRawRv)),
      querySession, 1000, resultSchema, Nil)

    val resultRows = resultObs.toListL.runToFuture.futureValue.map(_.rows().map
    (r => (r.getLong(0), r.getDouble(1))).toList)

    resultRows should have size 1
    val results = resultRows.head
    results should have size 3
    // At t=200000: window [0, 200000] contains samples at 100000->10, 200000->20, sum=30
    results(0)._1 shouldEqual 200000L
    results(0)._2 shouldEqual 30d
    // At t=300000: window [100000, 300000] contains 100000->10, 200000->20, 300000->30, sum=60
    results(1)._1 shouldEqual 300000L
    results(1)._2 shouldEqual 60d
    // At t=400000: window [200000, 400000] contains 200000->20, 300000->30, 400000->40, sum=90
    results(2)._1 shouldEqual 400000L
    results(2)._2 shouldEqual 90d
  }
}
