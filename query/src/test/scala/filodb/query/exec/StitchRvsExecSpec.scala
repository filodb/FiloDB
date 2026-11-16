package filodb.query.exec

import scala.annotation.tailrec

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.metadata.Column.ColumnType.{DoubleColumn, HistogramColumn, TimestampColumn}
import filodb.core.query._
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.MetricsTestData
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.vectors.{CustomBuckets, HistogramWithBuckets, LongHistogram, MutableHistogram}
import filodb.query.QueryResult

// scalastyle:off null
class StitchRvsExecSpec extends AnyFunSpec with Matchers with ScalaFutures {
  val error = 0.0000001d

  it ("should merge with two overlapping RVs correctly") {
    val rvs = Seq (
      Seq(  (10L, 3d),
            (20L, 3d),
            (30L, 3d),
            (40L, 3d),
            (50L, 3d)
      ),
      Seq(  (30L, 4d),
            (50L, 4d),
            (60L, 3d),
            (70L, 3d),
            (80L, 3d),
            (90L, 3d),
            (100L, 3d)
      )
    )
    val expected =
      Seq(  (10L, 3d),
            (20L, 3d),
            (30L, Double.NaN),
            (40L, 3d),
            (50L, Double.NaN),
            (60L, 3d),
            (70L, 3d),
            (80L, 3d),
            (90L, 3d),
            (100L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }

  it ("should merge with two overlapping RVs with NaNs correctly") {
    val rvs = Seq (
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d),
        (60L, Double.NaN),
        (70L, Double.NaN),
        (80L, Double.NaN),
        (90L, Double.NaN),
        (100L, Double.NaN)
      ),
      Seq(  (10L, Double.NaN),
        (20L, Double.NaN),
        (30L, 4d),
        (50L, 4d),
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      )
    )
    val expected =
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, Double.NaN),
        (40L, 3d),
        (50L, Double.NaN),
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }

  it ("should merge one RV correctly") {
    val input =       Seq(  (10L, 3d),
      (20L, 3d),
      (30L, Double.NaN),
      (40L, 3d),
      (50L, Double.NaN),
      (60L, 3d),
      (70L, 3d),
      (80L, 3d),
      (90L, 3d),
      (100L, 3d)
    )
    mergeAndValidate(Seq(input), input)
  }
  it ("should merge with three overlapping RVs correctly") {
    val rvs = Seq (
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d)
      ),
      Seq(  (30L, 4d),
        (50L, 4d),
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      ),
      Seq(  (30L, 4d),
        (55L, 3d)
      )
    )
    val expected =
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, Double.NaN),
        (40L, 3d),
        (50L, Double.NaN),
        (55L, 3d),
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }

  it ("should output range passed in to StitchRvsExec") {

    val rvsData = Seq (
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d)
      ),
      Seq(
        // No data for 60 in wither inputs, should be NaN in o/p
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d),
        // 110 missing, should be present in o/p as NaN
        (120L, 3d),
        (130L, 3d)
      )
    )
    val expected =
      Seq(
        (0L, Double.NaN),
        (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d),
        (60L, Double.NaN),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d),
        (110L, Double.NaN),
        (120L, 3d),
        (130L, 3d),
        (140L, Double.NaN),
        (150L, Double.NaN)
      )

    // Output range is from 0 to 150, o/p must have data starting from 0 to 150 with a step of 10. If data is
    // missing in the input Rvs, it should be NaN
    // null needed below since there is a require in code that prevents empty children
    val exec = StitchRvsExec(QueryContext(), InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig), Some(RvRange(0, 10, 150)),
      Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))
    val rs = ResultSchema(List(ColumnInfo("timestamp",
      TimestampColumn), ColumnInfo("value", DoubleColumn)), 1)

    val res0 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty, rvsData.head, RvRange(10, 10, 50))))
    val res1 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty, rvsData(1), RvRange(30, 10, 100))))

    val inputRes = Seq(res0, res1).zipWithIndex
    val output = exec.compose(Observable.fromIterable(inputRes), Task.eval(null), QuerySession.makeForTestingOnly())
      .toListL.runToFuture.futureValue
    output.size shouldEqual 1

    // Notice how the output RVRange is same as the one passed during initialization of the StitchRvsExec
    output.head.outputRange shouldEqual Some(RvRange(0, 10, 150))
    compareIter(output.head.rows().map(r => (r.getLong(0), r.getDouble(1))) , expected.toIterator)
  }


  it ("should fail when None is passed as the output range") {
    // null needed below since there is a require in code that prevents empty children

      val rvsData = Seq (
          Seq(  (10L, 3d),
              (20L, 3d),
              (30L, 3d),
              (40L, 3d),
              (50L, 3d)
              ),
          Seq(
              (60L, 3d),
              (70L, 3d),
              (80L, 3d),
              (90L, 3d),
              (100L, 3d)
              )
          )
      val expected =
          Seq(
              (10L, 3d),
              (20L, 3d),
              (30L, 3d),
              (40L, 3d),
              (50L, 3d),
              (60L, 3d),
              (70L, 3d),
              (80L, 3d),
              (90L, 3d),
              (100L, 3d)
              )

    // rvRange is None, this is the case when stitch is called on raw series
    val exec = StitchRvsExec(QueryContext(), InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig), None,
            Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))
    val rs = ResultSchema(List(ColumnInfo("timestamp",
        TimestampColumn), ColumnInfo("value", DoubleColumn)), 1)

    val res0 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty, rvsData.head, RvRange(10, 10, 50))))
    val res1 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty, rvsData(1), RvRange(30, 10, 100))))

    val inputRes = Seq(res0, res1).zipWithIndex
    val output = exec.compose(Observable.fromIterable(inputRes), Task.eval(null), QuerySession.makeForTestingOnly())
        .toListL.runToFuture.futureValue
        output.size shouldEqual 1
        output.head.outputRange shouldEqual None
        compareIter(output.head.rows().map(r => (r.getLong(0), r.getDouble(1))) , expected.toIterator)
  }

  it ("should reduce result schemas with different fixedVecLengths without error") {

    // null needed below since there is a require in code that prevents empty children
    val exec = StitchRvsExec(QueryContext(), InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig),
      Some(RvRange(0, 10, 100)), Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))

    val rs1 = ResultSchema(List(ColumnInfo("timestamp",
      TimestampColumn), ColumnInfo("value", DoubleColumn)), 1, Map(), Some(430), List(0, 1))
    val rs2 = ResultSchema(List(ColumnInfo("timestamp",
      TimestampColumn), ColumnInfo("value", DoubleColumn)), 1, Map(), Some(147), List(0, 1))

    val reduced = exec.reduceSchemas(rs1, rs2)
    reduced.columns shouldEqual rs1.columns
    reduced.numRowKeyColumns shouldEqual rs1.numRowKeyColumns
    reduced.brSchemas shouldEqual rs1.brSchemas
    reduced.fixedVectorLen shouldEqual Some(430 + 147)
    reduced.colIDs shouldEqual rs1.colIDs
  }

  it ("should merge with no overlap correctly") {
    val rvs = Seq (
      Seq(
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      ),
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d)
      )
    )
    val expected =
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d),
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }

  it ("should cap results honoring RVRange despite rvs having data") {
    val rvs = Seq (
      Seq(
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      ),
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d)
      )
    )
    val expected =
      Seq(
        (0L, Double.NaN),
        (10L, 3d),
        (20L, 3d),
        (30L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }


  it ("should merge with one empty rv correctly") {
    val rvs = Seq (
      Seq(
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      ),
      Seq()
    )
    val expected =
      Seq(
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }

  it("should merge avg correctly when there are gaps") {
    // The test relies on mergeAndValidate to generate the expected RvRange to (40, 10, 90)
    // The merge functionality should honor the passed RvRange and accordingly generate the rows
    val rvs = Seq(
      Seq(
        (50L, 1L, 2.0),
        (60L, 2L, 5.0),
      ),
      Seq(
        (30L, 4L, 5.0)
      )
    )
    val expected = Seq(
        (30L, 4L, 5.0),
        (40L, 0L, Double.NaN),
        (50L, 1L, 2.0),
        (60L, 2L, 5.0)
      )
    val inputSeq = rvs.map { rows =>
      new NoCloseCursor(rows.iterator.map(r => {
        val row = new AvgAggTransientRow()
        row.setLong(0, r._1)
        row.setLong(2, r._2)
        row.setDouble(1, r._3)
        row
      }))
    }
    val (minTs, maxTs) = expected.foldLeft((Long.MaxValue, Long.MinValue)) {
      case ((allMin, allMax), (thisTs, _, _)) => (allMin.min(thisTs), allMax.max(thisTs)) }
    val expectedStep = expected match {
      case (t1, _, _) :: (t2, _, _) :: _ => t2 - t1
      case _ => 1
    }
    val result = StitchRvsExec.merge(inputSeq, Some(RvRange(startMs = minTs, endMs = maxTs, stepMs = expectedStep)))
      .map(r => (r.getLong(0), r.getLong(2), r.getDouble(1)))
    compareIterAvg(result, expected.toIterator)
  }

  it("should honor the passed RvRange from planner when generating rows") {
    // The test relies on mergeAndValidate to generate the expected RvRange to (40, 10, 90)
    // The merge functionality should honor the passed RvRange and accordingly generate the rows
    val rvs = Seq(
      Seq(
        (60L, 3d),
        (70L, 3d)
      ),
      Seq()
    )
    val expected =
      Seq(
        (40L, Double.NaN),
        (50L, Double.NaN),
        (60L, 3d),
        (70L, 3d),
        (80L, Double.NaN),
        (90L, Double.NaN)
      )
    mergeAndValidate(rvs, expected)
  }

  it("should merge histograms correctly when there are no gaps") {
    // The test relies on mergeAndValidate to generate the expected RvRange to (40, 10, 90)
    // The merge functionality should honor the passed RvRange and accordingly generate the rows
    val h1 = LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), (0L to 10L by 5).toArray)
    val h2 = LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), (0L to 10L by 2).toArray)
    val rvs = Seq(
      Seq(
        (50L, h1),
        (60L, h1),
      ),
      Seq(
        (30L, h2),
        (40L, h2)
      )
    )
    val expected =
      Seq(
        (30L, h2),
        (40L, h2),
        (50L, h1),
        (60L, h1)
      )
    mergeAndValidateHistogram(rvs, expected)
  }

  it("should merge histograms correctly when there are gaps") {
    // The test relies on mergeAndValidate to generate the expected RvRange to (40, 10, 90)
    // The merge functionality should honor the passed RvRange and accordingly generate the rows
    val h1 = LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), (0L to 10L by 5).toArray)
    val h2 = LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), (0L to 10L by 2).toArray)
    val rvs = Seq(
      Seq(
        (50L, h1),
        (60L, h1),
      ),
      Seq(
        (30L, h2)
      )
    )
    val expected =
      Seq(
        (30L, h2),
        (40L, HistogramWithBuckets.empty),
        (50L, h1),
        (60L, h1)
      )
    mergeAndValidateHistogram(rvs, expected)
  }
  it("should merge histograms correctly when there are gaps and empty data points1") {
    // The test relies on mergeAndValidate to generate the expected RvRange to (40, 10, 90)
    // The merge functionality should honor the passed RvRange and accordingly generate the rows
    val h1 = LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), (0L to 10L by 5).toArray)
    val h2 = LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), (0L to 10L by 2).toArray)
    val rvs = Seq(
      Seq(
        (30L, h2),
        (50L, h1),
        (60L, h1),
      ),
      Seq()
    )
    val expected =
      Seq(
        (30L, h2),
        (40L, HistogramWithBuckets.empty),
        (50L, h1),
        (60L, h1)
      )
    mergeAndValidateHistogram(rvs, expected)
  }

  it("should merge histograms correctly when there are gaps and empty data points2") {
    // The test relies on mergeAndValidate to generate the expected RvRange to (40, 10, 90)
    // The merge functionality should honor the passed RvRange and accordingly generate the rows
    val h1 = LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), (0L to 10L by 5).toArray)
    val h2 = LongHistogram(CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity)), (0L to 10L by 2).toArray)
    val rvs = Seq(
      Seq(),
      Seq(
        (30L, h2),
        (50L, h1),
        (60L, h1),
      )
    )
    val expected =
      Seq(
        (30L, h2),
        (40L, HistogramWithBuckets.empty),
        (50L, h1),
        (60L, h1)
      )
    mergeAndValidateHistogram(rvs, expected)
  }


  it("should sum overlapping values when overlapMerge is Sum") {
    val rvs = Seq(
      Seq((10L, 1d), (20L, 2d), (30L, 3d), (40L, Double.NaN)),
      Seq((30L, 10d), (40L, 20d), (50L, 30d))
    )
    val expected = Seq((10L, 1d), (20L, 2d), (30L, 13d), (40L, 20d), (50L, 30d))
    mergeAndValidate(rvs, expected, StitchOverlapMerge.Sum)
  }

  it("should sum overlapping values of three RVs and emit NaN for gaps when overlapMerge is Sum") {
    val rvs = Seq(
      Seq((10L, 1d), (20L, 1d)),
      Seq((20L, 2d)),
      Seq((20L, 3d), (40L, 3d))
    )
    val expected = Seq((10L, 1d), (20L, 6d), (30L, Double.NaN), (40L, 3d))
    mergeAndValidate(rvs, expected, StitchOverlapMerge.Sum)
  }

  it("should emit NaN when all overlapping values are NaN when overlapMerge is Sum") {
    val rvs = Seq(
      Seq((10L, 1d), (20L, Double.NaN)),
      Seq((20L, Double.NaN), (30L, 2d))
    )
    val expected = Seq((10L, 1d), (20L, Double.NaN), (30L, 2d))
    mergeAndValidate(rvs, expected, StitchOverlapMerge.Sum)
  }

  it("should pick max and min of overlapping values when overlapMerge is Max and Min") {
    val rvs = Seq(
      Seq((10L, 1d), (20L, 5d), (30L, 3d)),
      Seq((20L, 4d), (30L, 7d), (40L, 2d))
    )
    mergeAndValidate(rvs, Seq((10L, 1d), (20L, 5d), (30L, 7d), (40L, 2d)), StitchOverlapMerge.Max)
    mergeAndValidate(rvs, Seq((10L, 1d), (20L, 4d), (30L, 3d), (40L, 2d)), StitchOverlapMerge.Min)
  }

  it("should sum overlapping histograms when overlapMerge is Sum") {
    val buckets = CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity))
    val h1 = LongHistogram(buckets, Array(1L, 2L, 3L))
    val h2 = LongHistogram(buckets, Array(10L, 20L, 30L))
    val rvs = Seq(
      Seq((10L, h1), (20L, h1), (30L, h1)),
      Seq((20L, h2), (30L, HistogramWithBuckets.empty), (40L, h2))
    )
    val expected = Seq(
      (10L, h1),
      (20L, MutableHistogram(buckets, Array(11d, 22d, 33d))),
      (30L, h1),
      (40L, h2)
    )
    mergeAndValidateHistogram(rvs, expected, StitchOverlapMerge.Sum)
  }

  it("should fail to merge overlapping histograms when overlapMerge is Max") {
    val buckets = CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity))
    val h1 = LongHistogram(buckets, Array(1L, 2L, 3L))
    val rvs = Seq(Seq((10L, h1)), Seq((10L, h1)))
    intercept[IllegalArgumentException] {
      mergeAndValidateHistogram(rvs, Seq((10L, h1)), StitchOverlapMerge.Max)
    }
  }

  it("should sum overlapping partial results in compose when overlapMerge is Sum") {
    val exec = StitchRvsExec(QueryContext(), InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig),
      Some(RvRange(10, 10, 60)), Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]),
      overlapMerge = StitchOverlapMerge.Sum)
    val rs = ResultSchema(List(ColumnInfo("timestamp", TimestampColumn), ColumnInfo("value", DoubleColumn)), 1)
    // Partition 1 has results up to 40 and partition 2 from 30, both have partial results for 30 and 40
    val res0 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty,
      Seq((10L, 1d), (20L, 1d), (30L, 0.5d), (40L, 0.25d)), RvRange(10, 10, 40))))
    val res1 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty,
      Seq((30L, 0.5d), (40L, 0.75d), (50L, 1d)), RvRange(30, 10, 50))))
    val output = exec.compose(Observable.fromIterable(Seq(res0, res1).zipWithIndex), Task.eval(null),
      QuerySession.makeForTestingOnly()).toListL.runToFuture.futureValue
    output.size shouldEqual 1
    compareIter(output.head.rows().map(r => (r.getLong(0), r.getDouble(1))),
      Seq((10L, 1d), (20L, 1d), (30L, 1d), (40L, 1d), (50L, 1d), (60L, Double.NaN)).toIterator)
  }

  it("should sum overlapping histogram partial results in compose when overlapMerge is Sum") {
    val buckets = CustomBuckets(Array(1.0, 2.0, Double.PositiveInfinity))
    val h1 = LongHistogram(buckets, Array(1L, 2L, 3L))
    val h2 = LongHistogram(buckets, Array(10L, 20L, 30L))
    def histRv(data: Seq[(Long, HistogramWithBuckets)], range: RvRange): RangeVector = new RangeVector {
      override def key: RangeVectorKey = CustomRangeVectorKey.empty
      override def rows(): RangeVectorCursor =
        new NoCloseCursor(data.iterator.map(r => new TransientHistRow(r._1, r._2)))
      override def outputRange: Option[RvRange] = Some(range)
    }
    val exec = StitchRvsExec(QueryContext(), InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig),
      Some(RvRange(10, 10, 30)), Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]),
      overlapMerge = StitchOverlapMerge.Sum)
    val rs = ResultSchema(List(ColumnInfo("timestamp", TimestampColumn), ColumnInfo("h", HistogramColumn)), 1)
    val res0 = QueryResult("id", rs, Seq(histRv(Seq((10L, h1), (20L, h1)), RvRange(10, 10, 20))))
    val res1 = QueryResult("id", rs, Seq(histRv(Seq((20L, h2), (30L, h2)), RvRange(20, 10, 30))))
    val output = exec.compose(Observable.fromIterable(Seq(res0, res1).zipWithIndex), Task.eval(null),
      QuerySession.makeForTestingOnly()).toListL.runToFuture.futureValue
    output.size shouldEqual 1
    compareIterHist(output.head.rows().map(r => (r.getLong(0), r.getHistogram(1).asInstanceOf[HistogramWithBuckets])),
      Seq((10L, h1), (20L, MutableHistogram(buckets, Array(11d, 22d, 33d))), (30L, h2)).toIterator)
  }

  def mergeAndValidateHistogram(rvs: Seq[Seq[(Long, HistogramWithBuckets)]],
                                expected: Seq[(Long, HistogramWithBuckets)],
                                overlapMerge: StitchOverlapMerge = StitchOverlapMerge.NaNOnConflict): Unit = {
    val inputSeq = rvs.map { rows =>
      new NoCloseCursor(rows.iterator.map(r => new TransientHistRow(r._1, r._2)))
    }
    val (minTs, maxTs) = expected.foldLeft((Long.MaxValue, Long.MinValue)) { case ((allMin, allMax), (thisTs, _)) => (allMin.min(thisTs), allMax.max(thisTs)) }
    val expectedStep = expected match {
      case (t1, _) :: (t2, _) :: _ => t2 - t1
      case _ => 1
    }
    val result = StitchRvsExec.merge(inputSeq, Some(RvRange(startMs = minTs, endMs = maxTs, stepMs = expectedStep)),
        overlapMerge = overlapMerge, isHistogram = true)
      .map(r => (r.getLong(0), r.getHistogram(1).asInstanceOf[HistogramWithBuckets]))

    compareIterHist(result, expected.toIterator)
  }

  def mergeAndValidate(rvs: Seq[Seq[(Long, Double)]], expected: Seq[(Long, Double)],
                       overlapMerge: StitchOverlapMerge = StitchOverlapMerge.NaNOnConflict): Unit = {
    val inputSeq = rvs.map { rows =>
      new NoCloseCursor(rows.iterator.map(r => new TransientRow(r._1, r._2)))
    }
    val (minTs, maxTs) = expected.foldLeft((Long.MaxValue, Long.MinValue))
      {case ((allMin, allMax), (thisTs, _)) => (allMin.min(thisTs), allMax.max(thisTs))}
    val expectedStep = expected match {
      case (t1, _) :: (t2, _) :: _ => t2 - t1
      case _                       => 1
    }
    val result = StitchRvsExec.merge(inputSeq, Some(RvRange(startMs = minTs, endMs = maxTs, stepMs = expectedStep)),
        overlapMerge = overlapMerge)
      .map(r => (r.getLong(0), r.getDouble(1)))
    compareIter(result, expected.toIterator)
  }

  @tailrec
  final private def compareIter(it1: Iterator[(Long, Double)], it2: Iterator[(Long, Double)]) : Unit = {
    (it1.hasNext, it2.hasNext) match{
      case (true, true) =>
        val v1 = it1.next()
        val v2 = it2.next()
        v1._1 shouldEqual v2._1
        if (v1._2.isNaN) v2._2.isNaN shouldEqual true
        else Math.abs(v1._2-v2._2) should be < error
        compareIter(it1, it2)
      case (false, false) => ()
      case _ => fail("Unequal lengths")
    }
  }

  @tailrec
  final private def compareIterAvg(it1: Iterator[(Long, Long, Double)], it2: Iterator[(Long, Long, Double)]): Unit = {
    (it1.hasNext, it2.hasNext) match {
      case (true, true) =>
        val v1 = it1.next()
        val v2 = it2.next()
        v1._1 shouldEqual v2._1
        v1._2 shouldEqual v2._2
        if (v1._3.isNaN) v2._3.isNaN shouldEqual true
        else Math.abs(v1._3 - v2._3) should be < error
        compareIterAvg(it1, it2)
      case (false, false) => ()
      case _ => fail("Unequal lengths")
    }
  }

  def arraysEqual(arr1: Array[Double], arr2: Array[Double]): Boolean = {
    if (arr1.length != arr2.length) return false
    for (i <- arr1.indices) {
      if (arr1(i) != arr2(i)) return false
    }
    true
  }

  @tailrec
  final private def compareIterHist(it1: Iterator[(Long, HistogramWithBuckets)],
                                    it2: Iterator[(Long, HistogramWithBuckets)]): Unit = {
    (it1.hasNext, it2.hasNext) match {
      case (true, true) =>
        val v1 = it1.next()
        val v2 = it2.next()
        v1._1 shouldEqual v2._1
        v1._2.buckets.equals(v2._2.buckets) shouldEqual true
        arraysEqual(v1._2.valueArray, v2._2.valueArray) shouldEqual true
        compareIterHist(it1, it2)
      case (false, false) => ()
      case _ => fail("Unequal lengths")
    }
  }

  it ("should have correct output range when stitched") {
    val r1 = new RangeVector {
      override def key: RangeVectorKey = CustomRangeVectorKey.empty
      override def rows(): RangeVectorCursor = Iterator.empty
      override def outputRange: Option[RvRange] = Some(RvRange(10, 10, 100))
    }

    val r2 = new RangeVector {
      override def key: RangeVectorKey = CustomRangeVectorKey.empty
      override def rows(): RangeVectorCursor = Iterator.empty
      override def outputRange: Option[RvRange] = Some(RvRange(90, 10, 200))
    }

    val r = StitchRvsExec.stitch(r1, r2, Some(RvRange(10, 10, 200)))
    r.outputRange shouldEqual Some(RvRange(10, 10, 200))

  }

  it("should fail if step is non positive number") {
    val caught = intercept[IllegalArgumentException] {
      StitchRvsExec(QueryContext(), InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig),
        Some(RvRange(startMs = 0, endMs = 100, stepMs = 0)),
        Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))
    }
    caught.getMessage shouldEqual "requirement failed: RvRange start <= end and step > 0"

  }

  it("should fail if start > end is non positive number") {
    val caught = intercept[IllegalArgumentException] {
      StitchRvsExec(QueryContext(), InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig),
        Some(RvRange(startMs = 110, endMs = 100, stepMs = 1)),
        Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))
    }
    caught.getMessage shouldEqual "requirement failed: RvRange start <= end and step > 0"
  }

  // Test with different step and no step not needed any more, refer to history of this file to see the removed tests
}

