package filodb.query.exec

import scala.annotation.tailrec

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.metadata.Column.ColumnType.{DoubleColumn, TimestampColumn}
import filodb.core.query._
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.MetricsTestData
import filodb.memory.format.UnsafeUtils
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
    val exec = StitchRvsExec(QueryContext(), InProcessPlanDispatcher(EmptyQueryConfig), Some(RvRange(0, 10, 150)),
      Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))
    val rs = ResultSchema(List(ColumnInfo("timestamp",
      TimestampColumn), ColumnInfo("value", DoubleColumn)), 1)

    val res0 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty, rvsData.head, RvRange(10, 10, 50))))
    val res1 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty, rvsData(1), RvRange(30, 10, 100))))

    val inputRes = Seq(res0, res1).zipWithIndex
    val output = exec.compose(Observable.fromIterable(inputRes), Task.now(null), QuerySession.makeForTestingOnly())
      .toListL.runAsync.futureValue
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
    val exec = StitchRvsExec(QueryContext(), InProcessPlanDispatcher(EmptyQueryConfig), None,
            Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))
    val rs = ResultSchema(List(ColumnInfo("timestamp",
        TimestampColumn), ColumnInfo("value", DoubleColumn)), 1)

    val res0 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty, rvsData.head, RvRange(10, 10, 50))))
    val res1 = QueryResult("id", rs, Seq(MetricsTestData.makeRv(CustomRangeVectorKey.empty, rvsData(1), RvRange(30, 10, 100))))

    val inputRes = Seq(res0, res1).zipWithIndex
    val output = exec.compose(Observable.fromIterable(inputRes), Task.now(null), QuerySession.makeForTestingOnly())
        .toListL.runAsync.futureValue
        output.size shouldEqual 1
        output.head.outputRange shouldEqual None
        compareIter(output.head.rows().map(r => (r.getLong(0), r.getDouble(1))) , expected.toIterator)
  }

  it ("should reduce result schemas with different fixedVecLengths without error") {

    // null needed below since there is a require in code that prevents empty children
    val exec = StitchRvsExec(QueryContext(), InProcessPlanDispatcher(EmptyQueryConfig),
      Some(RvRange(0, 10, 100)), Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))

    val rs1 = ResultSchema(List(ColumnInfo("timestamp",
      TimestampColumn), ColumnInfo("value", DoubleColumn)), 1, Map(), Some(430), List(0, 1))
    val rs2 = ResultSchema(List(ColumnInfo("timestamp",
      TimestampColumn), ColumnInfo("value", DoubleColumn)), 1, Map(), Some(147), List(0, 1))

    val reduced = exec.reduceSchemas(rs1, QueryResult("someId", rs2, Seq.empty))
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




  it ("should honor the passed RvRange from planner when generating rows") {
    // The test relies on mergeAndValidate to generate the expected RvRange to (40, 10, 90)
    // The merge functionality should honor the passed RvRange and accordingly generate the rows
    val rvs = Seq (
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

  def mergeAndValidate(rvs: Seq[Seq[(Long, Double)]], expected: Seq[(Long, Double)]): Unit = {
    val inputSeq = rvs.map { rows =>
      new NoCloseCursor(rows.iterator.map(r => new TransientRow(r._1, r._2)))
    }
    val (minTs, maxTs) = expected.foldLeft((Long.MaxValue, Long.MinValue))
      {case ((allMin, allMax), (thisTs, _)) => (allMin.min(thisTs), allMax.max(thisTs))}
    val expectedStep = expected match {
      case (t1, _) :: (t2, _) :: _ => t2 - t1
      case _                       => 1
    }
    val result = StitchRvsExec.merge(inputSeq, Some(RvRange(startMs = minTs, endMs = maxTs, stepMs = expectedStep)))
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
      case (false, false) => Unit
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
      StitchRvsExec(QueryContext(), InProcessPlanDispatcher(EmptyQueryConfig),
        Some(RvRange(startMs = 0, endMs = 100, stepMs = 0)),
        Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))
    }
    caught.getMessage shouldEqual "requirement failed: RvRange start <= end and step > 0"

  }

  it("should fail if start > end is non positive number") {
    val caught = intercept[IllegalArgumentException] {
      StitchRvsExec(QueryContext(), InProcessPlanDispatcher(EmptyQueryConfig),
        Some(RvRange(startMs = 110, endMs = 100, stepMs = 1)),
        Seq(UnsafeUtils.ZeroPointer.asInstanceOf[ExecPlan]))
    }
    caught.getMessage shouldEqual "requirement failed: RvRange start <= end and step > 0"
  }

  // Test with different step and no step not needed any more, refer to history of this file to see the removed tests
}

