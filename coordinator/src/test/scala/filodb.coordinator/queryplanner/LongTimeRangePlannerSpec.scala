package filodb.coordinator.queryplanner

import scala.concurrent.duration._

import monix.execution.Scheduler
import org.scalatest.{FunSpec, Matchers}

import filodb.core.DatasetRef
import filodb.core.query.{QueryContext, QuerySession}
import filodb.core.store.ChunkSource
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{LogicalPlan, PeriodicSeriesPlan, PeriodicSeriesWithWindowing}
import filodb.query.exec._

class LongTimeRangePlannerSpec extends FunSpec with Matchers {

  class MockExecPlan(val name: String, val lp: LogicalPlan) extends ExecPlan {
    override def queryContext: QueryContext = QueryContext()
    override def children: Seq[ExecPlan] = ???
    override def submitTime: Long = ???
    override def dataset: DatasetRef = ???
    override def dispatcher: PlanDispatcher = ???
    override def doExecute(source: ChunkSource, querySession: QuerySession)
                          (implicit sched: Scheduler): ExecResult = ???
    override protected def args: String = ???
  }

  val rawPlanner = new QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      new MockExecPlan("raw", logicalPlan)
    }
  }

  val downsamplePlanner = new QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      new MockExecPlan("downsample", logicalPlan)
    }
  }

  val rawRetention = 10.minutes
  val now = System.currentTimeMillis() / 1000 * 1000
  val earliestRawTime = now - rawRetention.toMillis
  val latestDownsampleTime = now - 4.minutes.toMillis // say it takes 4 minutes to downsample
  val splitThreshold = 6.minutes
  val splitSize = 6.minutes

  private def disp = InProcessPlanDispatcher
  val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
                                                 earliestRawTime, latestDownsampleTime, disp)


  val earliestRawTimeForSplits = now - 20.minutes.toMillis
  val longTermPlannerWithTimeSplits = new LongTimeRangeSplitPlanner(rawPlanner, downsamplePlanner,
                                                               earliestRawTimeForSplits, latestDownsampleTime,
                                                               splitThreshold.toMillis, splitSize.toMillis, disp)

  it("should direct raw-cluster-only queries to raw planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[2m])",
      TimeStepParams(now/1000 - 7.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 1.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "raw"
    ep.lp shouldEqual logicalPlan
  }

  it("should direct downsample-only queries to downsample planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[2m])",
      TimeStepParams(now/1000 - 20.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 15.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "downsample"
    ep.lp shouldEqual logicalPlan
  }

  it("should direct overlapping queries to both raw & downsample planner and stitch") {

    val start = now/1000 - 30.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000 - 2.minutes.toSeconds
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[2m])",
                                                     TimeStepParams(start, step, end))
                                                    .asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext())
    val stitchExec = ep.asInstanceOf[StitchRvsExec]
    stitchExec.children.size shouldEqual 2

    val rawEp = stitchExec.children.head.asInstanceOf[MockExecPlan]
    val downsampleEp = stitchExec.children.last.asInstanceOf[MockExecPlan]

    rawEp.name shouldEqual "raw"
    downsampleEp.name shouldEqual "downsample"
    val rawLp = rawEp.lp.asInstanceOf[PeriodicSeriesPlan]
    val downsampleLp = downsampleEp.lp.asInstanceOf[PeriodicSeriesPlan]

    // find first instant with range available within raw data
    val rawStart = ((start*1000) to (end*1000) by (step*1000)).find { instant =>
      instant - 2.minutes.toMillis > earliestRawTime
    }.get

    rawLp.startMs shouldEqual rawStart
    rawLp.endMs shouldEqual logicalPlan.endMs

    downsampleLp.startMs shouldEqual logicalPlan.startMs
    downsampleLp.endMs shouldEqual rawStart - 1.minute.toMillis
  }

  it("should delegate to downsample cluster and omit recent instants when there is a long lookback") {

    val start = now/1000 - 30.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000
    // notice raw data retention is 10m but lookback is 20m
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[20m])",
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext())
    val downsampleLp = ep.asInstanceOf[MockExecPlan]
    downsampleLp.lp.asInstanceOf[PeriodicSeriesPlan].startMs shouldEqual logicalPlan.startMs
    downsampleLp.lp.asInstanceOf[PeriodicSeriesPlan].endMs shouldEqual latestDownsampleTime

  }

  it("should delegate to downsample cluster and retain endTime when there is a long lookback with offset that causes " +
    "recent data to not be used") {

    val start = now/1000 - 30.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000
    // notice raw data retention is 10m but lookback is 20m
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[20m] offset 5m)",
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext())
    val downsampleLp = ep.asInstanceOf[MockExecPlan]
    downsampleLp.lp.asInstanceOf[PeriodicSeriesPlan].startMs shouldEqual logicalPlan.startMs
    // endTime is retained even with long lookback because 5m offset compensates
    // for 4m delay in downsample data population
    downsampleLp.lp.asInstanceOf[PeriodicSeriesPlan].endMs shouldEqual logicalPlan.endMs

  }

  it("should direct raw-data queries to both raw planner only irrespective of time length") {

    Seq(5, 10, 20).foreach { t =>
      val logicalPlan = Parser.queryToLogicalPlan(s"foo[${t}m]", now, 1000)
      val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
      ep.name shouldEqual "raw"
      ep.lp shouldEqual logicalPlan
    }
  }

  it("should direct raw-cluster-only queries to raw planner for scalar vector queries") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("scalar(vector(1)) * 10",
      TimeStepParams(now/1000 - 7.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 1.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "raw"
    ep.lp shouldEqual logicalPlan
  }

  it("should direct overlapping offset queries to both raw & downsample planner and stitch") {

    val start = now/1000 - 30.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000 - 2.minutes.toSeconds
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[5m] offset 2m)",
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext())
    val stitchExec = ep.asInstanceOf[StitchRvsExec]
    stitchExec.children.size shouldEqual 2

    val rawEp = stitchExec.children.head.asInstanceOf[MockExecPlan]
    val downsampleEp = stitchExec.children.last.asInstanceOf[MockExecPlan]

    rawEp.name shouldEqual "raw"
    downsampleEp.name shouldEqual "downsample"
    val rawLp = rawEp.lp.asInstanceOf[PeriodicSeriesPlan]
    val downsampleLp = downsampleEp.lp.asInstanceOf[PeriodicSeriesPlan]

    // find first instant with range available within raw data
    val rawStart = ((start*1000) to (end*1000) by (step*1000)).find { instant =>
      instant - (5 + 2).minutes.toMillis > earliestRawTime // subtract lookback & offset
    }.get

    rawLp.startMs shouldEqual rawStart
    rawLp.endMs shouldEqual logicalPlan.endMs
    rawLp.asInstanceOf[PeriodicSeriesWithWindowing].offsetMs.get shouldEqual(120000)

    downsampleLp.startMs shouldEqual logicalPlan.startMs
    downsampleLp.endMs shouldEqual rawStart - (step * 1000)
    downsampleLp.asInstanceOf[PeriodicSeriesWithWindowing].offsetMs.get shouldEqual(120000)
  }

  // Split query plan along time-dimension with LongTimeRangeSplitPlanner
  // Creates stitchExec plans
  it("should split the plan, direct raw-cluster-only queries to raw planner and stitch") {

    val start = now/1000 - 18.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000 - 1.minutes.toSeconds

    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[2m])",
      TimeStepParams(start, step, end)).asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlannerWithTimeSplits.materialize(logicalPlan, QueryContext())

    val stitchExec = ep.asInstanceOf[StitchRvsExec]
    stitchExec.children.size shouldEqual 3

    val rawEp1 = stitchExec.children.head.asInstanceOf[MockExecPlan]
    val rawEp2 = stitchExec.children(1).asInstanceOf[MockExecPlan]
    val rawEp3 = stitchExec.children.last.asInstanceOf[MockExecPlan]

    rawEp1.name shouldEqual "raw"
    rawEp2.name shouldEqual "raw"
    rawEp3.name shouldEqual "raw"

    val rawLp1 = rawEp1.lp.asInstanceOf[PeriodicSeriesPlan]
    val rawLp2 = rawEp2.lp.asInstanceOf[PeriodicSeriesPlan]
    val rawLp3 = rawEp3.lp.asInstanceOf[PeriodicSeriesPlan]

    val rawLpStartMs1 = logicalPlan.startMs
    val rawLpStartMs2 = rawLpStartMs1 + logicalPlan.stepMs + splitSize.toMillis
    val rawLpStartMs3 = rawLpStartMs2 + logicalPlan.stepMs + splitSize.toMillis

    rawLp1.startMs shouldEqual rawLpStartMs1
    rawLp1.endMs shouldEqual rawLpStartMs1 + splitSize.toMillis

    rawLp2.startMs shouldEqual rawLpStartMs2
    rawLp2.endMs shouldEqual rawLpStartMs2 + splitSize.toMillis

    rawLp3.startMs shouldEqual rawLpStartMs3

    // split size is up-to logicalPlan.endMs
    rawLp3.endMs shouldEqual Math.min(rawLpStartMs3 + splitSize.toMillis, logicalPlan.endMs)
  }

  it("should split the plan, direct downsample-cluster-only queries to downsample planner and stitch") {

    val start = now/1000 - 40.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000 - 25.minutes.toSeconds

    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[2m])",
      TimeStepParams(start, step, end)).asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlannerWithTimeSplits.materialize(logicalPlan, QueryContext())

    val stitchExec = ep.asInstanceOf[StitchRvsExec]
    stitchExec.children.size shouldEqual 3

    val downsampleEp1 = stitchExec.children.head.asInstanceOf[MockExecPlan]
    val downsampleEp2 = stitchExec.children(1).asInstanceOf[MockExecPlan]
    val downsampleEp3 = stitchExec.children.last.asInstanceOf[MockExecPlan]

    downsampleEp1.name shouldEqual "downsample"
    downsampleEp2.name shouldEqual "downsample"
    downsampleEp3.name shouldEqual "downsample"

    val downsampleLp1 = downsampleEp1.lp.asInstanceOf[PeriodicSeriesPlan]
    val downsampleLp2 = downsampleEp2.lp.asInstanceOf[PeriodicSeriesPlan]
    val downsampleLp3 = downsampleEp3.lp.asInstanceOf[PeriodicSeriesPlan]

    val downsampleLpStartMs1 = logicalPlan.startMs
    val downsampleLpStartMs2 = downsampleLpStartMs1 + logicalPlan.stepMs + splitSize.toMillis
    val downsampleLpStartMs3 = downsampleLpStartMs2 + logicalPlan.stepMs + splitSize.toMillis


    downsampleLp1.startMs shouldEqual downsampleLpStartMs1
    downsampleLp1.endMs shouldEqual downsampleLpStartMs1 + splitSize.toMillis

    downsampleLp2.startMs shouldEqual downsampleLpStartMs2
    downsampleLp2.endMs shouldEqual downsampleLpStartMs2 + splitSize.toMillis

    downsampleLp3.startMs shouldEqual downsampleLpStartMs3
    downsampleLp3.endMs shouldEqual Math.min(downsampleLpStartMs3 + splitSize.toMillis, logicalPlan.endMs)
  }

  it("should split and direct overlapping queries to both raw & downsample planner and stitch") {

    val start = now/1000 - 30.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000 - 15.minutes.toSeconds

    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[2m])",
      TimeStepParams(start, step, end)).asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlannerWithTimeSplits.materialize(logicalPlan, QueryContext())

    val stitchExec = ep.asInstanceOf[StitchRvsExec]
    stitchExec.children.size shouldEqual 2

    val rawEp = stitchExec.children.head.asInstanceOf[MockExecPlan]
    val childStitchEp = stitchExec.children.last.asInstanceOf[StitchRvsExec]

    childStitchEp.children.size shouldEqual 2
    val downsampleEp1 = childStitchEp.children.head.asInstanceOf[MockExecPlan]
    val downsampleEp2 = childStitchEp.children.last.asInstanceOf[MockExecPlan]
    downsampleEp1.name shouldEqual "downsample"
    downsampleEp2.name shouldEqual "downsample"

    val downsampleLp1 = downsampleEp1.lp.asInstanceOf[PeriodicSeriesPlan]
    val downsampleLp2 = downsampleEp2.lp.asInstanceOf[PeriodicSeriesPlan]

    val downsampleLpStartMs1 = logicalPlan.startMs

    downsampleLp1.startMs shouldEqual downsampleLpStartMs1
    downsampleLp1.endMs shouldEqual downsampleLpStartMs1 + splitSize.toMillis

    downsampleLp2.startMs shouldEqual downsampleLp1.endMs + logicalPlan.stepMs
    downsampleLp2.endMs shouldEqual (earliestRawTimeForSplits + 2.minutes.toMillis) // plus look back

    rawEp.name shouldEqual "raw"
    val rawLp = rawEp.lp.asInstanceOf[PeriodicSeriesPlan]
    rawLp.startMs shouldEqual earliestRawTimeForSplits + 2.minutes.toMillis + logicalPlan.stepMs // plus look back
    rawLp.endMs shouldEqual logicalPlan.endMs
  }

  it("should split and delegate to downsample-only cluster and omit recent instants when there is a long lookback") {

    val start = now/1000 - 15.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000
    // notice raw data retention is 10m but lookback is 25m
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[25m])",
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlannerWithTimeSplits.materialize(logicalPlan, QueryContext())

    val stitchExec = ep.asInstanceOf[StitchRvsExec]

    stitchExec.children.size shouldEqual 2

    val downsampleEp1 = stitchExec.children.head.asInstanceOf[MockExecPlan]
    val downsampleEp2 = stitchExec.children.last.asInstanceOf[MockExecPlan]

    downsampleEp1.name shouldEqual "downsample"
    downsampleEp2.name shouldEqual "downsample"

    val downsampleLp1 = downsampleEp1.lp.asInstanceOf[PeriodicSeriesPlan]
    val downsampleLp2 = downsampleEp2.lp.asInstanceOf[PeriodicSeriesPlan]

    val downsampleLpStartMs1 = logicalPlan.startMs
    val downsampleLpStartMs2 = downsampleLpStartMs1 + logicalPlan.stepMs + splitSize.toMillis


    downsampleLp1.startMs shouldEqual downsampleLpStartMs1
    downsampleLp1.endMs shouldEqual downsampleLpStartMs1 + splitSize.toMillis

    downsampleLp2.startMs shouldEqual downsampleLpStartMs2
    downsampleLp2.endMs shouldEqual latestDownsampleTime //query till latest downsample time
  }

  it("should split and delegate to downsample cluster and retain endTime when there is a long lookback " +
    "with offset that causes recent data to not be used") {

    val start = now/1000 - 15.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000
    // notice raw data retention is 10m but lookback is 25m
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[25m] offset 5m)",
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlannerWithTimeSplits.materialize(logicalPlan, QueryContext())

    val stitchExec = ep.asInstanceOf[StitchRvsExec]

    stitchExec.children.size shouldEqual 3

    val downsampleEp1 = stitchExec.children.head.asInstanceOf[MockExecPlan]
    val downsampleEp2 = stitchExec.children(1).asInstanceOf[MockExecPlan]
    val downsampleEp3 = stitchExec.children.last.asInstanceOf[MockExecPlan]

    downsampleEp1.name shouldEqual "downsample"
    downsampleEp2.name shouldEqual "downsample"
    downsampleEp3.name shouldEqual "downsample"

    val downsampleLp1 = downsampleEp1.lp.asInstanceOf[PeriodicSeriesPlan]
    val downsampleLp2 = downsampleEp2.lp.asInstanceOf[PeriodicSeriesPlan]
    val downsampleLp3 = downsampleEp3.lp.asInstanceOf[PeriodicSeriesPlan]

    val downsampleLpStartMs1 = logicalPlan.startMs
    val downsampleLpStartMs2 = downsampleLpStartMs1 + logicalPlan.stepMs + splitSize.toMillis
    val downsampleLpStartMs3 = downsampleLpStartMs2 + logicalPlan.stepMs + splitSize.toMillis

    downsampleLp1.startMs shouldEqual downsampleLpStartMs1
    downsampleLp1.endMs shouldEqual downsampleLpStartMs1 + splitSize.toMillis

    downsampleLp2.startMs shouldEqual downsampleLpStartMs2
    downsampleLp2.endMs shouldEqual downsampleLpStartMs2 + splitSize.toMillis

    downsampleLp3.startMs shouldEqual downsampleLpStartMs3
    downsampleLp3.endMs shouldEqual logicalPlan.endMs
  }

}
