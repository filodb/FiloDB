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

  private def disp = InProcessPlanDispatcher
  val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
                                                 earliestRawTime, latestDownsampleTime, disp)

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

  it("should direct overlapping instant queries correctly to raw or downsample clusters") {
    // this instant query spills into downsample period
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[2m])",
      TimeStepParams(now/1000 - 12.minutes.toSeconds, 0, now/1000 - 12.minutes.toSeconds))

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
}
