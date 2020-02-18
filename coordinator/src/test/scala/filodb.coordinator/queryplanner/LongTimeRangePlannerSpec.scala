package filodb.coordinator.queryplanner

import scala.concurrent.duration._
import monix.execution.Scheduler
import org.scalatest.{FunSpec, Matchers}
import filodb.core.DatasetRef
import filodb.core.query.QueryContext
import filodb.core.store.ChunkSource
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{LogicalPlan, PeriodicSeriesPlan, QueryConfig}
import filodb.query.exec._

class LongTimeRangePlannerSpec extends FunSpec with Matchers {

  class MockExecPlan(val name: String, val lp: LogicalPlan) extends ExecPlan {
    override def queryContext: QueryContext = QueryContext()
    override def children: Seq[ExecPlan] = ???
    override def submitTime: Long = ???
    override def dataset: DatasetRef = ???
    override def dispatcher: PlanDispatcher = ???
    override def doExecute(source: ChunkSource, queryConfig: QueryConfig)
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

  private def disp = InProcessPlanDispatcher
  val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner, earliestRawTime, disp)

  it("should direct raw-cluster-only queries to raw planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("foo",
      TimeStepParams(now/1000 - 7.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 1.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "raw"
    ep.lp shouldEqual logicalPlan
  }

  it("should direct downsample-only queries to downsample planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("foo",
      TimeStepParams(now/1000 - 20.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 15.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "downsample"
    ep.lp shouldEqual logicalPlan
  }

  it("should direct overlapping queries to both raw & downsample planner and stitch") {

    val logicalPlan = Parser.queryRangeToLogicalPlan("foo",
      TimeStepParams(now/1000 - 30.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 5.minutes.toSeconds))
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

    downsampleLp.startMs shouldEqual logicalPlan.startMs
    downsampleLp.endMs shouldEqual logicalPlan.startMs + 20.minutes.toMillis

    rawLp.startMs shouldEqual logicalPlan.startMs + 21.minutes.toMillis
    rawLp.endMs shouldEqual logicalPlan.endMs

  }

  it("should direct raw-data queries to both raw planner only irrespective of time length") {

    Seq(5, 10, 20).foreach { t =>
      val logicalPlan = Parser.queryToLogicalPlan(s"foo[${t}m]", now)
      val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
      ep.name shouldEqual "raw"
      ep.lp shouldEqual logicalPlan
    }
  }
}
