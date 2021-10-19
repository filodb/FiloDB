package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import filodb.coordinator.ShardMapper

import scala.concurrent.duration._
import monix.execution.Scheduler
import filodb.core.{DatasetRef, MetricsTestData}
import filodb.core.metadata.Schemas
import filodb.core.query.{EmptyQueryConfig, PromQlQueryParams, QueryConfig, QueryContext, QuerySession}
import filodb.core.store.ChunkSource
import filodb.prometheus.ast.{TimeStepParams, WindowConstants}
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class LongTimeRangePlannerSpec extends AnyFunSpec with Matchers {

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

  private val config = ConfigFactory.load("application_test.conf")
  private val queryConfig = new QueryConfig(config.getConfig("filodb.query"))

  private def disp = InProcessPlanDispatcher(EmptyQueryConfig)
  val dataset = MetricsTestData.timeseriesDataset
  val datasetMetricColumn = dataset.options.metricColumn

  val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
    earliestRawTime, latestDownsampleTime, disp,
    queryConfig, dataset)
  implicit val system = ActorSystem()
  val node = TestProbe().ref

  val mapper = new ShardMapper(32)
  val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)
  def mapperRef = mapper

  val dsRef = dataset.ref
  val schemas = Schemas(dataset.schema)

  it("should direct raw-cluster-only queries to raw planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[2m])",
      TimeStepParams(now/1000 - 7.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 1.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "raw"
    ep.lp shouldEqual logicalPlan
  }

  it("should direct raw-cluster-only top level subqueries to raw planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan(
      "foo[2m:1m]",
      TimeStepParams(now/1000 - 1.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 1.minutes.toSeconds)
    )

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "raw"
    ep.lp shouldEqual logicalPlan
  }

  it("should direct raw-cluster-only range function subqueries to raw planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan(
      "sum_over_time(foo[2m:1m])",
      TimeStepParams(now/1000 - 2.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 1.minutes.toSeconds)
    )

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "raw"
    ep.lp shouldEqual logicalPlan.asInstanceOf[SubqueryWithWindowing]
  }

  it("should direct downsample-only queries to downsample planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("rate(foo[2m])",
      TimeStepParams(now / 1000 - 20.minutes.toSeconds, 1.minute.toSeconds, now / 1000 - 15.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "downsample"
    ep.lp shouldEqual logicalPlan
  }

  it("should directed downsample-only top level subqueries to downsample planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("foo[2m:1m]",
      TimeStepParams(now/1000 - 20.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 20.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "downsample"
    ep.lp shouldEqual logicalPlan
  }

  it("should directed downsample-only range function subqueries to downsample planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("sum_over_time(foo[2m:1m])",
      TimeStepParams(now/1000 - 20.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 15.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "downsample"
    ep.lp shouldEqual logicalPlan.asInstanceOf[SubqueryWithWindowing]
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
    // 2 minutes is a lookback here, so, we make sure that raw cluster has enough data for lookback
    val rawStart = ((start * 1000) to (end * 1000) by (step * 1000)).find { instant =>
      instant - 2.minutes.toMillis > earliestRawTime
    }.get

    rawLp.startMs shouldEqual rawStart
    rawLp.endMs shouldEqual logicalPlan.endMs

    downsampleLp.startMs shouldEqual logicalPlan.startMs
    downsampleLp.endMs shouldEqual rawStart - 1.minute.toMillis
  }

  it("should direct overlapping top level subquery to both raw & downsample planner and stitch") {

    val start = now/1000 - 1.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000 - 1.minutes.toSeconds
    val logicalPlan = Parser.queryRangeToLogicalPlan("foo[28m:1m]",
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
    val actualStart = (start - 28.minutes.toSeconds)

    // LogicalPlanUtils.getLookBackMillis() will return 300s as a default stale data lookback
    val rawStartForSubquery =
      getStartForSubquery(earliestRawTime, 60000) + WindowConstants.staleDataLookbackMillis

    rawLp.startMs shouldEqual rawStartForSubquery
    rawLp.endMs shouldEqual logicalPlan.endMs

    downsampleLp.startMs shouldEqual logicalPlan.startMs
    downsampleLp.endMs shouldEqual rawStartForSubquery - 1.minute.toMillis
  }

  it("should direct subquery with windowing to downsample planner and verify subquery lookback") {
    val start = now / 1000 - 30.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 20.minutes.toSeconds
    val logicalPlan = Parser.queryRangeToLogicalPlan("max_over_time(foo[3m:1m])",
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]
    val ep = longTermPlanner.materialize(logicalPlan, QueryContext())
    val exp = ep.asInstanceOf[MockExecPlan]
    exp.name shouldEqual "downsample"
    val downsampleLp = exp.lp.asInstanceOf[PeriodicSeriesPlan]
    downsampleLp.startMs shouldEqual logicalPlan.startMs
    downsampleLp.endMs shouldEqual logicalPlan.endMs
  }

  def getStartForSubquery(startMs: Long, stepMs: Long) : Long = {
    val remainder = startMs % stepMs
    startMs - remainder + stepMs
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

  it("should direct overlapping binary join offset queries to both raw & downsample planner and stitch") {

    val start = now/1000 - 30.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000 - 2.minutes.toSeconds
    val query = "sum(foo) - sum(foo offset 2m)"
    val logicalPlan = Parser.queryRangeToLogicalPlan(query, TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(PromQlQueryParams(query, start, step, end)))
    val binaryJoinExec = ep.asInstanceOf[BinaryJoinExec]

    binaryJoinExec.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[StitchRvsExec] shouldEqual (true)
    binaryJoinExec.rhs.head.isInstanceOf[StitchRvsExec] shouldEqual (true)
  }

  it("should direct overlapping binary join offset queries with vector(0) " +
    "to both raw & downsample planner and stitch") {

    val start = now/1000 - 5.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes
    val downsampleRetention= 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query ="""sum(rate(foo{job = "app"}[5m]) or vector(0)) - sum(rate(foo{job = "app"}[5m] offset 8d)) * 0.5"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    ep.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    val binaryJoinExec = ep.asInstanceOf[BinaryJoinExec]

    binaryJoinExec.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    binaryJoinExec.rhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
  }

  it("should direct binary join to raw cluster and use ActorPlanDispatcher") {

    val start = now/1000 - 5.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now/1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes
    val downsampleRetention= 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """sum(rate(foo{job = "app"}[5m])) - sum(rate(foo{job = "app"}[5m] offset 2d))"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    ep.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    val binaryJoinExec = ep.asInstanceOf[BinaryJoinExec]

    // Since LHS and RHS both belong to raw cluster, we use ActorPlanDispatcher
    ep.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    binaryJoinExec.rhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
  }

  it("should direct binary join with abs to raw & downsample cluster and use Inprocessdispatcher") {

    val start = now / 1000 - 5.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """abs(sum(rate(foo{job = "app"}[5m])) - sum(rate(foo{job = "app"}[5m] offset 8d)))"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
    ep.isInstanceOf[BinaryJoinExec] shouldEqual (true)

    val binaryJoinExec = ep.asInstanceOf[BinaryJoinExec]
    binaryJoinExec.rangeVectorTransformers.head.isInstanceOf[InstantVectorFunctionMapper] shouldEqual true
    ep.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    binaryJoinExec.rhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
  }

  it("should run multiple binary join") {

    val start = now / 1000 - 5.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """abs(sum(rate(foo{job = "app"}[5m])) - sum(rate(foo{job = "app"}[5m] offset 8d) ) * sum(rate(foo{job = "app"}[5m] offset 8d)))"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
    ep.asInstanceOf[BinaryJoinExec].binaryOp shouldEqual BinaryOperator.SUB
    ep.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[BinaryJoinExec].binaryOp shouldEqual BinaryOperator.MUL
    ep.asInstanceOf[BinaryJoinExec].lhs.head.dispatcher.clusterName shouldEqual "raw"
    ep.asInstanceOf[BinaryJoinExec].rhs.head.dispatcher.clusterName shouldEqual "downsample"
  }

  it("should run multiple binary join across raw & downsample cluster") {
    val start = now / 1000 - 5.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """abs(sum(rate(foo{job = "app"}[5m] offset 8d)) - sum(rate(foo{job = "app"}[5m] ) ) * sum(rate(foo{job = "app"}[5m] offset 8d)))"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)
    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    ep.asInstanceOf[BinaryJoinExec].binaryOp shouldEqual BinaryOperator.SUB
    ep.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[BinaryJoinExec].binaryOp shouldEqual BinaryOperator.MUL

    ep.asInstanceOf[BinaryJoinExec].dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual(true)

    ep.asInstanceOf[BinaryJoinExec].rhs.head.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual(true)
  }

  it("should run sum on raw cluster") {
    val start = now / 1000 - 5.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """sum(foo{job = "app"})"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)
    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
    ep.asInstanceOf[LocalPartitionReduceAggregateExec].dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
  }

  it("should run sum on downsample cluster") {
    val start = now / 1000 - 9.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 8.days.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """sum(foo{job = "app"})"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)
    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
    ep.asInstanceOf[LocalPartitionReduceAggregateExec].dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
  }

  it("should run sum on downsample & raw cluster & stitch") {
    val start = now / 1000 - 8.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 5.minutes.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """sum(foo{job = "app"})"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)
    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
    ep.isInstanceOf[StitchRvsExec] shouldEqual true
    ep.children.head.asInstanceOf[LocalPartitionReduceAggregateExec].dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
  }

  it("should direct binary join with abs to raw cluster") {

    val start = now / 1000 - 5.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """abs(sum(rate(foo{job = "app"}[5m])) - sum(rate(foo{job = "app"}[5m] offset 4d)))"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    /* T~InstantVectorFunctionMapper(function=Abs)
  -E~BinaryJoinExec(binaryOp=SUB, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1961257886],raw)
  --T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634623854,60,1634624034))
  ---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1961257886],raw)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  -----T~PeriodicSamplesMapper(start=1634623854000, step=60000, end=1634624034000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634623554000,1634624034000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1961257886],raw)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  -----T~PeriodicSamplesMapper(start=1634623854000, step=60000, end=1634624034000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634623554000,1634624034000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1961257886],raw)
  --T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634623854,60,1634624034))
  ---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1961257886],raw)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  -----T~PeriodicSamplesMapper(start=1634623854000, step=60000, end=1634624034000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634277954000,1634278434000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1961257886],raw)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  -----T~PeriodicSamplesMapper(start=1634623854000, step=60000, end=1634624034000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634277954000,1634278434000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1961257886],raw)
*/

    ep.isInstanceOf[BinaryJoinExec] shouldEqual (true)

    val binaryJoinExec = ep.asInstanceOf[BinaryJoinExec]
    binaryJoinExec.rangeVectorTransformers.head.isInstanceOf[InstantVectorFunctionMapper] shouldEqual true
    // Since LHS and RHS both belong to raw cluster, we use ActorPlanDispatcher
    ep.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    binaryJoinExec.lhs.head.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.dispatcher.clusterName.equals("raw")
    binaryJoinExec.rhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    binaryJoinExec.rhs.head.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
    binaryJoinExec.rhs.head.dispatcher.clusterName.equals("downsample")
  }

  it("should direct binary join with count to raw & downsample cluster") {

    val start = now / 1000 - 5.minutes.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """count(sum(rate(foo{job = "app"}[5m])) - sum(rate(foo{job = "app"}[5m] offset 8d)))"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

 /* T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1634623765,60,1634623945))
  -E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@21263314)
  --T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List())
  ---E~BinaryJoinExec(binaryOp=SUB, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@21263314)
  ----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634623765,60,1634623945))
  -----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1460719381],raw)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  -------T~PeriodicSamplesMapper(start=1634623765000, step=60000, end=1634623945000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634623465000,1634623945000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1460719381],raw)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  -------T~PeriodicSamplesMapper(start=1634623765000, step=60000, end=1634623945000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634623465000,1634623945000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1460719381],raw)
  ----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634623765,60,1634623945))
  -----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1460719381],downsample)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  -------T~PeriodicSamplesMapper(start=1634623765000, step=60000, end=1634623945000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=Some(691200000))
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633932265000,1633932745000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1460719381],downsample)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  -------T~PeriodicSamplesMapper(start=1634623765000, step=60000, end=1634623945000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=Some(691200000))
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633932265000,1633932745000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1460719381],downsample)
*/

    ep.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    ep.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual(true)

    val binaryJoinExec = ep.children.head.asInstanceOf[BinaryJoinExec]

    ep.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.dispatcher.clusterName shouldEqual "raw"
    binaryJoinExec.lhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    binaryJoinExec.lhs.head.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
    binaryJoinExec.rhs.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    binaryJoinExec.rhs.head.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
    binaryJoinExec.rhs.head.dispatcher.clusterName shouldEqual "downsample"
  }

  it("should direct long range binary join to raw & downsample cluster when query range is beyond raw retention") {

    val start = now / 1000 - 8.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query = """count(sum(foo{job = "app"}) - sum(foo{job = "app"} offset 4d))"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
    //TODO Optimization - Push BinaryJoinExec to raw and downsample respectively and do stitch later

    /* T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1633932035,60,1634623115))
  -E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2e86807a)
  --T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List())
  ---E~BinaryJoinExec(binaryOp=SUB, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2e86807a)
  ----E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@4548d254)
  -----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634018195,60,1634623115))
  ------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],raw)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  --------T~PeriodicSamplesMapper(start=1634018195000, step=60000, end=1634623115000, window=None, functionId=None, rawSource=true, offsetMs=None)
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634017895000,1634623115000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],raw)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  --------T~PeriodicSamplesMapper(start=1634018195000, step=60000, end=1634623115000, window=None, functionId=None, rawSource=true, offsetMs=None)
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634017895000,1634623115000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],raw)
  -----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633932035,60,1634018135))
  ------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],downsample)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  --------T~PeriodicSamplesMapper(start=1633932035000, step=60000, end=1634018135000, window=None, functionId=None, rawSource=true, offsetMs=None)
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633931735000,1634018135000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],downsample)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  --------T~PeriodicSamplesMapper(start=1633932035000, step=60000, end=1634018135000, window=None, functionId=None, rawSource=true, offsetMs=None)
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633931735000,1634018135000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],downsample)
  ----E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@4548d254)
  -----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634363795,60,1634623115))
  ------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],raw)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  --------T~PeriodicSamplesMapper(start=1634363795000, step=60000, end=1634623115000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634017895000,1634277515000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],raw)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  --------T~PeriodicSamplesMapper(start=1634363795000, step=60000, end=1634623115000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634017895000,1634277515000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],raw)
  -----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633932035,60,1634363735))
  ------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],downsample)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  --------T~PeriodicSamplesMapper(start=1633932035000, step=60000, end=1634363735000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633586135000,1634018135000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],downsample)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
  --------T~PeriodicSamplesMapper(start=1633932035000, step=60000, end=1634363735000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633586135000,1634018135000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1486396335],downsample)
*/
    ep.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    ep.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual(true)

    val binaryJoinExec = ep.children.head.asInstanceOf[BinaryJoinExec]

    ep.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[StitchRvsExec] shouldEqual (true)
    binaryJoinExec.lhs.head.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true

    binaryJoinExec.lhs.head.children.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)
    binaryJoinExec.lhs.head.children.head.dispatcher.clusterName equals ("raw")
    binaryJoinExec.rhs.head.children.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)
    binaryJoinExec.lhs.head.children.head.dispatcher.clusterName equals ("downsample")

    binaryJoinExec.rhs.head.isInstanceOf[StitchRvsExec] shouldEqual (true)
    binaryJoinExec.rhs.head.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true

    binaryJoinExec.rhs.head.children.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)
    binaryJoinExec.rhs.head.children.head.dispatcher.clusterName equals ("raw")
    binaryJoinExec.rhs.head.children.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)
    binaryJoinExec.rhs.head.children.head.dispatcher.clusterName equals ("downsample")
  }

  it("""should generate plan for sum(foo{job = "app "} offset 4d) by (col1) + sum(bar{job = " app"}) by (col1) *
      sum(baz {job = "app"}) by (col1)""") {

    val start = now / 1000 - 8.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query =
      """sum(foo{job = "app"} offset 4d) by (col1) + sum(bar{job = "app"}) by (col1) *
        |sum(baz{job = "app"}) by (col1)""".stripMargin
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

/* E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@40a72ecd)
  -E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@73971965)
  --T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634363574,60,1634622894))
  ---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],raw)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1634363574000, step=60000, end=1634622894000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634017674000,1634277294000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],raw)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1634363574000, step=60000, end=1634622894000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634017674000,1634277294000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],raw)
  --T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633931814,60,1634363514))
  ---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],downsample)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1633931814000, step=60000, end=1634363514000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633585914000,1634017914000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],downsample)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1633931814000, step=60000, end=1634363514000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633585914000,1634017914000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],downsample)
  -E~BinaryJoinExec(binaryOp=MUL, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@40a72ecd)
  --E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@73971965)
  ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634017974,60,1634622894))
  ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],raw)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1634017974000, step=60000, end=1634622894000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634017674000,1634622894000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],raw)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1634017974000, step=60000, end=1634622894000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634017674000,1634622894000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],raw)
  ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633931814,60,1634017914))
  ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],downsample)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1633931814000, step=60000, end=1634017914000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1633931514000,1634017914000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],downsample)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1633931814000, step=60000, end=1634017914000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1633931514000,1634017914000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],downsample)
  --E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@73971965)
  ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634017974,60,1634622894))
  ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],raw)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1634017974000, step=60000, end=1634622894000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634017674000,1634622894000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],raw)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1634017974000, step=60000, end=1634622894000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1634017674000,1634622894000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],raw)
  ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633931814,60,1634017914))
  ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],downsample)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1633931814000, step=60000, end=1634017914000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633931514000,1634017914000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],downsample)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1633931814000, step=60000, end=1634017914000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1633931514000,1634017914000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1417811245],downsample)
*/
    val binaryJoinExec = ep.asInstanceOf[BinaryJoinExec]

    binaryJoinExec.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[StitchRvsExec] shouldEqual (true)
    binaryJoinExec.lhs.head.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true

    binaryJoinExec.rhs.head.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[StitchRvsExec] shouldEqual(true)
    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.isInstanceOf[StitchRvsExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.dispatcher.
      isInstanceOf[ActorPlanDispatcher] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.dispatcher.
      clusterName.equals("raw") shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.last.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.last.dispatcher.
      clusterName.equals("downsample") shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.head.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.head.dispatcher.
      isInstanceOf[ActorPlanDispatcher] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.head.dispatcher.
      clusterName.equals("raw") shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.last.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.last.dispatcher.
      clusterName.equals("downsample") shouldEqual(true)

  }

  it("""should generate plan for sum(foo{job = "app "} offset 4d) by (col1) + sum(bar{job = " app"}) by (col1) *
    sum(baz{job = "app"} offset 8d) by (col1)""") {

    val start = now / 1000 - 8.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query =
      """sum(foo{job = "app"} offset 4d) by (col1) + sum(bar{job = "app"}) by (col1) *
        |sum(baz{job = "app"} offset 8d) by (col1)""".stripMargin
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
  /* E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@40a72ecd)
  -E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@73971965)
  --T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634363254,60,1634622574))
  ---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],raw)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1634363254000, step=60000, end=1634622574000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634017354000,1634276974000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],raw)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1634363254000, step=60000, end=1634622574000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634017354000,1634276974000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],raw)
  --T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633931494,60,1634363194))
  ---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],downsample)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1633931494000, step=60000, end=1634363194000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633585594000,1634017594000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],downsample)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1633931494000, step=60000, end=1634363194000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633585594000,1634017594000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],downsample)
  -E~BinaryJoinExec(binaryOp=MUL, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@40a72ecd)
  --E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@73971965)
  ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634017654,60,1634622574))
  ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],raw)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1634017654000, step=60000, end=1634622574000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634017354000,1634622574000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],raw)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1634017654000, step=60000, end=1634622574000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634017354000,1634622574000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],raw)
  ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633931494,60,1634017594))
  ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],downsample)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1633931494000, step=60000, end=1634017594000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1633931194000,1634017594000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],downsample)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1633931494000, step=60000, end=1634017594000, window=None, functionId=None, rawSource=true, offsetMs=None)
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1633931194000,1634017594000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],downsample)
  --T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633931494,60,1634622574))
  ---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],downsample)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1633931494000, step=60000, end=1634622574000, window=None, functionId=None, rawSource=true, offsetMs=Some(691200000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633239994000,1633931374000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],downsample)
  ----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -----T~PeriodicSamplesMapper(start=1633931494000, step=60000, end=1634622574000, window=None, functionId=None, rawSource=true, offsetMs=Some(691200000))
  ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1633239994000,1633931374000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1789189102],downsample)
*/
    val binaryJoinExec = ep.asInstanceOf[BinaryJoinExec]

    binaryJoinExec.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[StitchRvsExec] shouldEqual (true)
    binaryJoinExec.lhs.head.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true

    binaryJoinExec.rhs.head.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[StitchRvsExec] shouldEqual(true)


    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.dispatcher.
      isInstanceOf[ActorPlanDispatcher] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.dispatcher.
      clusterName.equals("raw") shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.last.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.last.dispatcher.
      clusterName.equals("downsample") shouldEqual(true)

    // sum(baz{job = "app"} offset 8d) by (col1) will be routed to downsample cluster only
    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.dispatcher.
      isInstanceOf[ActorPlanDispatcher] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.dispatcher.clusterName.
      equals("downsample") shouldEqual(true)
  }

  it("""should generate plan for abs(sum(foo{job = "app"} offset 4d) by (col1) + sum(bar{job = "app"}) by (col1) *
       |sum(baz{job = "app"}) by (col1))""".stripMargin) {

    val start = now / 1000 - 8.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query =
      """abs(sum(foo{job = "app"} offset 4d) by (col1) + sum(bar{job = "app"}) by (col1) *
        |sum(baz{job = "app"}) by (col1))""".stripMargin
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

   /* T~InstantVectorFunctionMapper(function=Abs)
  -E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@591a4f8e)
  --E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@53ed80d3)
  ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634363080,60,1634622400))
  ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],raw)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1634363080000, step=60000, end=1634622400000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634017180000,1634276800000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],raw)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1634363080000, step=60000, end=1634622400000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634017180000,1634276800000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],raw)
  ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633931320,60,1634363020))
  ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],downsample)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1633931320000, step=60000, end=1634363020000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633585420000,1634017420000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],downsample)
  -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ------T~PeriodicSamplesMapper(start=1633931320000, step=60000, end=1634363020000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633585420000,1634017420000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],downsample)
  --E~BinaryJoinExec(binaryOp=MUL, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@591a4f8e)
  ---E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@53ed80d3)
  ----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634017480,60,1634622400))
  -----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],raw)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -------T~PeriodicSamplesMapper(start=1634017480000, step=60000, end=1634622400000, window=None, functionId=None, rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634017180000,1634622400000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],raw)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -------T~PeriodicSamplesMapper(start=1634017480000, step=60000, end=1634622400000, window=None, functionId=None, rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634017180000,1634622400000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],raw)
  ----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633931320,60,1634017420))
  -----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],downsample)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -------T~PeriodicSamplesMapper(start=1633931320000, step=60000, end=1634017420000, window=None, functionId=None, rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1633931020000,1634017420000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],downsample)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -------T~PeriodicSamplesMapper(start=1633931320000, step=60000, end=1634017420000, window=None, functionId=None, rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1633931020000,1634017420000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],downsample)
  ---E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@53ed80d3)
  ----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634017480,60,1634622400))
  -----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],raw)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -------T~PeriodicSamplesMapper(start=1634017480000, step=60000, end=1634622400000, window=None, functionId=None, rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634017180000,1634622400000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],raw)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -------T~PeriodicSamplesMapper(start=1634017480000, step=60000, end=1634622400000, window=None, functionId=None, rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1634017180000,1634622400000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],raw)
  ----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633931320,60,1634017420))
  -----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],downsample)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -------T~PeriodicSamplesMapper(start=1633931320000, step=60000, end=1634017420000, window=None, functionId=None, rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633931020000,1634017420000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],downsample)
  ------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  -------T~PeriodicSamplesMapper(start=1633931320000, step=60000, end=1634017420000, window=None, functionId=None, rawSource=true, offsetMs=None)
  --------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1633931020000,1634017420000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1862263235],downsample)
    */

    ep.rangeVectorTransformers.head.isInstanceOf[InstantVectorFunctionMapper] shouldEqual(true)
    val binaryJoinExec = ep.asInstanceOf[BinaryJoinExec]

    binaryJoinExec.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[StitchRvsExec] shouldEqual (true)
    binaryJoinExec.lhs.head.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true

    binaryJoinExec.rhs.head.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[StitchRvsExec] shouldEqual(true)
    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.isInstanceOf[StitchRvsExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.dispatcher.
      isInstanceOf[ActorPlanDispatcher] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.dispatcher.
      clusterName.equals("raw") shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.last.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.last.dispatcher.
      clusterName.equals("downsample") shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.head.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.head.dispatcher.
      isInstanceOf[ActorPlanDispatcher] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.head.dispatcher.
      clusterName.equals("raw") shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.last.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.children.last.dispatcher.
      clusterName.equals("downsample") shouldEqual(true)
  }

  it("""should generate plan for count(sum(foo{job = "app"} offset 4d) by (col1) + sum(bar{job = "app"}) by (col1) *
       |sum(baz{job = "app"} offset 8d) by (col1))""".stripMargin) {

    val start = now / 1000 - 8.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 10090.minutes // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 12.hours.toMillis

    val query =
      """count(sum(foo{job = "app"} offset 4d) by (col1) + sum(bar{job = "app"}) by (col1) *
        |sum(baz{job = "app"} offset 8d) by (col1))""".stripMargin
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = earliestRawTime,
      queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

  /* T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1633905157,60,1634596237))
  -E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@493b01ef)
  --T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List())
  ---E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@493b01ef)
  ----E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@36c2d629)
  -----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634336917,60,1634596237))
  ------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],raw)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  --------T~PeriodicSamplesMapper(start=1634336917000, step=60000, end=1634596237000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633991017000,1634250637000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],raw)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  --------T~PeriodicSamplesMapper(start=1634336917000, step=60000, end=1634596237000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633991017000,1634250637000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],raw)
  -----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633905157,60,1634336857))
  ------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],downsample)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  --------T~PeriodicSamplesMapper(start=1633905157000, step=60000, end=1634336857000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633559257000,1633991257000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],downsample)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  --------T~PeriodicSamplesMapper(start=1633905157000, step=60000, end=1634336857000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633559257000,1633991257000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],downsample)
  ----E~BinaryJoinExec(binaryOp=MUL, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@493b01ef)
  -----E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@36c2d629)
  ------T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633991317,60,1634596237))
  -------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],raw)
  --------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ---------T~PeriodicSamplesMapper(start=1633991317000, step=60000, end=1634596237000, window=None, functionId=None, rawSource=true, offsetMs=None)
  ----------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1633991017000,1634596237000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],raw)
  --------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ---------T~PeriodicSamplesMapper(start=1633991317000, step=60000, end=1634596237000, window=None, functionId=None, rawSource=true, offsetMs=None)
  ----------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1633991017000,1634596237000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],raw)
  ------T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633905157,60,1633991257))
  -------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],downsample)
  --------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ---------T~PeriodicSamplesMapper(start=1633905157000, step=60000, end=1633991257000, window=None, functionId=None, rawSource=true, offsetMs=None)
  ----------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1633904857000,1633991257000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],downsample)
  --------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  ---------T~PeriodicSamplesMapper(start=1633905157000, step=60000, end=1633991257000, window=None, functionId=None, rawSource=true, offsetMs=None)
  ----------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1633904857000,1633991257000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],downsample)
  -----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633905157,60,1634596237))
  ------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],downsample)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  --------T~PeriodicSamplesMapper(start=1633905157000, step=60000, end=1634596237000, window=None, functionId=None, rawSource=true, offsetMs=Some(691200000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633213657000,1633905037000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-854504634],downsample)
  -------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
  --------T~PeriodicSamplesMapper(start=1633905157000, step=60000, end=1634596237000, window=None, functionId=None, rawSource=true, offsetMs=Some(691200000))
  ---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1633213657000,1633905037000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher
   */
    ep.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)
    ep.asInstanceOf[LocalPartitionReduceAggregateExec].aggrOp shouldEqual(AggregationOperator.Count)

    val binaryJoinExec = ep.children.head.asInstanceOf[BinaryJoinExec]

    binaryJoinExec.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    binaryJoinExec.lhs.head.isInstanceOf[StitchRvsExec] shouldEqual (true)
    binaryJoinExec.lhs.head.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true

    binaryJoinExec.rhs.head.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[StitchRvsExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.dispatcher.
      isInstanceOf[ActorPlanDispatcher] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.head.dispatcher.
      clusterName.equals("raw") shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.last.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].lhs.head.children.last.dispatcher.
      clusterName.equals("downsample") shouldEqual(true)

    // sum(baz{job = "app"} offset 8d) by (col1) will be routed to downsample cluster only
    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.
      isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.dispatcher.
      isInstanceOf[ActorPlanDispatcher] shouldEqual(true)

    binaryJoinExec.rhs.head.asInstanceOf[BinaryJoinExec].rhs.head.dispatcher.clusterName.
      equals("downsample") shouldEqual(true)
  }
}
