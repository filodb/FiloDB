package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import filodb.coordinator.{ActorPlanDispatcher, ShardMapper}

import scala.concurrent.duration._
import monix.execution.Scheduler
import filodb.core.{DatasetRef, MetricsTestData}
import filodb.core.metadata.Schemas
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext, QuerySession}
import filodb.core.store.ChunkSource
import filodb.prometheus.ast.{TimeStepParams, WindowConstants}
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class LongTimeRangePlannerSpec extends AnyFunSpec with Matchers with PlanValidationSpec {

  class MockExecPlan(val name: String, val lp: LogicalPlan) extends ExecPlan {
    override def queryContext: QueryContext = QueryContext()
    override def children: Seq[ExecPlan] = ???
    override def submitTime: Long = ???
    override def dataset: DatasetRef = ???
    override def dispatcher: PlanDispatcher = ???
    override def doExecute(source: ChunkSource,
                           querySession: QuerySession)
                          (implicit sched: Scheduler): ExecResult = ???
    override protected def args: String = ???
  }

  val rawPlanner = new QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      new MockExecPlan("raw", logicalPlan)
    }
    def childPlanners(): Seq[QueryPlanner] = Nil
    private var rootPlanner: Option[QueryPlanner] = None
    def getRootPlanner(): Option[QueryPlanner] = rootPlanner
    def setRootPlanner(rootPlanner: QueryPlanner): Unit = {
      this.rootPlanner = Some(rootPlanner)
    }
    initRootPlanner()
  }

  val downsamplePlanner = new QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      new MockExecPlan("downsample", logicalPlan)
    }
    def childPlanners(): Seq[QueryPlanner] = Nil
    private var rootPlanner: Option[QueryPlanner] = None
    def getRootPlanner(): Option[QueryPlanner] = rootPlanner
    def setRootPlanner(rootPlanner: QueryPlanner): Unit = {
      this.rootPlanner = Some(rootPlanner)
    }
    initRootPlanner()
  }

  val rawRetention = 10.minutes
  private val now = 1634777330000L
  val earliestRawTime = now - rawRetention.toMillis
  val latestDownsampleTime = now - 4.minutes.toMillis // say it takes 4 minutes to downsample

  private val config = ConfigFactory.load("application_test.conf")
  private val queryConfig = QueryConfig(config.getConfig("filodb.query"))

  private def disp = InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig)
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
    ep.lp shouldEqual logicalPlan.asInstanceOf[TopLevelSubquery].innerPeriodicSeries
  }

  it("should direct raw-cluster-only range function subqueries to raw planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan(
      "sum_over_time(foo[2m:1m])",
      TimeStepParams(now/1000 - 2.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 1.minutes.toSeconds)
    )

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "raw"
    ep.lp shouldEqual logicalPlan.asInstanceOf[SubqueryWithWindowing].innerPeriodicSeries
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
    ep.lp shouldEqual logicalPlan.asInstanceOf[TopLevelSubquery].innerPeriodicSeries
  }

  it("should directed downsample-only range function subqueries to downsample planner") {
    val logicalPlan = Parser.queryRangeToLogicalPlan("sum_over_time(foo[2m:1m])",
      TimeStepParams(now/1000 - 20.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 15.minutes.toSeconds))

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "downsample"
    ep.lp shouldEqual logicalPlan.asInstanceOf[SubqueryWithWindowing].innerPeriodicSeries
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
    exp.lp.asInstanceOf[PeriodicSeriesPlan]
    // skipping check of startMs and endMs as we are not using a proper root planner to plan the subquery
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

  it("should direct instant raw-data queries with lookback to raw cluster when only need raw data"){
    val start = now/1000
    val step = 1.second.toSeconds

    // raw retention is 10m
    val logicalPlan = Parser.queryToLogicalPlan("foo[9m]", start, step)
    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "raw"
    ep.lp shouldEqual logicalPlan

    val logicalPlan2 = Parser.queryToLogicalPlan("foo[4m] offset 5m", start, step)
    val ep2 = longTermPlanner.materialize(logicalPlan2, QueryContext()).asInstanceOf[MockExecPlan]
    ep2.name shouldEqual "raw"
    ep2.lp shouldEqual logicalPlan2
  }

  it("should direct instant raw-data queries with lookback to downSample cluster when range is over raw retention"){
    val start = now/1000
    val step = 1.second.toSeconds

    // raw retention is 10m
    val logicalPlan = Parser.queryToLogicalPlan("foo[10m]", start, step)
    val ep = longTermPlanner.materialize(logicalPlan, QueryContext()).asInstanceOf[MockExecPlan]
    ep.name shouldEqual "downsample"
    ep.lp shouldEqual logicalPlan

    val logicalPlan2 = Parser.queryToLogicalPlan("foo[10m] offset 5m", start, step)
    val ep2 = longTermPlanner.materialize(logicalPlan2, QueryContext()).asInstanceOf[MockExecPlan]
    ep2.name shouldEqual "downsample"
    ep2.lp shouldEqual logicalPlan2

    val logicalPlan3 = Parser.queryToLogicalPlan("foo[10m] offset 20m", start, step)
    val ep3= longTermPlanner.materialize(logicalPlan3, QueryContext()).asInstanceOf[MockExecPlan]
    ep3.name shouldEqual "downsample"
    ep3.lp shouldEqual logicalPlan3
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

  it("tsCardinality should span to both downsample and raw for version 2") {
    val logicalPlan = TsCardinalities(Seq("a","b"), 2, Seq("longtime-prometheus"))

    val cardExecPlan = longTermPlanner.materialize(
      logicalPlan,
      QueryContext(origQueryParams = promQlQueryParams.copy(promQl = ""))).asInstanceOf[TsCardReduceExec]

    cardExecPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    cardExecPlan.children.size shouldEqual 2
    val rawEp = cardExecPlan.children.head.asInstanceOf[MockExecPlan]
    val downsampleEp = cardExecPlan.children.last.asInstanceOf[MockExecPlan]

    rawEp.name shouldEqual "raw"
    downsampleEp.name shouldEqual "downsample"
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

  it("HAPlan promql params should not have transformers when remote-route is used") {
    val tp = TimeStepParams(1722361236, 60, 1722362236)
    val query = "1 - (sum(rate(Counter3{_ws_=\"test\",_ns_=\"test2\"}[2m])) / " +
      "sum(rate(Counter3{_ws_=\"test\",_ns_=\"test2\"}[2m])))"
    val lp = Parser.queryRangeToLogicalPlan(query, tp)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(1722360236000L, 1722363236000L), false))
      }
    }
    val promQlParams = PromQlQueryParams(query, 1722361236, 60, 1722362236)

    // Create new config with routing config
    val routingConfigString = "routing {partition_name = p1 \n  remote {\n    http {\n" +
      "      endpoint = localhost\n      timeout = 10000\n    }\n  }\n}"
    val routingConfig = ConfigFactory.parseString(routingConfigString)

    val localConfig = ConfigFactory.load("application_test.conf").getConfig("filodb.query").
      withFallback(routingConfig)
    val queryConfigWithSelector = QueryConfig(localConfig).copy(plannerSelector = Some("plannerSelector"))

    val queryConfigWithGrpcEndpoint = QueryConfig(
      localConfig.withValue("routing.remote.grpc.endpoint", ConfigValueFactory.fromAnyRef("grpcEndpoint")))
      .copy(plannerSelector = Some("plannerSelector"))

    // SingleClusterPlanner which is passed to HAPlanner
    val localPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0,
      queryConfigWithSelector,"raw")

    val highAvailabilityPlannerRaw = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider,
      queryConfigWithGrpcEndpoint,
      workUnit = "", buddyWorkUnit = "", clusterName = "", useShardLevelFailover = false)

    val longTimeRangePlanner = new LongTimeRangePlanner(highAvailabilityPlannerRaw, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfigWithGrpcEndpoint, dataset)

    val execPlan = longTimeRangePlanner.materialize(lp, QueryContext(origQueryParams = promQlParams))
    execPlan.isInstanceOf[PromQLGrpcRemoteExec] shouldEqual (true)
    execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      "(sum(rate(Counter3{_ws_=\"test\",_ns_=\"test2\"}[120s])) / " +
        "sum(rate(Counter3{_ws_=\"test\",_ns_=\"test2\"}[120s])))"
    execPlan.rangeVectorTransformers.size shouldEqual 1
    execPlan.rangeVectorTransformers.head.isInstanceOf[ScalarOperationMapper] shouldEqual true
    execPlan.rangeVectorTransformers.head.asInstanceOf[ScalarOperationMapper].operator shouldEqual BinaryOperator.SUB
    execPlan.rangeVectorTransformers.head.asInstanceOf[ScalarOperationMapper].scalarOnLhs shouldEqual true
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
    val expectedPlan =
      """T~InstantVectorFunctionMapper(function=Abs)
        |-E~BinaryJoinExec(binaryOp=SUB, on=None, ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@939ff41)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634777030,60,1634777210))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-478643822],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634777030000, step=60000, end=1634777210000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634776730000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-478643822],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634777030000, step=60000, end=1634777210000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634776730000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-478643822],raw)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634777030,60,1634777210))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-478643822],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634777030000, step=60000, end=1634777210000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=Some(691200000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634085530000,1634086010000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-478643822],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634777030000, step=60000, end=1634777210000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=Some(691200000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634085530000,1634086010000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-478643822],downsample)""".stripMargin
    validatePlan(ep, expectedPlan)
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

    // This case of nested Binary join with aggregation, but the LHS as offset and RHS does not
    //                  + (in process)
    //            /           \
    //    has offset           * (pushed down)
    //                      /     \
    //                no offset  no offset
    //  Expected behavior here is to perform the top level binary join in-process, the RHS however should be
    //  entirely pushed down

    val query =
      """sum(foo{job = "app"} offset 4d) by (col1) + sum(bar{job = "app"}) by (col1) *
        |sum(baz{job = "app"}) by (col1)""".stripMargin
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
                                              earliestRetainedTimestampFn = earliestRawTime,
                                              queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
    val expectedPlan =
      """E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@3fb9a67f)
        |-E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@6d8796c1)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634517890,60,1634777210))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-----T~PeriodicSamplesMapper(start=1634517890000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634171990000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-----T~PeriodicSamplesMapper(start=1634517890000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634171990000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634517830))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634517830000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633740230000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634517830000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633740230000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)
        |-E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@6d8796c1)
        |--E~BinaryJoinExec(binaryOp=MUL, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172290,60,1634777210))
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172290,60,1634777210))
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],raw)
        |--E~BinaryJoinExec(binaryOp=MUL, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)
        |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634172230))
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)
        |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634172230))
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-16967916],downsample)""".stripMargin

    validatePlan(ep, expectedPlan)

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

    // This case of abs function applied nested Binary join with aggregation, but the LHS as offset and RHS does not
    //                  abs(in process)
    //                  |
    //                  + (in process)
    //            /           \
    //    has offset           * (pushed down)
    //                      /     \
    //                no offset  no offset
    //  Expected behavior here is to perform the top level binary join in-process, the RHS however should be
    //  entirely pushed down

    val expectedPlan =
      """T~InstantVectorFunctionMapper(function=Abs)
        |-E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@7ce85af2)
        |--E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@1a565afb)
        |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634517890,60,1634777210))
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634517890000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634171990000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634517890000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634171990000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634517830))
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634517830000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633740230000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634517830000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633740230000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)
        |--E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@1a565afb)
        |---E~BinaryJoinExec(binaryOp=MUL, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172290,60,1634777210))
        |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172290,60,1634777210))
        |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],raw)
        |---E~BinaryJoinExec(binaryOp=MUL, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)
        |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634172230))
        |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)
        |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634172230))
        |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#139560088],downsample)""".stripMargin

    validatePlan(ep, expectedPlan)
  }


  it("""should generate plan for abs(sum(foo{job = "app"}) by (col1) + sum(bar{job = "app"}) by (col1) *
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
      """abs(sum(foo{job = "app"}) by (col1) + sum(bar{job = "app"}) by (col1) *
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

    // This case of abs function applied nested Binary join with aggregation, there are no offsets, entire binary join
    // should be pushed down and just stitch should happen inProcess
    //                Stitch (inProcess with ABS RangeVectorTransformer (RVT))
    //                   /                                     \
    //                  + (pushed down to DS)                   + (pushed down to Raw)
    //            /           \                            /           \
    //     no offset           * (pushed down)      no offset           * (pushed down)
    //                      /     \                                   /     \
    //                no offset  no offset                      no offset  no offset

    // Its is also valid for abs function to be pushed down and Stitch with No RVT in it

    val expectedPlan =
      """T~InstantVectorFunctionMapper(function=Abs)
        |-E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@49fa1d74)
        |--E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172290,60,1634777210))
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |---E~BinaryJoinExec(binaryOp=MUL, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172290,60,1634777210))
        |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172290,60,1634777210))
        |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634172290000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1634171990000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],raw)
        |--E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634172230))
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |---E~BinaryJoinExec(binaryOp=MUL, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634172230))
        |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634172230))
        |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)
        |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(col1))
        |-------T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172230000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(1634085830000,1634172230000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#669282060],downsample)""".stripMargin
    validatePlan(ep, expectedPlan)
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

  it("should push down aggregation to ds and raw cluster") {

    val start = now / 1000 - 8.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 7.days // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 6.hours.toMillis

    // should span across both clusters and stitch
    val query = """avg(foo{job="app"} offset 4d)"""
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

    val expectedPlan =
      """E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@1ee47d9e)
        |-T~AggregatePresenter(aggrOp=Avg, aggrParams=List(), rangeParams=RangeParams(1634518490,60,1634777210))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Avg, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1999941542],raw)
        |---T~AggregateMapReduce(aggrOp=Avg, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634518490000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634172590000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1999941542],raw)
        |---T~AggregateMapReduce(aggrOp=Avg, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634518490000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634172590000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1999941542],raw)
        |-T~AggregatePresenter(aggrOp=Avg, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634518430))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Avg, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1999941542],downsample)
        |---T~AggregateMapReduce(aggrOp=Avg, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634518430000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633740230000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1999941542],downsample)
        |---T~AggregateMapReduce(aggrOp=Avg, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634518430000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633740230000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1999941542],downsample)""".stripMargin

    validatePlan(ep, expectedPlan)
  }

  it("should push down Binary join with scalar to individual clusters") {

    val start = now / 1000 - 8.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 7.days // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 6.hours.toMillis

    // should span across both clusters and stitch
    val query = """sum(foo{job="app"} offset 4d) + 10"""
    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestRawTime, queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep1 = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    // TODO: Though not bad, the plan can be optimized since one operator of this Binary operator is scalar,
    //  entire join should have been pushed down despite offset being present
    val expectedPlan1 =
      """T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |-FA1~StaticFuncArgs(10.0,RangeParams(1634086130,60,1634777210))
        |-E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@41b13f3d)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634518490,60,1634777210))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-381101616],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634518490000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634172590000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-381101616],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634518490000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634172590000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-381101616],raw)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634518430))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-381101616],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634518430000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633740230000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-381101616],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634518430000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633740230000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-381101616],downsample)""".stripMargin

    validatePlan(ep1, expectedPlan1)

    val query1 = """sum(foo{job="app"}) + 10"""
    val ep2 = longTermPlanner.materialize( Parser.queryRangeToLogicalPlan(query1, TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan], QueryContext(origQueryParams = promQlQueryParams))

    // Unlike above case, the scalar operator operation is pushed down
    val expectedPlan2=
    """E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@5807efad)
      |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
      |--FA1~StaticFuncArgs(10.0,RangeParams(1634086130,60,1634777210))
      |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172890,60,1634777210))
      |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1285276689],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634172890000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634172590000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1285276689],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634172890000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634172590000,1634777210000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1285276689],raw)
      |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
      |--FA1~StaticFuncArgs(10.0,RangeParams(1634086130,60,1634777210))
      |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634172830))
      |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1285276689],downsample)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634085830000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1285276689],downsample)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634085830000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1285276689],downsample)""".stripMargin
    validatePlan(ep2, expectedPlan2)
  }

  it("should push down binary join when all offsets are same") {


    val start = now / 1000 - 8.days.toSeconds
    val step = 1.minute.toSeconds
    val end = now / 1000 - 2.minutes.toSeconds

    val rawRetention = 7.days // 7 days
    val downsampleRetention = 183.days
    val earliestRawTime = now - rawRetention.toMillis
    val earliestDownSampleTime = now - downsampleRetention.toMillis
    val latestDownsampleTime = now - 6.hours.toMillis

    // Binary join has offset but same for all, we can optimize by pushing this join down to
    // individual clusters
    val query = """sum(foo{job = "app"} offset 4d) + sum(bar{job= "app"} offset 4d)"""

    val logicalPlan = Parser.queryRangeToLogicalPlan(query,
      TimeStepParams(start, step, end))
      .asInstanceOf[PeriodicSeriesPlan]

    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestRawTime, queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

    val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    val expectedPlan =
    """E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@2fd954f)
      |-E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],raw)
      |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634518490,60,1634777210))
      |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634518490000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1634172590000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634518490000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1634172590000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],raw)
      |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634518490,60,1634777210))
      |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634518490000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1634172590000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634518490000, step=60000, end=1634777210000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1634172590000,1634431610000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],raw)
      |-E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],downsample)
      |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634518430))
      |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],downsample)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634518430000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=9, chunkMethod=TimeRangeChunkScan(1633740230000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],downsample)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634518430000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=25, chunkMethod=TimeRangeChunkScan(1633740230000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],downsample)
      |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634518430))
      |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],downsample)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634518430000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1633740230000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],downsample)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634518430000, window=None, functionId=None, rawSource=true, offsetMs=Some(345600000))
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1633740230000,1634172830000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#208769433],downsample)""".stripMargin

    validatePlan(ep, expectedPlan)

  }

  it("Materializing Binary join with or vector(0) with long look back should not throw error") {
      val now = 1634777330123L
      val start = now / 1000 - 8.days.toSeconds
      val step = 1.minute.toSeconds
      val end = now / 1000 - 2.minutes.toSeconds

      val rawRetention = 7.days // 7 days
      val downsampleRetention = 183.days
      val earliestRawTime = now - rawRetention.toMillis
      val earliestDownSampleTime = now - downsampleRetention.toMillis
      // making sure that the  latestDownsampleTime is not a multiple of 1000
      val latestDownsampleTime = now - 6.hours.toMillis + 123

      val query = """sum(rate(bar{job= "app"}[30d])) or vector(0)"""

      val logicalPlan = Parser.queryRangeToLogicalPlan(query,
        TimeStepParams(start, step, end))
        .asInstanceOf[PeriodicSeriesPlan]

      val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
        earliestRetainedTimestampFn = earliestRawTime, queryConfig, "raw")
      val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
        earliestRetainedTimestampFn = earliestDownSampleTime, queryConfig, "downsample")
      val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
        earliestRawTime, latestDownsampleTime, disp, queryConfig, dataset)

      val ep = longTermPlanner.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

      val expected = """E~SetOperatorExec(binaryOp=LOR, on=None, ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,true,false,true,Set(),None))
                       |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634086130,60,1634755730))
                       |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1009481049],downsample)
                       |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                       |----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634755730000, window=Some(2592000000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                       |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(1631494130000,1634755730000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1009481049],downsample)
                       |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                       |----T~PeriodicSamplesMapper(start=1634086130000, step=60000, end=1634755730000, window=Some(2592000000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                       |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(1631494130000,1634755730000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1009481049],downsample)
                       |-T~VectorFunctionMapper(funcParams=List())
                       |--E~ScalarFixedDoubleExec(params = RangeParams(1634086130,60,1634755730), value = 0.0) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,true,false,true,Set(),None))""".stripMargin

      validatePlan(ep, expected)
    }
  }
