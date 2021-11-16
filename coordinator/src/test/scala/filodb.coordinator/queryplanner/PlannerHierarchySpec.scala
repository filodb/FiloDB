package filodb.coordinator.queryplanner

import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.coordinator.ShardMapper
import filodb.core.MetricsTestData
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.query.Filter.{Equals, EqualsRegex}
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.prometheus.parse.Parser.Antlr
import filodb.query.{LabelCardinality, PlanValidationSpec}
import filodb.query.exec._

// scalastyle:off line.size.limit
class PlannerHierarchySpec extends AnyFunSpec with Matchers with PlanValidationSpec{
  private implicit val system: ActorSystem = ActorSystem()
  private val node = TestProbe().ref

  private val mapper = new ShardMapper(2)
  for {i <- 0 until 2} mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  private val dataset = MetricsTestData.timeseriesDatasetWithMetric
  private val schemas = Schemas(dataset.schema)

  private val routingConfigString = "routing {\n  remote {\n    http {\n      timeout = 10000\n    }\n  }\n}"
  private val routingConfig = ConfigFactory.parseString(routingConfigString)

  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").withFallback(routingConfig)
  private val queryConfig = new QueryConfig(config)

  private val now = 1634777330000L

  private val rawRetention = 7.days.toMillis
  val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
    earliestRetainedTimestampFn = now - rawRetention, queryConfig, "raw")

  private val downsampleRetention = 30.days.toMillis
  val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
    earliestRetainedTimestampFn = now - downsampleRetention, queryConfig, "downsample")

  private def inProcessDispatcher =  InProcessPlanDispatcher(EmptyQueryConfig)

  private val timeToDownsample = 6.hours.toMillis
  private val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
    earliestRawTimestampFn = now - rawRetention, latestDownsampleTimestampFn = now - timeToDownsample,
    inProcessDispatcher, queryConfig, dataset)

  private val rrRetention = 30.days.toMillis
  val recordingRulesPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
    earliestRetainedTimestampFn = now - rrRetention,
    queryConfig, "recordingRules")

  private val plannerSelector = (metricName: String) => {
    if (metricName.contains(":1m")) "recordingRules" else "longTerm"
  }
  val planners = Map("longTerm" -> longTermPlanner, "recordingRules" -> recordingRulesPlanner)
  val singlePartitionPlanner = new SinglePartitionPlanner(planners, plannerSelector, "_metric_", queryConfig)

  private val partitionLocationProvider = new PartitionLocationProvider {
    override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
      if (routingKey("_ns_") == "localNs") {
        List(PartitionAssignment("localPartition", "localPartition-url", TimeRange(timeRange.startMs, timeRange.endMs)))
      } else {
        List(PartitionAssignment("remotePartition", "remotePartition-url",
          TimeRange(timeRange.startMs, timeRange.endMs)))
      }
    }

    override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("localPartition", "localPartition-url", TimeRange(timeRange.startMs, timeRange.endMs)),
        PartitionAssignment("remotePartition", "remotePartition-url", TimeRange(timeRange.startMs, timeRange.endMs)))
  }
  val multiPartitionPlanner = new MultiPartitionPlanner(partitionLocationProvider, singlePartitionPlanner,
    "localPartition", dataset, queryConfig)

  private val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
    // to ensure that tests dont call something else that is not configured
    require(shardColumnFilters.exists(f => f.column == "_ns_" && f.filter.isInstanceOf[EqualsRegex]
      && f.filter.asInstanceOf[EqualsRegex].pattern.toString == ".*Ns"))
    Seq(
      Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("localNs"))),
      Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("remoteNs")))
    )
  }
  val rootPlanner = new ShardKeyRegexPlanner(dataset, multiPartitionPlanner, shardKeyMatcherFn, queryConfig)

  private val startSeconds = now / 1000 - 10.days.toSeconds
  private val endSeconds = now / 1000
  private val step = 300

  private val queryParams = PromQlQueryParams("notUsedQuery", 100, 1, 1000)


  it("should generate plan for one namespace query across raw/downsample") {
    val lp = Parser.queryRangeToLogicalPlan(
      """sum(foo{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" })""",
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))

    val expected =
      """E~StitchRvsExec() on InProcessPlanDispatcher
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1314561820],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1314561820],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1314561820],raw)
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1314561820],downsample)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1314561820],downsample)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1314561820],downsample)""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should generate plan for one recording rule query") {
    val lp = Parser.queryRangeToLogicalPlan(
      """sum(foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" })""",
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))

    val expected =
      """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
        |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1054960625],recordingRules)
        |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1054960625],recordingRules)
        |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1054960625],recordingRules)""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should generate plan for single partition query that does not live in local partition") {
    val query = """sum(foo:1m{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" })"""
    val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """E~PromQlRemoteExec(PromQlQueryParams(sum(foo:1m{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" }),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,30000,1000000,100000,100000,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@39de9bda)""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should generate plan for recording rule query spanning multiple partitions") {
    val query =
      """sum(foo:1m{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" }) +
        |  sum(foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" })
        |""".stripMargin
    val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@20c812c8)
        |-E~PromQlRemoteExec(PromQlQueryParams(sum(foo:1m{_ws_="demo",_ns_="RemoteNs",instance="Inst-1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,30000,1000000,100000,100000,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@20c812c8)
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1684950125],recordingRules)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1684950125],recordingRules)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1684950125],recordingRules)""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should generate plan for raw query spanning multiple partitions, and push down aggregations") {
    val query =
      """count(sum(foo{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" }) +
        |  sum(foo{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }))
        |""".stripMargin
    val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
        |-E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@159424e2)
        |--T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List())
        |---E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@159424e2)
        |----E~PromQlRemoteExec(PromQlQueryParams(sum(foo{_ws_="demo",_ns_="RemoteNs",instance="Inst-1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,30000,1000000,100000,100000,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@159424e2)
        |----E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@1f736d00)
        |-----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
        |------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1672762875],raw)
        |-------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |--------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1672762875],raw)
        |-------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |--------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1672762875],raw)
        |-----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
        |------E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1672762875],downsample)
        |-------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |--------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1672762875],downsample)
        |-------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |--------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1672762875],downsample)""".stripMargin


    validatePlan(execPlan, expected)
  }

  it("should generate plan for raw query spanning multiple partitions with namespace regex, and push down aggregations") {
    val query =
      """sum(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" })
        |""".stripMargin
    val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
        |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5974b7e8)
        |--E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@2839e3c8)
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1211350849],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1211350849],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1211350849],raw)
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1211350849],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1211350849],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1211350849],downsample)
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,30000,1000000,100000,100000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5974b7e8)""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should generate plan for binary join query spanning multiple partitions with namespace regex, and push down aggregations") {
    val query =
      """sum(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }) *
        |sum(bar{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" })
        |""".stripMargin
    val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """E~BinaryJoinExec(binaryOp=MUL, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5eabff6b)
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
        |--E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5eabff6b)
        |---E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@5b000fe6)
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],raw)
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],downsample)
        |---E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,30000,1000000,100000,100000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5eabff6b)
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
        |--E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5eabff6b)
        |---E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@5b000fe6)
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],raw)
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#542289610],downsample)
        |---E~PromQlRemoteExec(PromQlQueryParams(sum(bar{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,30000,1000000,100000,100000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5eabff6b)""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should generate correct plan for LabelCardinality on all planners") {
    val filters = Seq(
        ColumnFilter("_ws_", Equals("ws")),
        ColumnFilter("_ns_", Equals("ns")),
        ColumnFilter("_metric_", Equals("metric"))
      )

    val logicalPlan = LabelCardinality(filters, startSeconds * 1000L, endSeconds * 1000L)
    val queryContext = QueryContext(origQueryParams = UnavailablePromQlQueryParams,
      plannerParams = PlannerParams(processMultiPartition = true))
    val rawPlan = rawPlanner.materialize(logicalPlan, queryContext)

    val expectedRawPlannerPlan =
    """T~LabelCardinalityPresenter(LabelCardinalityPresenter)
       |-E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-93033340],raw)
       |--E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1633913330000, endMs=1634777330000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-93033340],raw)
       |--E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1633913330000, endMs=1634777330000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-93033340],raw)""".stripMargin
    validatePlan(rawPlan, expectedRawPlannerPlan)

    // LTRPlanner cases
    // Case 1: startTime < rawStart < dsStartTime < endTime
    // Should go to both clusters
    // DS Time range should be [userStartTime, dsEndTime]
    // Raw Time range should be [rawStartTime, userEndTime]

    val longTermPlan = rootPlanner.materialize(logicalPlan, queryContext)

    val expectedLongTermPlan =
           """T~LabelCardinalityPresenter(LabelCardinalityPresenter)
             |-E~LabelCardinalityReduceExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@10272bbb)
             |--E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1598944375],downsample)
             |---E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1633913330000, endMs=1634755730000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1598944375],downsample)
             |---E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1633913330000, endMs=1634755730000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1598944375],downsample)
             |--E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1598944375],raw)
             |---E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634172530000, endMs=1634777330000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1598944375],raw)
             |---E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634172530000, endMs=1634777330000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1598944375],raw)""".stripMargin
    validatePlan(longTermPlan, expectedLongTermPlan)

    // Case 2: startTime < endTime < rawStart < dsEndTime
    // Should go completely to DS Cluster
    val case2EndTime = endSeconds - 8.days.toSeconds
    val logicalPlan2 = LabelCardinality(filters, startSeconds * 1000L, case2EndTime * 1000L)
    val case2Plan = rootPlanner.materialize(logicalPlan2, queryContext)
    val expectedDSOnlyPlan =
      """T~LabelCardinalityPresenter(LabelCardinalityPresenter)
        |-E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#872531041],downsample)
        |--E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1633913330000, endMs=1634086130000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#872531041],downsample)
        |--E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1633913330000, endMs=1634086130000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#872531041],downsample)""".stripMargin

    validatePlan(case2Plan, expectedDSOnlyPlan)

    // Case 3: rawStart < dsEndTime < startTime  < endTime
    // Should go completely to RawCluster

    val case3startTime = endSeconds - 2.hours.toSeconds
    val case3EndTime = endSeconds - 1.hour.toSeconds
    val logicalPlan4 = LabelCardinality(filters, case3startTime * 1000L, case3EndTime * 1000L)
    val case3Plan = rootPlanner.materialize(logicalPlan4, queryContext)
    val expectedRawOnlyPlan =
    """T~LabelCardinalityPresenter(LabelCardinalityPresenter)
      |-E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-652694969],raw)
      |--E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634770130000, endMs=1634773730000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-652694969],raw)
      |--E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634770130000, endMs=1634773730000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-652694969],raw)""".stripMargin

    validatePlan(case3Plan, expectedRawOnlyPlan)

    // Case 4: rawStart < startTime < endTime < dsEndTime
    // Should go to both clusters but the start and endtime in both clusters should be user provided times

    val case4startTime = endSeconds - 4.days.toSeconds
    val case4EndTime = endSeconds - 3.days.toSeconds
    val logicalPlan5 = LabelCardinality(filters, case4startTime * 1000L, case4EndTime * 1000L)
    val case4Plan = rootPlanner.materialize(logicalPlan5, queryContext)
    val expectedCase4Plan =
    """T~LabelCardinalityPresenter(LabelCardinalityPresenter)
      |-E~LabelCardinalityReduceExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@1f2f0109)
      |--E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],downsample)
      |---E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634431730000, endMs=1634518130000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],downsample)
      |---E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634431730000, endMs=1634518130000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],downsample)
      |--E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],raw)
      |---E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634431730000, endMs=1634518130000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],raw)
      |---E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634431730000, endMs=1634518130000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],raw)""".stripMargin

    validatePlan(case4Plan, expectedCase4Plan)

    // Case 5, should go entirely to Recording rule cluster.
    val rrFilters = Seq(
      ColumnFilter("_ws_", Equals("ws")),
      ColumnFilter("_ns_", Equals("ns")),
      ColumnFilter("_metric_", Equals("metric:1m"))
    )
    val logicalPlan6 = LabelCardinality(rrFilters, startSeconds * 1000L, endSeconds * 1000L)
    val case5Plan = rootPlanner.materialize(logicalPlan6, queryContext)
    val expectedCase5Plan =
    """T~LabelCardinalityPresenter(LabelCardinalityPresenter)
      |-E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1163475801],recordingRules)
      |--E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric:1m))), limit=1000000, startMs=1633913330000, endMs=1634777330000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1163475801],recordingRules)
      |--E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric:1m))), limit=1000000, startMs=1633913330000, endMs=1634777330000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1163475801],recordingRules)""".stripMargin
    validatePlan(case5Plan, expectedCase5Plan)


  }
}