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
import filodb.prometheus.ast.{TimeStepParams, WindowConstants}
import filodb.prometheus.parse.Parser
import filodb.prometheus.parse.Parser.Antlr
import filodb.query.{BadQueryException, IntervalSelector, LabelCardinality, PlanValidationSpec, RawSeries}
import filodb.query.exec._

// scalastyle:off line.size.limit
class PlannerHierarchySpec extends AnyFunSpec with Matchers with PlanValidationSpec {
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
  private val queryConfig = QueryConfig(config)

  private val now = 1634777330000L

  private val rawRetention = 7.days.toMillis
  val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
    earliestRetainedTimestampFn = now - rawRetention, queryConfig, "raw")

  private val downsampleRetention = 30.days.toMillis
  val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
    earliestRetainedTimestampFn = now - downsampleRetention, queryConfig, "downsample")

  private def inProcessDispatcher =  InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig)

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
  val singlePartitionPlanner = new SinglePartitionPlanner(planners, "longTerm", plannerSelector,
    dataset, queryConfig)

  private val partitionLocationProvider = new PartitionLocationProvider {
    override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
      if(routingKey.contains("_ns_")) {
        if (routingKey("_ns_") == "localNs") {
          List(PartitionAssignment("localPartition", "localPartition-url", TimeRange(timeRange.startMs, timeRange.endMs)))
        } else {
          List(PartitionAssignment("remotePartition", "remotePartition-url",
            TimeRange(timeRange.startMs, timeRange.endMs)))
        }
      } else {
        List.empty
      }
    }

    override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                       timeRange: TimeRange): List[PartitionAssignment] =
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

  private val remoteShardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
    // to ensure that tests dont call something else that is not configured
    require(shardColumnFilters.exists(f => f.column == "_ns_" && f.filter.isInstanceOf[EqualsRegex]
      && f.filter.asInstanceOf[EqualsRegex].pattern.toString == ".*remoteNs"))
    Seq(
      Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("remoteNs")))
    )
  }

  val rootPlanner = new ShardKeyRegexPlanner(dataset, multiPartitionPlanner, shardKeyMatcherFn, queryConfig)
  val oneRemoteRootPlanner = new ShardKeyRegexPlanner(dataset, multiPartitionPlanner, remoteShardKeyMatcherFn, queryConfig)


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
      """E~PromQlRemoteExec(PromQlQueryParams(sum(foo:1m{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" }),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@39de9bda)""".stripMargin

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
        |-E~PromQlRemoteExec(PromQlQueryParams(sum(foo:1m{_ws_="demo",_ns_="RemoteNs",instance="Inst-1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@20c812c8)
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
        |----E~PromQlRemoteExec(PromQlQueryParams(sum(foo{_ws_="demo",_ns_="RemoteNs",instance="Inst-1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@159424e2)
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
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5974b7e8)""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should generate plan for raw query spanning multiple partitions with namespace regex which would be an equivalent of TopLevelSubquery") {
    val query ="""sum(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" })"""
    val endSecs = 1634775000L
    val startSecs = endSecs - 10.days.toSeconds
    val stepSecs = 300
    val queryParams = PromQlQueryParams(query, startSecs, stepSecs, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSecs, stepSecs, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633911000,300,1634775000))
        |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher
        |--E~StitchRvsExec() on InProcessPlanDispatcher
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634172900000, step=300000, end=1634775000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172600000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634172900000, step=300000, end=1634775000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172600000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633911000000, step=300000, end=1634172600000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633910700000,1634172600000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633911000000, step=300000, end=1634172600000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633910700000,1634172600000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633911000,300,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should generate plan for TopLevelSubquery spanning multiple partitions with namespace regex, and push down aggregations") {
    val query ="""sum(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" })[10d:300s]"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634775000,0,1634775000))
        |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher
        |--E~StitchRvsExec() on InProcessPlanDispatcher
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634172900000, step=300000, end=1634775000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172600000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634172900000, step=300000, end=1634775000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172600000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633911000000, step=300000, end=1634172600000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633910700000,1634172600000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633911000000, step=300000, end=1634172600000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633910700000,1634172600000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633911000,300,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin

    validatePlan(execPlan, expected)
  }

  // TODO subquery does not push the aggregation down; though this case is NOT a normal case for subquery usage
  // if we fix materializeOthers() creates MultiPartitionDistConcatExec
  // this is because of Line bla
  it("should push down aggregation in generated plan for SubqueryWithWindowing spanning multiple partitions with namespace regex") {
    val query ="""sum_over_time(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[5d:300s])"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~PeriodicSamplesMapper(start=1634775000000, step=0, end=1634775000000, window=Some(432000000), functionId=Some(SumOverTime), rawSource=false, offsetMs=None)
        |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~PeriodicSamplesMapper(start=1634343000000, step=300000, end=1634775000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634342700000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~PeriodicSamplesMapper(start=1634343000000, step=300000, end=1634775000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634342700000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1634343000,300,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate plan for a time range function with proper push down aggregation") {
    val query ="""sum_over_time(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[5d])"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher
        |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |--T~PeriodicSamplesMapper(start=1634775000000, step=0, end=1634775000000, window=Some(432000000), functionId=Some(SumOverTime), rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634343000000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |--T~PeriodicSamplesMapper(start=1634775000000, step=0, end=1634775000000, window=Some(432000000), functionId=Some(SumOverTime), rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634343000000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |-E~PromQlRemoteExec(PromQlQueryParams(sum_over_time(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[432000s]),1634775000,0,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("verification of common subquery cases: smoothing of an aggregate over a sliding window") {
    val query ="""rate(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[10m])[1h:1m]"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher
        |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |--T~PeriodicSamplesMapper(start=1634771400000, step=60000, end=1634775000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634770800000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |--T~PeriodicSamplesMapper(start=1634771400000, step=60000, end=1634775000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634770800000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s]),1634771400,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("verification of common subquery cases: aggregation over smoothed aggregated sliding window") {
    val query ="""sum(rate(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[10m]))[1h:1m]"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634775000,0,1634775000))
        |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634771400000, step=60000, end=1634775000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634770800000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634771400000, step=60000, end=1634775000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634770800000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s])),1634771400,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("verification of common subquery cases: range aggregation over aggregate over smoothed sliding window") {
    val query ="""max_over_time(sum(rate(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[10m]))[1h:1m])"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~PeriodicSamplesMapper(start=1634775000000, step=0, end=1634775000000, window=Some(3600000), functionId=Some(MaxOverTime), rawSource=false, offsetMs=None)
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634775000,0,1634775000))
        |--E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634771400000, step=60000, end=1634775000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634770800000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634771400000, step=60000, end=1634775000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634770800000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---E~PromQlRemoteExec(PromQlQueryParams(sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s])),1634771400,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("verification of common subquery cases: range aggregation over aggregate over smoothed sliding window with one remote partition") {
    val query ="""max_over_time(sum(rate(foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" }[10m]))[1h:1m])"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = oneRemoteRootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """E~PromQlRemoteExec(PromQlQueryParams(max_over_time(sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s]))[3600s:60s]),1634775000,0,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher"""
    validatePlan(execPlan, expected)
  }

  // TODO max_over_time should have been pushed down for subqueries but ShardKeyRegexPlanner
  // in materializeOthers() creates MultiPartitionDistConcatExec
  it("verification of common subquery cases: aggregation of smoothed aggregation function") {
    val query ="""max_over_time(rate(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[10m])[1h:1m])"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~PeriodicSamplesMapper(start=1634775000000, step=0, end=1634775000000, window=Some(3600000), functionId=Some(MaxOverTime), rawSource=false, offsetMs=None)
        |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~PeriodicSamplesMapper(start=1634771400000, step=60000, end=1634775000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634770800000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~PeriodicSamplesMapper(start=1634771400000, step=60000, end=1634775000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634770800000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |--E~PromQlRemoteExec(PromQlQueryParams(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s]),1634771400,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
    validatePlan(execPlan, expected)
  }

  // TODO max_over_time should have been pushed down for subqueries but ShardKeyRegexPlanner
  // in materializeOthers() creates MultiPartitionDistConcatExec
  it("verification of common subquery cases: nested subquery") {
    val query ="""max_over_time(deriv(rate(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[1m])[5m:1m])[1h:1m])"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~PeriodicSamplesMapper(start=1634775000000, step=0, end=1634775000000, window=Some(3600000), functionId=Some(MaxOverTime), rawSource=false, offsetMs=None)
        |-T~PeriodicSamplesMapper(start=1634771400000, step=60000, end=1634775000000, window=Some(300000), functionId=Some(Deriv), rawSource=false, offsetMs=None)
        |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher
        |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |----T~PeriodicSamplesMapper(start=1634771100000, step=60000, end=1634775000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634771040000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |----T~PeriodicSamplesMapper(start=1634771100000, step=60000, end=1634775000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634771040000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---E~PromQlRemoteExec(PromQlQueryParams(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[60s]),1634771100,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
    validatePlan(execPlan, expected)
  }

  // TODO timestamp function does not take binary expression although it works well with Prometheus
  // timestamp is used often with subqueries to find a particular event
  //  it("verification of common subquery cases: using timestamp funtion to find an occurence of a particular event") {
  //    val query ="""max_over_time(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" } > 20)[24h:1m])"""
  //    //java.lang.IllegalArgumentException: Expected type instant vector in call to function timestamp, got BinaryExpression
  //    val endSecs = 1634775000L
  //    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
  //    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
  //    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
  //      plannerParams = PlannerParams(processMultiPartition = true)))
  //
  //    val expected = ""
  //    validatePlan(execPlan, expected)
  //  }


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
        |---E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5eabff6b)
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
        |---E~PromQlRemoteExec(PromQlQueryParams(sum(bar{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5eabff6b)""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should NOT push down binary join across long time range when offset is provided (for now)") {
    val query =
      """sum(foo{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }
        | + bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" } offset 5m)
        |""".stripMargin
    val execPlan = longTermPlanner.materialize(
      Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr),
      QueryContext(origQueryParams = queryParams,
        plannerParams = PlannerParams(processMultiPartition = true)))
    // Since there is offset, we pull all data in QS and not push down the Binary join
    val expectedPlan =
      """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
        |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@19c1820d)
        |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |---E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@19c1820d)
        |----E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@18dd5ed3)
        |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],raw)
        |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],raw)
        |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],raw)
        |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],downsample)
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],downsample)
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],downsample)
        |----E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@18dd5ed3)
        |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],raw)
        |------T~PeriodicSamplesMapper(start=1634173430000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=Some(300000))
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777030000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],raw)
        |------T~PeriodicSamplesMapper(start=1634173430000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=Some(300000))
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777030000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],raw)
        |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],downsample)
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634173130000, window=None, functionId=None, rawSource=true, offsetMs=Some(300000))
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633912730000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],downsample)
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634173130000, window=None, functionId=None, rawSource=true, offsetMs=Some(300000))
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633912730000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1196383288],downsample)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }

  it("should push down entire aggregation along with binary join across long time range") {
    val query =
      """sum(foo{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }
        | + bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" })
        |""".stripMargin
    val execPlan = rootPlanner.materialize(
      Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr),
      QueryContext(origQueryParams = queryParams,
        plannerParams = PlannerParams(processMultiPartition = true)))

    val expectedExecPlan =
      """E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@6ecc02bb)
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],raw)
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],raw)
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],raw)
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],raw)
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],raw)
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],downsample)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],downsample)
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],downsample)
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],downsample)
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],downsample)
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1246893206],downsample)""".stripMargin

    validatePlan(execPlan, expectedExecPlan)

  }

  it("should push down binary join across long time range") {
    val query = """sum(foo{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }) *
                  |sum(bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" })
                  |""".stripMargin
    val execPlan = rootPlanner.materialize(
      Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr),
      QueryContext(origQueryParams = queryParams,
        plannerParams = PlannerParams(processMultiPartition = true)))

    val expectedExecPlan =
      """E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@6adc5b9c)
        |-E~BinaryJoinExec(binaryOp=MUL, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],raw)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],raw)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],raw)
        |-E~BinaryJoinExec(binaryOp=MUL, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],downsample)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],downsample)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],downsample)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1189855027],downsample)""".stripMargin
    validatePlan(execPlan, expectedExecPlan)

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
        |---E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1633913330000, endMs=1634172530000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1598944375],downsample)
        |---E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1633913330000, endMs=1634172530000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1598944375],downsample)
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
        |---E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634431730000, endMs=1634172530000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],downsample)
        |---E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634431730000, endMs=1634172530000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],downsample)
        |--E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],raw)
        |---E~LabelCardinalityExec(shard=0, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634172530000, endMs=1634518130000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],raw)
        |---E~LabelCardinalityExec(shard=1, filters=List(ColumnFilter(_ws_,Equals(ws)), ColumnFilter(_ns_,Equals(ns)), ColumnFilter(_metric_,Equals(metric))), limit=1000000, startMs=1634172530000, endMs=1634518130000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1578174761],raw)""".stripMargin

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

  it("Plans with Scalar plans should be pushed down") {
    val query = """
                  |bottomk(1,
                  |  min by (userName, time) (
                  |    foo{_ws_ ="demo", _ns_="remoteNs"}
                  |    * on (userID) group_left(userName) (bar{_ws_="demo", _ns_ = "remoteNs"})
                  |  ) - time() > 0
                  |)
                  |""".stripMargin

    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(
      lp, QueryContext(origQueryParams = queryParams.copy(promQl = LogicalPlanParser.convertToQuery(lp)),
        plannerParams = PlannerParams(processMultiPartition = true)))

    val expectedPlan = "E~PromQlRemoteExec(PromQlQueryParams(bottomk(1.0,((min((foo{_ws_=\"demo\",_ns_=\"remoteNs\"} * on(userID) group_left(userName) bar{_ws_=\"demo\",_ns_=\"remoteNs\"})) by (userName,time) - time()) > 0.0)),1634777330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@570ba13)"
    validatePlan(execPlan, expectedPlan)

  }

  it(" must have a non empty config plans with scalar operations in") {
      val query =
        """min((1000 - foo{_ws_="demo",_ns_="localNs"})
          |/
          |(delta(foo{_ws_="demo",_ns_="localNs"}[1h]) > 0 or vector(1)))""".stripMargin
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSeconds, step, endSeconds), Antlr)
      val execPlan = rootPlanner.materialize(lp,
        QueryContext(origQueryParams = queryParams.copy(promQl = LogicalPlanParser.convertToQuery(lp))))
    val expectedPlan =
        """T~AggregatePresenter(aggrOp=Min, aggrParams=List(), rangeParams=RangeParams(1634777330,300,1634777330))
           |-E~LocalPartitionReduceAggregateExec(aggrOp=Min, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@66e21568)
           |--T~AggregateMapReduce(aggrOp=Min, aggrParams=List(), without=List(), by=List())
           |---E~BinaryJoinExec(binaryOp=DIV, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@66e21568)
           |----T~ScalarOperationMapper(operator=SUB, scalarOnLhs=true)
           |-----FA1~StaticFuncArgs(1000.0,RangeParams(1634777330,300,1634777330))
           |-----T~PeriodicSamplesMapper(start=1634777330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
           |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634777030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1012526996],raw)
           |----T~ScalarOperationMapper(operator=SUB, scalarOnLhs=true)
           |-----FA1~StaticFuncArgs(1000.0,RangeParams(1634777330,300,1634777330))
           |-----T~PeriodicSamplesMapper(start=1634777330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
           |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634777030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1012526996],raw)
           |----E~SetOperatorExec(binaryOp=LOR, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@66e21568)
           |-----T~ScalarOperationMapper(operator=GTR, scalarOnLhs=false)
           |------FA1~StaticFuncArgs(0.0,RangeParams(1634777330,300,1634777330))
           |------T~PeriodicSamplesMapper(start=1634777330000, step=300000, end=1634777330000, window=Some(3600000), functionId=Some(Delta), rawSource=true, offsetMs=None)
           |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634773730000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1012526996],raw)
           |-----T~ScalarOperationMapper(operator=GTR, scalarOnLhs=false)
           |------FA1~StaticFuncArgs(0.0,RangeParams(1634777330,300,1634777330))
           |------T~PeriodicSamplesMapper(start=1634777330000, step=300000, end=1634777330000, window=Some(3600000), functionId=Some(Delta), rawSource=true, offsetMs=None)
           |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634773730000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1012526996],raw)
           |-----T~VectorFunctionMapper(funcParams=List())
           |------E~ScalarFixedDoubleExec(params = RangeParams(1634777330,300,1634777330), value = 1.0) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@66e21568)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }


  // SinglePartitionPlanner tests

  it("Binary join on RR and Raw should happen in process") {
    val lp = Parser.queryRangeToLogicalPlan(
      """bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }
        | + foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None))
                         |-E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
                         |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-677889979],raw)
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-677889979],raw)
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-677889979],raw)
                         |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-677889979],downsample)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-677889979],downsample)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-677889979],downsample)
                         |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-677889979],recordingRules)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-677889979],recordingRules)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-677889979],recordingRules)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }


  it("Binary join on Non RR metric and another Non RR metric should be pushed down to underlying planner") {
    val lp = Parser.queryRangeToLogicalPlan(
      """bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }
        | + foo{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],raw)
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1642816958],downsample)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }


  it("Binary join on RR  metric and RR metric should be pushed down to RR planner") {
    val lp = Parser.queryRangeToLogicalPlan(
      """bar:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }
        | + foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2005616082],recordingRules)
                         |-T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2005616082],recordingRules)
                         |-T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2005616082],recordingRules)
                         |-T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2005616082],recordingRules)
                         |-T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2005616082],recordingRules)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }

  it("Aggregate on a Binary join of RR and Raw should happen in process") {
    val lp = Parser.queryRangeToLogicalPlan(
      """sum(bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }
        | + foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" })""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
                         |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None))
                         |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |---E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None))
                         |----E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
                         |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1280141752],raw)
                         |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1280141752],raw)
                         |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1280141752],raw)
                         |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1280141752],downsample)
                         |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1280141752],downsample)
                         |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1280141752],downsample)
                         |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1280141752],recordingRules)
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1280141752],recordingRules)
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1280141752],recordingRules)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }

  it("Aggregate of a Binary join on RR metric and RR metric should be pushed down to RR planner") {
    val lp = Parser.queryRangeToLogicalPlan(
      """sum(bar:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }
        | + foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" })""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
                         |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#108182148],recordingRules)
                         |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |---E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#108182148],recordingRules)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#108182148],recordingRules)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#108182148],recordingRules)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#108182148],recordingRules)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#108182148],recordingRules)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }

  it("Should perform the binary join on histogram_quantile(instant function) on a raw metric joined with RR in process") {
    val lp = Parser.queryRangeToLogicalPlan(
      """histogram_quantile(0.9, sum(rate(bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }[2m])))
        | + foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None))
                         |-E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
                         |--T~InstantVectorFunctionMapper(function=HistogramQuantile)
                         |---FA1~StaticFuncArgs(0.9,RangeParams(1633913330,300,1634777330))
                         |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172830,300,1634777330))
                         |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-286439423],raw)
                         |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |------T~PeriodicSamplesMapper(start=1634172830000, step=300000, end=1634777330000, window=Some(120000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172710000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-286439423],raw)
                         |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |------T~PeriodicSamplesMapper(start=1634172830000, step=300000, end=1634777330000, window=Some(120000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172710000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-286439423],raw)
                         |--T~InstantVectorFunctionMapper(function=HistogramQuantile)
                         |---FA1~StaticFuncArgs(0.9,RangeParams(1633913330,300,1634777330))
                         |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172530))
                         |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-286439423],downsample)
                         |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172530000, window=Some(120000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913210000,1634172530000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-286439423],downsample)
                         |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172530000, window=Some(120000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913210000,1634172530000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-286439423],downsample)
                         |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-286439423],recordingRules)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-286439423],recordingRules)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-286439423],recordingRules)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }


  it("Should perform the Scalar vector binary operation on a raw metric joined with RR in process") {
    val lp = Parser.queryRangeToLogicalPlan(
      """(histogram_quantile(0.9, sum(rate(bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }[2m])))
        | + foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }) > 1""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """T~ScalarOperationMapper(operator=GTR, scalarOnLhs=false)
                         |-FA1~StaticFuncArgs(1.0,RangeParams(1633913330,300,1634777330))
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
                         |---T~InstantVectorFunctionMapper(function=HistogramQuantile)
                         |----FA1~StaticFuncArgs(0.9,RangeParams(1633913330,300,1634777330))
                         |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172830,300,1634777330))
                         |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-913106743],raw)
                         |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |-------T~PeriodicSamplesMapper(start=1634172830000, step=300000, end=1634777330000, window=Some(120000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                         |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172710000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-913106743],raw)
                         |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |-------T~PeriodicSamplesMapper(start=1634172830000, step=300000, end=1634777330000, window=Some(120000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                         |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172710000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-913106743],raw)
                         |---T~InstantVectorFunctionMapper(function=HistogramQuantile)
                         |----FA1~StaticFuncArgs(0.9,RangeParams(1633913330,300,1634777330))
                         |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172530))
                         |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-913106743],downsample)
                         |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |-------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172530000, window=Some(120000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                         |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913210000,1634172530000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-913106743],downsample)
                         |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |-------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172530000, window=Some(120000), functionId=Some(Rate), rawSource=true, offsetMs=None)
                         |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913210000,1634172530000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-913106743],downsample)
                         |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-913106743],recordingRules)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-913106743],recordingRules)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-913106743],recordingRules)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }


  it("Scalar fixed double plan with a Recording rule should be materialized in RR cluster") {
    val lp = Parser.queryRangeToLogicalPlan(
      """foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" } + 5""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-315616408],recordingRules)
                         |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
                         |--FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-315616408],recordingRules)
                         |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
                         |--FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-315616408],recordingRules)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }


  it("Vector and scalar plans with RR should be pushed down to RR cluster") {
    val lp = Parser.queryRangeToLogicalPlan(
      """vector(scalar(foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }))""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """T~VectorFunctionMapper(funcParams=List())
                         |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                         |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#870489724],recordingRules)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#870489724],recordingRules)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#870489724],recordingRules)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }


  it("Vector plan on a binary join in  RR and raw happen in memory") {
    val lp = Parser.queryRangeToLogicalPlan(
      """vector(scalar(foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" } *
        | bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" }))""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """T~VectorFunctionMapper(funcParams=List())
                         |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                         |--E~BinaryJoinExec(binaryOp=MUL, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None))
                         |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#658539445],recordingRules)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#658539445],recordingRules)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#658539445],recordingRules)
                         |---E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
                         |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#658539445],raw)
                         |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#658539445],raw)
                         |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#658539445],raw)
                         |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#658539445],downsample)
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#658539445],downsample)
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#658539445],downsample)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }

  it("Raw data export on RR cluster should be pushed down to rr cluster and others to raw cluster only") {
    val lp = RawSeries(IntervalSelector(startSeconds, endSeconds),
        Seq(ColumnFilter("__name__", Equals("foo:1m")), ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("localNs")),
        ColumnFilter("instance", Equals("Inst-1"))), Seq("job", "instance"), Some(300000), None)

    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1736228159],recordingRules)
                         |-E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633613330,1634777330), filters=List(ColumnFilter(_metric_,Equals(foo:1m)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1))), colName=Some(job), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1736228159],recordingRules)
                         |-E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633613330,1634777330), filters=List(ColumnFilter(_metric_,Equals(foo:1m)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1))), colName=Some(job), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1736228159],recordingRules)""".stripMargin


    val lp1 = RawSeries(IntervalSelector(startSeconds, endSeconds),
      Seq(ColumnFilter("__name__", Equals("bar")), ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("localNs")),
        ColumnFilter("instance", Equals("Inst-1"))), Seq("job", "instance"), Some(300000), None)

    val execPlan1 = rootPlanner.materialize(lp1, QueryContext(origQueryParams = queryParams))
    val expectedPlan1 = """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1736228159],raw)
                         |-E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633613330,1634777330), filters=List(ColumnFilter(_metric_,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1))), colName=Some(job), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1736228159],raw)
                         |-E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633613330,1634777330), filters=List(ColumnFilter(_metric_,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1))), colName=Some(job), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1736228159],raw)""".stripMargin
    validatePlan(execPlan1, expectedPlan1)
  }

  it("should materialize the plan inprocess when we perform a > operation on a raw and a RR") {
    val lp = Parser.queryRangeToLogicalPlan(
      """bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" } >
        |scalar(foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" })""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expectedPlan = """T~ScalarOperationMapper(operator=GTR, scalarOnLhs=false)
                         |-FA1~
                         |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                         |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2057863312],recordingRules)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2057863312],recordingRules)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2057863312],recordingRules)
                         |-E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
                         |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2057863312],raw)
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2057863312],raw)
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2057863312],raw)
                         |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2057863312],downsample)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2057863312],downsample)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2057863312],downsample)""".stripMargin
    validatePlan(execPlan, expectedPlan)

    val lp1 = Parser.queryRangeToLogicalPlan(
      """foo:1m{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" } >
        | scalar(bar{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" })""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan1 = rootPlanner.materialize(lp1, QueryContext(origQueryParams = queryParams))
    val expectedPlan1 = """T~ScalarOperationMapper(operator=GTR, scalarOnLhs=false)
                          |-FA1~
                          |-E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
                          |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-204023117],raw)
                          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-204023117],raw)
                          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-204023117],raw)
                          |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-204023117],downsample)
                          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-204023117],downsample)
                          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-204023117],downsample)
                          |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-204023117],recordingRules)
                          |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-204023117],recordingRules)
                          |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo:1m))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-204023117],recordingRules)""".stripMargin
    validatePlan(execPlan1, expectedPlan1)
  }

  it ("should pushdown when leaf plans are split across partitions") {
    val startSec = 0
    val stepSec = 3
    val endSec = 9999
    val splitSec = 5000
    val staleLookbackSec = WindowConstants.staleDataLookbackMillis / 1000
    val expectedUrls = Seq("remote0-url", "remote1-url")

    case class Test(query: String, lookbackSec: Long = staleLookbackSec, offsetSec: Long = 0, expected: String = "") {
      def getExpectedRangesSec(): Seq[(Long, Long)] = {
        val snappedSecondStart = LogicalPlanUtils.snapToStep(timestamp = splitSec + offsetSec + lookbackSec,
                                                             step = stepSec,
                                                             origin = startSec)
        Seq((startSec, splitSec + offsetSec),
            (snappedSecondStart, endSec))
      }
    }

    val tests = Seq(
      // aggregate
      Test("""sum(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sum(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sum(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""count(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""group(rate(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(group(rate(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(group(rate(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""sum(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sum(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sum(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // instant
      Test("""sgn(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sgn(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sgn(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""ln(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,00000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(ln(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(ln(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""exp(rate(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(exp(rate(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(exp(rate(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""floor(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(floor(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(floor(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // binary join
      Test("""test{job="app"} + test{job="app"}""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + test{job="app"},0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + test{job="app"},5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""test{job="app"} + (test{job="app"} + test{job="app"})""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + (test{job="app"} + test{job="app"}),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + (test{job="app"} + test{job="app"}),5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""count(test{job="app"}) + sum(test{job="app"})""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count(test{job="app"}) + sum(test{job="app"}),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count(test{job="app"}) + sum(test{job="app"}),5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""count_over_time(foo{job="app"}[15m]) unless rate(bar{job="app"}[5m])""",
        lookbackSec = 900,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app"}[15m]) unless rate(bar{job="app"}[5m]),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app"}[15m]) unless rate(bar{job="app"}[5m]),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""count_over_time(foo{job="app1"}[5m]) unless rate(bar{job="app1"}[15m:30s])""",
        lookbackSec = 900 + staleLookbackSec,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app1"}[5m]) unless rate(bar{job="app1"}[15m:30s]),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app1"}[5m]) unless rate(bar{job="app1"}[15m:30s]),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""rate(foo{job="app1"}[5m]) + (rate(bar{job="app1"}[20m]) + count_over_time(baz{job="app1"}[5m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m]) + (rate(bar{job="app1"}[20m]) + count_over_time(baz{job="app1"}[5m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m]) + (rate(bar{job="app1"}[20m]) + count_over_time(baz{job="app1"}[5m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""rate(foo{job="app1"}[5m:30s]) + (rate(bar{job="app1"}[20m:30s]) + count_over_time(baz{job="app1"}[5m:30s]))""",
        lookbackSec = 1200 + staleLookbackSec,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m:30s]) + (rate(bar{job="app1"}[20m:30s]) + count_over_time(baz{job="app1"}[5m:30s])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m:30s]) + (rate(bar{job="app1"}[20m:30s]) + count_over_time(baz{job="app1"}[5m:30s])),6501,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // scalar vector join
      Test("""test{job="app"} + 123""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + 123,0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + 123,5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""123 + sgn(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sgn(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sgn(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""123 + sum(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""123 + group(rate(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + group(rate(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + group(rate(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""123 + sum(count_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(count_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(count_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // absent
      Test("""absent(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""absent(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""absent(count_over_time(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(count_over_time(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(count_over_time(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""absent(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // scalar
      Test("""scalar(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""scalar(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""scalar(count_over_time(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(count_over_time(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(count_over_time(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""scalar(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
    )
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String],
                                 timeRange: TimeRange): List[PartitionAssignment] = {
        val splitMs = 1000 * splitSec
        List(PartitionAssignment("remote0", "remote0-url", TimeRange(timeRange.startMs, splitMs)),
             PartitionAssignment("remote1", "remote1-url", TimeRange(splitMs + 1, timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                         timeRange: TimeRange): List[PartitionAssignment] =
        throw new RuntimeException("should not use")
    }
    val engine = new MultiPartitionPlanner(
      partitionLocationProvider, singlePartitionPlanner, "local",
      MetricsTestData.timeseriesDataset, queryConfig
    )
    for (test <- tests) {
      val lp = Parser.queryRangeToLogicalPlan(test.query, TimeStepParams(startSec, stepSec, endSec))
      val promQlQueryParams = PromQlQueryParams(test.query, startSec, stepSec, endSec)
      val execPlan = engine.materialize(lp,
        QueryContext(origQueryParams = promQlQueryParams,
          plannerParams = PlannerParams(processMultiPartition = true))
      )
      // All should have this form:
      // E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
      // -E~PromQlRemoteExec(PromQlQueryParams(sgn(test{job="app"}) + 123,123,45,3306,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
      // -E~PromQlRemoteExec(PromQlQueryParams(sgn(test{job="app"}) + 123,3633,45,6789,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
      val root = execPlan.asInstanceOf[StitchRvsExec]
      // Make sure one PromQlRemoteExec for each partition.
      root.children.size shouldEqual 2
      // Extract the endpoint/TimeStepParams and make sure they are as-expected.
      val expectedQueryParams = {
        val timeStepParams = test.getExpectedRangesSec().map { case (startSecExp, endSecExp) =>
          TimeStepParams(startSecExp, stepSec, endSecExp)
        }
        expectedUrls.zip(timeStepParams)
      }.toSet
      root.children.map{ child =>
        val remote = child.asInstanceOf[PromQlRemoteExec]
        val params = remote.promQlQueryParams
        // Each plan should dispatch the same query.
        params.promQl shouldEqual test.query
        (remote.queryEndpoint, TimeStepParams(params.startSecs, params.stepSecs, params.endSecs))
      }.toSet shouldEqual expectedQueryParams
      // sanity check
      validatePlan(root, test.expected)
    }
  }

  it ("should fail to materialize unsupported split-partition queries with binary joins") {
    val startSec = 123
    val stepSec = 456
    val endSec = 789
    val queries = Seq(
      // aggregate
      """sum(foo{job="app"} + bar{job="app2"})""",
      """sum(foo{job="app"} + (bar{job="app"} + baz{job="app2"}))""",
      """sum(foo{job="app"} offset 1h + bar{job="app"})""",
      """sum(foo{job="app"} + (bar{job="app"} + baz{job="app"} offset 1h))""",
      // instant
      """sgn(foo{job="app"} + bar{job="app2"})""",
      """exp(foo{job="app"} + (bar{job="app"} + baz{job="app2"}))""",
      """ln(foo{job="app"} offset 1h + bar{job="app"})""",
      """log2(foo{job="app"} + (bar{job="app"} + baz{job="app"} offset 1h))""",
      // binary join
      """foo{job="app"} + bar{job="app2"}""",
      """foo{job="app"} + (bar{job="app"} + baz{job="app2"})""",
      """foo{job="app"} offset 1h + bar{job="app"}""",
      """foo{job="app"} + (bar{job="app"} + baz{job="app"} offset 1h)""",
      // scalar vector join
      """123 + (foo{job="app"} + bar{job="app2"})""",
      """123 + (foo{job="app"} + (bar{job="app"} + baz{job="app2"}))""",
      """123 + (foo{job="app"} offset 1h + bar{job="app"})""",
      """123 + (foo{job="app"} + (bar{job="app"} + baz{job="app"} offset 1h))""",
      // scalar
      """scalar(foo{job="app"} + bar{job="app2"})""",
      """scalar(foo{job="app"} + (bar{job="app"} + baz{job="app2"}))""",
      """scalar(foo{job="app"} offset 1h + bar{job="app"})""",
      """scalar(foo{job="app"} + (bar{job="app"} + baz{job="app"} offset 1h))""",
      // absent
      """absent(foo{job="app"} + bar{job="app2"})""",
      """absent(foo{job="app"} + (bar{job="app"} + baz{job="app2"}))""",
      """absent(foo{job="app"} offset 1h + bar{job="app"})""",
      """absent(foo{job="app"} + (bar{job="app"} + baz{job="app"} offset 1h))""",
    )
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String],
                                 timeRange: TimeRange): List[PartitionAssignment] = {
        val midTime = (timeRange.startMs + timeRange.endMs) / 2
        List(PartitionAssignment("remote0", "remote0-url", TimeRange(timeRange.startMs, midTime)),
             PartitionAssignment("remote1", "remote1-url", TimeRange(midTime, timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                         timeRange: TimeRange): List[PartitionAssignment] =
        throw new RuntimeException("should not use")
    }
    val engine = new MultiPartitionPlanner(
      partitionLocationProvider, singlePartitionPlanner, "local",
      MetricsTestData.timeseriesDataset, queryConfig
    )
    for (query <- queries) {
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSec, stepSec, endSec))
      val promQlQueryParams = PromQlQueryParams(query, startSec, stepSec, endSec)
      assertThrows[BadQueryException] {
        engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,
          plannerParams = PlannerParams(processMultiPartition = true)))
      }
    }
  }
}
