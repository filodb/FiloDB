package filodb.coordinator.queryplanner

import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.{FunctionalTargetSchemaProvider, StaticSpreadProvider}
import filodb.core.{MetricsTestData, SpreadChange, StaticTargetSchemaProvider, TargetSchemaChange}
import filodb.core.metadata.{Dataset, Schemas}
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

  private val dataset = MetricsTestData.timeseriesDatasetWithMetric
  private val schemas = Schemas(dataset.schema)

  private val routingConfigString = "routing {\n  remote {\n    http {\n      timeout = 10000\n    }\n  }\n}"
  private val routingConfig = ConfigFactory.parseString(routingConfigString)

  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").withFallback(routingConfig)
  private val queryConfig = QueryConfig(config).copy(plannerSelector = Some("plannerSelector"))

  private val now = 1634777330000L

  private val rawRetention = 7.days.toMillis
  private val downsampleRetention = 30.days.toMillis
  private val rrRetention = 30.days.toMillis
  private val timeToDownsample = 6.hours.toMillis

  private def inProcessDispatcher =  InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig)

  private val plannerSelector = (metricName: String) => {
    if (metricName.contains(":1m")) "recordingRules" else "longTerm"
  }

  case class Planners(spp: SinglePartitionPlanner, lt: LongTimeRangePlanner, raw: SingleClusterPlanner,
                      ds: SingleClusterPlanner, rr: SingleClusterPlanner)

  def getPlanners(nShards: Int, dataset: Dataset): Planners = {
    val mapper = new ShardMapper(nShards)
    for {i <- 0 until nShards} mapper.registerNode(Seq(i), node)
    def mapperRef = mapper
    val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = now - rawRetention, queryConfig, "raw")
    val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = now - downsampleRetention, queryConfig, "downsample")
    val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
      earliestRawTimestampFn = now - rawRetention, latestDownsampleTimestampFn = now - timeToDownsample,
      inProcessDispatcher, queryConfig, dataset)
    val recordingRulesPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = now - rrRetention,
      queryConfig, "recordingRules")
    val planners = Map("longTerm" -> longTermPlanner, "recordingRules" -> recordingRulesPlanner)
    val singlePartitionPlanner = new SinglePartitionPlanner(planners, plannerSelector, dataset, queryConfig)
    Planners(singlePartitionPlanner, longTermPlanner, rawPlanner, downsamplePlanner, recordingRulesPlanner)
  }

  val (singlePartitionPlanner, longTermPlanner, rawPlanner) = {
    val planners = getPlanners(nShards = 2, dataset)
    (planners.spp, planners.lt, planners.raw)
  }

  private val oneRemotePartitionLocationProvider = new PartitionLocationProvider {
    override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
      routingKey("_ns_").split('|').map{ ns =>
        if (ns == "localNs" || ns == "localNs1") {
          PartitionAssignment("localPartition", "localPartition-url", TimeRange(timeRange.startMs, timeRange.endMs))
        } else {
          PartitionAssignment("remotePartition", "remotePartition-url", TimeRange(timeRange.startMs, timeRange.endMs))
        }
      }.toList
    }

    override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                       timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("localPartition", "localPartition-url", TimeRange(timeRange.startMs, timeRange.endMs)),
        PartitionAssignment("remotePartition", "remotePartition-url", TimeRange(timeRange.startMs, timeRange.endMs)))
  }


  private val twoRemotePartitionLocationProvider = new PartitionLocationProvider {
    override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
      routingKey("_ns_") match {
        case "localNs" =>
          List(PartitionAssignment("localPartition", "localPartition-url",
            TimeRange(timeRange.startMs, timeRange.endMs)))
        case "remoteNs0" =>
          List(PartitionAssignment("remotePartition0", "remotePartition-url0",
            TimeRange(timeRange.startMs, timeRange.endMs)))
        case "remoteNs1" =>
          List(PartitionAssignment("remotePartition1", "remote1Partition-url1",
            TimeRange(timeRange.startMs, timeRange.endMs)))
        case _ => throw new IllegalArgumentException("nope")
      }
    }
    override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                       timeRange: TimeRange): List[PartitionAssignment] = ???
  }

  val oneRemoteMultiPartitionPlanner = new MultiPartitionPlanner(oneRemotePartitionLocationProvider, singlePartitionPlanner,
    "localPartition", dataset, queryConfig)
  val twoRemoteMultiPartitionPlanner = new MultiPartitionPlanner(twoRemotePartitionLocationProvider, singlePartitionPlanner,
    "localPartition", dataset, queryConfig)

  private val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
    // we may have mixed of a regex filter and a non-regex filter.
    if (shardColumnFilters.exists(f =>  f.column == "_ns_" && f.filter.isInstanceOf[EqualsRegex])) {
      // to ensure that tests dont call something else that is not configured
      require(shardColumnFilters.exists(f => f.column == "_ns_" && f.filter.isInstanceOf[EqualsRegex]
        && (
        f.filter.asInstanceOf[EqualsRegex].pattern.toString == ".*Ns"
        || f.filter.asInstanceOf[EqualsRegex].pattern.toString == "localNs.*")))
      val nsCol = shardColumnFilters.find(_.column == "_ns_").get
      if (nsCol.filter.asInstanceOf[EqualsRegex].pattern.toString == "localNs.*") {
        Seq(
          Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("localNs"))),
          Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("localNs1")))
        )
      } else {
        Seq(
          Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("localNs"))),
          Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("remoteNs")))
        )
      }
    } else if (shardColumnFilters.exists(f => f.column == "_ns_" && f.filter.isInstanceOf[Equals])) {
      Seq(shardColumnFilters)
    } else {
      Nil
    }  // i.e. filters for a scalar
  }

  private val oneRemoteShardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
    if (shardColumnFilters.exists(f =>  f.column == "_ns_" && f.filter.isInstanceOf[EqualsRegex])) {
      // to ensure that tests dont call something else that is not configured
      require(shardColumnFilters.exists(f => f.column == "_ns_" && f.filter.isInstanceOf[EqualsRegex]
        && f.filter.asInstanceOf[EqualsRegex].pattern.toString == ".*remoteNs"))
      Seq(
        Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("remoteNs")))
      )
    } else if (shardColumnFilters.nonEmpty) {
      Seq(shardColumnFilters)
    }
    else {
      Nil
    }  // i.e. filters for a scalar
  }
  private val twoRemoteShardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
    if (shardColumnFilters.exists(f =>  f.column == "_ns_" && f.filter.isInstanceOf[EqualsRegex])) {
      // to ensure that tests dont call something else that is not configured
      require(shardColumnFilters.exists(f => f.column == "_ns_" && f.filter.isInstanceOf[EqualsRegex]
        && f.filter.asInstanceOf[EqualsRegex].pattern.toString == "remoteNs.*"))
      Seq(
        Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("remoteNs0"))),
        Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("remoteNs1"))),
      )
    } else if (shardColumnFilters.nonEmpty) {
      Seq(shardColumnFilters)
    } else {
      Nil
    }  // i.e. filters for a scalar
  }

  private val targetSchemaProvider = StaticTargetSchemaProvider()

  val rootPlanner = new ShardKeyRegexPlanner(dataset, oneRemoteMultiPartitionPlanner, shardKeyMatcherFn, oneRemotePartitionLocationProvider, queryConfig)
  val oneRemoteRootPlanner = new ShardKeyRegexPlanner(dataset, oneRemoteMultiPartitionPlanner, oneRemoteShardKeyMatcherFn, oneRemotePartitionLocationProvider, queryConfig)
  val twoRemoteRootPlanner = new ShardKeyRegexPlanner(dataset, twoRemoteMultiPartitionPlanner, twoRemoteShardKeyMatcherFn, twoRemotePartitionLocationProvider, queryConfig)


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

  it("should aggregate through ActorPlanDispatcher for localNs") {
    val lp = Parser.queryRangeToLogicalPlan(
      """count(foo{_ws_="demo", _ns_="localNs", instance="Inst-1" }
        | unless on (instance)
        | bar{_ws_ = "demo", _ns_="localNs", instance=~".*"} )""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expected =
    """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
      |-T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
      |--E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],raw)
      |---T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List())
      |----E~SetOperatorExec(binaryOp=LUnless, on=List(instance), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],raw)
      |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],raw)
      |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],raw)
      |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,EqualsRegex(.*)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],raw)
      |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,EqualsRegex(.*)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],raw)
      |-T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
      |--E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],downsample)
      |---T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List())
      |----E~SetOperatorExec(binaryOp=LUnless, on=List(instance), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],downsample)
      |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],downsample)
      |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],downsample)
      |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,EqualsRegex(.*)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],downsample)
      |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,EqualsRegex(.*)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1888594662],downsample)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should aggregate through InProcessPlanDispatcher for remote _ns_ using regex and see two local Ns") {
    val lp = Parser.queryRangeToLogicalPlan(
      """count(foo{_ws_="demo", _ns_="localNs", instance="Inst-1" }
        | unless on (instance)
        | bar{_ws_ = "demo", _ns_ =~"localNs.*", instance=~".*"} )""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
    val expected =
     """T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
       |-E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
       |--T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List())
       |---E~SetOperatorExec(binaryOp=LUnless, on=List(instance), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
       |----E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
       |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],raw)
       |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],raw)
       |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],raw)
       |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],downsample)
       |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],downsample)
       |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],downsample)
       |----E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
       |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],raw)
       |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,EqualsRegex(.*)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(localNs|localNs1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],raw)
       |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,EqualsRegex(.*)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(localNs|localNs1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],raw)
       |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],downsample)
       |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,EqualsRegex(.*)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(localNs|localNs1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],downsample)
       |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,EqualsRegex(.*)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(localNs|localNs1)), ColumnFilter(_metric_,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1825385416],downsample)""".stripMargin
    println(execPlan.printTree())
    validatePlan(execPlan, expected)
  }

  it("should aggregate through InProcessPlanDispatcher for remote _ns_ using regex and see both remote and local Ns") {
    val lp = Parser.queryRangeToLogicalPlan(
      """count(foo{_ws_="demo", _ns_="localNs", instance="Inst-1" }
        | unless on (instance)
        | bar{_ws_ = "demo", _ns_ =~"remoteNs.*", instance=~".*"} )""".stripMargin,
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = twoRemoteRootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))
    val expected =
     """T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
       |-E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
       |--T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List())
       |---E~SetOperatorExec(binaryOp=LUnless, on=List(instance), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
       |----E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
       |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2061408838],raw)
       |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2061408838],raw)
       |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2061408838],raw)
       |-----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2061408838],downsample)
       |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2061408838],downsample)
       |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2061408838],downsample)
       |----E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
       |-----E~PromQlRemoteExec(PromQlQueryParams(bar{instance=~".*",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
       |-----E~PromQlRemoteExec(PromQlQueryParams(bar{instance=~".*",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin
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
      """E~PromQlRemoteExec(PromQlQueryParams(sum(foo:1m{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" }),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@39de9bda)""".stripMargin

    validatePlan(execPlan, expected)
  }

  // Ignoring the test until we pickup this radar - rdar://108803361 (Fix vector(0) optimzation corner cases)
  ignore("should optimize plan for shard key regex query that has vector(0)") {
    val query = """sum(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }) or vector(0)"""
    val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |-FA1~StaticFuncArgs(0.0,RangeParams(1633913330,300,1634777330))
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
        |--E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher
        |---E~StitchRvsExec() on InProcessPlanDispatcher
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |---E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin

    validatePlan(execPlan, expected)
  }

  // Ignoring the test until we pickup this radar - rdar://108803361 (Fix vector(0) optimzation corner cases)
  ignore("should optimize plan for remote partition query that has vector(0)") {
    val query = """sum(foo{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" }) or vector(0)"""
    val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """E~PromQlRemoteExec(PromQlQueryParams(sum(foo{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" }) or vector(0),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin

    validatePlan(execPlan, expected)
  }

  // Ignoring the test until we pickup this radar - rdar://108803361 (Fix vector(0) optimzation corner cases)
  ignore("should optimize plan for local+remote partition query that has vector(0)") {
    val query =
      """(foo{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1" } or vector(0)) +
         (bar{_ws_ = "demo", _ns_ = "remoteNs0", instance = "Inst-1" } or vector(0))"""
    val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher
        |-E~StitchRvsExec() on InProcessPlanDispatcher
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |----FA1~StaticFuncArgs(0.0,RangeParams(1634173130,300,1634777330))
        |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |----FA1~StaticFuncArgs(0.0,RangeParams(1634173130,300,1634777330))
        |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |---T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |----FA1~StaticFuncArgs(0.0,RangeParams(1633913330,300,1634172830))
        |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |---T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |----FA1~StaticFuncArgs(0.0,RangeParams(1633913330,300,1634172830))
        |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |-E~PromQlRemoteExec(PromQlQueryParams((bar{_ws_="demo",_ns_="remoteNs0",instance="Inst-1"} or vector(0.0)),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin

    validatePlan(execPlan, expected)
  }


  it("should not pushdown root scalar operation into a RemoteExec's promql when query spans multiple partitions") {
    val queryExpectedPairs = Seq(
      // Instant selector without namespace regex.
      ("""foo{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" } > 5""",
            // Should include the scalar op in the promql, since the query does not span multiple partitions.
            """E~PromQlRemoteExec(PromQlQueryParams(foo{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" } > 5,1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))"""),
      // Instant selector with namesapce regex.
      // NOTE: this can be optimized by pushing down the scalar operation.
      ("""foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" } + 5""",
            """T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1122536221],raw)
              |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1122536221],raw)
              |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1122536221],raw)
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1122536221],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1122536221],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1122536221],downsample)
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // Join with namespace regex in both operands.
      ("""5 * (foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" } + foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" })""",
            """T~ScalarOperationMapper(operator=MUL, scalarOnLhs=true)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],raw)
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],downsample)
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],raw)
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1132741402],downsample)
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // Aggregate with namespace regex selector.
      ("""5 / sum(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" })""",
            """T~ScalarOperationMapper(operator=DIV, scalarOnLhs=true)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
              |--E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1557189041],raw)
              |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1557189041],raw)
              |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1557189041],raw)
              |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1557189041],downsample)
              |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1557189041],downsample)
              |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1557189041],downsample)
              |---E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // Function with namespace regex selector.
      // NOTE: this can be optimized by pushing down the scalar operation.
      ("""sgn(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }) == 5""",
            """T~ScalarOperationMapper(operator=EQL, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~InstantVectorFunctionMapper(function=Sgn)
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-931342189],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-931342189],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-931342189],raw)
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-931342189],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-931342189],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-931342189],downsample)
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // scalar-wrapped vector
      ("""foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" } - scalar(sum(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }))""",
            """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=false)
              |-FA1~
              |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
              |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
              |---E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |----E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],raw)
              |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |-------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],raw)
              |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |-------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],raw)
              |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],downsample)
              |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |-------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],downsample)
              |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |-------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],downsample)
              |----E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],raw)
              |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],raw)
              |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],raw)
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-855048932],downsample)
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // vector-wrapped scalar
      ("""foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" } > vector(5)""",
            """E~BinaryJoinExec(binaryOp=GTR, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893185297],raw)
              |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893185297],raw)
              |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893185297],raw)
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893185297],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893185297],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893185297],downsample)
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |-T~VectorFunctionMapper(funcParams=List())
              |--E~ScalarFixedDoubleExec(params = RangeParams(1633913330,300,1634777330), value = 5.0) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // join multiple adjacent scalars
      // NOTE: this can be optimized by pushing down the scalar operation.
      ("""5 * 5 * foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }""",
            """T~ScalarOperationMapper(operator=MUL, scalarOnLhs=true)
              |-FA1~
              |-E~ScalarBinaryOperationExec(params = RangeParams(1633913330,300,1634777330), operator = MUL, lhs = Left(5.0), rhs = Left(5.0)) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#609935711],raw)
              |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#609935711],raw)
              |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#609935711],raw)
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#609935711],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#609935711],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#609935711],downsample)
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // join multiple non-adjacent scalars
      // NOTE: this can be optimized by pushing down the scalar operation.
      ("""5 * foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" } * 5""",
            """T~ScalarOperationMapper(operator=MUL, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~ScalarOperationMapper(operator=MUL, scalarOnLhs=true)
              |--FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-483218976],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-483218976],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-483218976],raw)
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-483218976],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-483218976],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-483218976],downsample)
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // sum(rate)
      ("""sum(rate(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[5m])) > 5""",
            """T~ScalarOperationMapper(operator=GTR, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
              |--E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-725608106],raw)
              |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
              |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-725608106],raw)
              |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |------T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
              |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-725608106],raw)
              |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-725608106],downsample)
              |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
              |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-725608106],downsample)
              |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
              |------T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
              |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-725608106],downsample)
              |---E~PromQlRemoteExec(PromQlQueryParams(sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[300s])),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // subquery
      ("""rate(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[1h:5m]) - 5""",
            """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=Some(3600000), functionId=Some(Rate), rawSource=false, offsetMs=None)
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1279558207],raw)
              |-----T~PeriodicSamplesMapper(start=1634172900000, step=300000, end=1634777100000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172600000,1634777100000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1279558207],raw)
              |-----T~PeriodicSamplesMapper(start=1634172900000, step=300000, end=1634777100000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172600000,1634777100000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1279558207],raw)
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1279558207],downsample)
              |-----T~PeriodicSamplesMapper(start=1633909800000, step=300000, end=1634172600000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633909500000,1634172600000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1279558207],downsample)
              |-----T~PeriodicSamplesMapper(start=1633909800000, step=300000, end=1634172600000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633909500000,1634172600000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1279558207],downsample)
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633909800,300,1634777100,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // X_over_time
      ("""5 / count_over_time(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[20m])""",
            """T~ScalarOperationMapper(operator=DIV, scalarOnLhs=true)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1005674756],raw)
              |----T~PeriodicSamplesMapper(start=1634174030000, step=300000, end=1634777330000, window=Some(1200000), functionId=Some(CountOverTime), rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1005674756],raw)
              |----T~PeriodicSamplesMapper(start=1634174030000, step=300000, end=1634777330000, window=Some(1200000), functionId=Some(CountOverTime), rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1005674756],raw)
              |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1005674756],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634173730000, window=Some(1200000), functionId=Some(CountOverTime), rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633912130000,1634173730000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1005674756],downsample)
              |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634173730000, window=Some(1200000), functionId=Some(CountOverTime), rawSource=true, offsetMs=None)
              |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633912130000,1634173730000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1005674756],downsample)
              |--E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[1200s]),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // topk -- shard key regex not supported
      // ("""5 <= topk(2, foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" })""",
      //       """""".stripMargin),
      // histogram_quantile
      ("""histogram_quantile(0.9, foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }) - 5""",
            """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~InstantVectorFunctionMapper(function=HistogramQuantile)
              |--FA1~StaticFuncArgs(0.9,RangeParams(1633913330,300,1634777330))
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1532463646],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1532463646],raw)
              |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1532463646],raw)
              |----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1532463646],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1532463646],downsample)
              |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
              |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1532463646],downsample)
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
    )
    for ((query, rootPlan) <- queryExpectedPairs) {
      val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
      val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
        plannerParams = PlannerParams(processMultiPartition = true)))
      validatePlan(execPlan, rootPlan)
    }
  }

  it("should pushdown root scalar operations into a RemoteExec's promql when query spans single remote partition") {
    val queryExpectedPairs = Seq(
      // Instant selector without namespace regex.
      ("""foo{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" } > 5""",
            """E~PromQlRemoteExec(PromQlQueryParams(foo{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1" } > 5,1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))"""),
      // Instant selector with namespace regex.
      ("""foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" } + 5""",
            // Should include the scalar in the promql, since it will only hit the remote partition.
            """E~PromQlRemoteExec(PromQlQueryParams((foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"} + 5.0),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // Join with namespace regex in both operands.
      ("""5 * (foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" } + foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" })""",
            """E~PromQlRemoteExec(PromQlQueryParams((5.0 * (foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"} + foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"})),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))"""),
      // Aggregate with namespace regex selector.
      ("""5 / sum(foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" })""",
            """E~PromQlRemoteExec(PromQlQueryParams((5.0 / sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"})),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))"""),
      // Function with namespace regex selector.
      ("""sgn(foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" }) == 5""",
            """E~PromQlRemoteExec(PromQlQueryParams((sgn(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}) == 5.0),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))"""),
      // scalar-wrapped vector
      ("""foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" } - scalar(sum(foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" }))""",
            """E~PromQlRemoteExec(PromQlQueryParams((foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"} - sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"})),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // vector-wrapped scalar
      ("""foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" } > vector(5)""",
            """E~PromQlRemoteExec(PromQlQueryParams((foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"} > vector(5.0)),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // join multiple adjacent scalars
      ("""5 * 5 * foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" }""",
            """E~PromQlRemoteExec(PromQlQueryParams(((5.0 * 5.0) * foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // join multiple non-adjacent scalars
      ("""5 * foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" } * 5""",
            """E~PromQlRemoteExec(PromQlQueryParams(((5.0 * foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}) * 5.0),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // sum(rate)
      ("""sum(rate(foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" }[5m])) > 5""",
            """E~PromQlRemoteExec(PromQlQueryParams((sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[300s])) > 5.0),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // subquery
      ("""rate(foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" }[1h:5m]) - 5""",
            """E~PromQlRemoteExec(PromQlQueryParams((rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[3600s:300s]) - 5.0),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // X_over_time
      ("""5 / count_over_time(foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" }[20m])""",
            """E~PromQlRemoteExec(PromQlQueryParams((5.0 / count_over_time(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[1200s])),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // topk
      ("""5 <= topk(2, foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" })""",
            """E~PromQlRemoteExec(PromQlQueryParams((5.0 <= topk(2.0,foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"})),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // histogram_quantile
      ("""histogram_quantile(0.9, foo{_ws_ = "demo", _ns_ =~ ".*remoteNs", instance = "Inst-1" }) - 5""",
            """E~PromQlRemoteExec(PromQlQueryParams((histogram_quantile(0.9,foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}) - 5.0),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
    )
    for ((query, otherPlan) <- queryExpectedPairs) {
      val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
      val execPlan = oneRemoteRootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
        plannerParams = PlannerParams(processMultiPartition = true)))
      validatePlan(execPlan, otherPlan)
    }
  }

  it("should not pushdown root scalar operations into a RemoteExec's promql when query spans multiple remote partition") {
    val queryExpectedPairs = Seq(
      // Instant selector with namespace regex.
      // NOTE: this can be optimized by pushing down the scalar operation.
      ("""foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" } + 5""",
            // Should include the scalar in the promql, since it will only hit the remote partition.
            """T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // Join with namespace regex in both operands.
      ("""5 * (foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" } + foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" })""",
            """T~ScalarOperationMapper(operator=MUL, scalarOnLhs=true)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // Aggregate with namespace regex selector.
      ("""5 / sum(foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" })""",
            """T~ScalarOperationMapper(operator=DIV, scalarOnLhs=true)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
              |--E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // Function with namespace regex selector.
      // NOTE: this can be optimized by pushing down the scalar operation.
      ("""sgn(foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" }) == 5""",
            """T~ScalarOperationMapper(operator=EQL, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~InstantVectorFunctionMapper(function=Sgn)
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // scalar-wrapped vector
      ("""foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" } - scalar(sum(foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" }))""",
            """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=false)
              |-FA1~
              |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
              |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
              |---E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |----E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |----E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // vector-wrapped scalar (sanity check-- not expected to treat this as a scalar operation)
      ("""foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" } > vector(5)""",
            """E~BinaryJoinExec(binaryOp=GTR, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |-T~VectorFunctionMapper(funcParams=List())
              |--E~ScalarFixedDoubleExec(params = RangeParams(1633913330,300,1634777330), value = 5.0) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // join multiple adjacent scalars
      // NOTE: this can be optimized by pushing down the scalar operation.
      ("""5 * 5 * foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" }""",
            """T~ScalarOperationMapper(operator=MUL, scalarOnLhs=true)
              |-FA1~
              |-E~ScalarBinaryOperationExec(params = RangeParams(1633913330,300,1634777330), operator = MUL, lhs = Left(5.0), rhs = Left(5.0)) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // join multiple non-adjacent scalars
      // NOTE: this can be optimized by pushing down the scalar operation.
      ("""5 * foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" } * 5""",
            """T~ScalarOperationMapper(operator=MUL, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~ScalarOperationMapper(operator=MUL, scalarOnLhs=true)
              |--FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // sum(rate)
      ("""sum(rate(foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" }[5m])) > 5""",
            """T~ScalarOperationMapper(operator=GTR, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
              |--E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"}[300s])),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"}[300s])),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // subquery
      ("""rate(foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" }[1h:5m]) - 5""",
            """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634777330000, window=Some(3600000), functionId=Some(Rate), rawSource=false, offsetMs=None)
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633909800,300,1634777100,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633909800,300,1634777100,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // X_over_time
      ("""5 / count_over_time(foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" }[20m])""",
            """T~ScalarOperationMapper(operator=DIV, scalarOnLhs=true)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"}[1200s]),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |--E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"}[1200s]),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // topk -- unsupported for shard key regex
      // ("""5 <= topk(2, foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" })""",
      //       """""".stripMargin),
      // histogram_quantile
      ("""histogram_quantile(0.9, foo{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1" }) - 5""",
            """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=false)
              |-FA1~StaticFuncArgs(5.0,RangeParams(1633913330,300,1634777330))
              |-T~InstantVectorFunctionMapper(function=HistogramQuantile)
              |--FA1~StaticFuncArgs(0.9,RangeParams(1633913330,300,1634777330))
              |--E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
              |---E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote1Partition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
    )
    for ((query, otherPlan) <- queryExpectedPairs) {
      val queryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
      val execPlan = twoRemoteRootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
        plannerParams = PlannerParams(processMultiPartition = true)))
      validatePlan(execPlan, otherPlan, sort = true)
    }
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
        |-E~PromQlRemoteExec(PromQlQueryParams(sum(foo:1m{_ws_="demo",_ns_="RemoteNs",instance="Inst-1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@20c812c8)
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
        |----E~PromQlRemoteExec(PromQlQueryParams(sum(foo{_ws_="demo",_ns_="RemoteNs",instance="Inst-1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@159424e2)
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
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5974b7e8)""".stripMargin

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
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633911000,300,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin

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
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633911000,300,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin

    validatePlan(execPlan, expected)
  }

  it("should generate plan for simple TopLevelSubquery spanning multiple partitions with namespace regex") {
    val query = """foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[10d:300s]"""
    val endSecs = 1634775000L
    val queryParams = PromQlQueryParams(query, endSecs, 0, endSecs)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSecs, 0, endSecs), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expected =
      """E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher
        |-E~StitchRvsExec() on InProcessPlanDispatcher
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~PeriodicSamplesMapper(start=1634172900000, step=300000, end=1634775000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172600000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~PeriodicSamplesMapper(start=1634172900000, step=300000, end=1634775000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172600000,1634775000000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |---T~PeriodicSamplesMapper(start=1633911000000, step=300000, end=1634172600000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633910700000,1634172600000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |---T~PeriodicSamplesMapper(start=1633911000000, step=300000, end=1634172600000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633910700000,1634172600000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |-E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1633911000,300,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin

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
        |--E~PromQlRemoteExec(PromQlQueryParams(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"},1634343000,300,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
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
        |-E~PromQlRemoteExec(PromQlQueryParams(sum_over_time(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[432000s]),1634775000,0,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
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
        |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s]),1634771400,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
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
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s])),1634771400,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
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
        |---E~PromQlRemoteExec(PromQlQueryParams(sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s])),1634771400,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
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
      """E~PromQlRemoteExec(PromQlQueryParams(max_over_time(sum(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s]))[3600s:60s]),1634775000,0,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher"""
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
        |--E~PromQlRemoteExec(PromQlQueryParams(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[600s]),1634771400,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
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
        |---E~PromQlRemoteExec(PromQlQueryParams(rate(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}[60s]),1634771100,60,1634775000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher""".stripMargin
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
        |---E~PromQlRemoteExec(PromQlQueryParams(sum(foo{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5eabff6b)
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
        |---E~PromQlRemoteExec(PromQlQueryParams(sum(bar{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5eabff6b)""".stripMargin

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

    val expectedPlan = "E~PromQlRemoteExec(PromQlQueryParams(bottomk(1.0,((min((foo{_ws_=\"demo\",_ns_=\"remoteNs\"} * on(userID) group_left(userName) bar{_ws_=\"demo\",_ns_=\"remoteNs\"})) by (userName,time) - time()) > 0.0)),1634777330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@570ba13)"
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

  it("should pushdown when leaf plans are split across partitions") {
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
                      |-E~PromQlRemoteExec(PromQlQueryParams(sum(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sum(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""count(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""group(rate(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(group(rate(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(group(rate(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""sum(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sum(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sum(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // instant
      Test("""sgn(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sgn(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(sgn(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""ln(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,00000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(ln(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(ln(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""exp(rate(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(exp(rate(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(exp(rate(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""floor(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(floor(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(floor(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // binary join
      Test("""test{job="app"} + test{job="app"}""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + test{job="app"},0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + test{job="app"},5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""test{job="app"} + (test{job="app"} + test{job="app"})""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + (test{job="app"} + test{job="app"}),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + (test{job="app"} + test{job="app"}),5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""count(test{job="app"}) + sum(test{job="app"})""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count(test{job="app"}) + sum(test{job="app"}),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count(test{job="app"}) + sum(test{job="app"}),5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""count_over_time(foo{job="app"}[15m]) unless rate(bar{job="app"}[5m])""",
        lookbackSec = 900,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app"}[15m]) unless rate(bar{job="app"}[5m]),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app"}[15m]) unless rate(bar{job="app"}[5m]),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""count_over_time(foo{job="app1"}[5m]) unless rate(bar{job="app1"}[15m:30s])""",
        lookbackSec = 900 + staleLookbackSec,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app1"}[5m]) unless rate(bar{job="app1"}[15m:30s]),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app1"}[5m]) unless rate(bar{job="app1"}[15m:30s]),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""rate(foo{job="app1"}[5m]) + (rate(bar{job="app1"}[20m]) + count_over_time(baz{job="app1"}[5m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m]) + (rate(bar{job="app1"}[20m]) + count_over_time(baz{job="app1"}[5m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m]) + (rate(bar{job="app1"}[20m]) + count_over_time(baz{job="app1"}[5m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""rate(foo{job="app1"}[5m:30s]) + (rate(bar{job="app1"}[20m:30s]) + count_over_time(baz{job="app1"}[5m:30s]))""",
        lookbackSec = 1200 + staleLookbackSec,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m:30s]) + (rate(bar{job="app1"}[20m:30s]) + count_over_time(baz{job="app1"}[5m:30s])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m:30s]) + (rate(bar{job="app1"}[20m:30s]) + count_over_time(baz{job="app1"}[5m:30s])),6501,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // scalar vector join
      Test("""test{job="app"} + 123""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + 123,0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + 123,5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""123 + sgn(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sgn(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sgn(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""123 + sum(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""123 + group(rate(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + group(rate(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + group(rate(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""123 + sum(count_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(count_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(count_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // absent
      Test("""absent(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""absent(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""absent(count_over_time(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(count_over_time(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(count_over_time(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""absent(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(absent(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      // scalar
      Test("""scalar(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""scalar(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""scalar(count_over_time(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(count_over_time(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(count_over_time(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
      Test("""scalar(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
                      |-E~PromQlRemoteExec(PromQlQueryParams(scalar(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin),
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
      // -E~PromQlRemoteExec(PromQlQueryParams(sgn(test{job="app"}) + 123,123,45,3306,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
      // -E~PromQlRemoteExec(PromQlQueryParams(sgn(test{job="app"}) + 123,3633,45,6789,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
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


  it ("should pushdown when leaf plans are split across partitions when one partition has gRPC QS") {
    val startSec = 0
    val stepSec = 3
    val endSec = 9999
    val splitSec = 5000
    val staleLookbackSec = WindowConstants.staleDataLookbackMillis / 1000
    val expectedUrls = Seq("remote0-url", "grpc-remote1-url.execStreaming")

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
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(sum(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(sum(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""count(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(count(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(count(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""group(rate(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(group(rate(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(group(rate(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""sum(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(sum(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(sum(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      // instant
      Test("""sgn(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(sgn(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(sgn(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""ln(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(ln(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(ln(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""exp(rate(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(exp(rate(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(exp(rate(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""floor(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(floor(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(floor(rate(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      // binary join
      Test("""test{job="app"} + test{job="app"}""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + test{job="app"},0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(test{job="app"} + test{job="app"},5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""test{job="app"} + (test{job="app"} + test{job="app"})""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + (test{job="app"} + test{job="app"}),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(test{job="app"} + (test{job="app"} + test{job="app"}),5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""count(test{job="app"}) + sum(test{job="app"})""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(count(test{job="app"}) + sum(test{job="app"}),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(count(test{job="app"}) + sum(test{job="app"}),5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""count_over_time(foo{job="app"}[15m]) unless rate(bar{job="app"}[5m])""",
        lookbackSec = 900,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app"}[15m]) unless rate(bar{job="app"}[5m]),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(count_over_time(foo{job="app"}[15m]) unless rate(bar{job="app"}[5m]),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""count_over_time(foo{job="app1"}[5m]) unless rate(bar{job="app1"}[15m:30s])""",
        lookbackSec = 900 + staleLookbackSec,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(count_over_time(foo{job="app1"}[5m]) unless rate(bar{job="app1"}[15m:30s]),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(count_over_time(foo{job="app1"}[5m]) unless rate(bar{job="app1"}[15m:30s]),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""rate(foo{job="app1"}[5m]) + (rate(bar{job="app1"}[20m]) + count_over_time(baz{job="app1"}[5m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m]) + (rate(bar{job="app1"}[20m]) + count_over_time(baz{job="app1"}[5m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m]) + (rate(bar{job="app1"}[20m]) + count_over_time(baz{job="app1"}[5m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),

      Test("""rate(foo{job="app1"}[5m:30s]) + (rate(bar{job="app1"}[20m:30s]) + count_over_time(baz{job="app1"}[5m:30s]))""",
        lookbackSec = 1200 + staleLookbackSec,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m:30s]) + (rate(bar{job="app1"}[20m:30s]) + count_over_time(baz{job="app1"}[5m:30s])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(rate(foo{job="app1"}[5m:30s]) + (rate(bar{job="app1"}[20m:30s]) + count_over_time(baz{job="app1"}[5m:30s])),6501,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),

      // scalar vector join
      Test("""test{job="app"} + 123""",
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"} + 123,0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(test{job="app"} + 123,5301,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""123 + sgn(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(123 + sgn(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(123 + sgn(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""123 + sum(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(123 + sum(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""123 + group(rate(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(123 + group(rate(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(123 + group(rate(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),

      Test("""123 + sum(count_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(123 + sum(count_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(123 + sum(count_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      // absent
      Test("""absent(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(absent(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(absent(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""absent(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(absent(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(absent(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""absent(count_over_time(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(absent(count_over_time(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(absent(count_over_time(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""absent(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(absent(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(absent(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      // scalar
      Test("""scalar(test{job="app"} offset 10m)""",
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(scalar(test{job="app"} offset 10m),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(scalar(test{job="app"} offset 10m),5901,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""scalar(rate(test{job="app"}[20m] offset 10m))""",
        lookbackSec = 1200,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(scalar(rate(test{job="app"}[20m] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(scalar(rate(test{job="app"}[20m] offset 10m)),6801,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),

      Test("""scalar(count_over_time(test{job="app"}[20m:30s] offset 10m))""",
        lookbackSec = 1200 + staleLookbackSec,
        offsetSec = 600,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(scalar(count_over_time(test{job="app"}[20m:30s] offset 10m)),0,3,5600,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(scalar(count_over_time(test{job="app"}[20m:30s] offset 10m)),7101,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
      Test("""scalar(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m]))""",
        lookbackSec = 1200,
        expected = """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQlRemoteExec(PromQlQueryParams(scalar(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),0,3,5000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote0-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
                     |-E~PromQLGrpcRemoteExec(PromQlQueryParams(scalar(sum_over_time(test{job="app"}[5m]) + rate(test{job="app"}[20m])),6201,3,9999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=grpc-remote1-url.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))""".stripMargin),
    )
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String],
                                 timeRange: TimeRange): List[PartitionAssignment] = {
        val splitMs = 1000 * splitSec
        List(PartitionAssignment("remote0", "remote0-url", TimeRange(timeRange.startMs, splitMs)),
          PartitionAssignment("remote1", "remote1-url", TimeRange(splitMs + 1, timeRange.endMs), Some("grpc-remote1-url")))
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
        val (params, url) = child match {
          case remote: PromQlRemoteExec     => (remote.promQlQueryParams, remote.queryEndpoint)
          case grpc: PromQLGrpcRemoteExec   => (grpc.promQlQueryParams, grpc.queryEndpoint)
          case _                            => fail("unsupported execplan")
        }
        // Each plan should dispatch the same query.
        params.promQl shouldEqual test.query
        (url, TimeStepParams(params.startSecs, params.stepSecs, params.endSecs))
      }.toSet shouldEqual expectedQueryParams
      // sanity check
      println("\n\n" + root.printTree())
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

  it("should handle multi partition topk correctly") {
    // Cases to handle
    // Case 1: Simple top k operation to local partition, no regex present, should be supported
    val lp1 =
    Parser.queryRangeToLogicalPlan(
      """topk(2, test{_ws_ = "demo", _ns_ = "localNs", instance = "Inst-1"})""",
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan1 = rootPlanner.materialize(lp1,  QueryContext(origQueryParams = queryParams))
    val expectedPlan1 =
    """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
       |-T~AggregatePresenter(aggrOp=TopK, aggrParams=List(2.0), rangeParams=RangeParams(1634173130,300,1634777330))
       |--E~LocalPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1340782891],raw)
       |---T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
       |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1340782891],raw)
       |---T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
       |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1340782891],raw)
       |-T~AggregatePresenter(aggrOp=TopK, aggrParams=List(2.0), rangeParams=RangeParams(1633913330,300,1634172830))
       |--E~LocalPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1340782891],downsample)
       |---T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
       |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1340782891],downsample)
       |---T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
       |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
       |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1340782891],downsample)""".stripMargin
        validatePlan(execPlan1, expectedPlan1)

    // Case 2: Simple top k operation to remote partition, no regex present, should be supported
    val query2 = """topk(2, test{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1"})"""
    val lp2 =
      Parser.queryRangeToLogicalPlan(query2, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan2 = rootPlanner.materialize(lp2,
      QueryContext(
      origQueryParams = PromQlQueryParams(query2, startSeconds, step, endSeconds),
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expectedPlan2 = """E~PromQlRemoteExec(PromQlQueryParams(topk(2, test{_ws_ = "demo", _ns_ = "RemoteNs", instance = "Inst-1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))"""
    validatePlan(execPlan2, expectedPlan2)

    // Case 3: top k with regex, the resolved regex should be in local and remote partitions and use PromQLRemoteExec, should fail
    // TODO


    // Case 4: top k with regex, the resolved regex should be in local and remote partitions and use PromQLGrpcRemoteExec, should be supported

    // Planner that routes to remote partition that supports gRPC based endpoint
    val gRpcRemotePartitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.contains("_ns_")) {
          if (routingKey("_ns_") == "localNs" || routingKey("_ns_") == "localNs1") {
            List(PartitionAssignment("localPartition", "localPartition-url", TimeRange(timeRange.startMs, timeRange.endMs)))
          } else {
            List(PartitionAssignment("remotePartition", "remotePartition-url",
              TimeRange(timeRange.startMs, timeRange.endMs), Some("remotePartition-grpcUrl")))
          }
        } else {
          List.empty
        }
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange:  TimeRange): List[PartitionAssignment] = ???
}
    val grpcRemoteMultiPartitionPlanner = new MultiPartitionPlanner(gRpcRemotePartitionLocationProvider, singlePartitionPlanner,
      "localPartition", dataset, queryConfig)
    val gRpcRemoteRootPlanner = new ShardKeyRegexPlanner(dataset, grpcRemoteMultiPartitionPlanner, shardKeyMatcherFn, gRpcRemotePartitionLocationProvider, queryConfig)

    val query4 = """topk(2, test{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1"})"""
    val lp4 = Parser.queryRangeToLogicalPlan(query4, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan4 = gRpcRemoteRootPlanner.materialize(lp4,
      QueryContext(
        origQueryParams = PromQlQueryParams(query4, startSeconds, step, endSeconds),
        plannerParams = PlannerParams(processMultiPartition = true)))

    val expectedPlan4 =
     """T~AggregatePresenter(aggrOp=TopK, aggrParams=List(2.0), rangeParams=RangeParams(1633913330,300,1634777330))
        |-E~MultiPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
        |--E~PromQLGrpcRemoteExec(PromQlQueryParams(topk(2.0,test{instance="Inst-1",_ws_="demo",_ns_="remoteNs"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-grpcUrl.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
        |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,false,false,true))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1737727940],raw)
        |----T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1737727940],raw)
        |----T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1737727940],raw)
        |---E~LocalPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1737727940],downsample)
        |----T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1737727940],downsample)
        |----T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(localNs)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1737727940],downsample)""".stripMargin

    validatePlan(execPlan4, expectedPlan4)

    // Case 5: top k with regex, the resolved regex should all be in local partition, should be supported
    val query5 = """topk(2, test{_ws_ = "demo", _ns_ =~ "localNs.*", instance = "Inst-1"})"""
    val lp5 =
      Parser.queryRangeToLogicalPlan(query5, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan5 = rootPlanner.materialize(lp5,
      QueryContext(
        origQueryParams = PromQlQueryParams(query5, startSeconds, step, endSeconds),
        plannerParams = PlannerParams(processMultiPartition = true)))

    val expectedPlan5 =
      """E~StitchRvsExec() on InProcessPlanDispatcher
        |-T~AggregatePresenter(aggrOp=TopK, aggrParams=List(2.0), rangeParams=RangeParams(1634173130,300,1634777330))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(localNs|localNs1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(localNs|localNs1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |-T~AggregatePresenter(aggrOp=TopK, aggrParams=List(2.0), rangeParams=RangeParams(1633913330,300,1634172830))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |---T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(localNs|localNs1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)
        |---T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(localNs|localNs1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],downsample)""".stripMargin

    validatePlan(execPlan5, expectedPlan5)
    // Case 6: top k with regex, the resolved regex should all be two remote partition, both  PromQLRemoteExec should fail
    val query6 = """topk(2, test{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1"})"""
    val lp6 =
      Parser.queryRangeToLogicalPlan(query6, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    intercept[UnsupportedOperationException] {
      twoRemoteRootPlanner.materialize(lp6,
        QueryContext(
          origQueryParams = PromQlQueryParams(query6, startSeconds, step, endSeconds),
          plannerParams = PlannerParams(processMultiPartition = true)))
    }

    val twoRemoteShardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      if (shardColumnFilters.nonEmpty) {
        // to ensure that tests dont call something else that is not configured
        require(shardColumnFilters.exists(f => f.column == "_ns_" && f.filter.isInstanceOf[EqualsRegex]
          && f.filter.asInstanceOf[EqualsRegex].pattern.toString == "remoteNs.*"))
        Seq(
          Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("remoteNs0"))),
          Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("remoteNs1"))),
        )
      } else {
        Nil
      }  // i.e. filters for a scalar
    }

    // Case 7: top k with regex, the resolved regex should all be two remote partition, one using PromQLRemoteExec and other PromQLGrpcRemoteExec, should fail
    // TODO
    // Case 8: top k with regex, the resolved regex should all be two remote partition, both use PromQLGrpcRemoteExec, should be supported
    val twoRemoteGrpcPartitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        routingKey("_ns_") match {
          case "remoteNs0" =>
            List(
              PartitionAssignment("remotePartition1", "remotePartition-url1",
              TimeRange(timeRange.startMs, timeRange.endMs), Some("remotePartition-grpc-url1"))
            )
          case _          =>
            List(PartitionAssignment("remotePartition2", "remotePartition-url2",
              TimeRange(timeRange.startMs, timeRange.endMs), Some("remotePartition-grpc-url2")))
        }
      }
      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                         timeRange: TimeRange): List[PartitionAssignment] = ???
    }

    val twoGrpcRemoteMpPlanner = new MultiPartitionPlanner(twoRemoteGrpcPartitionLocationProvider, singlePartitionPlanner,
      "localPartition", dataset, queryConfig)
    val twoGrpcRemoteShardKeyRegexPlanner = new ShardKeyRegexPlanner(dataset, twoGrpcRemoteMpPlanner, twoRemoteShardKeyMatcherFn, twoRemoteGrpcPartitionLocationProvider, queryConfig)

    val query8 = """topk(2, test{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1"})"""
    val lp8 =
      Parser.queryRangeToLogicalPlan(query8, TimeStepParams(startSeconds, step, endSeconds), Antlr)

    val execPlan8 = twoGrpcRemoteShardKeyRegexPlanner.materialize(lp8,
      QueryContext(
        origQueryParams = PromQlQueryParams(query8, startSeconds, step, endSeconds),
        plannerParams = PlannerParams(processMultiPartition = true)))

    val expectedPlan8 =
      """T~AggregatePresenter(aggrOp=TopK, aggrParams=List(2.0), rangeParams=RangeParams(1633913330,300,1634777330))
        |-E~MultiPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
        |--E~PromQLGrpcRemoteExec(PromQlQueryParams(topk(2.0,test{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-grpc-url2.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))
        |--E~PromQLGrpcRemoteExec(PromQlQueryParams(topk(2.0,test{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-grpc-url1.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true))""".stripMargin

    validatePlan(execPlan8, expectedPlan8)
    // Case 9: top k with regex, the resolved regex should all be one remote partition, using PromQLRemoteExec, should be supported BUT fails

    val singleRemotePartitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        routingKey("_ns_") match {
          case _ =>
            List(PartitionAssignment("remotePartition", "remotePartition-url0",
              TimeRange(timeRange.startMs, timeRange.endMs)))
        }
      }
      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                         timeRange: TimeRange): List[PartitionAssignment] = ???
    }

    val oneRemoteMpPlanner = new MultiPartitionPlanner(singleRemotePartitionLocationProvider, singlePartitionPlanner,
      "localPartition", dataset, queryConfig)
    val oneRemoteShardKeyRegexPlanner = new ShardKeyRegexPlanner(dataset, oneRemoteMpPlanner, twoRemoteShardKeyMatcherFn, singleRemotePartitionLocationProvider, queryConfig)
    val query9 = """topk(2, test{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1"})"""
    val lp9 =
      Parser.queryRangeToLogicalPlan(query9, TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan9 = oneRemoteShardKeyRegexPlanner.materialize(lp9,
        QueryContext(
          origQueryParams = PromQlQueryParams(query9, startSeconds, step, endSeconds),
          plannerParams = PlannerParams(processMultiPartition = true)))
    val expectedPlan9 = """E~PromQlRemoteExec(PromQlQueryParams(topk(2.0,test{instance="Inst-1",_ws_="demo",_ns_=~"remoteNs0|remoteNs1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remotePartition-url0, requestTimeoutMs=10000) on InProcessPlanDispatcher"""
    validatePlan(execPlan9, expectedPlan9)

    // Case 10: MPP configured not to use grpc but partition assignment has grpc endpoint configured

    val twoGrpcRemoteMpPlannerNoGrpc = new MultiPartitionPlanner(twoRemoteGrpcPartitionLocationProvider, singlePartitionPlanner,
      "localPartition", dataset,
      queryConfig.copy(grpcPartitionsDenyList = Set("remotepartition1")))
    val twoGrpcRemoteShardKeyRegexPlannerNoGrpc =
      new ShardKeyRegexPlanner(dataset, twoGrpcRemoteMpPlannerNoGrpc, twoRemoteShardKeyMatcherFn, twoRemoteGrpcPartitionLocationProvider, queryConfig)

    val query10 = """sum(test{_ws_ = "demo", _ns_ =~ "remoteNs.*", instance = "Inst-1"})"""
    val lp10 =
      Parser.queryRangeToLogicalPlan(query10, TimeStepParams(startSeconds, step, endSeconds), Antlr)

    val execPlan10 = twoGrpcRemoteShardKeyRegexPlannerNoGrpc.materialize(lp10,
      QueryContext(
        origQueryParams = PromQlQueryParams(query8, startSeconds, step, endSeconds),
        plannerParams = PlannerParams(processMultiPartition = true)))

    // remoteNs0 is configured to use gRPC in PartitionAssignment but explicitly denied to use gRPC Remote exec from
    // config. Thus we see it falls back to using PromQlRemoteExec

    val expectedPlan10 =
      """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
        |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set()))
        |--E~PromQLGrpcRemoteExec(PromQlQueryParams(sum(test{instance="Inst-1",_ws_="demo",_ns_="remoteNs1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-grpc-url2.execStreaming, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set(remotepartition1)))
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(test{instance="Inst-1",_ws_="demo",_ns_="remoteNs0"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remotePartition-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(10000),None,true,false,true,Set(remotepartition1)))""".stripMargin

    validatePlan(execPlan10, expectedPlan10)
  }

  it ("should correctly pushdown shard-key-regex queries when a target-schema exists") {

    val LOCAL = "local.*"
    val ONE_REMOTE = "oneRemote.*"
    val TWO_REMOTE = "twoRemote.*"
    val BOTH = "both.*"
    val TSCHEMA_LABEL = "tschemaLabel"
    val timeParams = TimeStepParams(startSeconds, step, endSeconds)
    val dataset = MetricsTestData.timeseriesDatasetMultipleShardKeys

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        val ns = routingKey("_ns_")
        if (ns.contains("local")) {
          List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
        } else if (ns.contains("oneRemote") || ns.equals("remote")) {
          List(PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs, timeRange.endMs)))
        } else if (ns.contains("twoRemote")) {
          val part = ns.drop("twoRemote".size)
          List(PartitionAssignment(part, part + "-url", TimeRange(timeRange.startMs, timeRange.endMs)))
        } else {
          throw new IllegalArgumentException("unexpected _ns_: " + ns)
        }
      }
      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                         timeRange: TimeRange): List[PartitionAssignment] = ???
    }

    case class Test(query: String, expected: String, tschemaEnabled: Set[String] = Set())
    val tests = Seq(
            // no namespaces with target-schemas enabled
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set(),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
                         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#448366741],raw)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#448366741],raw)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#448366741],raw)
                         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
                         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#448366741],downsample)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#448366741],downsample)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#448366741],downsample)""".stripMargin),
            // one namespace has a target-schema assigned
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("local1"),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
                         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1875351417],raw)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1875351417],raw)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1875351417],raw)
                         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
                         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1875351417],downsample)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1875351417],downsample)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1875351417],downsample)""".stripMargin),
      // both namespaces are assigned target-schemas; should concat results
      Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
        tschemaEnabled=Set("local1", "local2"),
        expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                   |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#271026309],raw)
                   |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
                   |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#271026309],raw)
                   |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                   |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                   |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                   |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
                   |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#271026309],raw)
                   |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                   |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                   |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                   |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#271026309],downsample)
                   |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
                   |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#271026309],downsample)
                   |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                   |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                   |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                   |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
                   |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#271026309],downsample)
                   |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                   |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                   |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remove "by" clause; should not concatenate
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"})""",
              tschemaEnabled=Set("local1", "local2"),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
                         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1229126031],raw)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1229126031],raw)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1229126031],raw)
                         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
                         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1229126031],downsample)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1229126031],downsample)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1229126031],downsample)""".stripMargin),
            // "by" clause is missing a target-schema label
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"}) by (notATargetSchemaLabel)""",
              tschemaEnabled=Set("local1", "local2"),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
                         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-122847774],raw)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(notATargetSchemaLabel))
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-122847774],raw)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(notATargetSchemaLabel))
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-122847774],raw)
                         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
                         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-122847774],downsample)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(notATargetSchemaLabel))
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-122847774],downsample)
                         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(notATargetSchemaLabel))
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-122847774],downsample)""".stripMargin),
            // remote partition, no target-schemas assigned
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set(),
              expected="""E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote partition, one ns has a target-schema assigned
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("oneRemote1"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote partition, both ns have a target-schema assigned
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // two remote partitions, both ns have a target-schema assigned
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$TWO_REMOTE",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("twoRemote1", "twoRemote2"),
              expected="""E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_="twoRemote2"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=2-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_="twoRemote1"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // two remote partitions, one ns has a target-schema assigned
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$TWO_REMOTE",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("twoRemote1"),
              expected="""T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
                         |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_="twoRemote2"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=2-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_="twoRemote1"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote partition, both ns have a target-schema assigned, but "by" clause is missing
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"})""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote/local partitions, no ns has a tschema assigned
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$BOTH",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set(),
              expected="""T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
                         |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074189990],raw)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074189990],raw)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074189990],raw)
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074189990],downsample)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074189990],downsample)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074189990],downsample)
                         |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_="remote"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote/local partitions, local ns has a tschema assigned
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$BOTH",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("local"),
              expected="""T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
                         |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-970102164],raw)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-970102164],downsample)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_="remote"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote/local partitions, remote ns has a tschema assigned
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$BOTH",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("remote"),
              expected="""T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634777330))
                         |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-86517651],raw)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-86517651],raw)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-86517651],raw)
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-86517651],downsample)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-86517651],downsample)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-86517651],downsample)
                         |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{tschemaLabel="bar",_ws_="demo",_ns_="remote"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema, but tschema label missing from selector
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams(sum(foo{_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema, but tschema label missing from selector and "by" clause
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE"}) by (notALabel)""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams(sum(foo{_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}) by (notALabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // local partition, and all ns are assigned a tschema, but tschema label missing from selector
            Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$LOCAL"}) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("local1", "local2"),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2145452488],raw)
                         |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2145452488],raw)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634173130,300,1634777330))
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2145452488],raw)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2145452488],downsample)
                         |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2145452488],downsample)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172830))
                         |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2145452488],downsample)
                         |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(tschemaLabel))
                         |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema; nested aggregation with all target-schema labels present in each "by" clause
            Test(query=s"""sum(sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE"}) by ($TSCHEMA_LABEL)) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams(sum(sum(foo{_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}) by (tschemaLabel)) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema; nested aggregation with target-schema label missing from outer "by" clause
            Test(query=s"""sum(sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE"}) by ($TSCHEMA_LABEL))""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams(sum(sum(foo{_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}) by (tschemaLabel)),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema; nested aggregation with target-schema label missing from inner "by" clause
            Test(query=s"""sum(sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE"})) by ($TSCHEMA_LABEL)""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams(sum(sum(foo{_ws_="demo",_ns_=~"oneRemote1|oneRemote2"})) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),

            // ------- begin binary-join tests --------

            // no namespaces with target-schemas enabled
            Test(query=s"""foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set(),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],raw)
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-505514305],downsample)""".stripMargin),
            // one namespace has a target-schema assigned
            Test(query=s"""foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("local1"),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],raw)
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1475089001],downsample)""".stripMargin),
            // both namespaces are assigned target-schemas; should concat results
            Test(query=s"""foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("local1", "local2"),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#56172550],raw)
                         |--E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#56172550],raw)
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#56172550],raw)
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#56172550],downsample)
                         |--E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#56172550],downsample)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#56172550],downsample)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remove "on" clause; should not concatenate
            Test(query=s"""foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"} + baz{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("local1", "local2"),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],raw)
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-362906598],downsample)""".stripMargin),
            // "on" clause is missing a target-schema label
            Test(query=s"""foo{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bar"} + on (notATargetSchemaLabel) baz{_ws_="demo",_ns_=~"$LOCAL",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("local1", "local2"),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(notATargetSchemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],raw)
                         |--T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],raw)
                         |-E~BinaryJoinExec(binaryOp=ADD, on=List(notATargetSchemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],downsample)
                         |--T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-65785618],downsample)""".stripMargin),
            // remote partition, no target-schemas assigned
            Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set(),
              expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(tschemaLabel) baz{tschemaLabel="bat",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote partition, one ns has a target-schema assigned
            Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("oneRemote1"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(tschemaLabel) baz{tschemaLabel="bat",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote partition, both ns have a target-schema assigned
            Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(tschemaLabel) baz{tschemaLabel="bat",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // two remote partitions, both ns have a target-schema assigned
            Test(query=s"""foo{_ws_="demo",_ns_=~"$TWO_REMOTE",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$TWO_REMOTE",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("twoRemote1", "twoRemote2"),
              expected="""E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_="twoRemote2"} + on(tschemaLabel) baz{tschemaLabel="bat",_ws_="demo",_ns_="twoRemote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=2-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_="twoRemote1"} + on(tschemaLabel) baz{tschemaLabel="bat",_ws_="demo",_ns_="twoRemote1"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=1-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote partition, both ns have a target-schema assigned, but "on" clause is missing
            Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} + baz{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + baz{tschemaLabel="bat",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote/local partitions, no ns has a tschema assigned
            Test(query=s"""foo{_ws_="demo",_ns_=~"$BOTH",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$BOTH",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set(),
              expected="""E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],raw)
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],raw)
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],raw)
                         |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],downsample)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],downsample)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],downsample)
                         |--E~PromQlRemoteExec(PromQlQueryParams(foo{tschemaLabel="bar",_ws_="demo",_ns_="remote"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],raw)
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],raw)
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],raw)
                         |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],downsample)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],downsample)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1390409631],downsample)
                         |--E~PromQlRemoteExec(PromQlQueryParams(baz{tschemaLabel="bat",_ws_="demo",_ns_="remote"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote/local partitions, local ns has a tschema assigned
            Test(query=s"""foo{_ws_="demo",_ns_=~"$BOTH",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$BOTH",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("local"),
              expected="""E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1840938912],raw)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1840938912],downsample)
                         |--E~PromQlRemoteExec(PromQlQueryParams(foo{tschemaLabel="bar",_ws_="demo",_ns_="remote"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1840938912],raw)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1840938912],downsample)
                         |--E~PromQlRemoteExec(PromQlQueryParams(baz{tschemaLabel="bat",_ws_="demo",_ns_="remote"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // remote/local partitions, remote ns has a tschema assigned
            Test(query=s"""foo{_ws_="demo",_ns_=~"$BOTH",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$BOTH",$TSCHEMA_LABEL="bat"}""",
              tschemaEnabled=Set("remote"),
              expected="""E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],raw)
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],raw)
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],raw)
                         |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],downsample)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],downsample)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bar)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],downsample)
                         |--E~PromQlRemoteExec(PromQlQueryParams(foo{tschemaLabel="bar",_ws_="demo",_ns_="remote"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],raw)
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],raw)
                         |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],raw)
                         |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],downsample)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],downsample)
                         |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(tschemaLabel,Equals(bat)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(local)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-110345099],downsample)
                         |--E~PromQlRemoteExec(PromQlQueryParams(baz{tschemaLabel="bat",_ws_="demo",_ns_="remote"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema, but tschema label missing from selector
            Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$ONE_REMOTE"}""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(tschemaLabel) baz{_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema, but tschema label missing from selector and "by" clause
            Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE"} + on (notATargetSchemaLabel) baz{_ws_="demo",_ns_=~"$ONE_REMOTE"}""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(notATargetSchemaLabel) baz{_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // local partition, and all ns are assigned a tschema, but tschema label missing from selector
            Test(query=s"""foo{_ws_="demo",_ns_=~"$LOCAL"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$LOCAL"}""",
              tschemaEnabled=Set("local1", "local2"),
              expected="""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
                         |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#170274853],raw)
                         |--E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#170274853],raw)
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#170274853],raw)
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#170274853],downsample)
                         |--E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#170274853],downsample)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |--E~BinaryJoinExec(binaryOp=ADD, on=List(tschemaLabel), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#170274853],downsample)
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                         |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
                         |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema; nested join
            Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) (baz{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} - on($TSCHEMA_LABEL) bak{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"})""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(tschemaLabel) (baz{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} - on(tschemaLabel) bak{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"})),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema; nested aggregation with target-schema label missing from outer "by" clause
            Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} + on (notATargetSchemaLabel) (baz{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} - on($TSCHEMA_LABEL) bak{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"})""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(notATargetSchemaLabel) (baz{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} - on(tschemaLabel) bak{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"})),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
            // one remote partition, and all ns are assigned a tschema; nested aggregation with target-schema label missing from inner "by" clause
            Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) (baz{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} - on (notATargetSchemaLabel) bak{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"})""",
              tschemaEnabled=Set("oneRemote1", "oneRemote2"),
              expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(tschemaLabel) (baz{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} - on(notATargetSchemaLabel) bak{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"})),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),

      // ----------------- begin compound agg/join tests ---------------------

      // one remote partition, and all ns are assigned a tschema; nested join
      Test(query=s"""sum(foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) baz{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
        tschemaEnabled=Set("oneRemote1", "oneRemote2"),
        expected="""E~PromQlRemoteExec(PromQlQueryParams(sum((foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(tschemaLabel) baz{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"})) by (tschemaLabel),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),

      // one remote partition, and all ns are assigned a tschema; nested aggregation
      Test(query=s"""foo{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"} + on ($TSCHEMA_LABEL) sum(baz{_ws_="demo",_ns_=~"$ONE_REMOTE",$TSCHEMA_LABEL="bar"}) by ($TSCHEMA_LABEL)""",
        tschemaEnabled=Set("oneRemote1", "oneRemote2"),
        expected="""E~PromQlRemoteExec(PromQlQueryParams((foo{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"} + on(tschemaLabel) sum(baz{tschemaLabel="bar",_ws_="demo",_ns_=~"oneRemote1|oneRemote2"}) by (tschemaLabel)),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin)
    )
    for (test <- tests) {
      def tschemaProviderFunc(filters: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
        val ns = filters.find(f => f.column == "_ns_").get.filter.valuesStrings.toList.head.asInstanceOf[String]
        if (test.tschemaEnabled.contains(ns)) {
          Seq(TargetSchemaChange(0, Seq("_ws_", "_ns_", TSCHEMA_LABEL)))
        } else {
          Nil
        }
      }
      def shardKeyMatcherFunc(shardColumnFilters: Seq[ColumnFilter]): Seq[Seq[ColumnFilter]] = {
        if (shardColumnFilters.isEmpty) {
          return Nil
        }
        val nsColValue = shardColumnFilters.find(_.column == "_ns_").get.filter.asInstanceOf[EqualsRegex].pattern.toString
        nsColValue match {
          case LOCAL =>
            Seq(
              Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("local1"))),
              Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("local2")))
            )
          case ONE_REMOTE =>
            Seq(
              Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("oneRemote1"))),
              Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("oneRemote2")))
            )
          case BOTH =>
            Seq(
              Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("local"))),
              Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("remote")))
            )
          case TWO_REMOTE =>
            Seq(
              Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("twoRemote1"))),
              Seq(ColumnFilter("_ws_", Equals("demo")), ColumnFilter("_ns_", Equals("twoRemote2")))
            )
        }
      }

      val spp = getPlanners(2, dataset).spp
      val multiPartitionPlanner = new MultiPartitionPlanner(partitionLocationProvider, spp, "local", dataset, queryConfig)
      val shardKeyRegexPlanner = new ShardKeyRegexPlanner(dataset, multiPartitionPlanner, shardKeyMatcherFunc, partitionLocationProvider, queryConfig, targetSchemaProvider)

      val tschema = FunctionalTargetSchemaProvider(tschemaProviderFunc)

      val lp = Parser.queryRangeToLogicalPlan(test.query, timeParams)
      val context = QueryContext(
        origQueryParams = PromQlQueryParams(test.query, timeParams.start, timeParams.step, timeParams.end),
        plannerParams = PlannerParams(processMultiPartition = true, targetSchemaProviderOverride = Some(tschema)))
      val ep = shardKeyRegexPlanner.materialize(lp, context)
      try {
        validatePlan(ep, test.expected, sort = true)
      } catch {
        case _: Throwable => {
          Thread.sleep(1000)
          println("AMT =========")
          println(test.tschemaEnabled)
          println(test.query)
          println(ep.printTree())
          println("AMT =========")
          System.exit(4)
        }
      }
    }
  }

  it("should generate correct plan for aggregations/joins without selectors") {
    val queryExpectedPairs = Seq(
      // FIXME: this first plan is needlessly complex (but will return the correct result)
      ("""sum(vector(123)) by(foo)""",
            """E~StitchRvsExec() on InProcessPlanDispatcher
              |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634172830,300,1634777330))
              |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher
              |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(foo))
              |----T~VectorFunctionMapper(funcParams=List())
              |-----E~ScalarFixedDoubleExec(params = RangeParams(1634172830,300,1634777330), value = 123.0) on InProcessPlanDispatcher
              |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1633913330,300,1634172530))
              |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher
              |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(foo))
              |----T~VectorFunctionMapper(funcParams=List())
              |-----E~ScalarFixedDoubleExec(params = RangeParams(1633913330,300,1634172530), value = 123.0) on InProcessPlanDispatcher""".stripMargin),
      ("""vector(123) + on(foo) vector(123)""",
            """E~BinaryJoinExec(binaryOp=ADD, on=List(foo), ignoring=List()) on InProcessPlanDispatcher
              |-T~VectorFunctionMapper(funcParams=List())
              |--E~ScalarFixedDoubleExec(params = RangeParams(1633913330,300,1634777330), value = 123.0) on InProcessPlanDispatcher
              |-T~VectorFunctionMapper(funcParams=List())
              |--E~ScalarFixedDoubleExec(params = RangeParams(1633913330,300,1634777330), value = 123.0) on InProcessPlanDispatcher""".stripMargin),
    )
    val timeParams = TimeStepParams(startSeconds, step, endSeconds)
    for ((query, expected) <- queryExpectedPairs) {
      val lp = Parser.queryRangeToLogicalPlan(query, timeParams, Antlr)
      val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = queryParams))
      validatePlan(execPlan, expected)
    }
  }

  it ("should create one remote plan per remote partition for ns-regex queries") {
    val dataset = MetricsTestData.timeseriesDatasetMultipleShardKeys
    val shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]] = filters => Seq(
      // Selectors will always match four keys.
      // Note: _ws_ label is interpreted below as the partition name.
      Seq(ColumnFilter("_ws_", Filter.Equals("local")), ColumnFilter("_ns_", Filter.Equals("local1"))),
      Seq(ColumnFilter("_ws_", Filter.Equals("local")), ColumnFilter("_ns_", Filter.Equals("local2"))),
      Seq(ColumnFilter("_ws_", Filter.Equals("remote")), ColumnFilter("_ns_", Filter.Equals("remote1"))),
      Seq(ColumnFilter("_ws_", Filter.Equals("remote")), ColumnFilter("_ns_", Filter.Equals("remote2"))),
    )
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        // using the _ws_ label as the partition name
        List(PartitionAssignment(routingKey("_ws_"), "dummy-endpoint", timeRange))
      }
      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        getPartitions(nonMetricShardKeyFilters.map(filter => (filter.column, filter.filter.valuesStrings.head.toString)).toMap, timeRange)
    }

    val mppPlanner = new MultiPartitionPlanner(partitionLocationProvider, singlePartitionPlanner, "local", dataset, queryConfig)
    val planner = new ShardKeyRegexPlanner(dataset, mppPlanner, shardKeyMatcher, partitionLocationProvider, queryConfig)

    val timeParams = TimeStepParams(startSeconds, step, endSeconds)
    val qContext = QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(processMultiPartition = true))

    // Each should materialize plans for two partitions, and _ns_ labels should be filtered by EqualsRegex with pipe-separated values.
    val queryExpectedPairs = Seq(
      ("""foo{_ws_="dummy", _ns_=~"dummy.*", bar="hello"}""",
        """E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |-E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
          |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-187520432],raw)
          |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-187520432],raw)
          |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-187520432],raw)
          |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-187520432],downsample)
          |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-187520432],downsample)
          |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-187520432],downsample)
          |-E~PromQlRemoteExec(PromQlQueryParams(foo{bar="hello",_ws_="remote",_ns_=~"remote1|remote2"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=dummy-endpoint, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
      ("""sum(foo{_ws_="dummy", _ns_=~"dummy.*", bar="hello"})""",
          """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(100,1,1000))
            |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
            |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
            |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#173492473],raw)
            |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
            |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
            |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#173492473],raw)
            |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
            |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
            |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#173492473],raw)
            |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#173492473],downsample)
            |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
            |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
            |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#173492473],downsample)
            |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
            |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
            |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#173492473],downsample)
            |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{bar="hello",_ws_="remote",_ns_=~"remote1|remote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=dummy-endpoint, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
      ("""foo{_ws_="dummy", _ns_=~"dummy.*", bar="hello"} + foo{_ws_="dummy", _ns_=~"dummy.*", bar="goodbye"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],raw)
          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],raw)
          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],raw)
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],downsample)
          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],downsample)
          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],downsample)
          |--E~PromQlRemoteExec(PromQlQueryParams(foo{bar="hello",_ws_="remote",_ns_=~"remote1|remote2"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=dummy-endpoint, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],raw)
          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(goodbye)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],raw)
          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(goodbye)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],raw)
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],downsample)
          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(goodbye)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],downsample)
          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(goodbye)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1082498153],downsample)
          |--E~PromQlRemoteExec(PromQlQueryParams(foo{bar="goodbye",_ws_="remote",_ns_=~"remote1|remote2"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=dummy-endpoint, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
    )
    for ((query, expected) <- queryExpectedPairs) {
      val lp = Parser.queryRangeToLogicalPlan(query, timeParams)
      val exec = planner.materialize(lp, qContext)
      validatePlan(exec, expected)
    }
  }

  it ("should create one remote plan per remote partition for ns-regex queries with target-schemas") {
    val dataset = MetricsTestData.timeseriesDatasetMultipleShardKeys
    val shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]] = filters => Seq(
      // Selectors will always match four keys.
      // Note: _ws_ label is interpreted below as the partition name.
      Seq(ColumnFilter("_ws_", Filter.Equals("local")), ColumnFilter("_ns_", Filter.Equals("local1"))),
      Seq(ColumnFilter("_ws_", Filter.Equals("local")), ColumnFilter("_ns_", Filter.Equals("local2"))),
      Seq(ColumnFilter("_ws_", Filter.Equals("remote")), ColumnFilter("_ns_", Filter.Equals("remote1"))),
      Seq(ColumnFilter("_ws_", Filter.Equals("remote")), ColumnFilter("_ns_", Filter.Equals("remote2"))),
    )
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        // using the _ws_ label as the partition name
        List(PartitionAssignment(routingKey("_ws_"), "dummy-endpoint", timeRange))
      }
      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        getPartitions(nonMetricShardKeyFilters.map(filter => (filter.column, filter.filter.valuesStrings.head.toString)).toMap, timeRange)
    }
    val tschemaProvider = FunctionalTargetSchemaProvider(cf => Seq(TargetSchemaChange(0L, Seq("_ws_", "_ns_", "bar"))))

    // Since two _ns_ labels per partition and tschemas are enabled, we should expect the result plan to contain leaves for at most two shards.
    val nShards = 4
    val spreadProv = StaticSpreadProvider(SpreadChange(0L, Integer.numberOfTrailingZeros(nShards)))

    val singlePartitionPlanner = getPlanners(nShards, dataset).spp
    val mppPlanner = new MultiPartitionPlanner(partitionLocationProvider, singlePartitionPlanner, "local", dataset, queryConfig)
    val planner = new ShardKeyRegexPlanner(dataset, mppPlanner, shardKeyMatcher, partitionLocationProvider, queryConfig)

    val timeParams = TimeStepParams(startSeconds, step, endSeconds)
    val qContext = QueryContext(origQueryParams = queryParams,
      plannerParams = PlannerParams(
        processMultiPartition = true,
        targetSchemaProviderOverride = Some(tschemaProvider),
        spreadOverride = Some(spreadProv)))

    // Each should materialize plans for two partitions, and _ns_ labels should be filtered by EqualsRegex with pipe-separated values.
    // Leaves should be materialized for at most two shards per selector.
    val queryExpectedPairs = Seq(
      ("""foo{_ws_="dummy", _ns_=~"dummy.*", bar="hello"}""",
        """E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |-E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
          |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1677128202],raw)
          |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=2, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1677128202],raw)
          |---T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1677128202],raw)
          |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1677128202],downsample)
          |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=2, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1677128202],downsample)
          |---T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1677128202],downsample)
          |-E~PromQlRemoteExec(PromQlQueryParams(foo{bar="hello",_ws_="remote",_ns_=~"remote1|remote2"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,Some(StaticSpreadProvider(SpreadChange(0,2))),None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=dummy-endpoint, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
      ("""sum(foo{_ws_="dummy", _ns_=~"dummy.*", bar="hello"})""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(100,1,1000))
          |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893451961],raw)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=2, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893451961],raw)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |-----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893451961],raw)
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893451961],downsample)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=2, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893451961],downsample)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |-----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1893451961],downsample)
          |--E~PromQlRemoteExec(PromQlQueryParams(sum(foo{bar="hello",_ws_="remote",_ns_=~"remote1|remote2"}),1633913330,300,1634777330,None,false), PlannerParams(filodb,None,Some(StaticSpreadProvider(SpreadChange(0,2))),None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=dummy-endpoint, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
      ("""foo{_ws_="dummy", _ns_=~"dummy.*", bar="hello"} + foo{_ws_="dummy", _ns_=~"dummy.*", bar="goodbye"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],raw)
          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=2, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],raw)
          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],raw)
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],downsample)
          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=2, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],downsample)
          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(hello)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],downsample)
          |--E~PromQlRemoteExec(PromQlQueryParams(foo{bar="hello",_ws_="remote",_ns_=~"remote1|remote2"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,Some(StaticSpreadProvider(SpreadChange(0,2))),None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=dummy-endpoint, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
          |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,100,false,false,true,Set(),None,Map(filodb-query-exec-aggregate-large-container -> 65536, filodb-query-exec-metadataexec -> 8192)))
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],raw)
          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(goodbye)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],raw)
          |----T~PeriodicSamplesMapper(start=1634173130000, step=300000, end=1634777330000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634172830000,1634777330000), filters=List(ColumnFilter(bar,Equals(goodbye)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],raw)
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],downsample)
          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(goodbye)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],downsample)
          |----T~PeriodicSamplesMapper(start=1633913330000, step=300000, end=1634172830000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1633913030000,1634172830000), filters=List(ColumnFilter(bar,Equals(goodbye)), ColumnFilter(_ws_,Equals(local)), ColumnFilter(_ns_,EqualsRegex(local1|local2)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1460713003],downsample)
          |--E~PromQlRemoteExec(PromQlQueryParams(foo{bar="goodbye",_ws_="remote",_ns_=~"remote1|remote2"},1633913330,300,1634777330,None,false), PlannerParams(filodb,None,Some(StaticSpreadProvider(SpreadChange(0,2))),None,Some(FunctionalTargetSchemaProvider(~)),60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false), queryEndpoint=dummy-endpoint, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))""".stripMargin),
    )
    for ((query, expected) <- queryExpectedPairs) {
      val lp = Parser.queryRangeToLogicalPlan(query, timeParams)
      val exec = planner.materialize(lp, qContext)
      println(exec.printTree())
      validatePlan(exec, expected)
    }
  }
}
