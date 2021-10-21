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
import filodb.core.query.Filter.Equals
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.prometheus.parse.Parser.Antlr
import filodb.query.exec._

class PlannerHierarchySpec extends AnyFunSpec with Matchers {
  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val mapper = new ShardMapper(2)
  for {i <- 0 until 2} mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  private val dataset = MetricsTestData.timeseriesDatasetWithMetric
  private val schemas = Schemas(dataset.schema)

  private val routingConfigString = "routing {\n  remote {\n    http {\n      timeout = 10000\n    }\n  }\n}"
  private val routingConfig = ConfigFactory.parseString(routingConfigString)

  private val config = ConfigFactory.load("application_test.conf")
    .getConfig("filodb.query").withFallback(routingConfig)
  private val queryConfig = new QueryConfig(config)

  private val now = System.currentTimeMillis()

  private val rawRetention = 7.days.toMillis
  val rawPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
    earliestRetainedTimestampFn = now - rawRetention, queryConfig, "raw")

  private val downsampleRetention = 30.days.toMillis
  val downsamplePlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
    earliestRetainedTimestampFn = now - downsampleRetention, queryConfig, "downsample")

  private def inProcessDispatcher = InProcessPlanDispatcher(EmptyQueryConfig)

  private val timeToDownsample = 6.hours.toMillis
  private val longTermPlanner = new LongTimeRangePlanner(rawPlanner, downsamplePlanner,
    earliestRawTimestampFn = now - rawRetention, latestDownsampleTimestampFn = now - timeToDownsample,
    inProcessDispatcher, queryConfig, dataset)

  private val rrRetention = 30.days.toMillis
  val recordingRulesPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef,
    earliestRetainedTimestampFn = now - rrRetention,
    queryConfig, "recordingRules")

  val plannerSelector = (metricName: String) => {
    if (metricName.contains(":1m")) "recordingRules" else "longTerm"
  }
  val planners = Map("longTerm" -> longTermPlanner, "recordingRules" -> recordingRulesPlanner)
  val singlePartitionPlanner = new SinglePartitionPlanner(planners, plannerSelector, "_metric_", queryConfig)

  val partitionLocationProvider = new PartitionLocationProvider {
    override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("localPartition", "localPartition-url", TimeRange(timeRange.startMs, timeRange.endMs)))

    override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("localPartition", "localPartition-url", TimeRange(timeRange.startMs, timeRange.endMs)))
  }
  val multiPartitionPlanner = new MultiPartitionPlanner(partitionLocationProvider, singlePartitionPlanner,
    "localPartition", dataset, queryConfig)

  val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
    Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))
  }
  val rootPlanner = new ShardKeyRegexPlanner(dataset, multiPartitionPlanner, shardKeyMatcherFn, queryConfig)

  val startSeconds = now / 1000 - 10.days.toSeconds
  val endSeconds = now / 1000
  val step = 300

  private val notUsedQueryParams = PromQlQueryParams("notUsedQuery", 100, 1, 1000)

  it("should generate plan for simple query") {
    val lp = Parser.queryRangeToLogicalPlan(
      """sum(foo{_ws_ = "demo", _ns_ = "App", instance = "Inst-1" })""",
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = rootPlanner.materialize(lp, QueryContext(origQueryParams = notUsedQueryParams))
    println(s"Final Plan : ${execPlan.printTree()}")

    /*
T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1634774449448,100,1634775313448))
-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#686452204],raw)
--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#686452204],raw)
----T~PeriodicSamplesMapper(start=1634774449448000, step=100000, end=1634775313448000, window=None, functionId=None, rawSource=true, offsetMs=None)
-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(1634774449148000,1634775313448000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#686452204],raw)
----T~PeriodicSamplesMapper(start=1634774449448000, step=100000, end=1634775313448000, window=None, functionId=None, rawSource=true, offsetMs=None)
-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(1634774449148000,1634775313448000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#686452204],raw)
     */
  }
}
