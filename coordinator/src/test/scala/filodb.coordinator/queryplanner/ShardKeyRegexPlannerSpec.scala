package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.coordinator.{ActorPlanDispatcher, ShardMapper}
import filodb.core.{MetricsTestData, SpreadChange}
import filodb.core.metadata.Schemas
import filodb.prometheus.ast.TimeStepParams
import filodb.query.{InstantFunctionId, LogicalPlan, PlanValidationSpec, TsCardinalities}
import filodb.core.query.{ColumnFilter, PlannerParams, PromQlQueryParams, QueryConfig, QueryContext}
import filodb.core.query.Filter.Equals
import filodb.prometheus.parse.Parser
import filodb.query.exec._


class ShardKeyRegexPlannerSpec extends AnyFunSpec with Matchers with ScalaFutures with PlanValidationSpec {

  private val dataset = MetricsTestData.timeseriesDatasetMultipleShardKeys
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)
  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val routingConfigString = "routing {\n  buddy {\n    http {\n      timeout = 10.seconds\n    }\n  }\n}"
  private val routingConfig = ConfigFactory.parseString(routingConfigString)
  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").
    withFallback(routingConfig)
  private val queryConfig = QueryConfig(config)

  private val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000)

  private val localMapper = new ShardMapper(32)
  for {i <- 0 until 32} localMapper.registerNode(Seq(i), node)

  def makeLocalPlanner(shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]]): SingleClusterPlanner = {
    new SingleClusterPlanner(
      dataset, schemas, localMapper, earliestRetainedTimestampFn = 0,
      queryConfig, "raw", shardKeyMatcher = shardKeyMatcher,
      spreadProvider = StaticSpreadProvider(SpreadChange(time = Long.MinValue)))
  }

  def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
    TimeRange(timeRange.startMs, timeRange.endMs)))
  val mppPartitionLocationProvider = new PartitionLocationProvider {
    override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
      if (routingKey.equals(Map("_ns_" -> "App-1", "_ws_" -> "demo")))
        List(PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs, timeRange.endMs)))
      else
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }

    override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
      partitions(timeRange)
  }
  val simplePartitionLocationProvider = new PartitionLocationProvider {
    override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment(s"part-${routingKey("_ns_")}", "http://dummy-url.com", timeRange))
    override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
      getPartitions(nonMetricShardKeyFilters.map(f => (f.column, f.filter.valuesStrings.head.toString)).toMap, timeRange)
  }
  val c = QueryConfig(config).copy(plannerSelector = Some("plannerSelector"))

  val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
    Seq(
      Seq(
        ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))
      ),
      Seq(
        ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-2"))
      )
    )
  }

  val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
  val mpp = new MultiPartitionPlanner(
    mppPartitionLocationProvider, localPlanner, "local", dataset, c, shardKeyMatcher = shardKeyMatcherFn
  )
  val skrp = new ShardKeyRegexPlanner(dataset, mpp, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)

  it("should generate Exec plan for simple query") {
    val lp = Parser.queryToLogicalPlan("test{_ws_ = \"demo\", _ns_ =~ \"App.*\", instance = \"Inst-1\" }", 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1342019804],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1342019804],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1342019804],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1342019804],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1342019804],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for implicit ws query") {
    val lp = Parser.queryToLogicalPlan("test{_ns_ =~ \"App.*\", instance = \"Inst-1\" }", 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = PromQlQueryParams(
      """test{_ns_ =~ "App.*", instance = "Inst-1" }""", 100, 1, 1000)))
    val expected = """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should check for required non metric shard key filters") {
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)

    val nonMetricShardColumns = dataset.options.nonMetricShardColumns
    val implicitLp = Parser.queryToLogicalPlan("test{_ns_ =~ \"App.*\", instance = \"Inst-1\" }", 1000, 1000)
    engine.hasRequiredShardKeysPresent(LogicalPlan.getNonMetricShardKeyFilters(implicitLp, nonMetricShardColumns),
      nonMetricShardColumns) shouldEqual false

    val explicitLp = Parser.queryToLogicalPlan("test{_ws_ = \"demo\", _ns_ =~ \"App.*\", instance = \"Inst-1\" }",
      1000, 1000)
    engine.hasRequiredShardKeysPresent(LogicalPlan.getNonMetricShardKeyFilters(explicitLp, nonMetricShardColumns),
      nonMetricShardColumns) shouldEqual true
  }

  it("should generate Exec plan for subquery with windowing") {
    val expected =
    """T~PeriodicSamplesMapper(start=1000000, step=0, end=1000000, window=Some(300000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
      |-E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher
      |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
      |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
      |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
      |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
      |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
      |--E~PromQlRemoteExec(PromQlQueryParams(test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"},100,1,1000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true), queryEndpoint=remote-url, requestTimeoutMs=60000) on InProcessPlanDispatcher""".stripMargin
    val lp = Parser.queryToLogicalPlan(
      """avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m])""",
      1000, 1000
    )

    val engine = new ShardKeyRegexPlanner(dataset, mpp, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(
      lp,
      QueryContext(origQueryParams = promQlQueryParams, plannerParams = PlannerParams(processMultiPartition = true))
    )
    println(execPlan.printTree())
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for nested subquery ") {
    val lp = Parser.queryToLogicalPlan(
      """max_over_time(avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m])[10m:1m])""",
      1000, 1000
    )
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~PeriodicSamplesMapper(start=1000000, step=0, end=1000000, window=Some(600000), functionId=Some(MaxOverTime), rawSource=false, offsetMs=None)
                     |-T~PeriodicSamplesMapper(start=420000, step=60000, end=960000, window=Some(300000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
                     |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#613989854],raw)
                     |---T~PeriodicSamplesMapper(start=120000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(-180000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#613989854],raw)
                     |---T~PeriodicSamplesMapper(start=120000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(-180000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#613989854],raw)
                     |---T~PeriodicSamplesMapper(start=120000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(-180000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#613989854],raw)
                     |---T~PeriodicSamplesMapper(start=120000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(-180000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#613989854],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for subquery with windowing with aggregate") {
    val lp = Parser.queryToLogicalPlan(
      """sum(avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*",
        |instance = "Inst-1" }[5m:1m]))""".stripMargin,
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1,
      1000)))
    val expected = """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,0,1000))
                     |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#25320684],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=0, end=1000000, window=Some(300000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
                     |----T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#25320684],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=0, end=1000000, window=Some(300000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
                     |----T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#25320684],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=0, end=1000000, window=Some(300000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
                     |----T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#25320684],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=0, end=1000000, window=Some(300000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
                     |----T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#25320684],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for top level subquery") {
    val expected =
        """E~MultiPartitionDistConcatExec() on InProcessPlanDispatcher
          |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
          |--T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
          |--T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
          |--T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
          |--T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
          |-E~PromQlRemoteExec(PromQlQueryParams(test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"},100,1,1000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true), queryEndpoint=remote-url, requestTimeoutMs=60000) on InProcessPlanDispatcher""".stripMargin
    val lp = Parser.queryToLogicalPlan(
      """test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m]""",
      1000, 1000
    )
    val execPlan = skrp.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams = PlannerParams(processMultiPartition = true)))
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for Aggregate query") {
    val lp = Parser.queryToLogicalPlan("""sum(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" })""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1,
      1000)))
    val expected = """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2117041601],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2117041601],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2117041601],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2117041601],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2117041601],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for time()") {
    val lp = Parser.queryToLogicalPlan("time()", 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq((Seq.empty)) }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """E~TimeScalarGeneratorExec(params = RangeParams(1000,1000,1000), function = Time) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,25,true,false,true,Set(),None,Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))"""
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for Scalar Binary Operation") {
    val lp = Parser.queryToLogicalPlan("""1 + test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1"}""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = PromQlQueryParams(
    """1 + test{_ws_ = \"demo\",_ns_ =~ \"App.*\", instance = \"Inst-1\" }""", 100, 1, 1000)))
    val expected = """T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
                     |-FA1~StaticFuncArgs(1.0,RangeParams(1000,1000,1000))
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for Binary join without regex") {
    val lp = Parser.queryToLogicalPlan("""test1{_ws_ = "demo", _ns_ = "App"} + test2{_ws_ = "demo", _ns_ = "App"}""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams =
      PromQlQueryParams("""test1{_ws_="demo",_ns_="App"} + test2{_ws_="demo",_ns_="App"}""".stripMargin, 100,1, 1000)))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual(true)
    execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      ("""test1{_ws_="demo",_ns_="App"} + test2{_ws_="demo",_ns_="App"}""")
  }

  it ("should generate Exec plan for Metadata query") {
    val lp = Parser.metadataQueryToLogicalPlan("""http_requests_total{job="prometheus", method="GET"}""",
      TimeStepParams(1000, 1000, 3000))
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
//    val expected = """"""
//    validatePlan(execPlan, expected)
    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
  }

  it("should generate Exec plan for histogram quantile for Aggregate query") {
    val lp = Parser.queryToLogicalPlan("""histogram_quantile(0.2, sum(test{_ws_ = "demo", _ns_ =~ "App.*"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~InstantVectorFunctionMapper(function=HistogramQuantile)
                     |-FA1~StaticFuncArgs(0.2,RangeParams(1000,1000,1000))
                     |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for exp for Aggregate query") {
    val lp = Parser.queryToLogicalPlan("""exp(sum(test{_ws_ = "demo", _ns_ =~ "App.*"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~InstantVectorFunctionMapper(function=Exp)
                     |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1364224076],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate local Exec plan for query without regex") {
    val lp = Parser.queryToLogicalPlan("""test{_ws_ = "demo", _ns_ = "App-1" }""", 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-882843538],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-882843538],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-882843538],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for scalar - time()") {
    val lp = Parser.queryToLogicalPlan("""scalar(test{_ws_ = "demo", _ns_ =~ "App.*"}) - time()""",
      1000, 1000)
    val promQlQueryParams = PromQlQueryParams("""scalar(test{_ws_ = "demo", _ns_ =~ "App.*"}) - time()""", 100, 1, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=true)
                     |-FA1~
                     |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                     |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-E~TimeScalarGeneratorExec(params = RangeParams(1000,1000,1000), function = Time) on InProcessPlanDispatcher""".stripMargin
    validatePlan(execPlan, expected)
  }

  it ("should generate Exec plan for Metadata Label values query") {
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq.empty
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val lp = Parser.labelValuesQueryToLogicalPlan(Seq("""__metric__"""), Some("""_ws_="demo", _ns_=~".*" """),
      TimeStepParams(1000, 20, 5000) )

    val promQlQueryParams = PromQlQueryParams(
      "", 1000, 20, 5000, Some("/api/v2/label/values"))

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[LabelValuesDistConcatExec] shouldEqual (true)
  }

  it ("should generate ExecPlan for TsCardinalities") {
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Nil
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val lp = TsCardinalities(Seq("ws_foo", "ns_bar"), 3)
    val promQlQueryParams = PromQlQueryParams(
      "", 1000, 20, 5000, Some("/api/v1/metering/cardinality/timeseries"))
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))
    execPlan.isInstanceOf[TsCardReduceExec] shouldEqual true
    engine.isMetadataQuery(lp) shouldEqual true
  }

  it("should generate Exec plan for Binary join with regex") {
    val lp = Parser.queryToLogicalPlan(
      """test1{_ws_ = "demo", _ns_ =~ "App.*"} +
        |test2{_ws_ = "demo", _ns_ =~ "App.*"}""".stripMargin, 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,25,true,false,true,Set(),None,Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=14, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=30, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=4, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=20, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2074499967],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }



  it("should generate Exec plan for Binary join of subqueries with regex") {
    val lp = Parser.queryToLogicalPlan(
      """avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m]) +
        |avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m])""".stripMargin,
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,None,None,25,true,false,true,Set(),None,Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536)))
                     |-T~PeriodicSamplesMapper(start=1000000, step=0, end=1000000, window=Some(300000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
                     |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)
                     |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)
                     |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)
                     |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)
                     |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=0, end=1000000, window=Some(300000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
                     |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)
                     |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)
                     |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)
                     |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)
                     |---T~PeriodicSamplesMapper(start=720000, step=60000, end=960000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(420000,960000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(instance,Equals(Inst-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1923678294],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for Binary join with regex only on one side") {
    val lp = Parser.queryToLogicalPlan(
      """test1{_ws_ = "demo", _ns_ = "App-0"} +
        | test2{_ws_ = "demo", _ns_ =~ "App.*"}""".stripMargin, 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-2"))))
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=10, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-0)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=26, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-0)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=14, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=30, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=4, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=20, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for topk query with single matching value for regex") {
    val lp = Parser.queryToLogicalPlan(s"""topk(2, test{_ws_ = "demo", _ns_ =~ "App-1"})""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))))
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~AggregatePresenter(aggrOp=TopK, aggrParams=List(2.0), rangeParams=RangeParams(1000,1000,1000))
                     |-E~LocalPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

    it("should support for topk query with multiple matching values for regex if they are all local") {
      val lp = Parser.queryToLogicalPlan(s"""topk(2, test{_ws_ = "demo", _ns_ =~ "App.*"})""",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App-1"))),
          Seq(ColumnFilter("_ws_", Equals("demo")),
            ColumnFilter("_ns_", Equals("App-2"))))
      }
      val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      val expected = """T~AggregatePresenter(aggrOp=TopK, aggrParams=List(2.0), rangeParams=RangeParams(1000,1000,1000))
                       |-E~LocalPartitionReduceAggregateExec(aggrOp=TopK, aggrParams=List(2.0)) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#143127322],raw)
                       |--T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
                       |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                       |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#143127322],raw)
                       |--T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
                       |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                       |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#143127322],raw)
                       |--T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
                       |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                       |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#143127322],raw)
                       |--T~AggregateMapReduce(aggrOp=TopK, aggrParams=List(2.0), without=List(), by=List())
                       |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                       |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#143127322],raw)""".stripMargin
      validatePlan(execPlan, expected)
    }


  it("should generate Exec plan for histogram quantile for Aggregate query having single matching regex value") {
    val lp = Parser.queryToLogicalPlan("""histogram_quantile(0.2, sum(test{_ws_ = "demo", _ns_ =~ "App-1"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))))
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~InstantVectorFunctionMapper(function=HistogramQuantile)
                     |-FA1~StaticFuncArgs(0.2,RangeParams(1000,1000,1000))
                     |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=16, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should generate Exec plan for Binary join with single regex match") {
    val lp = Parser.queryToLogicalPlan(
      """test1{_ws_ = "demo", _ns_ =~ "App"} +
        | test2{_ws_ = "demo", _ns_ =~ "App"}""".stripMargin, 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App"))))
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=4, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=20, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should preserve brackets in Binary join with regex query") {
    val lp = Parser.queryToLogicalPlan(
      """sum(test1{_ws_ = "demo", _ns_ =~ "App"}) /
        |(sum(test2{_ws_ = "demo", _ns_ =~ "App"})+sum(test3{_ws_ = "demo", _ns_ =~ "App"}))""".stripMargin, 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App"))))
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """E~BinaryJoinExec(binaryOp=DIV, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |-E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=4, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=20, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test3))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App)), ColumnFilter(_metric_,Equals(test3))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2041966083],raw)""".stripMargin
    validatePlan(execPlan, expected)

  }

  it("should handle push down aggregation to right level case 1") {
    // Case 1 of Push down tests  when regex resolves to two different namespaces
    val lp = Parser.queryToLogicalPlan("""sum(count by(foo)(test1{_ws_ = "demo", _ns_ =~ "App-.*"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App1"))),
        Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App2"))
        )
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |----E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-----T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List(foo))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=4, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-----T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List(foo))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=20, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-----T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List(foo))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=8, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-----T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List(foo))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=24, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }


  it("should handle push down aggregation to right level case 2") {
    // Case 2 of Push down tests  when regex resolves to only one namespace
    val lp = Parser.queryToLogicalPlan("""sum(count by(foo)(test1{_ws_ = "demo", _ns_ =~ "App-.*"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App1")))
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#572058289],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |----E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#572058289],raw)
                     |-----T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List(foo))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=4, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#572058289],raw)
                     |-----T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List(foo))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=20, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#572058289],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should handle multiple binary joins appropriately, case 1") {

    // Case 1: Binary join uses metrics from same partition, entire plan should be materialized by
    // the queryPlanner and nothing in ShardKeyPlanner

    val lp = Parser.queryToLogicalPlan(
      """test1{_ws_ = "demo", _ns_ = "App-1"}
        |+ test2{_ws_ = "demo", _ns_ = "App-1"}""".stripMargin, 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App1")))
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(
      origQueryParams = PromQlQueryParams("""test1{_ws_="demo",_ns_="App-1"} + test2{_ws_="demo",_ns_="App-1"}""",
        100, 1, 1000)))
    val expected = """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=14, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=30, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }


  it("should handle multi partition multi level binary join appropriately") {

    // LHS of the Binary Join is on one partition, RHS is another binary join that has data on same partition

    val lp = Parser.queryToLogicalPlan(
      """test1{_ws_ = "demo", _ns_ = "App-1"} +
        | (test2{_ws_ = "demo", _ns_ = "App-2"} * test3{_ws_ = "demo", _ns_ = "App-2"})""".stripMargin,
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq(shardColumnFilters)

    // Top level plan to be done in-process, both left & right operations will be executed by the wrapped query planner
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~InstantVectorFunctionMapper(function=Ln)
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=4, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=20, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=8, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=24, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
  }


  it("should handle multi partition multi level binary join appropriately with instant fn") {

    val lp = Parser.queryToLogicalPlan("""ln(test1{_ws_ = "demo", _ns_ =~ "App.*"})""", 1000, 1000)

    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App1"))),
        Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App2"))
        )
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~InstantVectorFunctionMapper(function=Ln)
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=4, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=20, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=8, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=24, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App.*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }


  it("should handle simple instant function applied to single partition raw series with regex") {
    // Given the raw series is multi partition, the Final operation is done in process for query service
    val lp = Parser.queryToLogicalPlan("""ln(test1{_ws_ = "demo", _ns_ =~ "App-*"})""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))),
        Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App-2"))
        )
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~InstantVectorFunctionMapper(function=Ln)
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App-*)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should handle simple instant function applied to single partition raw series w/o regex") {
    val lp = Parser.queryToLogicalPlan("""ln(test1{_ws_ = "demo", _ns_ = "App-1"})""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq(shardColumnFilters)
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams =
      PromQlQueryParams("""ln(test1{_ws_="demo",_ns_="App-1"})""", 100, 1, 1000)))
    val expected = """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~InstantVectorFunctionMapper(function=Ln)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-T~InstantVectorFunctionMapper(function=Ln)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should handle simple instant function applied to single partition raw series w/o regex and sum") {
    val lp = Parser.queryToLogicalPlan("""sum(ln(test1{_ws_ = "demo", _ns_ = "App-1"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq(shardColumnFilters)
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams =
      PromQlQueryParams("""sum(ln(test1{_ws_="demo",_ns_="App-1"}))""", 100, 1, 1000)))
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    // Since the dispatcher is ActorDispatcher, its materialized by the underlying queryPlanner
    execPlan.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual (true)
    execPlan.children.isEmpty shouldEqual false
    execPlan.children.head.rangeVectorTransformers.find( _.isInstanceOf[InstantVectorFunctionMapper]) match {
      case Some(x: InstantVectorFunctionMapper) => x.function shouldEqual InstantFunctionId.Ln
      case _ => fail("Expected to see an InstantVectorFunctionMapper")
    }
    execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      ("""sum(ln(test1{_ws_="demo",_ns_="App-1"}))""")
  }

  it("should generate the appropriate plan with label_join") {
    val lp = Parser.queryToLogicalPlan(
      """label_join(foo{_ws_="Demo", _ns_ =~ "App*"},
        |"foo", ",", "instance", "job")""".stripMargin,
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))),
        Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App-2"))
        )
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~MiscellaneousFunctionMapper(function=LabelJoin, funcParams=List() funcStringParam=List(foo, ,, instance, job))
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=12, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=28, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }
  it("should generate the appropriate plan with sort") {
    val lp = Parser.queryToLogicalPlan("""sort(foo{_ws_="Demo", _ns_ =~ "App*"}, "foo", ",", "instance", "job")""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))),
        Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App-2"))
        )
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~SortFunctionMapper(function=Sort)
                     |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=12, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=28, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }


  it("should push down absent function appropriately to individual partitions") {
    val lp = Parser.queryToLogicalPlan("""sum(absent(foo{_ws_="Demo",_ns_ =~ "App*"}, "foo", "," ,"instance","job"))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))),
        Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App-2"))
        )
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    // val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |---T~AbsentFunctionMapper(columnFilter=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(__name__,Equals(foo))) rangeParams=RangeParams(1000,1000,1000) metricColumn=_metric_)
                     |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=12, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
                     |------T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=28, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }


  it("should execute absent in-process") {
    val lp = Parser.queryToLogicalPlan("""absent(foo{_ws_="Demo", _ns_ =~ "App*"}, "foo", ",", "instance", "job")""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))),
        Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App-2"))
        )
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~AbsentFunctionMapper(columnFilter=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(__name__,Equals(foo))) rangeParams=RangeParams(1000,1000,1000) metricColumn=_metric_)
                     |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
                     |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=12, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=28, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }


  it("should execute absent function using wrapped planner") {
    val lp = Parser.queryToLogicalPlan("""absent(foo{_ws_="Demo", _ns_ =~ "App-1"}, "foo", ",", "instance", "job")""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))))
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams =
      PromQlQueryParams("""absent(foo{_ws_="Demo", _ns_ =~ "App-1"} , "foo", "," "instance", "job")""",
        100, 1, 1000)))
    val expected = """T~AbsentFunctionMapper(columnFilter=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App-1)), ColumnFilter(__name__,Equals(foo))) rangeParams=RangeParams(1000,1000,1000) metricColumn=_metric_)
                     |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
                     |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(Demo)), ColumnFilter(_ns_,EqualsRegex(App-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should materialize vector appropriately for single partition") {
    val lp = Parser.queryToLogicalPlan("""vector(scalar(sum(foo{_ws_="demo", _ns_ = "App-1"})))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq(shardColumnFilters)
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams =
      PromQlQueryParams("""vector(scalar(sum(foo{_ws_="demo",_ns_="App-1"})))""", 100, 1, 1000)))
    val expected = """T~VectorFunctionMapper(funcParams=List())
                     |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                     |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should materialize vector appropriately for multi partition") {
    val lp = Parser.queryToLogicalPlan("""vector(scalar(sum(foo{_ws_="demo", _ns_ =~ "App*"})))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))),
        Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App-2"))
        )
      )
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected = """T~VectorFunctionMapper(funcParams=List())
                     |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                     |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,1000,1000))
                     |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1837220465],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1837220465],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1837220465],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=12, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1837220465],raw)
                     |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
                     |-----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
                     |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=28, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,EqualsRegex(App*)), ColumnFilter(_metric_,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1837220465],raw)""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should materialize absent function mapper correctly with implicit WS") {
    // TODO(a_theimer): need to verify it's correct to remove these tests.
    {
      // absent of sum_over_time: _ws_ in query
      val lp = Parser.queryToLogicalPlan(
        "absent(sum_over_time(test{_ws_ = \"demo\", _ns_ = \"App-1\", instance = \"Inst-1\" }[5m]))",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
      }
      val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
        .asInstanceOf[AbsentFunctionMapper].columnFilter.isEmpty shouldEqual true
    }
//    {
//      // absent of sum_over_time: _ws_ NOT in query
//      val lp = Parser.queryToLogicalPlan("absent(sum_over_time(test{_ns_ = \"App-1\", instance = \"Inst-1\" }[5m]))",
//        1000, 1000)
//      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
//        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
//      }
//      val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
//      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
//      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
//      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
//        .asInstanceOf[AbsentFunctionMapper].columnFilter.isEmpty shouldEqual true
//    }
    {
      // absent: _ws_ in query
      val lp = Parser.queryToLogicalPlan(
        "absent(test{_ws_ = \"demo\", _ns_ = \"App-1\", instance = \"Inst-1\" })",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
      }
      val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
        .asInstanceOf[AbsentFunctionMapper].columnFilter.size shouldEqual 4 // _ws_, _ns_, __name__ & instance
    }
//    {
//      // absent: _ws_ NOT in query
//      val lp = Parser.queryToLogicalPlan("absent(test{_ns_ = \"App-1\", instance = \"Inst-1\" })",
//        1000, 1000)
//      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
//        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
//      }
//      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
//      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
//      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
//        .asInstanceOf[AbsentFunctionMapper].columnFilter.size shouldEqual 4 // _ws_, _ns_, __name__ & instance
//    }
    {
      // absent_over_time: _ws_ in query
      val lp = Parser.queryToLogicalPlan(
        "absent_over_time(test{_ws_ = \"demo\", _ns_ = \"App-1\", instance = \"Inst-1\" }[5m])",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
      }
      val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
        .asInstanceOf[AbsentFunctionMapper].columnFilter.size shouldEqual 4 // _ws_, _ns_, __name__ & instance
    }
//    {
//      // absent_over_time: _ws_ NOT in query
//      val lp = Parser.queryToLogicalPlan("absent_over_time(test{_ns_ = \"App-1\", instance = \"Inst-1\" }[5m])",
//        1000, 1000)
//      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
//        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
//      }
//      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)
//      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
//      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
//        .asInstanceOf[AbsentFunctionMapper].columnFilter.size shouldEqual 4 // _ws_, _ns_, __name__ & instance
//    }
  }

  // TODO(a_theimer): need to confirm whether-or-not safe to remove this test.
  it("should materialize instant functions with args correctly with implicit WS") {
    // "expected" string is the printTree() of the first child (isInstanceOf[DistConcat] is asserted below--
    //   other children are identical except for their source shards)
    val queryExpectedPairs = Seq(
      // inst func with scalar() arg: _ws_ in query
      (s"""clamp_max(test{_ws_="demo",_ns_="App-1"}, scalar(sc_test{_ws_="demo",_ns_="App-1"}))""",
        """T~InstantVectorFunctionMapper(function=ClampMax)
          |-FA1~
          |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
          |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1006757749],raw)
          |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=14, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(sc_test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1006757749],raw)
          |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=30, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(sc_test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1006757749],raw)
          |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1006757749],raw)""".stripMargin),
//      // inst func with scalar() arg: _ws_ NOT in query
//      ("""clamp_max(test{_ns_="App-1"}, scalar(sc_test{_ns_="App-1"}))""",
//        """T~InstantVectorFunctionMapper(function=ClampMax)
//          |-FA1~
//          |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
//          |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
//          |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
//          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=14, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(sc_test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
//          |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
//          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=30, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(sc_test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
//          |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
//          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin)
    )
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
    }
    val localPlanner = makeLocalPlanner(shardKeyMatcherFn)
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, simplePartitionLocationProvider, queryConfig)

    queryExpectedPairs.foreach{ case (query, expected) =>
      val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.isInstanceOf[DistConcatExec] shouldEqual true
      validatePlan(execPlan.children.head, expected)
    }
  }
}
