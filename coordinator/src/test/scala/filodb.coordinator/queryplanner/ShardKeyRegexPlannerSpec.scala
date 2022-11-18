package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.coordinator.{ActorPlanDispatcher, ShardMapper}
import filodb.core.MetricsTestData
import filodb.core.metadata.Schemas
import filodb.prometheus.ast.TimeStepParams
import filodb.query.{BinaryOperator, InstantFunctionId, LogicalPlan, MiscellaneousFunctionId, PlanValidationSpec, SortFunctionId, TsCardinalities}
import filodb.core.query.{ColumnFilter, PlannerParams, PromQlQueryParams, QueryConfig, QueryContext}
import filodb.core.query.Filter.Equals
import filodb.prometheus.parse.Parser
import filodb.query.InstantFunctionId.{Exp, HistogramQuantile, Ln}
import filodb.query.exec._
import filodb.query.AggregationOperator._
import scala.language.postfixOps

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


  val localPlanner = new SingleClusterPlanner(dataset, schemas, localMapper, earliestRetainedTimestampFn = 0,
    queryConfig, "raw")

  it("should generate Exec plan for simple query") {
    val lp = Parser.queryToLogicalPlan("test{_ws_ = \"demo\", _ns_ =~ \"App.*\", instance = \"Inst-1\" }", 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
  }

  it("should generate Exec plan for implicit ws query") {
    val lp = Parser.queryToLogicalPlan("test{_ns_ =~ \"App.*\", instance = \"Inst-1\" }", 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = PromQlQueryParams(
      """test{_ns_ =~ "App.*", instance = "Inst-1" }""", 100, 1, 1000)))
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ws_", Equals("demo"))) shouldEqual(true)
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ws_", Equals("demo"))) shouldEqual(true)
  }

  it("should check for required non metric shard key filters") {
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)

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
    val lp = Parser.queryToLogicalPlan(
      """avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m])""",
      1000, 1000
    )
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
  }

  it("should generate Exec plan for nested subquery ") {
    val lp = Parser.queryToLogicalPlan(
      """max_over_time(avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m])[10m:1m])""",
      1000, 1000
    )
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
  }

  it("should generate Exec plan for subquery with windowing with aggregate") {
    val lp = Parser.queryToLogicalPlan(
      """sum(avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*",
        |instance = "Inst-1" }[5m:1m]))""".stripMargin,
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1,
      1000)))
    execPlan.isInstanceOf[MultiPartitionReduceAggregateExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
  }

  it("should generate Exec plan for top level subquery") {
    val lp = Parser.queryToLogicalPlan(
      """test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m]""",
      1000, 1000
    )
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
  }

  it("should generate Exec plan for Aggregate query") {
    val lp = Parser.queryToLogicalPlan("""sum(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" })""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1,
      1000)))
    execPlan.isInstanceOf[MultiPartitionReduceAggregateExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
  }

  it("should generate Exec plan for time()") {
    val lp = Parser.queryToLogicalPlan("time()", 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq((Seq.empty)) }
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[TimeScalarGeneratorExec] shouldEqual(true)
  }

  it("should generate Exec plan for Scalar Binary Operation") {
    val lp = Parser.queryToLogicalPlan("""1 + test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1"}""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = PromQlQueryParams(
    """1 + test{_ws_ = \"demo\",_ns_ =~ \"App.*\", instance = \"Inst-1\" }""", 100, 1, 1000)))
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual(true)
    execPlan.rangeVectorTransformers(0).isInstanceOf[ScalarOperationMapper] shouldEqual true
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]

    // Child plans should have only inner periodic query in PromQlQueryParams
    execPlan.children(1).children.head.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      "test{instance=\"Inst-1\",_ws_=\"demo\",_ns_=\"App-1\"}"
    execPlan.children(0).children.head.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      "test{instance=\"Inst-1\",_ws_=\"demo\",_ns_=\"App-2\"}"
    execPlan.children(0).children.head.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
  }

  it("should generate Exec plan for Binary join without regex") {
    val lp = Parser.queryToLogicalPlan("""test1{_ws_ = "demo", _ns_ = "App"} + test2{_ws_ = "demo", _ns_ = "App"}""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
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
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
  }

  it("should generate Exec plan for histogram quantile for Aggregate query") {
    val lp = Parser.queryToLogicalPlan("""histogram_quantile(0.2, sum(test{_ws_ = "demo", _ns_ =~ "App.*"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[MultiPartitionReduceAggregateExec] shouldEqual(true)
    execPlan.asInstanceOf[MultiPartitionReduceAggregateExec].rangeVectorTransformers(0).
      isInstanceOf[AggregatePresenter] shouldEqual true
    execPlan.asInstanceOf[MultiPartitionReduceAggregateExec].rangeVectorTransformers(1).
      isInstanceOf[InstantVectorFunctionMapper] shouldEqual true

    execPlan.asInstanceOf[MultiPartitionReduceAggregateExec].rangeVectorTransformers(1).
      asInstanceOf[InstantVectorFunctionMapper].function shouldEqual HistogramQuantile
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]

    // Plan for each map should not have histogram quantile
    execPlan.children(0).children.head.rangeVectorTransformers.length shouldEqual 2
    execPlan.children(0).children.head.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
    execPlan.children(0).children.head.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)

    // Child plans should have only sum query in PromQlQueryParams
    execPlan.children(1).children.head.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      """sum(test{_ws_="demo",_ns_="App-1"})"""
    execPlan.children(0).children.head.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      """sum(test{_ws_="demo",_ns_="App-2"})"""
  }

  it("should generate Exec plan for exp for Aggregate query") {
    val lp = Parser.queryToLogicalPlan("""exp(sum(test{_ws_ = "demo", _ns_ =~ "App.*"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[MultiPartitionReduceAggregateExec] shouldEqual(true)
    execPlan.asInstanceOf[MultiPartitionReduceAggregateExec].rangeVectorTransformers(0).
      isInstanceOf[AggregatePresenter] shouldEqual true
    execPlan.asInstanceOf[MultiPartitionReduceAggregateExec].rangeVectorTransformers(1).
      isInstanceOf[InstantVectorFunctionMapper] shouldEqual true

    execPlan.asInstanceOf[MultiPartitionReduceAggregateExec].rangeVectorTransformers(1).
      asInstanceOf[InstantVectorFunctionMapper].function shouldEqual Exp
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]

    // Plan for each map should not have exp
    execPlan.children(0).children.head.rangeVectorTransformers.length shouldEqual 2
    execPlan.children(0).children.head.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
    execPlan.children(0).children.head.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)

    // Child plans should have only sum query in PromQlQueryParams
    execPlan.children(1).children.head.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      """sum(test{_ws_="demo",_ns_="App-1"})"""
    execPlan.children(0).children.head.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      """sum(test{_ws_="demo",_ns_="App-2"})"""
  }

  it("should generate local Exec plan for query without regex") {
    val lp = Parser.queryToLogicalPlan("""test{_ws_ = "demo", _ns_ = "App-1" }""", 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual(true)
    execPlan.children(0).isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(0).asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
  }

  it("should generate Exec plan for scalar - time()") {
    val lp = Parser.queryToLogicalPlan("""scalar(test{_ws_ = "demo", _ns_ =~ "App.*"}) - time()""",
      1000, 1000)
    val promQlQueryParams = PromQlQueryParams("""scalar(test{_ws_ = "demo", _ns_ =~ "App.*"}) - time()""", 100, 1, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[TimeScalarGeneratorExec] shouldEqual(true)
    execPlan.rangeVectorTransformers.head.isInstanceOf[ScalarOperationMapper] shouldEqual true
    execPlan.rangeVectorTransformers.head.asInstanceOf[ScalarOperationMapper].funcParams.head.
      isInstanceOf[ExecPlanFuncArgs] shouldEqual true
    execPlan.rangeVectorTransformers.head.asInstanceOf[ScalarOperationMapper].funcParams.head.
      isInstanceOf[ExecPlanFuncArgs] shouldEqual true
    execPlan.rangeVectorTransformers.head.asInstanceOf[ScalarOperationMapper].funcParams.head.
      asInstanceOf[ExecPlanFuncArgs].execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual true

    val multiPartitionExec = execPlan.rangeVectorTransformers.head.asInstanceOf[ScalarOperationMapper].funcParams.head.
      asInstanceOf[ExecPlanFuncArgs].execPlan
    multiPartitionExec.rangeVectorTransformers.head.isInstanceOf[ScalarFunctionMapper] shouldEqual true

    // Child plans should have only inner query in PromQlQueryParams
    multiPartitionExec.children(1).children.head.queryContext.origQueryParams
      .asInstanceOf[PromQlQueryParams].promQl shouldEqual """test{_ws_="demo",_ns_="App-1"}"""
    multiPartitionExec.children(0).children.head.queryContext.origQueryParams
      .asInstanceOf[PromQlQueryParams].promQl shouldEqual """test{_ws_="demo",_ns_="App-2"}"""
  }

  it ("should generate Exec plan for Metadata Label values query") {
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq.empty
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, queryConfig)
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
    val engine = new ShardKeyRegexPlanner( dataset, localPlanner, shardKeyMatcherFn, queryConfig)
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
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual(true)
    execPlan.children(0).isInstanceOf[MultiPartitionDistConcatExec] shouldEqual(true)
    val lhs = execPlan.children(0).asInstanceOf[MultiPartitionDistConcatExec]
    lhs.children.length shouldEqual 2
    lhs.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    lhs.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    lhs.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
    val rhs = execPlan.children(1).asInstanceOf[MultiPartitionDistConcatExec]
    rhs.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    rhs.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    rhs.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
  }



  it("should generate Exec plan for Binary join of subqueries with regex") {
    val lp = Parser.queryToLogicalPlan(
      """avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m]) +
        |avg_over_time(test{_ws_ = "demo", _ns_ =~ "App.*", instance = "Inst-1" }[5m:1m])""".stripMargin,
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => { Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-1"))), Seq(ColumnFilter("_ws_", Equals("demo")),
      ColumnFilter("_ns_", Equals("App-2"))))}
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual(true)
    execPlan.children(0).isInstanceOf[MultiPartitionDistConcatExec] shouldEqual(true)
    val lhs = execPlan.children(0).asInstanceOf[MultiPartitionDistConcatExec]
    lhs.children.length shouldEqual 2
    lhs.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    lhs.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    lhs.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
    val rhs = execPlan.children(1).asInstanceOf[MultiPartitionDistConcatExec]
    rhs.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    rhs.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    rhs.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
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
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.children(0).isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true)
    val lhs = execPlan.children(0).asInstanceOf[LocalPartitionDistConcatExec]
    lhs.children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-0"))) shouldEqual (true)
    val rhs = execPlan.children(1).asInstanceOf[MultiPartitionDistConcatExec]
    rhs.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    rhs.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual (true)
    rhs.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual (true)
  }

  it("should generate Exec plan for topk query with single matching value for regex") {
    val lp = Parser.queryToLogicalPlan(s"""topk(2, test{_ws_ = "demo", _ns_ =~ "App-1"})""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))))
    }
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
  }

    it("should throw UnsupportedOperationException for topk query with multiple matching values for regex") {
      val lp = Parser.queryToLogicalPlan(s"""topk(2, test{_ws_ = "demo", _ns_ =~ "App.*"})""",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App-1"))),
          Seq(ColumnFilter("_ws_", Equals("demo")),
            ColumnFilter("_ns_", Equals("App-2"))))
      }
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
      the[UnsupportedOperationException] thrownBy {
        val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      } should have message "Shard Key regex not supported for TopK"
    }


  it("should generate Exec plan for histogram quantile for Aggregate query having single matching regex value") {
    val lp = Parser.queryToLogicalPlan("""histogram_quantile(0.2, sum(test{_ws_ = "demo", _ns_ =~ "App-1"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))))
    }
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers(0).
      isInstanceOf[AggregatePresenter] shouldEqual true
    execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers(1).
      isInstanceOf[InstantVectorFunctionMapper] shouldEqual true

    execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers(1).
      asInstanceOf[InstantVectorFunctionMapper].function shouldEqual HistogramQuantile
    execPlan.children.head.isInstanceOf[MultiSchemaPartitionsExec]

    execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      """histogram_quantile(0.2,sum(test{_ws_="demo",_ns_="App-1"}))"""

    execPlan.queryContext.plannerParams.skipAggregatePresent shouldEqual (false)
  }

  it("should generate Exec plan for Binary join with single regex match") {
    val lp = Parser.queryToLogicalPlan(
      """test1{_ws_ = "demo", _ns_ =~ "App"} +
        | test2{_ws_ = "demo", _ns_ =~ "App"}""".stripMargin, 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App"))))
    }
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      ("""(test1{_ws_="demo",_ns_="App"} + test2{_ws_="demo",_ns_="App"})""")
  }

  it("should preserve brackets in Binary join with regex query") {
    val lp = Parser.queryToLogicalPlan(
      """sum(test1{_ws_ = "demo", _ns_ =~ "App"}) /
        |(sum(test2{_ws_ = "demo", _ns_ =~ "App"})+sum(test3{_ws_ = "demo", _ns_ =~ "App"}))""".stripMargin, 1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App"))))
    }
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      ("""(sum(test1{_ws_="demo",_ns_="App"}) / (sum(test2{_ws_="demo",_ns_="App"}) + sum(test3{_ws_="demo",_ns_="App"})))""")
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
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
    execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].aggrOp shouldEqual Sum
    val mpExec = execPlan.children.head
    mpExec.isInstanceOf[MultiPartitionReduceAggregateExec] shouldEqual true
    mpExec.asInstanceOf[MultiPartitionReduceAggregateExec].aggrOp shouldEqual Count
    mpExec.rangeVectorTransformers.find(_.isInstanceOf[AggregateMapReduce]) match {
      case Some(AggregateMapReduce(op, _, _, _)) => op shouldEqual Sum
      case _ => fail("Expected AggregateMapReduce for the sum operation")
    }
    mpExec.children match {
      case plan1::plan2::Nil =>
        (plan2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl ::
          plan1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl :: Nil toSet) shouldEqual
          Set("""count(test1{_ws_="demo",_ns_="App2"}) by (foo)""",
            """count(test1{_ws_="demo",_ns_="App1"}) by (foo)""")
        plan1.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
      case _ => fail("Expected two children")
    }
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
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    // Since we resolve to just one namespace, the entire plan should be materialized by the wrapped planner
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
    execPlan.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
    execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      """sum(count(test1{_ws_="demo",_ns_="App1"}) by (foo))"""
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
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(
      origQueryParams = PromQlQueryParams("""test1{_ws_="demo",_ns_="App-1"} + test2{_ws_="demo",_ns_="App-1"}""",
        100, 1, 1000)))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    // Since the dispatcher is ActorDispatcher, its materialized by the underlying queryPlanner
    execPlan.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual (true)
    execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      ("""test1{_ws_="demo",_ns_="App-1"} + test2{_ws_="demo",_ns_="App-1"}""")
  }


  it("should handle multi partition multi level binary join appropriately") {

    // LHS of the Binary Join is on one partition, RHS is another binary join that has data on same partition

    val lp = Parser.queryToLogicalPlan(
      """test1{_ws_ = "demo", _ns_ = "App-1"} +
        | (test2{_ws_ = "demo", _ns_ = "App-2"} * test3{_ws_ = "demo", _ns_ = "App-2"})""".stripMargin,
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq(shardColumnFilters)

    // Top level plan to be done in-process, both left & right operations will be executed by the wrapped query planner
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
    val inJoin = execPlan.asInstanceOf[BinaryJoinExec]
    val (lhs, rhs) = (inJoin.lhs.head, inJoin.rhs.head)
    execPlan.asInstanceOf[BinaryJoinExec].binaryOp shouldEqual BinaryOperator.ADD
    execPlan.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
    lhs.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    lhs.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
    rhs.isInstanceOf[BinaryJoinExec] shouldEqual true
    rhs.asInstanceOf[BinaryJoinExec].binaryOp shouldEqual BinaryOperator.MUL
    rhs.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true
  }


  it("should handle multi partition multi level binary join appropriately with instant fn") {

    val lp = Parser.queryToLogicalPlan("""ln(test1{_ws_ = "demo", _ns_ =~ "App-1"})""", 1000, 1000)

    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App1"))),
        Seq(ColumnFilter("_ws_", Equals("demo")),
          ColumnFilter("_ns_", Equals("App2"))
        )
      )
    }

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual (true)
    // Since we get data from multiple partitions, the dispatcher will be in process
    execPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual (true)
    execPlan.children match {
      case plan1::plan2::Nil =>
        (plan2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl ::
          plan1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl :: Nil toSet) shouldEqual
          Set("""test1{_ws_="demo",_ns_="App1"}""",
            """test1{_ws_="demo",_ns_="App2"}""")
        plan1.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      case _ => fail("Expected two children")
    }
    execPlan.rangeVectorTransformers.size shouldEqual 1
    val transformer = execPlan.rangeVectorTransformers.head
    transformer.isInstanceOf[InstantVectorFunctionMapper] shouldEqual true
    transformer.asInstanceOf[InstantVectorFunctionMapper].function shouldEqual InstantFunctionId.Ln


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
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual (true)
    // Since the dispatcher is ActorDispatcher, its materialized by the underlying queryPlanner
    execPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual (true)
    execPlan.rangeVectorTransformers.size shouldEqual 1
    execPlan.rangeVectorTransformers.head.isInstanceOf[InstantVectorFunctionMapper] shouldEqual true
    execPlan.rangeVectorTransformers.head.asInstanceOf[InstantVectorFunctionMapper].function shouldEqual
      InstantFunctionId.Ln
    execPlan.children match {
      case plan1::plan2::Nil =>
        (plan2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl ::
          plan1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl :: Nil toSet) shouldEqual
          Set("""test1{_ws_="demo",_ns_="App-1"}""",
            """test1{_ws_="demo",_ns_="App-2"}""")
        plan1.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      case _ => fail("Expected two children")
    }
  }

  it("should handle simple instant function applied to single partition raw series w/o regex") {
    val lp = Parser.queryToLogicalPlan("""ln(test1{_ws_ = "demo", _ns_ = "App-1"})""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq(shardColumnFilters)

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams =
      PromQlQueryParams("""ln(test1{_ws_="demo",_ns_="App-1"})""", 100, 1, 1000)))
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true)
    // Since the dispatcher is ActorDispatcher, its materialized by the underlying queryPlanner
    execPlan.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual (true)
    execPlan.children.forall( child => {
      child.rangeVectorTransformers.find(_.isInstanceOf[InstantVectorFunctionMapper]) match {
        case Some(InstantVectorFunctionMapper(Ln, Nil)) => true
        case _ => false
      }
    }) shouldEqual true
    execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      ("""ln(test1{_ws_="demo",_ns_="App-1"})""")
  }

  it("should handle simple instant function applied to single partition raw series w/o regex and sum") {
    val lp = Parser.queryToLogicalPlan("""sum(ln(test1{_ws_ = "demo", _ns_ = "App-1"}))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq(shardColumnFilters)

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
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

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual (true)
    execPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual (true)
    execPlan.children.size shouldEqual 2
    execPlan.rangeVectorTransformers.find(_.isInstanceOf[MiscellaneousFunctionMapper]) match {
      case Some(x: MiscellaneousFunctionMapper) => x.function shouldEqual MiscellaneousFunctionId.LabelJoin
      case _ => fail("Expected to see an MiscellaneousFunctionMapper")
    }
    execPlan.children.foreach( x => {
      x.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      x.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual (true)
      // TODO: Should the LocalPartitionDistConcatExec have range transformers and just send the required data?
      x.rangeVectorTransformers.isEmpty shouldEqual true
    })
    execPlan.children match {
      case plan1::plan2::Nil =>
        (plan2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl ::
          plan1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl :: Nil toSet) shouldEqual
          Set("""foo{_ws_="demo",_ns_="App-2"}""",
            """foo{_ws_="demo",_ns_="App-1"}""")
        plan1.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      case _ => fail("Expected two children")
    }
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

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual (true)
    execPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual (true)
    execPlan.children.size shouldEqual 2
    execPlan.rangeVectorTransformers.find(_.isInstanceOf[SortFunctionMapper]) match {
      case Some(x: SortFunctionMapper) => x.function shouldEqual SortFunctionId.Sort
      case _ => fail("Expected to see an SortFunctionMapper")
    }
    execPlan.children.foreach( x => {
      x.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      x.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual (true)
      x.rangeVectorTransformers.isEmpty shouldEqual true
    })
    execPlan.children match {
      case plan1::plan2::Nil =>
        (plan2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl ::
          plan1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl :: Nil toSet) shouldEqual
          Set("""foo{_ws_="demo",_ns_="App-2"}""",
            """foo{_ws_="demo",_ns_="App-1"}""")
        plan1.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      case _ => fail("Expected two children")
    }
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

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    // val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[MultiPartitionReduceAggregateExec] shouldEqual (true)
    execPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual (true)
    execPlan.asInstanceOf[ReduceAggregateExec].aggrOp shouldEqual Sum
    execPlan.children.size shouldEqual 2
    execPlan.rangeVectorTransformers.find(_.isInstanceOf[AggregatePresenter]) match {
      case Some(x: AggregatePresenter) => x.aggrOp shouldEqual Sum
      case _ => fail("Expected to see an AggregatePresenter")
    }
    execPlan.children.foreach( x => {
      x.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
      x.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual (true)
      x.rangeVectorTransformers.isEmpty shouldEqual true
      x.asInstanceOf[ReduceAggregateExec].aggrOp shouldEqual Sum

    })
    execPlan.children match {
      case plan1::plan2::Nil =>
        (plan2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl ::
          plan1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl :: Nil toSet) shouldEqual
          Set("""sum(absent(foo{_ws_="demo",_ns_="App-2"}))""",
            """sum(absent(foo{_ws_="demo",_ns_="App-1"}))""")
      case _ => fail("Expected two children")
    }
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

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    execPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual (true)
    execPlan.asInstanceOf[ReduceAggregateExec].aggrOp shouldEqual Sum
    execPlan.children.size shouldEqual 1
    execPlan.rangeVectorTransformers.find(_.isInstanceOf[AbsentFunctionMapper]) match {
      case Some(_: AbsentFunctionMapper) =>
      case _ => fail("Expected to see an AbsentFunctionMapper")
    }
    val child = execPlan.children.head
    child.isInstanceOf[MultiPartitionDistConcatExec] shouldEqual true
    child.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual (true)
    child.rangeVectorTransformers.find(_.isInstanceOf[AggregateMapReduce]) match {
      case Some(x: AggregateMapReduce) => x.aggrOp shouldEqual Sum
      case _ => fail("Expected to see an AggregateMapReduce")
    }

    child.children match {
      case plan1::plan2::Nil =>
        (plan2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl ::
          plan1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl :: Nil toSet) shouldEqual
          Set("""foo{_ws_="demo",_ns_="App-2"}""",
            """foo{_ws_="demo",_ns_="App-1"}""")
      case _ => fail("Expected two children")
    }
  }


  it("should execute absent function using wrapped planner") {
    val lp = Parser.queryToLogicalPlan("""absent(foo{_ws_="Demo", _ns_ =~ "App-1"}, "foo", ",", "instance", "job")""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ws_", Equals("demo")),
        ColumnFilter("_ns_", Equals("App-1"))))
    }

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams =
      PromQlQueryParams("""absent(foo{_ws_="Demo", _ns_ =~ "App-1"} , "foo", "," "instance", "job")""",
        100, 1, 1000)))

    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    execPlan.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual (true)
    execPlan.asInstanceOf[ReduceAggregateExec].aggrOp shouldEqual Sum
    execPlan.rangeVectorTransformers.find(_.isInstanceOf[AbsentFunctionMapper]) match {
      case Some(x: AbsentFunctionMapper) =>
      case _ => fail("Expected to see an AggregatePresenter")
    }
  }

  it("should materialize vector appropriately for single partition") {
    val lp = Parser.queryToLogicalPlan("""vector(scalar(sum(foo{_ws_="demo", _ns_ = "App-1"})))""",
      1000, 1000)
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => Seq(shardColumnFilters)

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams =
      PromQlQueryParams("""vector(scalar(sum(foo{_ws_="demo",_ns_="App-1"})))""", 100, 1, 1000)))

    // Aggregate operation will be materialized on the wrapped planner, everything else in process

    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    execPlan.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual (true)
    execPlan.asInstanceOf[ReduceAggregateExec].aggrOp shouldEqual Sum
    execPlan.children.size shouldEqual 2
    execPlan.rangeVectorTransformers.contains(VectorFunctionMapper()) shouldEqual true
    execPlan.rangeVectorTransformers.find(_.isInstanceOf[ScalarFunctionMapper]) match {
      case Some(x: ScalarFunctionMapper) =>
      case _ => fail("Expected to see an ScalarFunctionMapper")
    }
    execPlan.children.head.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl shouldEqual
      """vector(scalar(sum(foo{_ws_="demo",_ns_="App-1"})))"""
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

    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    // Entre Plan should be materialized on the wrapped
    execPlan.isInstanceOf[MultiPartitionReduceAggregateExec] shouldEqual (true)
    execPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual (true)
    execPlan.asInstanceOf[ReduceAggregateExec].aggrOp shouldEqual Sum
    execPlan.children.size shouldEqual 2
    execPlan.rangeVectorTransformers.contains(VectorFunctionMapper()) shouldEqual true
    execPlan.rangeVectorTransformers.find(_.isInstanceOf[ScalarFunctionMapper]) match {
      case Some(x: ScalarFunctionMapper) =>
      case _ => fail("Expected to see an ScalarFunctionMapper")
    }

    execPlan.children match {
      case plan1::plan2::Nil =>
        (plan2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl ::
          plan1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].promQl :: Nil toSet) shouldEqual
          Set("""sum(foo{_ws_="demo",_ns_="App-1"})""",
            """sum(foo{_ws_="demo",_ns_="App-2"})""")
      case _ => fail("Expected two children")
    }
  }

  it("should materialize absent function mapper correctly with implicit WS") {
    {
      // absent of sum_over_time: _ws_ in query
      val lp = Parser.queryToLogicalPlan(
        "absent(sum_over_time(test{_ws_ = \"demo\", _ns_ = \"App-1\", instance = \"Inst-1\" }[5m]))",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
      }
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
        .asInstanceOf[AbsentFunctionMapper].columnFilter.isEmpty shouldEqual true
    }
    {
      // absent of sum_over_time: _ws_ NOT in query
      val lp = Parser.queryToLogicalPlan("absent(sum_over_time(test{_ns_ = \"App-1\", instance = \"Inst-1\" }[5m]))",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
      }
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
        .asInstanceOf[AbsentFunctionMapper].columnFilter.isEmpty shouldEqual true
    }
    {
      // absent: _ws_ in query
      val lp = Parser.queryToLogicalPlan(
        "absent(test{_ws_ = \"demo\", _ns_ = \"App-1\", instance = \"Inst-1\" })",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
      }
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
        .asInstanceOf[AbsentFunctionMapper].columnFilter.size shouldEqual 4 // _ws_, _ns_, __name__ & instance
    }
    {
      // absent: _ws_ NOT in query
      val lp = Parser.queryToLogicalPlan("absent(test{_ns_ = \"App-1\", instance = \"Inst-1\" })",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
      }
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
        .asInstanceOf[AbsentFunctionMapper].columnFilter.size shouldEqual 4 // _ws_, _ns_, __name__ & instance
    }
    {
      // absent_over_time: _ws_ in query
      val lp = Parser.queryToLogicalPlan(
        "absent_over_time(test{_ws_ = \"demo\", _ns_ = \"App-1\", instance = \"Inst-1\" }[5m])",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
      }
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
        .asInstanceOf[AbsentFunctionMapper].columnFilter.size shouldEqual 4 // _ws_, _ns_, __name__ & instance
    }
    {
      // absent_over_time: _ws_ NOT in query
      val lp = Parser.queryToLogicalPlan("absent_over_time(test{_ns_ = \"App-1\", instance = \"Inst-1\" }[5m])",
        1000, 1000)
      val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
        Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
      }
      val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head
        .asInstanceOf[AbsentFunctionMapper].columnFilter.size shouldEqual 4 // _ws_, _ns_, __name__ & instance
    }
  }

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
      // inst func with scalar() arg: _ws_ NOT in query
      ("""clamp_max(test{_ns_="App-1"}, scalar(sc_test{_ns_="App-1"}))""",
        """T~InstantVectorFunctionMapper(function=ClampMax)
          |-FA1~
          |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
          |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#739940931],raw)
          |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=14, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_metric_,Equals(sc_test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#739940931],raw)
          |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=30, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_metric_,Equals(sc_test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#739940931],raw)
          |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(_ns_,Equals(App-1)), ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_metric_,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#739940931],raw)""".stripMargin)
    )
    val shardKeyMatcherFn = (shardColumnFilters: Seq[ColumnFilter]) => {
      Seq(Seq(ColumnFilter("_ns_", Equals("App-1")), ColumnFilter("_ws_", Equals("demo"))))
    }
    val engine = new ShardKeyRegexPlanner(dataset, localPlanner, shardKeyMatcherFn, queryConfig)

    queryExpectedPairs.foreach{ case (query, expected) =>
      val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.isInstanceOf[DistConcatExec] shouldEqual true
      validatePlan(execPlan.children.head, expected)
    }
  }
}
