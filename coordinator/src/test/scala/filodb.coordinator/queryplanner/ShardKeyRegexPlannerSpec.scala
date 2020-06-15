package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.ShardMapper
import filodb.core.MetricsTestData
import filodb.core.metadata.Schemas
import filodb.core.query.Filter.Equals
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryConfig, QueryContext}
import filodb.prometheus.parse.Parser
import filodb.query.exec.{DistConcatExec, MultiSchemaPartitionsExec, ReduceAggregateExec}

class ShardKeyRegexPlannerSpec extends FunSpec with Matchers with ScalaFutures {

  private val dataset = MetricsTestData.timeseriesDatasetMultipleShardKeys
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)
  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val routingConfigString = "routing {\n  buddy {\n    http {\n      timeout = 10.seconds\n    }\n  }\n}"
  private val routingConfig = ConfigFactory.parseString(routingConfigString)
  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").
    withFallback(routingConfig)
  private val queryConfig = new QueryConfig(config)

  private val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty, "sum(heap_usage)", 100, 1, 1000, None)

  private val localMapper = new ShardMapper(32)
  for {i <- 0 until 32} localMapper.registerNode(Seq(i), node)


  val localPlanner = new SingleClusterPlanner(dsRef, schemas, localMapper, earliestRetainedTimestampFn = 0, queryConfig)

  it("should generate Exec plan for simple query") {
    val lp = Parser.queryToLogicalPlan("test{_ws_ = \"demo\", _ns_ =~ \"app.*\", instance = \"Inst-1\" }", 1000)
    val regexFieldMatcher = (regexColumn: RegexColumn) => { Seq("App-1", "App-2") }
    println("lp:" + lp)
    val engine = new ShardKeyRegexPlanner("_ws_", dataset, localPlanner, regexFieldMatcher)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[DistConcatExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
    println(execPlan.printTree())
  }

  it("should generate Exec plan for Aggregate query") {
    val lp = Parser.queryToLogicalPlan("sum(test{_ws_ = \"demo\", _ns_ =~ \"app.*\", instance = \"Inst-1\" })", 1000)
    val regexFieldMatcher = (regexColumn: RegexColumn) => { Seq("App-1", "App-2") }
    println("lp:" + lp)
    val engine = new ShardKeyRegexPlanner("_ws_", dataset, localPlanner, regexFieldMatcher)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[ReduceAggregateExec] shouldEqual(true)
    execPlan.children(0).children.head.isInstanceOf[MultiSchemaPartitionsExec]
    execPlan.children(0).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-1"))) shouldEqual(true)
    execPlan.children(1).children.head.asInstanceOf[MultiSchemaPartitionsExec].filters.
      contains(ColumnFilter("_ns_", Equals("App-2"))) shouldEqual(true)
    println(execPlan.printTree())
  }
}

