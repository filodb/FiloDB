package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler

import filodb.coordinator.ShardMapper
import filodb.core.{DatasetRef, MetricsTestData}
import filodb.core.metadata.Schemas
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext, QuerySession}
import filodb.core.store.ChunkSource
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SinglePartitionPlannerSpec extends AnyFunSpec with Matchers {
  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val localMapper = new ShardMapper(32)
  for {i <- 0 until 32} localMapper.registerNode(Seq(i), node)

  private val remoteMapper = new ShardMapper(16)
  for {i <- 0 until 32} localMapper.registerNode(Seq(i), node)

  private val dataset = MetricsTestData.timeseriesDataset
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)

  private val routingConfigString = "routing {\n  remote {\n   " +
    " http {\n      endpoint = localhost\n timeout = 10000\n    }\n  }\n}"

  private val routingConfig = ConfigFactory.parseString(routingConfigString)
  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").
    withFallback(routingConfig)
  private val queryConfig = new QueryConfig(config)

  private val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000)

  val localPlanner = new SingleClusterPlanner(dsRef, schemas, localMapper, earliestRetainedTimestampFn = 0, queryConfig)
  val remotePlanner = new SingleClusterPlanner(dsRef, schemas, remoteMapper, earliestRetainedTimestampFn = 0,
    queryConfig)

  val failureProvider = new FailureProvider {
    override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
      Seq(FailureTimeRange("local", datasetRef,
        TimeRange(300000, 400000), false))
    }
  }

  val highAvailabilityPlanner = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

  class MockExecPlan(val name: String, val lp: LogicalPlan) extends ExecPlan {
    override def queryContext: QueryContext = QueryContext()
    override def children: Seq[ExecPlan] = Nil
    override def submitTime: Long = 1000
    override def dataset: DatasetRef = ???
    override def dispatcher: PlanDispatcher = InProcessPlanDispatcher
    override def doExecute(source: ChunkSource, querySession: QuerySession)
                          (implicit sched: Scheduler): ExecResult = ???
    override protected def args: String = "mock-args"
  }

   val rrPlanner1 = new QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      new MockExecPlan("rules1", logicalPlan)
    }
  }

  val rrPlanner2 = new QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      new MockExecPlan("rules2", logicalPlan)
    }
  }

  val planners = Map("local" -> highAvailabilityPlanner, "rules1" -> rrPlanner1, "rules2" -> rrPlanner2)
  val plannerSelector = (metricName: String) => { if (metricName.equals("rr1")) "rules1"
  else if (metricName.equals("rr2")) "rules2" else "local" }

  val engine = new SinglePartitionPlanner(planners, plannerSelector, "_metric_", queryConfig)

  it("should generate Exec plan for simple query") {
    val lp = Parser.queryToLogicalPlan("test{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true)
    execPlan.children.length shouldEqual 2
    execPlan.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    execPlan.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true
  }

  it("should generate BinaryJoin Exec plan") {
    val lp = Parser.queryToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.children.foreach { l1 =>
      l1.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      l1.children.foreach { l2 =>
        l2.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        l2.rangeVectorTransformers.size shouldEqual 1
        l2.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      }
    }
  }

  it("should generate exec plan for nested Binary Join query") {
    val lp = Parser.queryToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"} + test3{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[BinaryJoinExec] shouldEqual true
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
  }

  it("should generate BinaryJoin Exec plan with remote and local cluster metrics") {
    val lp = Parser.queryToLogicalPlan("test{job = \"app\"} + rr1{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[MockExecPlan].name shouldEqual ("rules1")
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
  }

  it("should generate BinaryJoin Exec plan with remote cluster metrics") {
    val lp = Parser.queryToLogicalPlan("rr1{job = \"app\"} + rr2{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.asInstanceOf[MockExecPlan].name shouldEqual ("rules1")
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[MockExecPlan].name shouldEqual ("rules2")
  }

  it("should generate Exec plan for Metadata query") {
    val lp = Parser.metadataQueryToLogicalPlan("http_requests_total{job=\"prometheus\", method=\"GET\"}",
      TimeStepParams(1000, 10, 2000))

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
    execPlan.asInstanceOf[PartKeysDistConcatExec].children.length shouldEqual(3)

    // For Raw
    execPlan.asInstanceOf[PartKeysDistConcatExec].children(0).isInstanceOf[PartKeysDistConcatExec] shouldEqual true
    execPlan.asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysDistConcatExec].children.
      forall(_.isInstanceOf[PartKeysExec]) shouldEqual true

    execPlan.asInstanceOf[PartKeysDistConcatExec].children(1).asInstanceOf[MockExecPlan].name shouldEqual ("rules1")
    execPlan.asInstanceOf[PartKeysDistConcatExec].children(2).asInstanceOf[MockExecPlan].name shouldEqual ("rules2")
  }

  it("should generate Exec plan for Scalar query which does not have any metric") {
    val lp = Parser.queryToLogicalPlan("time()", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[TimeScalarGeneratorExec] shouldEqual true
  }

  it("should generate BinaryJoin Exec with remote exec's having lhs or rhs query") {
    val lp = Parser.queryRangeToLogicalPlan("""test1{job = "app"} + test2{job = "app"}""", TimeStepParams(300, 20, 500))

    val promQlQueryParams = PromQlQueryParams("test1{job = \"app\"} + test2{job = \"app\"}", 300, 20, 500)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    println(execPlan.printTree())
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.children(0).isInstanceOf[PromQlRemoteExec] shouldEqual(true)
    execPlan.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual(true)

    // LHS should have only LHS query and RHS should have oly RHS query
    execPlan.children(0).asInstanceOf[PromQlRemoteExec].params.promQl shouldEqual("""test1{job="app"}""")
    execPlan.children(1).asInstanceOf[PromQlRemoteExec].params.promQl shouldEqual("""test2{job="app"}""")
  }
}

