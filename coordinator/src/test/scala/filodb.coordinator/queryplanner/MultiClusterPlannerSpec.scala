package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler
import org.scalatest.{FunSpec, Matchers}

import filodb.coordinator.ShardMapper
import filodb.core.{DatasetRef, MetricsTestData}
import filodb.core.metadata.Schemas
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext, QuerySession}
import filodb.core.store.ChunkSource
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._

class MultiClusterPlannerSpec extends FunSpec with Matchers{
  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val localMapper = new ShardMapper(32)
  for {i <- 0 until 32} localMapper.registerNode(Seq(i), node)

  private val remoteMapper = new ShardMapper(16)
  for {i <- 0 until 32} localMapper.registerNode(Seq(i), node)

  private val dataset = MetricsTestData.timeseriesDataset
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)

  private val routingConfigString = "routing {\n  buddy {\n    http {\n      timeout = 10.seconds\n    }\n  }\n}"
  private val routingConfig = ConfigFactory.parseString(routingConfigString)
  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").
    withFallback(routingConfig)
  private val queryConfig = new QueryConfig(config)

  private val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty, "sum(heap_usage)", 100, 1, 1000, None)

  val localPlanner = new SingleClusterPlanner(dsRef, schemas, localMapper, earliestRetainedTimestampFn = 0, queryConfig)
  val remotePlanner = new SingleClusterPlanner(dsRef, schemas, remoteMapper, earliestRetainedTimestampFn = 0,
    queryConfig)

  val failureProvider = new FailureProvider {
    override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
      Seq(FailureTimeRange("local", datasetRef,
        TimeRange(100, 10000), false))
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

    override def getBasePlanner: SingleClusterPlanner = remotePlanner
  }

  val rrPlanner2 = new QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      new MockExecPlan("rules2", logicalPlan)
    }

    override def getBasePlanner: SingleClusterPlanner = remotePlanner
  }

  val plannerProvider = new PlannerProvider {
    override def getRemotePlanner(metricName: String): Option[QueryPlanner] =
      if (metricName.equals("rr1")) Some(rrPlanner1)
      else if (metricName.equals("rr2")) Some(rrPlanner2) else None

    override def getAllRemotePlanners: Seq[QueryPlanner] = Seq(rrPlanner1, rrPlanner2)
  }

  val engine = new MultiClusterPlanner(plannerProvider, highAvailabilityPlanner)

  it("should generate Exec plan for simple query") {
    val lp = Parser.queryToLogicalPlan("test{job = \"app\"}", 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[DistConcatExec] shouldEqual (true)
    execPlan.children.length shouldEqual 2
    execPlan.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    execPlan.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true
  }

  it("should generate BinaryJoin Exec plan") {
    val lp = Parser.queryToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"}", 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.children.foreach { l1 =>
      l1.isInstanceOf[DistConcatExec] shouldEqual true
      l1.children.foreach { l2 =>
        l2.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        l2.rangeVectorTransformers.size shouldEqual 1
        l2.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      }
    }
  }

  it("should generate BinaryJoin Exec plan with remote and local cluster metrics") {
    val lp = Parser.queryToLogicalPlan("test{job = \"app\"} + rr1{job = \"app\"}", 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[MockExecPlan].name shouldEqual ("rules1")
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[DistConcatExec] shouldEqual true
  }

  it("should generate BinaryJoin Exec plan with remote cluster metrics") {
    val lp = Parser.queryToLogicalPlan("rr1{job = \"app\"} + rr2{job = \"app\"}", 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.asInstanceOf[MockExecPlan].name shouldEqual ("rules1")
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[MockExecPlan].name shouldEqual ("rules2")
  }
}

