//// scalastyle:off
//package filodb.jmh
//
//import java.util.concurrent.TimeUnit
//import akka.actor.ActorSystem
//import akka.testkit.TestProbe
//import com.typesafe.config.ConfigFactory
//import com.typesafe.scalalogging.Logger
//import org.openjdk.jmh.annotations._
//
//import scala.concurrent.duration._
//import filodb.coordinator.ShardMapper
//import filodb.coordinator.client.QueryCommands.{FunctionalSpreadProvider, FunctionalTargetSchemaProvider}
//import filodb.coordinator.queryplanner.SingleClusterPlanner
//import filodb.core.{MetricsTestData, SpreadChange, SpreadProvider, TargetSchemaChange}
//import filodb.core.metadata.Schemas
//import filodb.core.query.{PlannerParams, PromQlQueryParams, QueryConfig, QueryContext}
//import filodb.prometheus.ast.TimeStepParams
//import filodb.prometheus.parse.Parser
//import filodb.query.exec.ExecPlan
//
//@State(Scope.Thread)
//class PlannerBenchmark {
//
//  // Run using the following in sbt
//  // jmh/jmh:run  -rf json -i 5 -wi 3 -f 1 -jvmArgsAppend -XX:MaxInlineLevel=20 -jvmArgsAppend -Xmx4g -jvmArgsAppend -XX:MaxInlineSize=99  -jvmArgsAppend -Dlogback.configurationFile=<filodb local path>/conf/logback-dev.xml  filodb.jmh.PlannerBenchmark -prof "async:libPath=<async profiler path>/lib/libasyncProfiler.dylib;output=jfr;alloc=0"
//
//
//  var system: Option[ActorSystem] = None
//  var planner: Option[SingleClusterPlanner] = None
//  var now = 0L;
//  val logger: Logger = Logger("PlannerBenchmark")
//  private val spreadProvider: Option[SpreadProvider] = Some(
//                    FunctionalSpreadProvider(
//                      _ => Seq(SpreadChange(0, 8)))
//                    )
//  private val tSchemaProvider: Option[FunctionalTargetSchemaProvider] = Some(
//                    FunctionalTargetSchemaProvider(
//                      _ => Seq(TargetSchemaChange(0, Seq("job", "app")))
//                    )
//                  )
//
//
//  var execPlan: Option[ExecPlan] = None
//
//
//  val query = """    foo { job="baz" , node!="", ns="ns"}
//                |      OR on (app)
//                |    bar { job="baz", node!="", ns="ns" }
//                |      * on (app) group_right()
//                |    baz{ job="baz", node!="", ns="ns" } == 1""".stripMargin
//
//
//  private def buildPlanners(): Unit = {
//    implicit val system: ActorSystem = ActorSystem()
//    this.system = Some(system)
//    val node = TestProbe().ref
//    val mapper = new ShardMapper(256)
//    for (i <- 0 until 256) {
//      mapper.registerNode(Seq(i), node)
//    }
//    this.now = System.currentTimeMillis()
//
//    val rawRetention = 7.days.toMillis
//
//    val routingConfigString = "routing {\n  remote {\n    http {\n      timeout = 10000\n    }\n  }\n}"
//    val routingConfig = ConfigFactory.parseString(routingConfigString)
//    val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").withFallback(routingConfig)
//    val queryConfig = QueryConfig(config).copy(plannerSelector = Some("plannerSelector"))
//
//    val dataset = MetricsTestData.timeseriesDataset
//    val schemas = Schemas(dataset.schema)
//
//    planner = Some(new SingleClusterPlanner(dataset, schemas, mapper,
//        earliestRetainedTimestampFn = now - rawRetention, queryConfig, "raw"))
//  }
//
//  @Setup(Level.Trial)
//  def setup(): Unit = {
//    buildPlanners()
//  }
//
//  @TearDown(Level.Trial)
//  def teardown(): Unit = {
//    this.system.foreach(_.terminate())
////    println("\n\n===================================\n\n")
////    print(s"${execPlan.get.printTree()}")
//  }
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.AverageTime))
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  @Warmup(iterations = 1, time = 1)
//  @Measurement(iterations = 5, time = 1)
//  @throws[Exception]
//  def benchmarkMaterializePlan(): Unit = {
//
//    @scala.annotation.unused var i = 0;
//    // Materialize the query every hour for past 5 days
//    for (endTime <- (now - 3.hour.toMillis) to  now by 1.hour.toMillis) {
//        val endSecs = endTime / 1000;
//        val timeParams = TimeStepParams(endSecs - 1.day.toSeconds, 60, endSecs)
//        val lp = Parser.queryRangeToLogicalPlan(query, timeParams)
//        execPlan = Some(planner.get.materialize(lp,
//            QueryContext(PromQlQueryParams("dummy", timeParams.start, timeParams.step, timeParams.end),
//                         plannerParams = PlannerParams(
//                                            spreadOverride = spreadProvider,
//                                            targetSchemaProviderOverride = tSchemaProvider,
//                                             queryTimeoutMillis = 1000000))))
//    }
//  }
//
//}
//
//
//
//
//
