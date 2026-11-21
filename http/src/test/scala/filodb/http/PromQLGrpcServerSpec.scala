package filodb.http

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator.{FilodbSettings, GrpcPlanDispatcher}
import filodb.coordinator.queryplanner.QueryPlanner
import filodb.core.MetricsTestData.timeseriesDatasetWithMetric
import filodb.core.query._
import filodb.grpc.GrpcCommonUtils
import filodb.http.RecordingQueryCompletionListener.awaitExactlyOnce
import filodb.query.{LogicalPlan, QueryResult}
import filodb.query.exec._

/**
 * Tests of the PromQL gRPC endpoints of [[PromQLGrpcServer]].
 */
class PromQLGrpcServerSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val s: Scheduler = Scheduler.Implicits.global
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(50, Millis))

  private val grpcPort = 48818

  private val settings = new FilodbSettings(
    ConfigFactory.parseString(
      s"""
         |filodb.grpc.bind-grpc-port = $grpcPort
         |server.flight-client-enabled = false
         |""".stripMargin).withFallback(ConfigFactory.load("application_test.conf")).resolve())

  /** Planner whose plans always return an empty result, whatever the query. */
  private class EmptyResultPlanner extends QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan =
      EmptyResultExec(qContext, timeseriesDatasetWithMetric.ref,
        new InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig))
  }

  private val listener = new RecordingQueryCompletionListener
  private val server = new PromQLGrpcServer(_ => new EmptyResultPlanner, settings, Scheduler.global, listener)
  server.start()

  override def afterAll(): Unit = {
    server.stop()
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private def queryContext(promQL: String = "up"): QueryContext =
    QueryContext(origQueryParams = PromQlQueryParams(promQL, 0L, 1L, 100L))

  private def execViaGrpc(qCtx: QueryContext): Unit = {
    val channel = GrpcCommonUtils.buildChannelFromEndpoint(s"localhost:$grpcPort")
    try {
      val remoteExec = PromQLGrpcRemoteExec(
        channel                 = channel,
        requestTimeoutMs        = 60000,
        queryContext            = qCtx,
        dispatcher              = new InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig),
        dataset                 = timeseriesDatasetWithMetric.ref,
        plannerSelector         = "testSelector",
        destinationTsdbWorkUnit = "test")
      val querySession = QuerySession(qCtx, QueryConfig.unitTestingQueryConfig)
      remoteExec.dispatcher
        .dispatch(ExecPlanWithClientParams(remoteExec, ClientParams(60000), querySession),
          UnsupportedChunkSource())
        .runToFuture
        .futureValue shouldBe a[QueryResult]
    } finally {
      channel.shutdownNow()
    }
  }

  private def executePlanViaGrpc(qCtx: QueryContext): Unit = {
    val emptyPlan = EmptyResultExec(qCtx, timeseriesDatasetWithMetric.ref,
      new InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig))
    val dispatcher = GrpcPlanDispatcher(s"localhost:$grpcPort", 60000)
    val genericRemoteExec = GenericRemoteExec(dispatcher, emptyPlan)
    val querySession = QuerySession(qCtx, QueryConfig.unitTestingQueryConfig)
    dispatcher
      .dispatch(ExecPlanWithClientParams(genericRemoteExec, ClientParams(60000), querySession),
        UnsupportedChunkSource())
      .runToFuture
      .futureValue shouldBe a[QueryResult]
  }

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------
  describe("PromQLGrpcServer - QueryCompletionListener") {

    it("should be notified exactly once for exec/execStreaming requests") {
      listener.clear()
      execViaGrpc(queryContext())

      awaitExactlyOnce(listener)
    }

    it("should be notified exactly once for executePlan requests") {
      listener.clear()
      val qCtx = queryContext()
      executePlanViaGrpc(qCtx)

      awaitExactlyOnce(listener)
    }

    it("should still return the query response when the listener throws") {
      listener.clear()
      listener.failOnNext = true
      execViaGrpc(queryContext())

      awaitExactlyOnce(listener)
      listener.failOnNext shouldEqual false // the throwing callback did run
    }
  }
}
