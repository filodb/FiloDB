package filodb.coordinator.flight

import java.util
import java.util.Optional
import java.util.concurrent.Executors

import akka.actor.{ActorSystem, Address, RootActorPath}
import akka.serialization.SerializationExtension
import com.typesafe.config.ConfigFactory
import io.grpc.netty.NettyServerBuilder
import io.grpc.{Server, ServerBuilder}
import org.apache.arrow.flight.auth.ServerAuthHandler
import org.apache.arrow.flight.{FlightGrpcUtils, Location, Ticket}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core.MachineMetricsData.records
import filodb.core.MetricsTestData.{timeSeriesData, timeseriesDatasetWithMetric}
import filodb.core.memstore.TimeSeriesMemStore
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.core.{SpreadChange, TestData}
import filodb.grpc.GrpcMultiPartitionQueryService
import filodb.memory.format.ZeroCopyUTF8String.StringToUTF8
import filodb.query.exec._
import filodb.query.{ProtoConverters, QueryResult}

/**
 * End-to-end tests for the FiloDBMultiPartitionFlightProducer path:
 *   Flight client → FiloDBMultiPartitionFlightProducer → SingleClusterPlanner
 *     → FlightPlanDispatcher → FiloDBSinglePartitionFlightProducer → TimeSeriesMemStore
 *
 * Port allocation (distinct from FiloDBSinglePartitionFlightProducerSpec which uses 33815):
 *   - Akka remote:                           33816
 *   - FiloDBSinglePartitionFlightProducer:   38816  (akkaPort + 5000)
 *   - FiloDBMultiPartitionFlightProducer:    48816
 */
class FiloDBMultiPartitionFlightProducerSpec extends AnyFunSpec
    with Matchers with BeforeAndAfterAll with ScalaFutures {

  System.setProperty("arrow.memory.debug.allocator", "true")
  implicit val s: monix.execution.Scheduler = monix.execution.Scheduler.Implicits.global
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(50, Millis))

  private val akkaPort          = 33816
  private val multiPartPort     = 48816
  private val multiPartLocation = Location.forGrpcInsecure("localhost", multiPartPort)

  // -----------------------------------------------------------------------
  // Config: port 33816 with explicit hostname so actor refs carry 127.0.0.1
  // -----------------------------------------------------------------------
  val config = ConfigFactory.parseString(
    s"""
       |filodb.memstore.max-partitions-on-heap-per-shard = 1100
       |filodb.memstore.ensure-block-memory-headroom-percent = 10
       |filodb.memstore.ensure-tsp-count-headroom-percent = 10
       |filodb.memstore.ensure-native-memory-headroom-percent = 10
       |filodb.memstore.index-updates-publishing-enabled = true
       |akka.remote.netty.tcp.port = $akkaPort
       |akka.remote.netty.tcp.hostname = "127.0.0.1"
       |""".stripMargin)
    .withFallback(ConfigFactory.load("application_test.conf"))
    .resolve()

  // -----------------------------------------------------------------------
  // Step 1: TimeSeriesMemStore + data ingestion
  // -----------------------------------------------------------------------
  private val memStore = new TimeSeriesMemStore(
    config.getConfig("filodb"), new NullColumnStore, new NullColumnStore, new InMemoryMetaStore())
  // Important: need 2 shards since default spread is 1, and PlannerParams does not propagate spread in proto
  // Ingest only in one shard for now. Second is empty
  memStore.setup(timeseriesDatasetWithMetric.ref, Schemas(timeseriesDatasetWithMetric.schema), 0,
    TestData.storeConf, 2)
  memStore.setup(timeseriesDatasetWithMetric.ref, Schemas(timeseriesDatasetWithMetric.schema), 1,
    TestData.storeConf, 2)
  private val rawData =
    timeSeriesData(Map("_ns_".utf8 -> "timeseries".utf8, "host".utf8 -> "host1".utf8, "region".utf8 -> "region1".utf8)).take(1000) ++
    timeSeriesData(Map("_ns_".utf8 -> "timeseries".utf8, "host".utf8 -> "host2".utf8, "region".utf8 -> "region1".utf8)).take(1000)
  private val ingestedData = records(timeseriesDatasetWithMetric, rawData)
  memStore.ingest(timeseriesDatasetWithMetric.ref, 0, ingestedData)
  memStore.refreshIndexForTesting(timeseriesDatasetWithMetric.ref)

  // -----------------------------------------------------------------------
  // Step 2: Start FiloDBSinglePartitionFlightProducer (leaf executor, port 38816)
  // -----------------------------------------------------------------------
  private val singlePartServer = FiloDBSinglePartitionFlightProducer.start(memStore, config)

  // -----------------------------------------------------------------------
  // Step 3: Build a remote actor ref whose path.address carries 127.0.0.1:33816
  //         so that akkaActorToFlightLocation() derives Location(127.0.0.1, 38816).
  //         We use resolveActorRef with a remote path so the address includes host+port.
  //         (Local TestProbe refs do NOT carry host:port in their path.address.)
  // -----------------------------------------------------------------------
  private val actorSystem = ActorSystem("filodb-test-multi", config)
  private val remoteShardAddr = Address("akka.tcp", "filodb-shard", "127.0.0.1", akkaPort)
  // SerializationExtension.system exposes ExtendedActorSystem.provider — same pattern as RemoteActorPlanDispatcher
  private val shardRef = SerializationExtension(actorSystem).system.provider.resolveActorRef(
    (RootActorPath(remoteShardAddr) / "user" / "shardCoord").toString)
  require(shardRef.path.address.host.isDefined,
    s"Actor ref must have a remote address; got ${shardRef.path.address}")

  private val mapper = new ShardMapper(2)
  mapper.registerNode(Seq(0), shardRef)
  mapper.registerNode(Seq(1), shardRef)

  // -----------------------------------------------------------------------
  // Step 4: SingleClusterPlanner with flightEnabled=true
  //         Leaf MSPEs will be dispatched via FlightPlanDispatcher(38816)
  // -----------------------------------------------------------------------
  private val queryConfig = QueryConfig(config.getConfig("filodb.query"))
  private val planner = new SingleClusterPlanner(
    timeseriesDatasetWithMetric,
    Schemas(timeseriesDatasetWithMetric.schema),
    mapper,
    0L,
    queryConfig,
    clusterName = "testCluster",
    flightEnabled = true
  )

  // -----------------------------------------------------------------------
  // Step 5: FiloDBMultiPartitionFlightProducer + plain gRPC server (port 48816)
  // -----------------------------------------------------------------------
  private val producer = new FiloDBMultiPartitionFlightProducer(
    _ => planner,                       // selector string is ignored; always use our planner
    FlightAllocator.serverAllocator,
    multiPartLocation,
    config)
  private val multiPartServer = startGrpcServer(producer, multiPartPort)

  // Allocator for test clients — child of root so leaks are detectable
  private val testAllocator =
    FlightAllocator.newChildAllocatorForTesting("MultiPartFlightClientTest", 0, 10_000_000L)

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------
  override def afterAll(): Unit = {
    multiPartServer.shutdown()
    singlePartServer.shutdown()
    memStore.shutdown()
    actorSystem.terminate()
    // Global allocators are shared across test suites; leave them open
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private def startGrpcServer(prod: FiloDBMultiPartitionFlightProducer, port: Int): Server = {
    val noAuth = new ServerAuthHandler {
      override def isValid(token: Array[Byte]): Optional[String] = Optional.of("")
      override def authenticate(out: ServerAuthHandler.ServerAuthSender,
                                in: util.Iterator[Array[Byte]]): Boolean = true
    }
    val executor = Executors.newCachedThreadPool()
    val svc = FlightGrpcUtils.createFlightService(
      FlightAllocator.serverAllocator, prod, noAuth, executor)
    ServerBuilder.forPort(port)
      .asInstanceOf[ServerBuilder[NettyServerBuilder]]
      .addService(svc)
      .build()
      .start()
  }

  /**
   * Dispatch a PromQL range query through the multi-partition Flight server and
   * return the QueryResult.  Uses PromQLFlightRemoteExec so that FlightPlanDispatcher
   * wraps the query as a GrpcMultiPartitionQueryService.Request ticket.
   */
  private def executeViaMultiPartFlightServer(
    promQL: String,
    startSecs: Long = 0L,
    stepSecs: Long  = 1L,
    endSecs: Long   = 100L
  ): QueryResult = {
    val qCtx = QueryContext(
      origQueryParams = PromQlQueryParams(promQL, startSecs, stepSecs, endSecs),
      plannerParams   = PlannerParams(spreadOverride = Some(StaticSpreadProvider(SpreadChange(0, 0)))))
    // unfortunately spread 0 above does not take effect since the proto request does not have the field,
    // and does not get propagated to remote planner
    val remoteExec = PromQLFlightRemoteExec(
      queryContext          = qCtx,
      dispatcher            = FlightPlanDispatcher(multiPartLocation, "testCluster"),
      queryEndpoint         = multiPartLocation.getUri.toString,
      requestTimeoutMs      = 60000,
      dataset               = timeseriesDatasetWithMetric.ref,
      plannerSelector       = "testCluster",
      destinationTsdbWorkUnit = "test")
    val querySession = QuerySession(qCtx, QueryConfig.unitTestingQueryConfig,
      flightAllocator = Some(new FlightAllocator(testAllocator)))
    remoteExec.dispatcher
      .dispatch(ExecPlanWithClientParams(remoteExec, ClientParams(60000), querySession),
        UnsupportedChunkSource())
      .runToFuture
      .futureValue
      .asInstanceOf[QueryResult]
  }

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------
  describe("FiloDBMultiPartitionFlightProducer — end-to-end via Flight") {

    it("should execute a range query and return 2 series via multi-partition Flight producer") {
      val allocBefore = testAllocator.getAllocatedMemory

      // Query for all cpu_usage series in region1; step=1s, range 0-100s → 101 timestamps
      val qRes = executeViaMultiPartFlightServer("""cpu_usage{region="region1", _ns_="timeseries"}""")

      qRes.result.length shouldEqual 2

      // Each series must have 101 data points at 0, 1000, 2000, ..., 100000 ms
      val expectedTs = (0 to 100000 by 1000).toList
      qRes.result.foreach { rv =>
        rv.rows().map(_.getLong(0)).toList shouldEqual expectedTs
      }

      qRes.result.foreach(_.asInstanceOf[ArrowSerializedRangeVector].vsrs.foreach(_.close()))
      testAllocator.getAllocatedMemory shouldEqual allocBefore
    }

    it("should execute count aggregation and return count=2 at every step") {
      val allocBefore = testAllocator.getAllocatedMemory

      val qRes = executeViaMultiPartFlightServer("""count(cpu_usage{region="region1", _ns_="timeseries"})""")

      qRes.result.length shouldEqual 1
      val rows = qRes.result.head.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
      rows should not be empty
      rows.foreach { case (_, v) => v shouldEqual 2.0 }

      qRes.result.foreach(_.asInstanceOf[ArrowSerializedRangeVector].vsrs.foreach(_.close()))
      testAllocator.getAllocatedMemory shouldEqual allocBefore
    }

    it("should propagate a parse error when the PromQL is invalid") {
      import ProtoConverters._
      // Build a bad request directly as proto bytes and send to the multi-partition server
      val badRequest = GrpcMultiPartitionQueryService.Request.newBuilder()
        .setQueryParams(PromQlQueryParams("bad{{{syntax", 0L, 1L, 100L).toProto)
        .setPlannerParams(PlannerParams().toProto)
        .setPlannerSelector("testCluster")
        .build()

      val flightClient = FlightClientManager.global.getClient(multiPartLocation)
      val ticket = new Ticket(badRequest.toByteArray)

      // The server calls listener.error(); the client observes it as an exception on stream.next()
      val ex = intercept[Exception] {
        val stream = flightClient.getStream(ticket)
        try { while (stream.next()) {} }
        finally { stream.close() }
      }
      ex should not be null
    }

    it("should execute a filtered query targeting a single series (label-value filtering)") {
      val allocBefore = testAllocator.getAllocatedMemory

      // Filter to only host1 — should return exactly 1 series
      val qRes = executeViaMultiPartFlightServer("""cpu_usage{region="region1", host="host1", _ns_="timeseries"}""")

      qRes.result.length shouldEqual 1

      val expectedTs = (0 to 100000 by 1000).toList
      qRes.result.head.rows().map(_.getLong(0)).toList shouldEqual expectedTs

      qRes.result.foreach(_.asInstanceOf[ArrowSerializedRangeVector].vsrs.foreach(_.close()))
      testAllocator.getAllocatedMemory shouldEqual allocBefore
    }
  }
}
