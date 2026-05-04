package filodb.jmh

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.{ActorSystem, Address, RootActorPath}
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.StrictLogging
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import monix.execution.Scheduler
import org.apache.arrow.flight.Location
import org.openjdk.jmh.annotations._

import filodb.coordinator._
import filodb.coordinator.client.Client.actorAsk
import filodb.coordinator.client.QueryCommands._
import filodb.coordinator.flight._
import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core.SpreadChange
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.StoreConfig
import filodb.http.PromQLGrpcServer
import filodb.query.{QueryError, QueryResult}
import filodb.query.exec._
import filodb.timeseries.TestTimeseriesProducer

/**
 * JMH benchmark comparing two PromQL multi-partition query execution paths:
 *
 *   A. Flight path  — PromQLGrpcServer (flight-multi-partition-service-enabled=true)
 *                     → FiloDBMultiPartitionFlightProducer
 *                     → SingleClusterPlanner(flightEnabled=true)
 *                     → FlightPlanDispatcher
 *                     → FiloDBSinglePartitionFlightProducer
 *                     → TimeSeriesMemStore
 *
 *   B. Akka/gRPC path — PromQLGrpcServer (flight disabled)
 *                       → PromQLGrpcService (execStreaming)
 *                       → SingleClusterPlanner(flightEnabled=false)
 *                       → ActorPlanDispatcher
 *                       → NodeCoordinatorActor / QueryActor
 *                       → TimeSeriesMemStore
 *
 * Both servers are backed by the same FilodbCluster / TimeSeriesMemStore,
 * ensuring a fair comparison of transport overhead.
 *
 * Port layout (avoids collisions with existing tests on 33815, 33816, 8888):
 *   akkaPort               = 33824   (Akka remote TCP)
 *   flightSinglePartPort   = 38824   (FiloDBSinglePartitionFlightProducer = akkaPort + 5000)
 *   flightGrpcPort         = 48824   (PromQLGrpcServer — flight path)
 *   akkaGrpcPort           = 58824   (PromQLGrpcServer — Akka/gRPC path)
 *
 * Run with:
 *   sbt "jmh/jmh:run -rf json -i 5 -wi 3 -f 1 \
 *     -jvmArgsAppend -Xmx6g \
 *     filodb.jmh.FlightMultiPartitionBenchmark"
 */
@State(Scope.Benchmark)
class FlightMultiPartitionBenchmark extends StrictLogging {

  {
    import ch.qos.logback.classic.encoder.PatternLayoutEncoder
    import ch.qos.logback.core.ConsoleAppender
    val ctx = org.slf4j.LoggerFactory.getILoggerFactory
      .asInstanceOf[ch.qos.logback.classic.LoggerContext]
    ctx.reset()   // wipe whatever logback-test.xml loaded
    val enc = new PatternLayoutEncoder
    enc.setContext(ctx)
    enc.setPattern("[%date{ISO8601}] %-5level %logger{36} - %msg%n")
    enc.start()
    val app = new ConsoleAppender[ch.qos.logback.classic.spi.ILoggingEvent]
    app.setContext(ctx)
    app.setTarget("System.err")
    app.setEncoder(enc)
    app.start()
    val root = ctx.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    root.setLevel(ch.qos.logback.classic.Level.ERROR)
    root.addAppender(app)
  }

  import NodeClusterActor._
  import monix.execution.Scheduler.Implicits.global

  // -------------------------------------------------------------------------
  // Benchmark parameters
  // -------------------------------------------------------------------------
  private val numShards        = 2
  private val numSeries        = 50
  private val numSamples       = 360           // 1 hour @ 10-second intervals
  private val spread           = 1             // 2^1 = 2 shards per query
  private val numQueries       = 50            // concurrent queries per invocation
  private val akkaPort         = 33824
  private val flightGrpcPort   = 48824
  private val akkaGrpcPort     = 58824

  private val startTime        = System.currentTimeMillis - (3600 * 1000)
  private val queryIntervalMin = 55
  private val queryStep        = 150           // step in seconds

  // -------------------------------------------------------------------------
  // Shared config — explicit akka TCP port so FiloDBSinglePartitionFlightProducer
  // derives the correct flight port (akkaPort + 5000 = 38824).
  // -------------------------------------------------------------------------
  val config = ConfigFactory.parseString(
      s"""
         |akka.remote.netty.tcp.port     = $akkaPort
         |akka.remote.netty.tcp.hostname = "127.0.0.1"
         |filodb.memstore.ingestion-buffer-mem-size = 30MB
         |filodb.memstore.max-partitions-on-heap-per-shard = 1100
         |""".stripMargin)
    .withFallback(ConfigFactory.load("filodb-defaults.conf"))
    .resolve()

  // -------------------------------------------------------------------------
  // FiloDB cluster — provides the coordinator actors (Akka path) and the
  // TimeSeriesMemStore reused by FiloDBSinglePartitionFlightProducer (flight path).
  // -------------------------------------------------------------------------
  val system       = ActorSystem("flight-benchmark", config)
  val cluster      = FilodbCluster(system)
  cluster.join()
  val coordinator  = cluster.coordinatorActor
  val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)

  val dataset      = TestTimeseriesProducer.dataset
  val schemas      = Schemas(dataset.schema)
  val queryConfig  = QueryConfig(cluster.settings.allConfig.getConfig("filodb.query"))

  // -------------------------------------------------------------------------
  // Dataset setup — registers shards with the coordinator so QueryActors are
  // ready to execute ActorPlanDispatcher-dispatched leaf plans.
  // -------------------------------------------------------------------------
  private val storeConf = StoreConfig(ConfigFactory.parseString(
    """
      | flush-interval        = 1h
      | shard-mem-size        = 96MB
      | groups-per-shard      = 4
      | demand-paging-enabled = false
    """.stripMargin))

  Await.result(cluster.metaStore.initialize(), 3.seconds)
  val setupCmd = SetupDataset(dataset, DatasetResourceSpec(numShards, 1), noOpSource, storeConf, overrideSchema = false)
  actorAsk(clusterActor, setupCmd) { case DatasetVerified => logger.info("dataset setup complete") }
  coordinator ! setupCmd

  // -------------------------------------------------------------------------
  // Akka shard mapper — every shard maps to the real coordinator actor.
  // Used by akkaPlanner (ActorPlanDispatcher path) and for data ingestion routing.
  // -------------------------------------------------------------------------
  private val akkaShardMapper = new ShardMapper(numShards)
  (0 until numShards).foreach { s =>
    akkaShardMapper.updateFromEvent(IngestionStarted(dataset.ref, s, coordinator))
  }

  // -------------------------------------------------------------------------
  // Flight shard mapper — uses a fake remote actor ref so that
  // akkaActorToFlightLocation can extract host:port from the actor path address.
  // Local actors have no host in their path, so None.get would fail otherwise.
  // The fake ref carries address 127.0.0.1:akkaPort; akkaActorToFlightLocation
  // then derives the flight port as akkaPort+5000 = 38824.
  // -------------------------------------------------------------------------
  private val fakeRemoteAddr    = Address("akka.tcp", "filodb-shard", "127.0.0.1", akkaPort)
  // SerializationExtension.system exposes ExtendedActorSystem.provider — same pattern as RemoteActorPlanDispatcher
  private val fakeCoordinatorRef = SerializationExtension(system).system.provider.resolveActorRef(
    (RootActorPath(fakeRemoteAddr) / "user" / "filoDB-coordinator").toString)
  require(fakeCoordinatorRef.path.address.host.isDefined,
    s"Actor ref must have a remote address; got ${fakeCoordinatorRef.path.address}")

  private val flightShardMapper = new ShardMapper(numShards)
  (0 until numShards).foreach { s =>
    flightShardMapper.registerNode(Seq(s), fakeCoordinatorRef)
  }

  // -------------------------------------------------------------------------
  // Data ingestion — pump data directly into the cluster memStore
  // -------------------------------------------------------------------------
  Thread.sleep(2000)  // give SetupDataset time to propagate to coordinator

  private val (producingFut, containerStream) = TestTimeseriesProducer.metricsToContainerStream(
    startTime, numShards, numSeries, numMetricNames = 1, numSamples * numSeries,
    dataset, akkaShardMapper, spread, publishIntervalSec = 10)

  private val ingestTask = containerStream.groupBy(_._1)
    .mapParallelUnordered(numShards) { groupedStream =>
      val shard = groupedStream.key
      val shardStream = groupedStream.zipWithIndex.flatMap { case ((_, bytes), idx) =>
        val data = bytes.map { arr => SomeData(RecordContainer(arr), idx) }
        monix.reactive.Observable.fromIterable(data)
      }
      monix.eval.Task.fromFuture(cluster.memStore.startIngestion(dataset.ref, shard, shardStream, global))
    }.countL.runToFuture

  Await.result(producingFut, 30.seconds)
  Thread.sleep(2000)
  cluster.memStore.asInstanceOf[TimeSeriesMemStore].refreshIndexForTesting(dataset.ref)
  logger.info("FlightMultiPartitionBenchmark: ingestion complete")

  // -------------------------------------------------------------------------
  // Planners — differ in shardMapper and flightEnabled flag.
  //   flightPlanner: flightShardMapper (fake remote refs) + flightEnabled=true
  //                  → FlightPlanDispatcher targeting 127.0.0.1:38824
  //   akkaPlanner:   akkaShardMapper (real local refs)  + flightEnabled=false
  //                  → ActorPlanDispatcher → coordinator actor
  // -------------------------------------------------------------------------
  private val flightPlanner = new SingleClusterPlanner(
    dataset, schemas, flightShardMapper, 0L, queryConfig,
    clusterName = "benchmark-flight", flightEnabled = true)

  private val akkaPlanner = new SingleClusterPlanner(
    dataset, schemas, akkaShardMapper, 0L, queryConfig,
    clusterName = "benchmark-akka", flightEnabled = false)

  // -------------------------------------------------------------------------
  // FiloDBSinglePartitionFlightProducer — leaf executor for the flight path.
  // Reads directly from the same TimeSeriesMemStore used by the Akka path.
  // Binds to akkaPort + 5000 = 38824.
  // -------------------------------------------------------------------------
  private val memStore               = cluster.memStore.asInstanceOf[TimeSeriesMemStore]
  private val singlePartFlightServer = FiloDBSinglePartitionFlightProducer.start(memStore, config)

  // -------------------------------------------------------------------------
  // PromQLGrpcServer — flight-enabled (also serves FiloDBMultiPartitionFlightProducer)
  // -------------------------------------------------------------------------
  private val flightSvrConfig = config
    .withValue("filodb.grpc.bind-grpc-port",
      ConfigValueFactory.fromAnyRef(flightGrpcPort))
    .withValue("filodb.grpc.flight-multi-partition-service-enabled",
      ConfigValueFactory.fromAnyRef(true))
    .withValue("server.flight-client-enabled",
      ConfigValueFactory.fromAnyRef(true))
    .withValue("server.flight-per-request-max-memory-bytes",
      ConfigValueFactory.fromAnyRef(52428800))   // 50 MB per request

  private val flightSettings   = new FilodbSettings(flightSvrConfig)
  private val flightGrpcServer = new PromQLGrpcServer(_ => flightPlanner, flightSettings, Scheduler.global)
  flightGrpcServer.start()

  // -------------------------------------------------------------------------
  // PromQLGrpcServer — Akka/non-flight path (standard gRPC streaming, no Arrow Flight)
  // -------------------------------------------------------------------------
  private val akkaSvrConfig = config
    .withValue("filodb.grpc.bind-grpc-port",
      ConfigValueFactory.fromAnyRef(akkaGrpcPort))
    .withValue("filodb.grpc.flight-multi-partition-service-enabled",
      ConfigValueFactory.fromAnyRef(false))
    .withValue("server.flight-client-enabled",
      ConfigValueFactory.fromAnyRef(false))

  private val akkaSettings   = new FilodbSettings(akkaSvrConfig)
  private val akkaGrpcServer = new PromQLGrpcServer(_ => akkaPlanner, akkaSettings, Scheduler.global)
  akkaGrpcServer.start()

  // -------------------------------------------------------------------------
  // Clients
  // -------------------------------------------------------------------------
  private val flightMultiPartLocation = Location.forGrpcInsecure("localhost", flightGrpcPort)

  // Shared root child-allocator for all flight client-side Arrow buffers.
  // Individual per-invocation child allocators are carved from this.
  private val flightRootAllocator =
    FlightAllocator.newChildAllocatorForTesting("FlightBenchmarkRoot", 0, 500_000_000L)

  // Non-flight gRPC channel (reused across invocations — gRPC channels are thread-safe)
  private val akkaGrpcChannel = NettyChannelBuilder
    .forAddress("127.0.0.1", akkaGrpcPort)
    .negotiationType(NegotiationType.PLAINTEXT)
    .maxInboundMessageSize(104857600)
    .build()

  // -------------------------------------------------------------------------
  // Query parameters shared by all benchmark methods
  // -------------------------------------------------------------------------
  private val promQL     = """heap_usage0{_ws_="demo",_ns_="App-2"}"""
  private val queryStart = startTime / 1000 + (7 * 60)
  private val queryEnd   = queryStart + queryIntervalMin * 60

  private val spreadProvider = StaticSpreadProvider(SpreadChange(0, spread))

  private val flightSucceeded = new AtomicInteger(0)
  private val flightFailed    = new AtomicInteger(0)
  private val akkaSucceeded   = new AtomicInteger(0)
  private val akkaFailed      = new AtomicInteger(0)

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------
  private def mkFlightCtx(): QueryContext = QueryContext(
    origQueryParams = PromQlQueryParams(promQL, queryStart, queryStep, queryEnd),
    plannerParams   = PlannerParams(spreadOverride = Some(spreadProvider), queryTimeoutMillis = 60000))

  private def mkAkkaCtx(): QueryContext = QueryContext(
    origQueryParams = PromQlQueryParams(promQL, queryStart, queryStep, queryEnd),
    plannerParams   = PlannerParams(spreadOverride = Some(spreadProvider), queryTimeoutMillis = 60000))

  // -------------------------------------------------------------------------
  // Teardown
  // -------------------------------------------------------------------------
  @TearDown
  def teardown(): Unit = {
    flightGrpcServer.stop()
    akkaGrpcServer.stop()
    singlePartFlightServer.shutdown()
    singlePartFlightServer.awaitTermination(30, TimeUnit.SECONDS)
    akkaGrpcChannel.shutdown()
    flightRootAllocator.close()
    cluster.shutdown()
    logger.info(s"Flight  — succeeded: ${flightSucceeded.get}  failed: ${flightFailed.get}")
    logger.info(s"Akka    — succeeded: ${akkaSucceeded.get}    failed: ${akkaFailed.get}")
    if (flightFailed.get > 0) throw new RuntimeException(s"${flightFailed.get} flight queries failed")
    if (akkaFailed.get  > 0) throw new RuntimeException(s"${akkaFailed.get} akka queries failed")
  }

  // =========================================================================
  // Throughput benchmark — Flight path
  //
  // Sends numQueries concurrent PromQLFlightRemoteExec requests.
  // Each request: client → FlightPlanDispatcher(flightGrpcPort)
  //   → FiloDBMultiPartitionFlightProducer → SingleClusterPlanner(flight)
  //   → FlightPlanDispatcher(38824) → FiloDBSinglePartitionFlightProducer
  //   → TimeSeriesMemStore
  // =========================================================================
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @OperationsPerInvocation(50)
  def flightMultiPartitionQuery(): Unit = {
    // Per-invocation child allocator isolates Arrow memory for this batch.
    // FlightAllocator.close() releases all VSRs registered by executeFlightRequest
    // before dropping the child allocator — guaranteed to be called in the finally block
    // after all futures have completed.
    val invocAllocator = flightRootAllocator.newChildAllocator(
      s"flight-bench-${System.nanoTime()}", 0, 200_000_000L)
    val flightAlloc = new FlightAllocator(invocAllocator)
    try {
      val tasks = (0 until numQueries).map { _ =>
        val ctx          = mkFlightCtx()
        val querySession = QuerySession(ctx, queryConfig, flightAllocator = Some(flightAlloc))
        val remoteExec   = PromQLFlightRemoteExec(
          queryContext            = ctx,
          dispatcher              = new InProcessPlanDispatcher(queryConfig),
          queryEndpoint           = flightMultiPartLocation.getUri.toString,
          requestTimeoutMs        = 60000,
          dataset                 = dataset.ref,
          plannerSelector         = "benchmark-flight",
          destinationTsdbWorkUnit = "benchmark")
        remoteExec.dispatcher
          .dispatch(ExecPlanWithClientParams(remoteExec, ClientParams(60000), querySession),
            UnsupportedChunkSource())
          .map {
            case _: QueryResult => flightSucceeded.incrementAndGet()
            case e: QueryError  => flightFailed.incrementAndGet(); e.t.printStackTrace()
          }
      }
      Await.result(monix.eval.Task.parSequenceUnordered(tasks).runToFuture, 120.seconds)
    } finally {
      flightAlloc.close()
    }
  }

  // =========================================================================
  // Throughput benchmark — Akka / non-flight gRPC path
  //
  // Sends numQueries concurrent PromQLGrpcRemoteExec requests.
  // Each request: client → gRPC execStreaming(akkaGrpcPort)
  //   → PromQLGrpcService → SingleClusterPlanner(akka)
  //   → ActorPlanDispatcher → NodeCoordinatorActor
  //   → TimeSeriesMemStore
  // =========================================================================
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @OperationsPerInvocation(50)
  def akkaGrpcMultiPartitionQuery(): Unit = {
    val tasks = (0 until numQueries).map { _ =>
      val ctx          = mkAkkaCtx()
      val querySession = QuerySession(ctx, queryConfig)
      val remoteExec   = PromQLGrpcRemoteExec(
        channel                 = akkaGrpcChannel,
        requestTimeoutMs        = 60000,
        queryContext            = ctx,
        dispatcher              = new InProcessPlanDispatcher(queryConfig),
        dataset                 = dataset.ref,
        plannerSelector         = "benchmark-akka",
        destinationTsdbWorkUnit = "benchmark")
      remoteExec.dispatcher
        .dispatch(ExecPlanWithClientParams(remoteExec, ClientParams(60000), querySession),
          UnsupportedChunkSource())
        .map {
          case _: QueryResult => akkaSucceeded.incrementAndGet()
          case _: QueryError  => akkaFailed.incrementAndGet()
        }
    }
    Await.result(monix.eval.Task.parSequenceUnordered(tasks).runToFuture, 120.seconds)
  }

  // =========================================================================
  // Latency benchmark — Flight path (SampleTime captures P50/P90/P99)
  // One query at a time; use alongside the throughput variants to get a full
  // latency distribution without queue buildup noise.
  // =========================================================================
  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def flightSingleQueryLatency(): Unit = {
    val ctx            = mkFlightCtx()
    val singleAlloc    = flightRootAllocator.newChildAllocator(
      s"flight-single-${System.nanoTime()}", 0, 50_000_000L)
    val flightAlloc    = new FlightAllocator(singleAlloc)
    try {
      val querySession = QuerySession(ctx, queryConfig, flightAllocator = Some(flightAlloc))
      val remoteExec   = PromQLFlightRemoteExec(
        queryContext            = ctx,
        dispatcher              = new InProcessPlanDispatcher(queryConfig),
        queryEndpoint           = flightMultiPartLocation.getUri.toString,
        requestTimeoutMs        = 60000,
        dataset                 = dataset.ref,
        plannerSelector         = "benchmark-flight",
        destinationTsdbWorkUnit = "benchmark")
      Await.result(
        remoteExec.dispatcher
          .dispatch(ExecPlanWithClientParams(remoteExec, ClientParams(60000), querySession),
            UnsupportedChunkSource())
          .map {
            case _: QueryResult => flightSucceeded.incrementAndGet()
            case _: QueryError  => flightFailed.incrementAndGet()
          }
          .runToFuture,
        60.seconds)
    } finally {
      flightAlloc.close()
    }
  }

  // =========================================================================
  // Latency benchmark — Akka / non-flight gRPC path (SampleTime)
  // =========================================================================
  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def akkaSingleQueryLatency(): Unit = {
    val ctx          = mkAkkaCtx()
    val querySession = QuerySession(ctx, queryConfig)
    val remoteExec   = PromQLGrpcRemoteExec(
      channel                 = akkaGrpcChannel,
      requestTimeoutMs        = 60000,
      queryContext            = ctx,
      dispatcher              = new InProcessPlanDispatcher(queryConfig),
      dataset                 = dataset.ref,
      plannerSelector         = "benchmark-akka",
      destinationTsdbWorkUnit = "benchmark")
    Await.result(
      remoteExec.dispatcher
        .dispatch(ExecPlanWithClientParams(remoteExec, ClientParams(60000), querySession),
          UnsupportedChunkSource())
        .map {
          case _: QueryResult => akkaSucceeded.incrementAndGet()
          case _: QueryError  => akkaFailed.incrementAndGet()
        }
        .runToFuture,
      60.seconds)
  }
}
