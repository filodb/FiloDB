package filodb.jmh

import java.util.concurrent.{Executors, ExecutorService, TimeUnit}

import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer

import ch.qos.logback.classic.{Level, Logger}
import io.grpc.{ManagedChannel, Server}
// The jmh classpath resolves a newer Netty than grpc-netty 1.50 supports (production resolves the matching one),
// so use the shaded transport, which bundles the Netty that grpc 1.50 was built against.
import io.grpc.netty.shaded.io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}
import org.apache.arrow.flight._
import org.apache.arrow.flight.auth.ServerAuthHandler
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.openjdk.jmh.annotations.{Level => JMHLevel, _}
import org.openjdk.jmh.infra.Blackhole

import filodb.coordinator.flight.ArrowSerializedRangeVectorOps
import filodb.coordinator.flight.ArrowSerializedRangeVectorOps.VsrPopulationState
import filodb.core.GlobalConfig
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}

/**
 * Quantifies the effect of filodb.flight.server.max-vector-length on the Flight result path.
 * maxVecLen is read once from config when ArrowSerializedRangeVectorOps loads, so each vector length runs in
 * its own forked JVM: see the concrete subclasses at the bottom of this file. They use jvmArgsPrepend because a
 * command-line -jvmArgsAppend replaces the annotation's jvmArgsAppend.
 *
 * Workloads model the leaf (per-shard) results of the dominant query types:
 *   alert         —    10 RVs x 1 row     (instant query, small aggregate)
 *   alertWide     — 1 000 RVs x 1 row     (instant query, by-many-labels alert)
 *   recordingRule — 20 000 RVs x 1 row    (instant query, high-cardinality rule output)
 *   dashboard     —   200 RVs x 360 rows  (1h range @ 10s step)
 *   dashboardLong — 1 000 RVs x 1 440 rows (24h range @ 1m step)
 *
 * Benchmarks:
 *   serverSerialize — server-side work of one request without the network: allocate VSRs from a per-request
 *                     child allocator, populate all RVs, unload each finished VSR into a record batch (what
 *                     FlightQueryResultStreaming does before putNext), release everything.
 *   flightRoundTrip — the same result streamed over a real loopback Flight/gRPC connection built the same way
 *                     as FiloDBSinglePartitionFlightProducer and FlightClientManager, with the server waiting on
 *                     listener readiness between batches and the client moving each batch into its own VSR and
 *                     decoding with convertVsrsIntoArrowSrvs, like FlightPlanDispatcher.
 *
 * At trial setup each fork also prints the per-request server memory footprint (peak allocator bytes),
 * number of batches and wire bytes for every workload.
 *
 * Run with (add -t 16 to model concurrent alert-evaluation bursts):
 *   sbt "jmh/jmh:run -i 5 -wi 3 -f 1 -jvmArgsAppend -Xmx4g filodb.jmh.FlightVectorLength.*Benchmark"
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
abstract class FlightVectorLengthBenchmark {

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)
  org.slf4j.LoggerFactory.getLogger("org.apache.arrow").asInstanceOf[Logger].setLevel(Level.ERROR)
  org.slf4j.LoggerFactory.getLogger("io.grpc").asInstanceOf[Logger].setLevel(Level.ERROR)

  @Param(Array("alert", "alertWide", "recordingRule", "dashboard", "dashboardLong"))
  var workload: String = _

  private val resSchema = new ResultSchema(Seq(
    ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn)
  ), 1)
  private val recSchema = resSchema.toRecordSchema
  private val queryStats = QueryStats()

  private val serverRootAllocator = new RootAllocator(Long.MaxValue)
  private val clientRootAllocator = new RootAllocator(Long.MaxValue)

  private var rvs: Seq[RangeVector] = _
  private var executor: ExecutorService = _
  private var streamExecutor: ExecutorService = _
  private var server: Server = _
  private var channel: ManagedChannel = _
  private var client: FlightClient = _

  // label set resembling a typical Prometheus series key
  private def rvKey(i: Int): RangeVectorKey = CustomRangeVectorKey(Map(
    UTF8Str("_ws_")      -> UTF8Str("demo_workspace"),
    UTF8Str("_ns_")      -> UTF8Str("demo_namespace"),
    UTF8Str("__name__")  -> UTF8Str("http_requests_total"),
    UTF8Str("job")       -> UTF8Str("api-server"),
    UTF8Str("instance")  -> UTF8Str(s"10.0.${i / 256}.${i % 256}:9090"),
    UTF8Str("method")    -> UTF8Str("GET"),
    UTF8Str("status")    -> UTF8Str("200"),
    UTF8Str("region")    -> UTF8Str("us-west-2")))

  private def buildRvs(numRvs: Int, rowsPerRv: Int, stepMs: Long): Seq[RangeVector] = {
    val start = 1_700_000_000_000L
    val samples = (0 until rowsPerRv).map(i => (start + i * stepMs, i.toDouble + 0.5))
    val range = RvRange(samples.head._1, stepMs, samples.last._1)
    (0 until numRvs).map { i =>
      val k = rvKey(i)
      new RangeVector {
        import NoCloseCursor._
        override def key: RangeVectorKey          = k
        override def rows(): RangeVectorCursor    = samples.iterator.map(s => new TransientRow(s._1, s._2))
        override def outputRange: Option[RvRange] = Some(range)
      }
    }
  }

  /**
   * Serializes `rvs` the way FlightQueryResultStreaming.streamResults does: populate one RV at a time and drain
   * finished VSRs (recycling them through freeVsrs) after each RV. `send` receives each batch as it would be
   * loaded into the flight VSR. Returns the number of batches.
   */
  private def serialize(allocator: BufferAllocator)(send: VectorSchemaRoot => Unit): Int = {
    val state = VsrPopulationState(flightVsr = ArrowSerializedRangeVectorOps.emptyVectorSchemaRoot(allocator))
    var numBatches = 0
    def drain(): Unit = while (state.finishedVsrs.nonEmpty) {
      val vsr = state.finishedVsrs.remove(0)
      send(vsr)
      numBatches += 1
      state.freeVsrs.offer(vsr)
    }
    try {
      rvs.foreach { rv =>
        ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(rv, recSchema, "bench", queryStats, allocator, state)
        drain()
      }
      if (state.currentVsr != null) state.finishedVsrs += state.finishAndGetCurrentVsr() // scalastyle:ignore
      drain()
    } finally {
      state.flightVsr.close()
      state.finishedVsrs.foreach(_.close())
      while (!state.freeVsrs.isEmpty) state.freeVsrs.poll().close()
      if (state.currentVsr != null) state.currentVsr.close() // scalastyle:ignore
    }
    numBatches
  }

  private def loadInto(target: VectorSchemaRoot, vsr: VectorSchemaRoot): Long = {
    val rb = new VectorUnloader(vsr).getRecordBatch
    try {
      new VectorLoader(target).load(rb)
      rb.computeBodyLength()
    } finally rb.close()
  }

  private class BenchProducer extends NoOpFlightProducer {
    override def getStream(context: FlightProducer.CallContext, ticket: Ticket,
                           listener: FlightProducer.ServerStreamListener): Unit = {
      val backpressure = new BackpressureStrategy.CallbackBackpressureStrategy()
      backpressure.register(listener)
      // Like FlightQueryResultStreaming, stream from another thread and return: getStream runs on the call's
      // serialized executor, so blocking here would keep gRPC from ever delivering the onReady callback.
      streamExecutor.execute { () =>
        val allocator = serverRootAllocator.newChildAllocator("request", 0, Long.MaxValue)
        val flightVsr = ArrowSerializedRangeVectorOps.emptyVectorSchemaRoot(allocator)
        try {
          listener.start(flightVsr)
          serialize(allocator) { vsr =>
            val status = backpressure.waitForListener(60000)
            if (status != BackpressureStrategy.WaitResult.READY) throw new IllegalStateException(s"listener $status")
            loadInto(flightVsr, vsr)
            listener.putNext()
          }
          listener.completed()
        } catch {
          case t: Throwable => listener.error(t)
        } finally {
          flightVsr.close()
          allocator.close()
        }
      }
    }
  }

  @Setup(JMHLevel.Trial)
  @nowarn("cat=deprecation")
  def setup(): Unit = {
    rvs = workload match {
      case "alert"         => buildRvs(10, 1, 60_000L)
      case "alertWide"     => buildRvs(1_000, 1, 60_000L)
      case "recordingRule" => buildRvs(20_000, 1, 60_000L)
      case "dashboard"     => buildRvs(200, 360, 10_000L)
      case "dashboardLong" => buildRvs(1_000, 1_440, 60_000L)
    }

    executor = Executors.newCachedThreadPool()
    streamExecutor = Executors.newCachedThreadPool()
    val svc = FlightGrpcUtils.createFlightService(serverRootAllocator, new BenchProducer,
      ServerAuthHandler.NO_OP, executor)
    server = NettyServerBuilder.forPort(0).addService(svc).build().start()
    val maxInbound = Math.toIntExact(
      GlobalConfig.systemConfig.getBytes("filodb.flight.client.max-inbound-message-size"))
    channel = NettyChannelBuilder.forAddress("localhost", server.getPort).usePlaintext()
      .maxInboundMessageSize(maxInbound).build()
    client = FlightGrpcUtils.createFlightClient(clientRootAllocator, channel)

    printFootprint()
  }

  private def printFootprint(): Unit = {
    val allocator = serverRootAllocator.newChildAllocator("footprint", 0, Long.MaxValue)
    var wireBytes = 0L
    val numBatches = try {
      val target = ArrowSerializedRangeVectorOps.emptyVectorSchemaRoot(allocator)
      try serialize(allocator)(vsr => wireBytes += loadInto(target, vsr)) finally target.close()
    } finally allocator.close()
    val maxVecLen = GlobalConfig.systemConfig.getBytes("filodb.flight.server.max-vector-length").longValue
    // scalastyle:off regex
    println(f"%n[footprint] maxVecLen=$maxVecLen%d " +
      f"maxNumRows=${ArrowSerializedRangeVectorOps.maxNumRows}%d workload=$workload%s " +
      f"peakServerBytesPerRequest=${allocator.getPeakMemoryAllocation}%d batches=$numBatches%d " +
      f"wireBodyBytes=$wireBytes%d")
    // scalastyle:on regex
  }

  @TearDown(JMHLevel.Trial)
  def teardown(): Unit = {
    client.close()
    channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS)
    server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS)
    executor.shutdownNow()
    streamExecutor.shutdownNow()
    serverRootAllocator.close()
    clientRootAllocator.close()
  }

  @Benchmark
  def serverSerialize(bh: Blackhole): Unit = {
    val allocator = serverRootAllocator.newChildAllocator("request", 0, Long.MaxValue)
    try {
      bh.consume(serialize(allocator) { vsr =>
        val rb = new VectorUnloader(vsr).getRecordBatch
        try bh.consume(rb.computeBodyLength()) finally rb.close()
      })
    } finally allocator.close()
  }

  @Benchmark
  def flightRoundTrip(bh: Blackhole): Unit = {
    val allocator = clientRootAllocator.newChildAllocator("request", 0, Long.MaxValue)
    val vsrs = ArrayBuffer[VectorSchemaRoot]()
    try {
      val stream = client.getStream(new Ticket(Array[Byte](1)))
      try {
        while (stream.next()) {
          val reqVsr = VectorSchemaRoot.create(ArrowSerializedRangeVectorOps.arrowSrvSchema, allocator)
          vsrs += reqVsr
          loadInto(reqVsr, stream.getRoot)
        }
      } finally stream.close()
      val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(vsrs.toSeq, resSchema)
      bh.consume(srvs.foldLeft(0)(_ + _.numRowsSerialized))
    } finally {
      vsrs.foreach(_.close())
      allocator.close()
    }
  }
}

@Fork(value = 1, jvmArgsPrepend = Array("-Dfilodb.flight.server.max-vector-length=1MiB"))
class FlightVectorLength1MiBBenchmark extends FlightVectorLengthBenchmark

@Fork(value = 1, jvmArgsPrepend = Array("-Dfilodb.flight.server.max-vector-length=256KiB"))
class FlightVectorLength256KiBBenchmark extends FlightVectorLengthBenchmark
