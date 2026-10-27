package filodb.coordinator.flight

import com.typesafe.config.ConfigFactory
import io.grpc.netty.NettyChannelBuilder
import org.apache.arrow.flight.{FlightClient, FlightGrpcUtils, FlightRuntimeException, Location, Ticket}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.MachineMetricsData.records
import filodb.core.MetricsTestData.{timeSeriesData, timeseriesDatasetWithMetric}
import filodb.core.TestData
import filodb.core.memstore.TimeSeriesMemStore
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{InMemoryMetaStore, NullColumnStore, TimeRangeChunkScan}
import filodb.memory.format.ZeroCopyUTF8String.StringToUTF8
import filodb.query.exec._

/**
 * End-to-end tests of Flight server-side backpressure: a client that stops reading (so gRPC is not ready)
 * and then cancels, or lets the query deadline pass, must not leave any Arrow memory allocated on the server.
 */
class FlightBackpressureIntegrationSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  System.setProperty("arrow.memory.debug.allocator", "true")
  implicit val s: monix.execution.Scheduler = monix.execution.Scheduler.Implicits.global
  implicit override val patienceConfig: PatienceConfig =PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  private val akkaPort = 33819
  private val config = ConfigFactory.parseString(
    s"""filodb.memstore.max-partitions-on-heap-per-shard = 1100
       |filodb.memstore.ensure-block-memory-headroom-percent = 10
       |filodb.memstore.ensure-tsp-count-headroom-percent = 10
       |filodb.memstore.ensure-native-memory-headroom-percent = 10
       |filodb.memstore.index-updates-publishing-enabled = true
       |filodb.flight.server.per-request-allocator-limit = 200000000
       |akka.remote.netty.tcp.port=$akkaPort
       |""".stripMargin)
    .withFallback(ConfigFactory.load("application_test.conf")).resolve()

  private val dataset = timeseriesDatasetWithMetric
  private val memStore = new TimeSeriesMemStore(config.getConfig("filodb"), new NullColumnStore,
    new NullColumnStore, new InMemoryMetaStore())
  memStore.setup(dataset.ref, Schemas(dataset.schema), 0, TestData.storeConf, 1)
  // several series; combined with the 1ms step below the result spans ~20 VSRs of ~1MB, far more than the
  // client's prefetch can absorb
  private val rawData = (1 to 4).flatMap { i =>
    timeSeriesData(Map("host".utf8 -> s"host$i".utf8, "region".utf8 -> "region1".utf8)).take(1000)
  }
  memStore.ingest(dataset.ref, 0, records(dataset, rawData.toStream))
  memStore.refreshIndexForTesting(dataset.ref)

  private val server = FiloDBSinglePartitionFlightProducer.start(memStore, config)
  private val location = Location.forGrpcInsecure("localhost",
    FiloDBSinglePartitionFlightProducer.akkaPortToFlightPort(akkaPort))

  override def afterAll(): Unit = {
    memStore.shutdown()
    server.shutdown()
  }

  private def bigPlan(timeoutMs: Int) = {
    val limits = PerQueryLimits(execPlanResultBytes = 500000000L, execPlanSamples = 100000000)
    val ctx = QueryContext(plannerParams = PlannerParams(queryTimeoutMillis = timeoutMs,
      enforcedLimits = limits, warnLimits = limits))
    val mspe = MultiSchemaPartitionsExec(ctx, FlightPlanDispatcher(location, "test"), dataset.ref, 0,
      Seq(ColumnFilter("region", Filter.Equals("region1"))), TimeRangeChunkScan(0, 100000),
      dataset.schema.partition.options.metricColumn)
    // step=1ms => 500001 samples per series => many full VSRs
    mspe.addRangeVectorTransformer(PeriodicSamplesMapper(0, 1, 500000, None, None))
    mspe
  }

  private def waitForServerMemory(baseline: Long): Unit = {
    val deadline = System.currentTimeMillis() + 8000
    while (FlightAllocator.serverAllocator.getAllocatedMemory != baseline &&
           System.currentTimeMillis() < deadline) Thread.sleep(50)
    FlightAllocator.serverAllocator.getAllocatedMemory shouldEqual baseline
  }

  private def withClient[T](f: FlightClient => T): T = {
    val clientAllocator = FlightAllocator.newChildAllocatorForTesting("BackpressureClient", 0, 50000000)
    // tiny HTTP/2 flow-control window so the server's outbound buffer fills and gRPC reports not-ready
    val channel = NettyChannelBuilder.forAddress(location.getUri.getHost, location.getUri.getPort)
      .usePlaintext().flowControlWindow(16 * 1024).build()
    val client = FlightGrpcUtils.createFlightClient(clientAllocator, channel)
    try f(client) finally {
      client.close()
      clientAllocator.close()
    }
  }

  describe("Flight server backpressure") {

    it("should release all server memory when client reads one batch, stalls, then cancels") {
      val baseline = FlightAllocator.serverAllocator.getAllocatedMemory
      withClient { client =>
        val stream = client.getStream(new Ticket(FlightKryoSerDeser.serializeToBytes(bigPlan(60000))))
        try {
          stream.next() shouldEqual true // header
          stream.next() shouldEqual true // first data batch; server now blocked on gRPC readiness
          Thread.sleep(1000)             // server is parked in awaitReady with unsent VSRs
          FlightAllocator.serverAllocator.getAllocatedMemory should be > baseline
          stream.cancel("client cancelled", null)
        } finally {
          try stream.close() catch { case _: FlightRuntimeException => } // cancel surfaces as an exception
        }
      }
      waitForServerMemory(baseline)
    }

    it("should release all server memory when client never reads and the query deadline passes") {
      val baseline = FlightAllocator.serverAllocator.getAllocatedMemory
      withClient { client =>
        val stream = client.getStream(new Ticket(FlightKryoSerDeser.serializeToBytes(bigPlan(2000))))
        try {
          stream.next() shouldEqual true // header only, then stall past the 2s deadline
          Thread.sleep(4000)
          FlightAllocator.serverAllocator.getAllocatedMemory shouldEqual baseline
        } finally {
          try stream.close() catch { case _: FlightRuntimeException => }
        }
      }
      waitForServerMemory(baseline)
    }

    it("should stream the full result without leaking when the client is merely slow") {
      val baseline = FlightAllocator.serverAllocator.getAllocatedMemory
      withClient { client =>
        val stream = client.getStream(new Ticket(FlightKryoSerDeser.serializeToBytes(bigPlan(60000))))
        try {
          var batches = 0
          while (stream.next()) {
            batches += 1
            Thread.sleep(200)
          }
          batches should be > 2
        } finally stream.close()
      }
      waitForServerMemory(baseline)
    }
  }
}
