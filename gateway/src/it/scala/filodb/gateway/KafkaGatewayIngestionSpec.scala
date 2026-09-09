package filodb.gateway

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import filodb.coordinator._
import filodb.coordinator.NodeClusterActor._
import filodb.core.DatasetRef
import filodb.core.TestData
import filodb.core.store.StoreConfig
import filodb.kafka.KafkaIngestionStreamFactory
import filodb.timeseries.TestTimeseriesProducer

object KafkaGatewayIngestionSpec extends ActorSpecConfig

/**
 * End-to-end ingestion integration test for the real production path:
 *
 *   gateway data generators  ->  Kafka (testcontainers)  ->  FiloDB (in-process cluster,
 *   KafkaIngestionStreamFactory)  ->  ingestion verified via memStore.numRowsIngested.
 *
 * This is an ingestion test, NOT a Prometheus-comparison test (that lives in the http module's
 * PromCompatSpec). The claim under test: data produced by the gateway lands in FiloDB through Kafka
 * and the ingested row count matches what was produced.
 *
 * Requires Docker at runtime (testcontainers pulls a Kafka image). Runs in CI via `gateway/it:test`.
 *
 * Offset note: KafkaIngestionStream uses consumer.assign() + optional seek(offset). With no
 * checkpoint the offset is None (no seek), so the start position falls back to `auto.offset.reset`.
 * We set it to "earliest" so every produced record is read regardless of produce/consume ordering.
 */
class KafkaGatewayIngestionSpec extends ActorTest(KafkaGatewayIngestionSpec.getNewSystem)
  with StrictLogging with ScalaFutures {

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(100, Millis))

  private val within = 30.seconds.dilated
  private val numShards = 4
  private val topic = "filodb-itest"
  private val dataset = TestTimeseriesProducer.dataset          // Dataset("prometheus", Schemas.promCounter)
  private val ref: DatasetRef = dataset.ref

  private val kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))

  private var cluster: FilodbCluster = _
  private var outerConfig: Config = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafka.start()
    // testcontainers returns "PLAINTEXT://host:port"; Kafka's bootstrap.servers wants "host:port".
    val bootstrap = kafka.getBootstrapServers.stripPrefix("PLAINTEXT://")

    // Create the topic with one partition per shard (the gateway publishes partition == shard).
    val props = new java.util.Properties()
    props.put("bootstrap.servers", bootstrap)
    val admin = Admin.create(props)
    try admin.createTopics(
      java.util.Collections.singletonList(new NewTopic(topic, numShards, 1.toShort))).all().get()
    finally admin.close()

    // The gateway producer reads num-shards + sourceconfig.{filo-topic-name,bootstrap.servers}.
    // The Kafka ingestion source reads the same sourceconfig block.
    outerConfig = ConfigFactory.parseString(
      s"""num-shards = $numShards
         |sourceconfig {
         |  filo-topic-name = "$topic"
         |  bootstrap.servers = [ "$bootstrap" ]
         |  group.id = "filodb-itest-consumer"
         |  auto.offset.reset = "earliest"
         |}
         |""".stripMargin)

    cluster = FilodbCluster(system)
    cluster.join()
    val coordinatorActor = cluster.coordinatorActor
    val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)
    cluster.metaStore.initialize().futureValue
    cluster.metaStore.clearAllData().futureValue

    val innerConfig = outerConfig.getConfig("sourceconfig")
    val storeConf = StoreConfig(
      ConfigFactory.parseString("store { flush-interval = 1 hour }")
        .withFallback(TestData.sourceConf).getConfig("store"))
    val ingestionSource = IngestionSource(classOf[KafkaIngestionStreamFactory].getName, innerConfig)
    val command = SetupDataset(dataset, DatasetResourceSpec(numShards, 1), ingestionSource, storeConf)
    coordinatorActor ! command
    Thread.sleep(2000)
    clusterActor ! command
    expectMsg(within, DatasetVerified)

    // Wait until all shards are set up on this node (consumer streams created). Combined with
    // auto.offset.reset=earliest, this makes produce/consume ordering irrelevant.
    val deadline = System.currentTimeMillis() + 60000
    while (cluster.memStore.activeShards(ref).size < numShards && System.currentTimeMillis() < deadline)
      Thread.sleep(200)
    require(cluster.memStore.activeShards(ref).size == numShards,
      s"only ${cluster.memStore.activeShards(ref).size}/$numShards shards were set up within 60s")
  }

  override def afterAll(): Unit = {
    try if (cluster != null) cluster.shutdown()
    finally {
      kafka.stop()
      super.afterAll()
    }
  }

  /** Polls the aggregate ingested-row count until it reaches `atLeast` or a 90s deadline. */
  private def waitForRows(atLeast: Long): Long = {
    val deadline = System.currentTimeMillis() + 90000
    var count = cluster.memStore.numRowsIngested(ref)
    while (count < atLeast && System.currentTimeMillis() < deadline) {
      Thread.sleep(1000)
      count = cluster.memStore.numRowsIngested(ref)
    }
    count
  }

  describe("gateway -> Kafka -> FiloDB ingestion") {
    it("ingests gauge data produced by the gateway generators via Kafka") {
      val numSamples = 200
      Await.result(
        TestTimeseriesProducer.produceMetrics(outerConfig, numMetrics = 1, numSamples = numSamples,
          numTimeSeries = 20, startMinutesAgo = 0L, publishIntervalSec = 1),
        30.seconds)
      waitForRows(numSamples) shouldEqual numSamples
    }

    it("ingests OTel-exponential histograms produced by the gateway generators via Kafka") {
      val before = cluster.memStore.numRowsIngested(ref)
      val numSamples = 200
      val shardMapper = new ShardMapper(numShards)
      val spread = if (numShards >= 2) (Math.log10(numShards / 2.0) / Math.log10(2.0)).toInt else 0
      val startTime = System.currentTimeMillis() - 5.minutes.toMillis
      // All-positional: startTimeMs, numShards, numTimeSeries, numMetricNames, numSamples, dataset,
      // shardMapper, spread, publishIntervalSec, expHist (numBuckets defaults).
      val (producing, stream) = TestTimeseriesProducer.metricsToContainerStream(
        startTime, numShards, 20, 1, numSamples, dataset, shardMapper, spread, 1, true)
      GatewayServer.setupKafkaProducer(outerConfig, stream)
      Await.result(producing, 30.seconds)
      waitForRows(before + numSamples) shouldEqual (before + numSamples)
    }
  }
}
