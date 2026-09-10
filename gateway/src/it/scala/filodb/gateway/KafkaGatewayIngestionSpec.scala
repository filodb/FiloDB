package filodb.gateway

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.admin.{Admin, NewTopic, OffsetSpec}
import org.apache.kafka.common.TopicPartition
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

object KafkaGatewayIngestionSpec extends ActorSpecConfig {
  // The gateway encodes records with the global Schemas object, whose partition-schema uses
  // filodb-defaults' predefined-keys (10 keys). application_test.conf overrides predefined-keys to
  // [] (a HOCON list replaces, not merges), so without this the shard would decode with an empty
  // table, fail to read the compacted tag keys, and silently drop every record (0 rows ingested).
  // Align the cluster's predefined-keys with filodb-defaults so encode and decode tables match.
  // Keep in sync with core/src/main/resources/filodb-defaults.conf (partition-schema.predefined-keys).
  override lazy val configString = defaultConfig +
    """
      |filodb.partition-schema.predefined-keys =
      |  ["_ws_", "_ns_", "app", "__name__", "instance", "dc", "le", "job", "exporter", "_pi_"]
      |""".stripMargin
}

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
 *
 * The verification checks two boundaries so a failure is self-diagnosing:
 *   1. the Kafka topic actually received container messages (producer side), and
 *   2. FiloDB ingested the expected row count (consumer/decode side).
 * Note: each Kafka message is a RecordContainer holding many records, so the topic end-offset counts
 * container messages (small), while numRowsIngested counts individual rows.
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
  private var bootstrap: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafka.start()
    // testcontainers returns "PLAINTEXT://host:port"; Kafka's bootstrap.servers wants "host:port".
    bootstrap = kafka.getBootstrapServers.stripPrefix("PLAINTEXT://")

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

  /** Sum of latest offsets across the topic's partitions = number of container messages produced. */
  private def containersOnTopic(): Long = {
    val props = new java.util.Properties()
    props.put("bootstrap.servers", bootstrap)
    val admin = Admin.create(props)
    try {
      val specs = (0 until numShards)
        .map(p => new TopicPartition(topic, p) -> OffsetSpec.latest()).toMap.asJava
      admin.listOffsets(specs).all().get().asScala.values.map(_.offset()).sum
    } finally admin.close()
  }

  /** Polls `probe` until it reaches `atLeast` or a 90s deadline; returns the last observed value. */
  private def waitUntil(atLeast: Long)(probe: => Long): Long = {
    val deadline = System.currentTimeMillis() + 90000
    var v = probe
    while (v < atLeast && System.currentTimeMillis() < deadline) {
      Thread.sleep(1000)
      v = probe
    }
    v
  }

  describe("gateway -> Kafka -> FiloDB ingestion") {
    it("ingests gauge data produced by the gateway generators via Kafka") {
      val numSamples = 200
      Await.result(
        TestTimeseriesProducer.produceMetrics(outerConfig, numMetrics = 1, numSamples = numSamples,
          numTimeSeries = 20, startMinutesAgo = 0L, publishIntervalSec = 1),
        30.seconds)
      val onTopic = waitUntil(1L)(containersOnTopic())          // producer boundary
      val ingested = waitUntil(numSamples)(cluster.memStore.numRowsIngested(ref)) // consumer boundary
      withClue(s" [diag] container-messages on topic=$onTopic, numRowsIngested=$ingested " +
        s"(expected $numSamples rows) — ") {
        onTopic should be > 0L
        ingested shouldEqual numSamples
      }
    }

    it("ingests OTel-exponential histograms produced by the gateway generators via Kafka") {
      val beforeRows = cluster.memStore.numRowsIngested(ref)
      val beforeContainers = containersOnTopic()
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
      val onTopic = waitUntil(beforeContainers + 1)(containersOnTopic())
      val ingested = waitUntil(beforeRows + numSamples)(cluster.memStore.numRowsIngested(ref))
      withClue(s" [diag] container-messages on topic=$onTopic (was $beforeContainers), " +
        s"numRowsIngested=$ingested (was $beforeRows, expected +$numSamples) — ") {
        onTopic should be > beforeContainers
        ingested shouldEqual (beforeRows + numSamples)
      }
    }
  }
}
