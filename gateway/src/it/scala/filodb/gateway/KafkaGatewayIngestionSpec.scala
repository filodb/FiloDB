package filodb.gateway

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler.Implicits.global
import org.apache.kafka.clients.admin.{Admin, NewTopic, OffsetSpec}
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import filodb.coordinator._
import filodb.coordinator.NodeClusterActor._
import filodb.core.DatasetRef
import filodb.core.GlobalConfig
import filodb.core.TestData
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, QueryConfig, QueryContext, QuerySession}
import filodb.core.store.{AllChunkScan, StoreConfig}
import filodb.gateway.conversion.MetricTagInputRecord
import filodb.kafka.KafkaIngestionStreamFactory
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query.QueryResult
import filodb.query.exec.{InProcessPlanDispatcher, MultiSchemaPartitionsExec}
import filodb.timeseries.TestTimeseriesProducer

object KafkaGatewayIngestionSpec extends ActorSpecConfig {
  // The gateway encodes each record's tag map with the global Schemas object, whose partition-schema
  // uses filodb-defaults' predefined-keys (10 keys) as a tag-key compaction dictionary.
  // application_test.conf overrides predefined-keys to [] (a HOCON list replaces, not merges). If the
  // shard decoded with an empty dictionary it could not read the compacted tag keys the gateway wrote.
  // Align the cluster's predefined-keys with filodb-defaults so the encode and decode dictionaries
  // match. (This governs tag-key decode; schema resolution is handled by overrideSchema = false below.)
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
    // overrideSchema = false so the shard registers ALL schemas from config (gauge, prom-counter,
    // otel-exp-delta-histogram, ...), not just the dataset's single schema. The gateway generators
    // emit gauge and otel-exp-delta-histogram records; with the default overrideSchema = true the
    // shard knows only promCounter and drops every record as "unknown schema" (0 rows ingested).
    val command = SetupDataset(dataset, DatasetResourceSpec(numShards, 1), ingestionSource, storeConf,
      overrideSchema = false)
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

    it("preserves OTel-exponential histogram bucket values exactly, end-to-end") {
      import filodb.memory.format.vectors.{Base2ExpHistogramBuckets, LongHistogram}

      // A small, fully DETERMINISTIC histogram dataset for one time series. We choose every bucket
      // value here (nothing random), publish it through the real gateway sharding + Kafka producer
      // path, read the raw histogram column back with a MultiSchemaPartitionsExec scan, and assert
      // every (timestamp, histogram) round-tripped unchanged. Histogram equality is scheme + values
      // (Histogram.equals delegates to compare == 0), so a produced LongHistogram compares equal to
      // the read-back histogram only when both the bucket scheme AND all bucket counts survive.
      val scheme = Base2ExpHistogramBuckets(3, -10, 20)   // same params genHistogramData uses (numBuckets = 20)
      val metricName = "http_request_latency_delta"        // otel-exp-delta is a *_delta metric
      val instanceTag = s"exact-hist-${System.currentTimeMillis()}"   // unique -> isolates this test's series
      val startTime = System.currentTimeMillis() - 10.minutes.toMillis
      val tags = Map(
        "_ws_".utf8     -> "demo".utf8,
        "_ns_".utf8     -> "App-0".utf8,
        "dc".utf8       -> "DC0".utf8,
        "host".utf8     -> "H0".utf8,
        "instance".utf8 -> instanceTag.utf8)

      // Five samples at distinct 10s-spaced timestamps; each bucket count is a known value.
      val samples: Seq[(Long, LongHistogram, MetricTagInputRecord)] = (0 until 5).map { i =>
        val ts = startTime + i * 10000L
        val buckets = Array.tabulate(scheme.numBuckets)(b => (b + 1).toLong * (i + 1))
        val hist = LongHistogram(scheme, buckets)
        val sum = buckets.sum.toDouble
        val count = buckets.sum.toDouble + 1   // +1 so neither sum nor count is zero (invalid data)
        val rec = new MetricTagInputRecord(
          Seq(ts, sum, count, hist, buckets.min.toDouble, buckets.max.toDouble),
          metricName, tags, Schemas.otelExpDeltaHistogram)
        (ts, hist, rec)
      }
      val expected: List[(Long, LongHistogram)] = samples.map { case (ts, hist, _) => (ts, hist) }.toList
      val records = samples.map(_._3)

      val beforeRows = cluster.memStore.numRowsIngested(ref)

      // Publish these exact records through the same gateway pipeline the generators use.
      val (shardQueues, containerStream) =
        GatewayServer.shardingPipeline(GlobalConfig.systemConfig, numShards, dataset)
      GatewayServer.setupKafkaProducer(outerConfig, containerStream)
      val shardMapper = new ShardMapper(numShards)
      val spread = if (numShards >= 2) (Math.log10(numShards / 2.0) / Math.log10(2.0)).toInt else 0
      records.foreach { rec =>
        val shard = shardMapper.ingestionShard(rec.shardKeyHash, rec.partitionKeyHash, spread)
        while (!shardQueues(shard).offer(rec)) Thread.sleep(50)
      }
      val ingested = waitUntil(beforeRows + records.size)(cluster.memStore.numRowsIngested(ref))

      // Read the raw histogram column ("h") back for this single time series.
      val queryConfig = QueryConfig(GlobalConfig.systemConfig.getConfig("filodb.query"))
      val querySession = QuerySession(QueryContext(), queryConfig)
      val filters = Seq(
        ColumnFilter("_metric_", Filter.Equals(metricName.utf8)),
        ColumnFilter("instance", Filter.Equals(instanceTag.utf8)))
      val queryShard =
        shardMapper.ingestionShard(records.head.shardKeyHash, records.head.partitionKeyHash, spread)
      val exec = MultiSchemaPartitionsExec(QueryContext(), InProcessPlanDispatcher(queryConfig), ref,
        queryShard, filters, AllChunkScan, "_metric_", colName = Some("h"))
      val resp = exec.execute(cluster.memStore, querySession).runToFuture.futureValue
      val result = resp.asInstanceOf[QueryResult]
      val readBack: List[(Long, filodb.memory.format.vectors.Histogram)] = result.result.headOption
        .map(_.rows().map(r => (r.getLong(0), r.getHistogram(1))).toList)
        .getOrElse(Nil)

      withClue(s" [diag] ingested=$ingested (was $beforeRows), readBack.size=${readBack.size}, " +
        s"rangeVectors=${result.result.size} — ") {
        readBack.size shouldEqual expected.size
        readBack.sortBy(_._1) shouldEqual expected   // expected is already timestamp-ascending
      }
    }
  }
}
