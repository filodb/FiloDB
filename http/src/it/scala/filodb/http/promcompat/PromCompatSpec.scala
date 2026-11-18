package filodb.http.promcompat

import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.coordinator._
import filodb.coordinator.sources.CsvStreamFactory
import filodb.core.{DatasetRef, TestData}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.SomeData
import filodb.core.metadata.Schemas
import filodb.http.{HttpSettings, PrometheusApiRoute, PrometheusIntegration}
import filodb.memory.MemFactory

object PromCompatSpec extends ActorSpecConfig {
  override lazy val configString = s"""
  filodb {
    memstore.groups-per-shard = 4
    // 8-shard single-node cluster (log2NumShards = 3). spread-default = 3 (the max) routes each
    // series by its own partitionHash across all 8 shards, and makes every query fan out to all 8
    // shards, so the cross-shard scatter-gather and merge path is genuinely exercised.
    spread-default = 3
    inline-dataset-configs = [
      {
        dataset = "prometheus"
        schema = "gauge"
        num-shards = 8
        min-num-nodes = 1
        sourcefactory = "${classOf[NoOpStreamFactory].getName}"
        sourceconfig {
          shutdown-ingest-after-stopped = false
          ${TestData.sourceConfStr}
        }
      }
    ]
  }"""
}

/**
 * Integration spec: bootstraps a real (single-node, in-process) FiloDB cluster, dual-ingests
 * synthetic gauge+counter data into both FiloDB (via the real CSV ingestion path) and a live
 * Prometheus (via remote-write), then runs the same PromQL query set against both engines through
 * their public query APIs and asserts the results match within tolerance.
 *
 * This test is tagged [[PrometheusIntegration]] and requires a reachable Prometheus at PROM_URL
 * (default http://localhost:9090). When Prometheus is unreachable, the test is skipped (via
 * `assume`), not failed.
 */
class PromCompatSpec extends AnyFunSpec with ScalatestRouteTest with Matchers {

  // Use our own ActorSystem with our test config so we can init the cluster properly.
  override def createActorSystem(): ActorSystem = PromCompatSpec.getNewSystem
  implicit val routeTimeout: RouteTestTimeout = RouteTestTimeout(2.minutes)

  override def afterAll(): Unit = {
    FilodbSettings.reset()
    super.afterAll()
  }

  private val promUrl = sys.env.getOrElse("PROM_URL", "http://localhost:9090")
  private val ref = DatasetRef("prometheus")
  private val stepSec = 15
  // Must match inline-dataset-configs num-shards and filodb.spread-default above so that the shard
  // we route each record to on ingest is one the query planner reads from.
  private val numShards = 8
  private val spread = 3
  private val shardMapper = new ShardMapper(numShards)

  private val probe = TestProbe()
  private lazy val cluster = FilodbCluster(system)

  // Full FiloDB cluster bootstrap (coordinator, cluster join, singleton proxy, dataset
  // registration wait) plus route construction, deferred until first use INSIDE the tagged
  // test body, AFTER assume(promReady()). This keeps a default `sbt http/test` run from ever
  // forcing cluster join/registration when Prometheus (and thus the whole test) is absent --
  // the suite is still discovered and instantiated, but assume() cancels the test before any
  // of this is forced, so construction itself can never hard-fail the run.
  private lazy val route: Route = {
    cluster.coordinatorActor
    cluster.join()
    val clusterProxy = cluster.clusterSingleton(ClusterRole.Server, None)
    // Trigger dataset registration / shard assignment, then wait until every shard is SET UP in
    // the memStore. We ingest directly into each shard, so each must exist first (otherwise routed
    // records are dropped). memStore.activeShards reflects setup, independent of ShardStatusActive
    // (which NoOpStreamFactory shards need not reach).
    probe.send(clusterProxy, NodeClusterActor.GetShardMap(ref))
    probe.expectMsgPF(30.seconds) { case CurrentShardSnapshot(_, _) => }
    val deadline = System.currentTimeMillis() + 60000
    while (cluster.memStore.activeShards(ref).size < numShards && System.currentTimeMillis() < deadline)
      Thread.sleep(200)
    require(cluster.memStore.activeShards(ref).size == numShards,
      s"only ${cluster.memStore.activeShards(ref).size}/$numShards shards were set up within 60s")
    val settings = new HttpSettings(PromCompatSpec.config, cluster.settings)
    new PrometheusApiRoute(cluster.coordinatorActor, settings).route
  }

  private def enc(q: String): String = java.net.URLEncoder.encode(q, "UTF-8")

  private def promReady(): Boolean = Try {
    val c = new URL(s"$promUrl/-/ready").openConnection().asInstanceOf[HttpURLConnection]
    c.setConnectTimeout(2000); c.setReadTimeout(2000)
    val code = c.getResponseCode; c.disconnect(); code == 200
  }.getOrElse(false)

  private def writeTemp(name: String, content: String): Path = {
    val p = Files.createTempFile(name, ".csv")
    Files.write(p, content.getBytes(StandardCharsets.UTF_8))
    p.toFile.deleteOnExit()
    p
  }

  private def ingestCsv(file: Path, schema: filodb.core.metadata.Schema, metric: String): Unit = {
    import monix.execution.Scheduler.Implicits.global
    val conf = ConfigFactory.parseString(
      s"""header = true
         |batch-size = 200
         |file = "${file.toAbsolutePath}"
         |""".stripMargin)
    val stream = new CsvStreamFactory().create(conf, Schemas(schema), 0, offset = None)
    // Route each record to the shard the query planner will read it from. shardValues are the
    // non-metric shard-key values, sorted by column name -> [_ns_, _ws_], matching
    // SingleClusterPlanner. The record's own partitionHash supplies the spread bits. addFromBr
    // copies raw record bytes (no re-encode), so the tags map and schemaID are preserved exactly.
    val shardKeyHash = RecordBuilder.shardKeyHash(Seq(DataGen.ns, DataGen.ws), "_metric_", metric)
    val ingestSchema = schema.ingestionSchema
    try {
      Await.result(stream.get.toListL.runToFuture, 60.seconds).foreach { sd =>
        val perShard = Array.fill(numShards)(new RecordBuilder(MemFactory.onHeapFactory))
        sd.records.iterate(ingestSchema).foreach { reader =>
          val partHash = ingestSchema.partitionHash(reader.recordBase, reader.recordOffset)
          val shard = shardMapper.ingestionShard(shardKeyHash, partHash, spread)
          perShard(shard).addFromBr(reader.recordBase, reader.recordOffset)
        }
        perShard.zipWithIndex.foreach { case (builder, shard) =>
          builder.allContainers.filterNot(_.isEmpty).foreach(c =>
            cluster.memStore.ingest(ref, shard, SomeData(c, 0)))
        }
      }
    } finally {
      stream.teardown()
    }
  }

  private def filodbInstant(promql: String, timeSec: Long): String = {
    var body = ""
    Get(s"/promql/prometheus/api/v1/query?query=${enc(promql)}&time=$timeSec") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      body = responseAs[String]
    }
    body
  }

  private def filodbRange(promql: String, s: Long, e: Long): String = {
    var body = ""
    Get(s"/promql/prometheus/api/v1/query_range?query=${enc(promql)}&start=$s&end=$e&step=$stepSec") ~> route ~> check {
      status shouldEqual StatusCodes.OK
      body = responseAs[String]
    }
    body
  }

  describe("FiloDB vs Prometheus PromQL compatibility") {
    it("matches Prometheus across gauge+counter query sets", PrometheusIntegration) {
      assume(promReady(), s"Prometheus not reachable at $promUrl")
      val _ = route // force full FiloDB cluster bootstrap + dataset registration before ingesting

      // 1. Generate data ending ~1 minute in the past (aligned to step).
      val endMs = ((System.currentTimeMillis() - 60000) / DataGen.stepMillis) * DataGen.stepMillis
      val data = DataGen.dataset(endMs)

      // 2. Dual-ingest.
      // Encode CSV records with the cluster's OWN schemas, not the static Schemas.gauge /
      // Schemas.promCounter. The shard decodes each record with `settings.schemas` (parsed from
      // this test's config, where partition `predefined-keys = []`). The static schemas come from
      // filodb-defaults.conf (10 predefined keys). If the two predefined-key tables differ, the
      // tags-map key compaction written at encode time is unreadable at decode time and ingestion
      // fails. Using the cluster's schemas keeps both tables identical.
      val gaugeSchema = cluster.settings.schemas.schemas("gauge")
      val counterSchema = cluster.settings.schemas.schemas("prom-counter")
      val prom = new PromRemoteClient(promUrl)
      val wr = prom.remoteWrite(data.writeRequest)
      withClue(s"Prometheus remote-write returned HTTP ${wr.code}: ${wr.body}\n" +
        "A 400 with \"out of order\"/\"out of bounds\" means Prometheus already holds these series " +
        "(reused container). Start a fresh Prometheus (scripts/run_prom_compat.sh) and re-run.\n") {
        wr.code should (be >= 200 and be < 300)
      }
      ingestCsv(writeTemp("gauge", data.gaugeCsv), gaugeSchema, "my_gauge")
      ingestCsv(writeTemp("counter", data.counterCsv), counterSchema, "my_counter")
      cluster.memStore.refreshIndexForTesting(ref)

      // 3. Give Prometheus a moment to make remote-written samples queryable.
      Thread.sleep(3000)

      // 4. Instant + range queries at data.endSec / [data.startSec, data.endSec].
      val instant = QuerySet.load("/promcompat/queries-instant.conf")
      val range = QuerySet.load("/promcompat/queries-range.conf")
      val failures = scala.collection.mutable.ListBuffer[String]()

      def run(category: String, q: PromQuery, filodbJson: String, promJson: String): Unit = {
        val outcome =
          try ResultComparator.compare(
            PromResult.parse(filodbJson), PromResult.parse(promJson),
            Tolerance.parse(q.errorLimit), ignoreLabels = false)
          catch {
            case e: Exception =>
              failures += s"[$category] ${q.promql}\n  PARSE/COMPARE ERROR: $e" +
                s"\n  FiloDB: ${filodbJson.take(1500)}\n  Prom:   ${promJson.take(1500)}"
              return
          }
        if (!outcome.matched) {
          failures += s"[$category] ${q.promql}\n  ${outcome.detail}\n  FiloDB: ${filodbJson.take(1200)}\n  Prom:   ${promJson.take(1200)}"
        }
      }

      instant.foreach { case (cat, queries) =>
        queries.foreach { q =>
          run(cat, q, filodbInstant(q.promql, data.endSec), prom.instantQuery(q.promql, data.endSec))
        }
      }
      range.foreach { case (cat, queries) =>
        queries.foreach { q =>
          run(cat, q, filodbRange(q.promql, data.startSec, data.endSec),
              prom.rangeQuery(q.promql, data.startSec, data.endSec, stepSec))
        }
      }

      withClue(s"\n${failures.size} query mismatch(es):\n${failures.mkString("\n\n")}\n") {
        failures shouldBe empty
      }
    }
  }
}
