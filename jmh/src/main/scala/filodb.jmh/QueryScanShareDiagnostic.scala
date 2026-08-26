// scalastyle:off
// package filodb.core.memstore (not filodb.jmh) so LuceneQueryBuilder (a package-private class in
// PartKeyLuceneIndex.scala) is visible, same as PartKeyIndexTermScanDiagnostic.scala in this dir.
package filodb.core.memstore

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, TotalHitCountCollector}
import org.apache.lucene.store.MMapDirectory

import filodb.coordinator.{IngestionStarted, ShardMapper, StaticSpreadProvider}
import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core.{DatasetRef, SpreadChange, TestData}
import filodb.core.binaryrecord2.{RecordBuilder, RecordContainer}
import filodb.core.metadata.Schemas
import filodb.core.metadata.Schemas.untyped
import filodb.core.query.{ColumnFilter, Filter, PerQueryLimits, PlannerParams, QueryConfig, QueryContext, QuerySession}
import filodb.core.store.{InMemoryMetaStore, NullColumnStore, StoreConfig}
import filodb.gateway.conversion.PrometheusGaugeRecord
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{QueryResponse, QueryError => QError, QueryResult => QueryResult2}
import filodb.timeseries.TestTimeseriesProducer

/*
 * For a full PromQL query sum(rate(metric{...instance=~".*7.*"}[5m])), what fraction of the query's CPU
 * is the Lucene part-key term-scan (index label lookup) vs the data path (chunk read + rate + sum)?
 *
 * Part A: full-query CPU through the real engine (SingleClusterPlanner -> ExecPlan.execute), single shard,
 *         single thread. NO akka cluster (direct TimeSeriesMemStore ingest) so it cannot hang.
 * Part B: scan-only CPU via a standalone PartKeyLuceneIndex of the SAME instance cardinality.
 *
 * Run: sbt "jmh/runMain filodb.core.memstore.QueryScanShareDiagnostic"
 */
object QueryScanShareDiagnostic {

  private val cpuBean = {
    val b = ManagementFactory.getThreadMXBean
    if (b.isThreadCpuTimeSupported) b.setThreadCpuTimeEnabled(true)
    b
  }

  // ---------------- Part B: standalone scan-only index (instance = "Instance-$n", flat dictionary) ----------------
  private def buildScanIndex(numSeries: Int, ns: String): (PartKeyLuceneIndex, Long) = {
    val ref = DatasetRef("prometheus")
    val index = new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis)
    val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
    val now = System.currentTimeMillis()
    var n = 0
    while (n < numSeries) {
      val tags = Map("_ws_" -> "demo", "_ns_" -> ns, "instance" -> s"Instance-$n")
      PrometheusGaugeRecord(tags, "heap_usage0", now, 1.0 + (n % 7)).addToBuilder(ingestBuilder)
      n += 1
    }
    val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
    val converter = new BinaryRegionConsumer {
      def onNext(base: Any, offset: Long): Unit =
        untyped.comparator.buildPartKeyFromIngest(base, offset, partKeyBuilder)
    }
    ingestBuilder.allContainers.foreach(_.consumeRecords(converter))
    var partId = 1
    val consumer = new BinaryRegionConsumer {
      def onNext(base: Any, offset: Long): Unit = {
        index.addPartKey(untyped.partition.binSchema.asByteArray(base, offset), partId, now)()
        partId += 1
      }
    }
    partKeyBuilder.allContainers.foreach(_.consumeRecords(consumer))
    index.refreshReadersBlocking()
    (index, now)
  }

  private def buildLuceneQuery(filters: Seq[ColumnFilter], start: Long, end: Long) =
    new LuceneQueryBuilder().buildQueryWithStartAndEnd(filters, start, end)

  private def timeScanOnly(searcher: IndexSearcher, q: org.apache.lucene.search.Query,
                           warmup: Int, iters: Int): (Double, Int) = {
    var w = 0
    while (w < warmup) { searcher.search(q, new TotalHitCountCollector); w += 1 }
    val cpu0 = cpuBean.getCurrentThreadCpuTime
    var i = 0; var hits = 0
    while (i < iters) { val c = new TotalHitCountCollector(); searcher.search(q, c); hits = c.getTotalHits; i += 1 }
    ((cpuBean.getCurrentThreadCpuTime - cpu0) / 1000.0 / iters, hits)
  }

  def main(args: Array[String]): Unit = {
    filodb.coordinator.KamonSingleton.initOnce()
    org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

    val numShards = 1            // single shard so the leaf's index scan covers the WHOLE dictionary,
    val numSeries = 4000         // matching Part B's standalone index exactly (apples-to-apples scan)
    val numSamples = 100
    val spread = 0
    val publishIntervalSec = 10

    // bare ActorSystem ONLY for a dummy ActorRef in the ShardMapper; no cluster join -> cannot hang
    val system = ActorSystem("qss", ConfigFactory.load("filodb-defaults.conf"))
    val dummyRef = system.actorOf(Props.empty)

    val filodbConf = ConfigFactory.load("filodb-defaults.conf").getConfig("filodb")
    val queryConfig = QueryConfig(filodbConf.getConfig("query"))
    val dataset = TestTimeseriesProducer.dataset
    val schemas = Schemas(dataset.schema)

    val storeConf = StoreConfig(ConfigFactory.parseString(
      """
        | max-chunks-size = 100
        | disk-time-to-live = 10 hours
        | shard-mem-size = 256MB
        | groups-per-shard = 4
        | max-buffer-pool-size = 10000
        | flush-interval = 1 hour
        | part-index-flush-max-delay = 10 seconds
        | part-index-flush-min-delay = 2 seconds
      """.stripMargin))

    val memStore = new TimeSeriesMemStore(filodbConf, new NullColumnStore, new NullColumnStore,
      new InMemoryMetaStore(), Some(new FixedMaxPartitionsEvictionPolicy(200000)))

    val shardMapper = new ShardMapper(numShards)
    (0 until numShards).foreach { s =>
      memStore.setup(dataset.ref, schemas, s, storeConf, numShards)
      shardMapper.updateFromEvent(IngestionStarted(dataset.ref, s, dummyRef))
    }

    // ---- ingest samples directly into the memstore (no akka) ----
    val startTime = System.currentTimeMillis - (3600 * 1000)
    val (producingFut, containerStream) = TestTimeseriesProducer.metricsToContainerStream(startTime, numShards,
      numSeries, numMetricNames = 1, numSamples * numSeries, dataset, shardMapper, spread, publishIntervalSec)
    val offset = new AtomicLong(0)
    val ingestFut = containerStream.foreachL { case (shard, bytes) =>
      memStore.ingest(dataset.ref, shard, SomeData(RecordContainer(bytes), offset.getAndIncrement()))
    }.runToFuture
    Await.result(producingFut, 120.seconds)
    Await.result(ingestFut, 120.seconds)
    memStore.refreshIndexForTesting(dataset.ref)
    println(s"Ingestion ended ($numSeries series, $numSamples samples, $numShards shard)")

    val engine = new SingleClusterPlanner(dataset, schemas, shardMapper, 0, queryConfig, "raw")
    val queryTime = startTime + (7 * 60 * 1000)
    val qParams = TimeStepParams(queryTime / 1000, 150, (queryTime / 1000) + 55 * 60)
    val qContext = QueryContext(plannerParams =
      new PlannerParams(spreadOverride = Some(StaticSpreadProvider(SpreadChange(0, spread))),
        enforcedLimits = PerQueryLimits(execPlanSamples = 1000000),
        queryTimeoutMillis = 2.hours.toMillis.toInt))
    val querySched = Scheduler.singleThread("q")
    val ns = "App-0"   // TestTimeseriesProducer stamps _ns_="App-0"

    def buildLeaf(promql: String) =
      engine.materialize(Parser.queryRangeToLogicalPlan(promql, qParams), qContext).children.head

    def runOnce(ep: filodb.query.exec.ExecPlan): QueryResponse =
      Await.result(ep.execute(memStore, QuerySession(qContext, queryConfig))(querySched).runToFuture(querySched), 60.seconds)

    def runBatch(ep: filodb.query.exec.ExecPlan, n: Int): Unit = {
      val f = Observable.fromIterable(0 until n).mapEval { _ =>
        ep.execute(memStore, QuerySession(qContext, queryConfig))(querySched)
      }.executeOn(querySched).countL.runToFuture(querySched)
      Await.result(f, 120.seconds)
    }

    def cpuNow(): Long =
      Await.result(monix.eval.Task.eval(cpuBean.getCurrentThreadCpuTime).executeOn(querySched)
        .runToFuture(querySched), 5.seconds)

    def timeFull(promql: String, warmup: Int, iters: Int): (Double, Int) = {
      val ep = buildLeaf(promql)
      val matched = runOnce(ep) match {
        case q: QueryResult2 => q.result.size
        case e: QError       => println(s"QUERY ERROR: ${e.t.getMessage}"); 0
      }
      runBatch(ep, warmup)
      val cpu0 = cpuNow()
      runBatch(ep, iters)
      ((cpuNow() - cpu0) / 1000.0 / iters, matched)
    }

    // FULL_FEW: expensive unanchored instance scan, subset match
    val regexCandidates = Seq(".*7.*", ".*1.*", ".*2.*")
    var chosen = regexCandidates.head; var fewCpu = 0.0; var fewMatched = 0; var found = false
    for (re <- regexCandidates if !found) {
      val (cpu, matched) = timeFull(s"""sum(rate(heap_usage0{_ws_="demo",_ns_="$ns",instance=~"$re"}[5m]))""", 50, 200)
      if (matched > 0) { chosen = re; fewCpu = cpu; fewMatched = matched; found = true }
      else println(s"REGEX $re matched 0, trying next")
    }
    println(f"FULL_FEW us_per_query=$fewCpu%.2f matched=$fewMatched regex=$chosen")

    // FULL_MANY: no instance filter, matches all series (big data path, no scan)
    val (manyCpu, manyMatched) = timeFull(s"""sum(rate(heap_usage0{_ws_="demo",_ns_="$ns"}[5m]))""", 50, 200)
    println(f"FULL_MANY us_per_query=$manyCpu%.2f matched=$manyMatched")

    // Part B: scan-only, same cardinality + regex
    val (idx, nowB) = buildScanIndex(numSeries, ns)
    idx.closeIndex()
    val reader = DirectoryReader.open(new MMapDirectory(idx.indexDiskLocation))
    val searcher = new IndexSearcher(reader); searcher.setQueryCache(null)
    val scanQ = buildLuceneQuery(Seq(
      ColumnFilter("_ws_", Filter.Equals("demo")),
      ColumnFilter("_metric_", Filter.Equals("heap_usage0")),
      ColumnFilter("_ns_", Filter.Equals(ns)),
      ColumnFilter("instance", Filter.EqualsRegex(chosen))), nowB, nowB + 1000)
    val (scanCpu, scanHits) = timeScanOnly(searcher, scanQ, 300, 2000)
    reader.close()
    println(f"SCAN_ONLY us_per_search=$scanCpu%.2f hits=$scanHits")

    println(f"SCAN_SHARE_FEW  = ${if (fewCpu > 0) scanCpu / fewCpu else Double.NaN}%.4f")
    println(f"SCAN_SHARE_MANY = ${if (manyCpu > 0) scanCpu / manyCpu else Double.NaN}%.4f")

    memStore.shutdown()
    Await.result(system.terminate(), 10.seconds)
    println("Diagnostic complete.")
    sys.exit(0)
  }
}
