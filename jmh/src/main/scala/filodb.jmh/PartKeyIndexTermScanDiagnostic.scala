package filodb.core.memstore

// scalastyle:off
import java.lang.management.ManagementFactory
import java.lang.reflect.Method

import scala.concurrent.duration._

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.search.{IndexSearcher, Query, RegexpQuery, TotalHitCountCollector}
import org.apache.lucene.store.{ByteBuffersDirectory, MMapDirectory}
import org.apache.lucene.util.automaton.RegExp

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.metadata.Schemas.untyped
import filodb.core.query.{ColumnFilter, Filter}
import filodb.gateway.conversion.PrometheusGaugeRecord
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.timeseries.TestTimeseriesProducer

/*
  Reports the EXACT number of term-dictionary entries Lucene's block-tree walk examines per query type.

  This requires a patched lucene-core where
  org.apache.lucene.codecs.blocktree.IntersectTermsEnum exposes a static counter
  (getAndResetTermsExamined()). Because the module compiles against the stock jar, we read that
  counter via reflection. At runtime the patched IntersectTermsEnum classes must appear on the
  classpath BEFORE lucene-core-8.8.2.jar.

  Run (after copying the patched classes into the jmh classes dir):
    sbt "jmh/runMain filodb.core.memstore.PartKeyIndexTermScanDiagnostic [numSeries]"
 */
object PartKeyIndexTermScanDiagnostic {

  // Reflection: compiled against the stock jar, resolved against the patched class at runtime.
  // setAccessible is required because IntersectTermsEnum is a package-private class (even though
  // the method itself is public static).
  private lazy val getAndReset: Method = {
    val m = Class.forName("org.apache.lucene.codecs.blocktree.IntersectTermsEnum")
      .getMethod("getAndResetTermsExamined")
    m.setAccessible(true)
    m
  }

  private def termsExamined(): Long =
    getAndReset.invoke(null).asInstanceOf[java.lang.Long].longValue()

  private val cpuBean = {
    val b = ManagementFactory.getThreadMXBean
    if (b.isThreadCpuTimeSupported) b.setThreadCpuTimeEnabled(true)
    b
  }

  // Warmed timing loop: returns (avg wall-clock micros, avg CPU micros) per search.
  private def timeQuery(searcher: IndexSearcher, q: Query, warmup: Int = 300, iters: Int = 2000): (Double, Double) = {
    var i = 0
    while (i < warmup) { searcher.search(q, new TotalHitCountCollector); i += 1 }
    val cpu0 = cpuBean.getCurrentThreadCpuTime
    val wall0 = System.nanoTime()
    i = 0
    while (i < iters) { searcher.search(q, new TotalHitCountCollector); i += 1 }
    val wallUs = (System.nanoTime() - wall0) / 1000.0 / iters
    val cpuUs = (cpuBean.getCurrentThreadCpuTime - cpu0) / 1000.0 / iters
    (wallUs, cpuUs)
  }

  // Proves the patched counter is live and counts SCANNED (not matched) terms:
  // a ".*5" regex over 10 distinct terms must examine all 10 while matching only 1.
  private def selfCheck(): Unit = {
    val dir = new ByteBuffersDirectory()
    val w = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))
    (0 until 10).foreach { i =>
      val d = new Document()
      d.add(new StringField("k", s"v$i", Field.Store.NO))
      w.addDocument(d)
    }
    w.commit(); w.close()

    val searcher = new IndexSearcher(DirectoryReader.open(dir))
    searcher.setQueryCache(null)
    termsExamined() // reset
    val coll = new TotalHitCountCollector()
    searcher.search(new RegexpQuery(new Term("k", ".*5"), RegExp.NONE), coll)
    val examined = termsExamined()
    require(examined >= 10, s"self-check: expected >=10 terms examined for .*5 over 10 terms, got $examined " +
      "(patched IntersectTermsEnum not loaded, or it counts matches not scans)")
    require(coll.getTotalHits == 1, s"self-check: expected 1 hit, got ${coll.getTotalHits}")
    println(s"SELF-CHECK PASSED (patched counter live): '.*5' over 10 terms -> examined=$examined matched=1")
  }

  // Mirrors PartKeyIndexBenchmark setup: generate numSeries untyped series, build part keys, index them.
  private def buildIndex(numSeries: Int): (PartKeyLuceneIndex, Long) = {
    val ref = DatasetRef("prometheus")
    val index = new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis)

    val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
    val untypedData = TestTimeseriesProducer.timeSeriesData(0, numSeries, numMetricNames = 1,
      publishIntervalSec = 10, Schemas.untyped) take numSeries
    untypedData.foreach(_.addToBuilder(ingestBuilder))

    val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
    val converter = new BinaryRegionConsumer {
      def onNext(base: Any, offset: Long): Unit =
        untyped.comparator.buildPartKeyFromIngest(base, offset, partKeyBuilder)
    }
    ingestBuilder.allContainers.foreach(_.consumeRecords(converter))

    val now = System.currentTimeMillis()
    var partId = 1
    val consumer = new BinaryRegionConsumer {
      def onNext(base: Any, offset: Long): Unit = {
        val partKey = untyped.partition.binSchema.asByteArray(base, offset)
        index.addPartKey(partKey, partId, now)()
        partId += 1
      }
    }
    partKeyBuilder.allContainers.foreach(_.consumeRecords(consumer))
    index.refreshReadersBlocking()
    (index, now)
  }

  private def buildQuery(filters: Seq[ColumnFilter], start: Long, end: Long): Query =
    new LuceneQueryBuilder().buildQueryWithStartAndEnd(filters, start, end)

  // 3 contiguous namespaces. compositeInstance=false -> instance="Instance-n" (flat, shared dictionary);
  // compositeInstance=true -> instance="App-N/Instance-n" (each namespace's instances sort together
  // under an "App-N/" prefix, so a prefix-anchored regex scans only that namespace's slice).
  private def buildNsIndex(numSeries: Int, compositeInstance: Boolean): (PartKeyLuceneIndex, Long) = {
    val ref = DatasetRef("prometheus")
    val index = new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis)

    val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
    val now = System.currentTimeMillis()
    val nsBlock = math.max(1, numSeries / 3)
    var n = 0
    while (n < numSeries) {
      val ns = "App-" + math.min(2, n / nsBlock)
      val tags = Map(
        "_ws_" -> "demo",
        "_ns_" -> ns,
        "instance" -> (if (compositeInstance) s"$ns/Instance-$n" else s"Instance-$n"),
        "host" -> ("H" + ((n >> 4) & 3)),
        "dc" -> ("DC" + (n & 1)))
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
        val partKey = untyped.partition.binSchema.asByteArray(base, offset)
        index.addPartKey(partKey, partId, now)()
        partId += 1
      }
    }
    partKeyBuilder.allContainers.foreach(_.consumeRecords(consumer))
    index.refreshReadersBlocking()
    (index, now)
  }

  // ---- Pod-name experiment: instance = <deployment>-<hash10>-<rand5>, 3 deployment prefix types ----
  private val podTypes = Array("metrics-gateway-auth-tsdb", "filodb-query-server", "kube-state-metrics")

  private def b36(x: Long, len: Int): String = {
    val s = java.lang.Long.toString(math.abs(x) + 1L, 36)
    if (s.length >= len) s.takeRight(len) else "0" * (len - s.length) + s
  }
  private def replicaHash(typeIdx: Int, dep: Int, hashIdx: Int): String =
    b36((((typeIdx.toLong * 100000) + dep) * 10 + hashIdx) * 2654435761L, 10)
  // deterministic, unique per pod: 3 types x ~500 deps x 10 replicasets x 100 pods.
  // Returns (full pod name, group=deployment, group_replica=deployment+replicaset).
  private def podParts(n: Int, total: Int): (String, String, String) = {
    val perType = math.max(1, total / 3)
    val typeIdx = math.min(2, n / perType)
    val localN = n - typeIdx * perType
    val dep = localN / 1000
    val within = localN % 1000
    val hashIdx = within / 100
    val podIdx = within % 100
    val group = s"${podTypes(typeIdx)}${f"$dep%03d"}"                    // deployment
    val groupReplica = s"$group-${replicaHash(typeIdx, dep, hashIdx)}"   // deployment + replicaset
    val seed3 = b36((((typeIdx.toLong * 100000) + dep) * 10 + hashIdx) * 40503L, 3).takeRight(3)
    (s"$groupReplica-$seed3${b36(podIdx, 2)}", group, groupReplica)
  }
  private def podName(n: Int, total: Int): String = podParts(n, total)._1

  private def buildPodNameIndex(numSeries: Int, numNs: Int): (PartKeyLuceneIndex, Long) = {
    val ref = DatasetRef("prometheus")
    val index = new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis)
    val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
    val now = System.currentTimeMillis()
    val nsBlock = math.max(1, numSeries / numNs)
    var n = 0
    while (n < numSeries) {
      val ns = if (numNs <= 1) "App-0" else "App-" + math.min(numNs - 1, n / nsBlock)
      val (pod, group, groupReplica) = podParts(n, numSeries)
      val tags = Map("_ws_" -> "demo", "_ns_" -> ns, "instance" -> pod,
        "group" -> group, "group_replica" -> groupReplica, "host" -> ("H" + ((n >> 4) & 3)))
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

  private def runPodNames(numSeries: Int, numNs: Int): Unit = {
    println(s"Building POD-NAME index: $numSeries series, $numNs namespace(s), " +
      "instance=<deployment>-<hash>-<rand5>, 3 deployment prefix types")
    val startNs = System.nanoTime()
    val (index, now) = buildPodNameIndex(numSeries, numNs)
    index.closeIndex()
    println(s"Index built in ${(System.nanoTime() - startNs) / 1000000000L}s")

    val searcher = new IndexSearcher(DirectoryReader.open(new MMapDirectory(index.indexDiskLocation)))
    searcher.setQueryCache(null)
    val base = Seq(ColumnFilter("_ws_", Filter.Equals("demo")),
      ColumnFilter("_metric_", Filter.Equals("heap_usage0")),
      ColumnFilter("_ns_", Filter.Equals("App-0")))

    val (sample, sampleGroup, sampleGroupReplica) = podParts(42 * 1000, numSeries) // type0, dep 042, rs 0, pod 0
    val rsFrag = replicaHash(0, 42, 0).substring(0, 4)
    def re(field: String, pat: String) = ColumnFilter(field, Filter.EqualsRegex(pat))
    def eq(field: String, v: String) = ColumnFilter(field, Filter.Equals(v))
    // each grouping level via instance-regex (scan) vs via the dedicated grouping label
    val ladder = Seq(
      ("instance type",    "PrefixQuery", s"instance=~${podTypes(0)}.*",        re("instance", s"${podTypes(0)}.*")),
      ("group type",       "PrefixQuery", s"group=~${podTypes(0)}.*",           re("group", s"${podTypes(0)}.*")),
      ("instance dep",     "PrefixQuery", s"instance=~$sampleGroup-.*",         re("instance", s"$sampleGroup-.*")),
      ("group dep",        "TermQuery",   s"group=$sampleGroup",                eq("group", sampleGroup)),
      ("instance rs",      "PrefixQuery", s"instance=~$sampleGroupReplica-.*",  re("instance", s"$sampleGroupReplica-.*")),
      ("group_replica rs", "TermQuery",   s"group_replica=$sampleGroupReplica", eq("group_replica", sampleGroupReplica)),
      ("instance substr",  "RegexpQuery", s"instance=~.*$rsFrag.*",             re("instance", s".*$rsFrag.*"))
    )

    println()
    println(f"${"query"}%-18s ${"lucene"}%-12s ${"terms examined"}%16s ${"docs"}%10s ${"latency us"}%12s ${"cpu us"}%10s   example")
    ladder.foreach { case (name, lucene, exStr, cf) =>
      val q = buildQuery(base :+ cf, now, now + 1000)
      termsExamined()
      val coll = new TotalHitCountCollector()
      searcher.search(q, coll)
      val examined = termsExamined()
      val (wallUs, cpuUs) = timeQuery(searcher, q, warmup = 100, iters = 800)
      println(f"$name%-18s $lucene%-12s $examined%16d ${coll.getTotalHits}%10d $wallUs%12.1f $cpuUs%10.1f   $exStr")
    }
    println(s"\nsample pod: $sample")
    println("labels: group=deployment (card ~1,500), group_replica=deployment+replicaset (card ~15,000), " +
      "instance=pod (card ~1,500,000).")
  }

  // Flat unique instance (pod-<n>), pods round-robin across numNs namespaces. Same builder for
  // numNs=1 and numNs=3, so the ONLY difference is namespace count -> isolates the _ns_-filter effect.
  private def buildFlatNsIndex(numSeries: Int, numNs: Int): (PartKeyLuceneIndex, Long) = {
    val ref = DatasetRef("prometheus")
    val index = new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis)
    val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
    val now = System.currentTimeMillis()
    var n = 0
    while (n < numSeries) {
      val ns = "App-" + (n % numNs)
      val tags = Map("_ws_" -> "demo", "_ns_" -> ns, "instance" -> f"pod-$n%06d", "host" -> ("H" + ((n >> 4) & 3)))
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

  // Proof: the _ns_ filter does NOT reduce the automaton (terms-examined) count. Every query pins
  // _ns_=App-0; only docs-matched shrinks with more namespaces, terms-examined stays the same.
  private def runNsProof(numSeries: Int, numNs: Int): Unit = {
    println(s"\n=== NS-FILTER PROOF: $numSeries pods, $numNs namespace(s), instance=pod-<n> unique, ns=App-(n mod $numNs) ===")
    val (index, now) = buildFlatNsIndex(numSeries, numNs)
    index.closeIndex()
    val searcher = new IndexSearcher(DirectoryReader.open(new MMapDirectory(index.indexDiskLocation)))
    searcher.setQueryCache(null)
    val base = Seq(ColumnFilter("_ws_", Filter.Equals("demo")),
      ColumnFilter("_metric_", Filter.Equals("heap_usage0")),
      ColumnFilter("_ns_", Filter.Equals("App-0")))                 // every query pins _ns_=App-0
    val queries = Seq(
      ("prefix pod-1.*",  "pod-1.*"),
      ("prefix pod-19.*", "pod-19.*"),
      ("suffix .*7",      ".*7"),
      ("suffix .*99",     ".*99"))
    println(s"instance dictionary (distinct pods, ALL namespaces) = $numSeries")
    println(f"${"query"}%-16s ${"terms examined (automaton)"}%28s ${"docs matched (_ns_=App-0)"}%26s")
    for ((name, pat) <- queries) {
      val q = buildQuery(base :+ ColumnFilter("instance", Filter.EqualsRegex(pat)), now, now + 1000)
      termsExamined()
      val coll = new TotalHitCountCollector()
      searcher.search(q, coll)
      println(f"$name%-16s ${termsExamined()}%28d ${coll.getTotalHits}%26d")
    }
  }

  // Realistic pod-name patterns (label key = "pod", no grouping label). Total = 150,000 pods.
  //   query-service:         query-service-<hash10>-<rand5>                        (ReplicaSet)
  //   cloudphotosgatekeeper: prod-p00-cloudphotosgatekeeper-100pct-<hash10>-<rand5> (ReplicaSet)
  //   Spark executor:        spark-<13digit>-<driverid>-exec-<N>
  //   DaemonSet:             <name>-<rand5>
  //   CronJob (Job):         <name>-<epoch/60>-<rand5>
  //   StatefulSet:           <name>-<ordinal>
  //   ReplicationController:  <name>-<rand5>
  private val ppQsRs = 2000; private val ppQsPodsPerRs = 10     // 20,000 query-service pods
  private val ppCpRs = 2500; private val ppCpPodsPerRs = 10     // 25,000 cloudphotosgatekeeper pods
  private val ppDrivers = 500; private val ppExecPerDriver = 60 // 30,000 spark exec pods
  private val ppCpBase = "prod-p00-cloudphotosgatekeeper-100pct"
  // (name, pod count) — uneven group sizes give per-type variance
  private val ppDsGroups   = Array(("falco", 5000), ("node-exporter", 5000), ("fluentd-logging", 4000),
    ("kube-proxy", 3500), ("csi-node-driver", 2500))                                     // 20,000
  private val ppCronGroups = Array(("update-solr-deployment-data-cron", 9000), ("backup-metadata-cron", 6000)) // 15,000
  private val ppStsGroups  = Array(("filodb-raw-tsdb95", 6000), ("cassandra-prod", 4000),
    ("zookeeper-ensemble", 3000), ("kafka-broker", 2000))                                // 15,000
  private val ppRcGroups   = Array(("mongo-3", 6000), ("legacy-web-2", 5000), ("redis-1", 4000)) // 15,000
  // ReplicaSet, name varies by "p-number": prod-p<NNN>-sharedstreams-migration-<hash10>-<rand5>
  private val ppMigDeploys = 100; private val ppMigRsPerDeploy = 5; private val ppMigPodsPerRs = 20 // 10,000 migration pods
  private def ppMigName(d: Int): String = s"prod-p${100 + d}-sharedstreams-migration"
  private def ppMigHash(d: Int, rs: Int): String = b36(((d.toLong + 1) * 31 + rs) * 2654435761L, 10)
  private def ppMigPod(d: Int, rs: Int, pod: Int): String =
    s"${ppMigName(d)}-${ppMigHash(d, rs)}-${b36((((d.toLong + 1) * 31 + rs) * 100 + pod) * 40503L, 5)}"
  private def ppQsHash(rs: Int): String = b36(rs.toLong * 2654435761L, 10)
  private def ppQsPod(rs: Int, pod: Int): String =
    s"query-service-${ppQsHash(rs)}-${b36((rs.toLong * 100 + pod) * 40503L, 5)}"
  private def ppCpHash(rs: Int): String = b36((rs.toLong + 7777) * 2654435761L, 10)
  private def ppCpPod(rs: Int, pod: Int): String =
    s"$ppCpBase-${ppCpHash(rs)}-${b36(((rs.toLong + 7777) * 100 + pod) * 40503L, 5)}"
  private def ppDriver(d: Int): String =
    s"spark-${f"${1700000000000L + d.toLong}%013d"}-${b36(d.toLong * 2654435761L, 15)}${b36(d.toLong * 40503L + 7, 14)}"
  private def ppSparkPod(d: Int, exec: Int): String = s"${ppDriver(d)}-exec-$exec"
  private def ppRand5(uid: Long): String = b36(uid * 2654435761L, 5)   // unique per uid, random-looking
  private def ppDsPod(name: String, uid: Long): String = s"$name-${ppRand5(uid)}"
  private def ppCronPod(name: String, epochMin: Long, uid: Long): String = s"$name-$epochMin-${ppRand5(uid)}"
  private def ppStsPod(name: String, ordinal: Int): String = s"$name-$ordinal"
  private def ppRcPod(name: String, uid: Long): String = s"$name-${ppRand5(uid)}"

  private def buildPodPatternIndex(numNs: Int): (PartKeyLuceneIndex, Long, Int) = {
    val ref = DatasetRef("prometheus")
    val index = new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis)
    val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
    val now = System.currentTimeMillis()
    var total = 0
    def add(podName: String, ns: String): Unit = {
      val tags = Map("_ws_" -> "demo", "_ns_" -> ns, "pod" -> podName, "host" -> ("H" + (total & 3)))
      PrometheusGaugeRecord(tags, "heap_usage0", now, 1.0 + (total % 7)).addToBuilder(ingestBuilder)
      total += 1
    }
    var rs = 0
    while (rs < ppQsRs) {
      val ns = "App-" + (rs % numNs)
      var p = 0
      while (p < ppQsPodsPerRs) { add(ppQsPod(rs, p), ns); p += 1 }
      rs += 1
    }
    var cp = 0
    while (cp < ppCpRs) {
      val ns = "App-" + (cp % numNs)
      var p = 0
      while (p < ppCpPodsPerRs) { add(ppCpPod(cp, p), ns); p += 1 }
      cp += 1
    }
    var d = 0
    while (d < ppDrivers) {
      val ns = "App-" + (d % numNs)
      var e = 0
      while (e < ppExecPerDriver) { add(ppSparkPod(d, e), ns); e += 1 }
      d += 1
    }
    var uid = 0L                                            // global counter -> unique random5
    def addGroups(groups: Array[(String, Int)], mk: (String, Long) => String): Unit =
      for ((name, count) <- groups) { var i = 0; while (i < count) { add(mk(name, uid), "App-" + (uid % numNs)); uid += 1; i += 1 } }
    addGroups(ppDsGroups, (n, u) => ppDsPod(n, u))                                      // DaemonSet
    addGroups(ppCronGroups, (n, u) => ppCronPod(n, 29751705L + (u / 50), u))            // CronJob (epoch/60 slot)
    for ((name, count) <- ppStsGroups) { var i = 0; while (i < count) { add(ppStsPod(name, i), "App-" + (uid % numNs)); uid += 1; i += 1 } } // StatefulSet ordinal
    addGroups(ppRcGroups, (n, u) => ppRcPod(n, u))                                      // ReplicationController
    var mdep = 0                                                                        // migration ReplicaSets
    while (mdep < ppMigDeploys) {
      var rs = 0
      while (rs < ppMigRsPerDeploy) {
        var p = 0
        while (p < ppMigPodsPerRs) { add(ppMigPod(mdep, rs, p), "App-" + (uid % numNs)); uid += 1; p += 1 }
        rs += 1
      }
      mdep += 1
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
    (index, now, total)
  }

  private def runNsProofPods(numNs: Int): Unit = {
    println(s"\n=== NS-FILTER PROOF (real pod patterns): $numNs namespace(s), label 'pod', 7 workload types, 150K pods ===")
    val (index, now, total) = buildPodPatternIndex(numNs)
    index.closeIndex()
    val searcher = new IndexSearcher(DirectoryReader.open(new MMapDirectory(index.indexDiskLocation)))
    searcher.setQueryCache(null)
    val base = Seq(ColumnFilter("_ws_", Filter.Equals("demo")),
      ColumnFilter("_metric_", Filter.Equals("heap_usage0")),
      ColumnFilter("_ns_", Filter.Equals("App-0")))                 // every query pins _ns_=App-0
    val queries = Seq(
      ("prefix query-service (app)", "query-service-.*"),
      ("prefix query-service (rs)",  s"query-service-${ppQsHash(0)}-.*"),
      ("prefix falco (daemonset)",   "falco-.*"),
      ("prefix filodb-raw (sts)",    "filodb-raw-tsdb95-.*"),
      ("suffix .*<cp rand5>",        s".*${ppCpPod(0, 0).takeRight(5)}"),
      ("suffix .*cloudphotos.*",     ".*cloudphotosgatekeeper.*"),
      ("suffix .*-exec-.*",          ".*-exec-.*"))
    println(s"pod dictionary (distinct pods, ALL namespaces) = $total")
    println(f"${"query"}%-22s ${"terms examined (automaton)"}%28s ${"docs (_ns_=App-0)"}%18s   pattern")
    for ((name, pat) <- queries) {
      val q = buildQuery(base :+ ColumnFilter("pod", Filter.EqualsRegex(pat)), now, now + 1000)
      termsExamined()
      val coll = new TotalHitCountCollector()
      searcher.search(q, coll)
      println(f"$name%-22s ${termsExamined()}%28d ${coll.getTotalHits}%18d   $pat")
    }
  }

  // Profile mode: build the 150K index once, warm ONE query, then hot-loop it for `durationSec`
  // while an external `asprof` attaches (via the pid/ready sentinels). Uses stock Lucene — do NOT
  // copy the patched blocktree classes for a profiling run (the counter sits in the hot scan loop).
  private val profileQueries: Map[String, String] = Map(
    "prefix_qs"    -> "query-service-.*",                          // prefix, coarse (whole service)
    "prefix_mig"   -> "prod-p137-sharedstreams-migration-.*",      // prefix, one deployment
    "suffix_exec"  -> ".*-exec-.*",                                // suffix/substring, big collection
    "suffix_rand5" -> ("REPLACED_AT_RUNTIME"),                     // suffix, 1 doc (set below)
    "pattern_p13"  -> "prod-p13.*migration.*",                     // anchored prefix + interior substring
    "pattern_p14"  -> "prod-p14.*migration.*")                     // anchored prefix + interior substring

  private def runProfile(queryKey: String, durationSec: Int): Unit = {
    val (index, now, total) = buildPodPatternIndex(1)
    index.closeIndex()
    val searcher = new IndexSearcher(DirectoryReader.open(new MMapDirectory(index.indexDiskLocation)))
    searcher.setQueryCache(null)
    val base = Seq(ColumnFilter("_ws_", Filter.Equals("demo")),
      ColumnFilter("_metric_", Filter.Equals("heap_usage0")),
      ColumnFilter("_ns_", Filter.Equals("App-0")))
    val pat = queryKey match {
      case "suffix_rand5" => s".*${ppCpPod(0, 0).takeRight(5)}"
      case k              => profileQueries.getOrElse(k, sys.error(s"unknown key $k; keys=${profileQueries.keys.mkString(",")}"))
    }
    val q = buildQuery(base :+ ColumnFilter("pod", Filter.EqualsRegex(pat)), now, now + 1000)
    var w = 0
    while (w < 300) { searcher.search(q, new TotalHitCountCollector); w += 1 }   // warm the JIT
    val pid = ProcessHandle.current().pid()
    val dir = java.nio.file.Paths.get("/tmp/prof"); java.nio.file.Files.createDirectories(dir)
    java.nio.file.Files.write(dir.resolve(s"$queryKey.pid"), pid.toString.getBytes)
    println(s"PROFILE_READY key=$queryKey pid=$pid pattern=$pat pods=$total")
    java.nio.file.Files.write(dir.resolve(s"$queryKey.ready"), "1".getBytes)
    val endNs = System.nanoTime() + durationSec.toLong * 1000000000L
    var iters = 0L; var hits = 0
    while (System.nanoTime() < endNs) {
      val coll = new TotalHitCountCollector(); searcher.search(q, coll); hits = coll.getTotalHits; iters += 1
    }
    println(s"PROFILE_DONE key=$queryKey iters=$iters hits=$hits")
  }

  // ---- 14k pod / grouping CPU test, aligned to ns-rules-sample-150.json ----
  // Reads /tmp/t14k/deployments.tsv (stem, region, k8sns, kind, metricsCsv) and queries.tsv
  // (qid, shape, podField, op, pattern, region, k8sns, metric), both produced by extract_queries.py.
  private val T14K_DIR = "/tmp/t14k"
  private def readTsv(path: String): Vector[Array[String]] = {
    val src = scala.io.Source.fromFile(path)
    try src.getLines().filter(_.nonEmpty).map(_.split("\t", -1)).toVector finally src.close()
  }
  private def rsHash(seed: Long): String = b36(seed * 2654435761L, 10)
  private def podHash(seed: Long): String = b36(seed * 40503L + 7, 5)

  private def build14kIndex(): (PartKeyLuceneIndex, Long, Int) = {
    val ref = DatasetRef("prometheus")
    val index = new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis)
    val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
    val now = System.currentTimeMillis()
    var total = 0
    var uid = 0L
    def add(pod: String, group: String, region: String, ns: String, metric: String): Unit = {
      val tags = Map("_ws_" -> "aci-kubernetes", "_ns_" -> region,
        "pod" -> pod, "exported_pod" -> pod, "grouping" -> group, "namespace" -> ns)
      PrometheusGaugeRecord(tags, metric, now, 1.0 + (total % 7)).addToBuilder(ingestBuilder)
      total += 1
    }
    // one replicaset per (deployment, region); grouping = <stem>-<rsHash> (sts groups by stem)
    def genBucket(rows: Vector[Array[String]], target: Int, seedBase: Long): Unit = {
      if (rows.nonEmpty) {
        val per = math.max(1, target / rows.length)
        var extra = target - per * rows.length
        var di = 0
        for (r <- rows) {
          val stem = r(0); val region = r(1); val ns = r(2); val kind = r(3)
          val m0 = r(4).split(",").filter(_.nonEmpty)
          val ms = if (m0.isEmpty) Array("container_cpu_usage_seconds_total") else m0
          val count = per + (if (extra > 0) { extra -= 1; 1 } else 0)
          val rHash = rsHash(seedBase + di)
          val group = if (kind == "sts") stem else s"$stem-$rHash"
          var i = 0
          while (i < count) {
            val pod = if (kind == "sts") s"$stem-$i" else s"$stem-$rHash-${podHash(uid)}"
            add(pod, group, region, ns, ms(i % ms.length)); uid += 1; i += 1
          }
          di += 1
        }
      }
    }
    // filler pods (not referenced by any query): only inflate the pod dictionary
    def genFiller(app: String, target: Int, deps: Int, seedBase: Long): Unit = {
      val per = math.max(1, target / deps)
      var made = 0; var d = 0
      while (made < target) {
        val stem = f"prod-p01-$app$d%02d"
        val rHash = rsHash(seedBase + d)
        val group = s"$stem-$rHash"
        var i = 0
        while (i < per && made < target) {
          add(s"$stem-$rHash-${podHash(uid)}", group, "us-central-1h", s"$app-ns",
            "container_cpu_usage_seconds_total")
          uid += 1; i += 1; made += 1
        }
        d += 1
      }
    }
    val deps = readTsv(s"$T14K_DIR/deployments.tsv")
    genBucket(deps.filter(_(2).startsWith("icloud")), 3000, 1000)   // icloud
    genBucket(deps.filter(_(2).startsWith("apc")), 2000, 5000)      // object-store
    genFiller("query-service", 300, 3, 20000)
    genFiller("mosaic-gateway", 300, 3, 30000)
    genFiller("instance", 8400, 40, 40000)

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
    (index, now, total)
  }

  private def run14k(field: String, shape: String, dur: Int, qps: Int, threads: Int): Unit = {
    println(s"Building 14k index (aligned) for field=$field shape=$shape ...")
    val (index, now, total) = build14kIndex()
    index.closeIndex()
    val searcher = new IndexSearcher(DirectoryReader.open(new MMapDirectory(index.indexDiskLocation)))
    searcher.setQueryCache(null)
    def opFilter(fieldName: String, op: String, pat: String): ColumnFilter = op match {
      case "="  => ColumnFilter(fieldName, Filter.Equals(pat))
      case "!~" => ColumnFilter(fieldName, Filter.NotEqualsRegex(pat))
      case "!=" => ColumnFilter(fieldName, Filter.NotEquals(pat))
      case _    => ColumnFilter(fieldName, Filter.EqualsRegex(pat))
    }
    val rows = readTsv(s"$T14K_DIR/queries.tsv").filter(r => shape == "all" || r(1) == shape)
    val queries: Vector[Query] = rows.map { r =>
      val pf = r(2); val op = r(3); val pat = r(4); val region = r(5); val k8sns = r(6); val metric = r(7)
      var filters: Seq[ColumnFilter] = Seq(
        ColumnFilter("_ws_", Filter.Equals("aci-kubernetes")),
        ColumnFilter("_ns_", Filter.Equals(region)))
      if (metric != "-") filters = filters :+ ColumnFilter("_metric_", Filter.Equals(metric))
      if (k8sns != "-")  filters = filters :+ ColumnFilter("namespace", Filter.Equals(k8sns))
      if (pat != "-" && pf != "-")
        filters = filters :+ opFilter(if (field == "grouping") "grouping" else "pod", op, pat)
      buildQuery(filters, now, now + 1000)
    }
    val warmEnd = System.nanoTime() + 8L * 1000000000L    // ~8s warm so C2 JIT settles before sampling
    while (System.nanoTime() < warmEnd) {
      var i = 0
      while (i < queries.size) { searcher.search(queries(i), new TotalHitCountCollector); i += 1 }
    }
    java.nio.file.Files.write(java.nio.file.Paths.get(T14K_DIR).resolve("run.ready"), "1".getBytes)
    println(s"RUN14K_READY field=$field shape=$shape queries=${queries.size} qps=$qps threads=$threads pods=$total")
    if (qps <= 0) {                                  // busy loop: measure single-core capacity
      val endNs = System.nanoTime() + dur.toLong * 1000000000L
      var iters = 0L; var hits = 0L
      while (System.nanoTime() < endNs) {
        var i = 0
        while (i < queries.size) {
          val c = new TotalHitCountCollector(); searcher.search(queries(i), c); hits += c.getTotalHits; i += 1
        }
        iters += 1
      }
      val served = iters * queries.size
      println(s"RUN14K_DONE field=$field shape=$shape iters=$iters served=$served capacity_qps=${served / math.max(1, dur)} totalHits=$hits")
    } else {                                         // paced at fixed qps with a thread pool
      val pool = java.util.concurrent.Executors.newFixedThreadPool(threads)
      val completed = new java.util.concurrent.atomic.AtomicLong(0)
      val interval = 1000000000L / qps
      val start = System.nanoTime()
      val totalTasks = qps.toLong * dur
      var i = 0L
      while (i < totalTasks) {
        val q = queries((i % queries.size).toInt)
        pool.execute(new Runnable { def run(): Unit = { searcher.search(q, new TotalHitCountCollector); completed.incrementAndGet() } })
        val sleepNs = (start + (i + 1) * interval) - System.nanoTime()
        if (sleepNs > 0) java.util.concurrent.locks.LockSupport.parkNanos(sleepNs)
        i += 1
      }
      val submitElapsed = (System.nanoTime() - start) / 1e9
      pool.shutdownNow()
      pool.awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS)
      println(f"RUN14K_DONE field=$field shape=$shape offered_qps=$qps submitted=$totalTasks " +
        f"submit_secs=$submitElapsed%.1f completed=${completed.get()} completed_qps=${completed.get() / dur}")
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.contains("profile")) {                 // stock Lucene; no selfCheck (patched counter absent)
      val key = args(args.indexOf("profile") + 1)
      val dur = args.find(a => a.startsWith("dur=")).map(_.substring(4).toInt).getOrElse(35)
      runProfile(key, dur)
      return
    }
    if (args.contains("t14k")) {                    // 14k pod/grouping CPU test; stock Lucene, no selfCheck
      val field = args.find(_.startsWith("field=")).map(_.substring(6)).getOrElse("pod")
      val shape = args.find(_.startsWith("shape=")).map(_.substring(6)).getOrElse("all")
      val dur = args.find(_.startsWith("dur=")).map(_.substring(4).toInt).getOrElse(30)
      val qps = args.find(_.startsWith("qps=")).map(_.substring(4).toInt).getOrElse(0)
      val threads = args.find(_.startsWith("threads=")).map(_.substring(8).toInt).getOrElse(4)
      run14k(field, shape, dur, qps, threads)
      return
    }
    selfCheck()

    val composite = args.contains("composite")
    val multins = args.contains("multins")
    val numSeries = args.find(a => a.nonEmpty && a.forall(_.isDigit)).map(_.toInt).getOrElse(1000000)
    if (args.contains("podnames")) { runPodNames(numSeries, if (multins) 3 else 1); return }
    if (args.contains("nsproof")) { runNsProof(numSeries, if (multins) 3 else 1); return }
    if (args.contains("podpattern")) { runNsProofPods(if (multins) 3 else 1); return }
    val mode = if (composite) "composite: 3 namespaces, instance=App-N/Instance-n"
      else if (multins) "multins: 3 namespaces, flat instance=Instance-n"
      else "flat: 1 namespace"
    println(s"Building index with $numSeries series ($mode) ...")
    val startNs = System.nanoTime()
    val (index, now) =
      if (composite) buildNsIndex(numSeries, compositeInstance = true)
      else if (multins) buildNsIndex(numSeries, compositeInstance = false)
      else buildIndex(numSeries)
    index.closeIndex() // commit segments so we can open our own reader
    println(s"Index built in ${(System.nanoTime() - startNs) / 1000000000L}s at ${index.indexDiskLocation}")

    val baseReader = DirectoryReader.open(new MMapDirectory(index.indexDiskLocation))
    val searcher = new IndexSearcher(baseReader)
    searcher.setQueryCache(null)

    val ws = ColumnFilter("_ws_", Filter.Equals("demo"))
    val ns0 = ColumnFilter("_ns_", Filter.Equals("App-0"))
    val metric = ColumnFilter("_metric_", Filter.Equals("heap_usage0"))
    val host0 = ColumnFilter("host", Filter.Equals("H0"))
    val p = if (composite) "App-0/" else ""  // instance-value prefix that anchors the scan to one namespace
    val enumPattern = (1 to 30).map(i => s"${p}Instance-$i").mkString("|")

    val cases = Seq(
      ("equals", "TermQuery",       Seq(ns0, ws, host0, metric)),
      ("empty",  "TermQuery(none)", Seq(ColumnFilter("_ns_", Filter.Equals("App-999")), ws, host0, metric)),
      ("prefix", "PrefixQuery",     Seq(ns0, ws, metric, ColumnFilter("instance", Filter.EqualsRegex(s"${p}Instance-2.*")))),
      ("suffix", "RegexpQuery",     Seq(ns0, ws, metric, ColumnFilter("instance", Filter.EqualsRegex(s"${p}.*2")))),
      ("enum",   "TermInSetQuery",  Seq(ns0, ws, metric, ColumnFilter("instance", Filter.EqualsRegex(enumPattern))))
    )

    println()
    println(f"${"query"}%-9s ${"lucene"}%-16s ${"terms examined"}%16s ${"docs"}%10s ${"latency us"}%12s ${"cpu us"}%10s ${"cpu%"}%6s")
    cases.foreach { case (name, lucene, filters) =>
      val q = buildQuery(filters, now, now + 1000)
      termsExamined() // reset before the measured query
      val coll = new TotalHitCountCollector()
      searcher.search(q, coll)
      val examined = termsExamined()
      val docs = coll.getTotalHits
      val (wallUs, cpuUs) = timeQuery(searcher, q)
      val cpuPct = if (wallUs > 0) cpuUs / wallUs * 100 else 0.0
      println(f"$name%-9s $lucene%-16s $examined%16d $docs%10d $wallUs%12.2f $cpuUs%10.2f $cpuPct%6.0f")
    }

    // --- Experiment: CPU for the ENTIRE test, over growing workloads ---
    val exactQ  = buildQuery(Seq(ns0, ws, host0, metric), now, now + 1000)
    val prefixQ = buildQuery(Seq(ns0, ws, metric,
      ColumnFilter("instance", Filter.EqualsRegex(s"${p}Instance-2.*"))), now, now + 1000)
    val suffixQ = buildQuery(Seq(ns0, ws, metric,
      ColumnFilter("instance", Filter.EqualsRegex(s"${p}.*2"))), now, now + 1000)

    def runTest(name: String, queries: Seq[Query], warmup: Int = 50, iters: Int = 500): Unit = {
      var w = 0
      while (w < warmup) { queries.foreach(q => searcher.search(q, new TotalHitCountCollector)); w += 1 }
      val cpu0  = cpuBean.getCurrentThreadCpuTime
      val wall0 = System.nanoTime()
      var i = 0
      while (i < iters) { queries.foreach(q => searcher.search(q, new TotalHitCountCollector)); i += 1 }
      val wallMs = (System.nanoTime() - wall0) / 1e6
      val cpuMs  = (cpuBean.getCurrentThreadCpuTime - cpu0) / 1e6
      println(f"$name%-28s wall=$wallMs%8.1f ms   cpu=$cpuMs%8.1f ms   cpu%%=${cpuMs / wallMs * 100}%4.0f")
    }

    println("\n=== Experiment: CPU for the entire test (500 iters each) ===")
    runTest("Test1 exact",                Seq(exactQ))
    runTest("Test2 exact+prefix",         Seq(exactQ, prefixQ))
    runTest("Test3 exact+prefix+suffix",  Seq(exactQ, prefixQ, suffixQ))

    baseReader.close()
    println("\nterms examined = block-tree entries the intersect walk stepped over (0 = exact seek, no scan).")
    if (composite)
      println("Composite: instance=App-N/Instance-n across 3 namespaces; prefix/suffix anchored on 'App-0/' " +
        "so the scan is bounded to one namespace's slice (~1/3 of the dictionary).")
    else if (multins)
      println("Multi-ns: 3 namespaces, FLAT instance; _ns_=App-0 filters docs but the instance regex still " +
        "full-scans the whole shared instance dictionary -> same scan as flat, fewer docs.")
    else
      println("Note: only App-0 has data. equals/enum use direct seeks (SegmentTermsEnum); Lucene-only.")
  }
}
