# PartKey Index Term-Scan Diagnostic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a standalone diagnostic that reports how many Lucene term-dictionary entries each `PartKeyLuceneIndex` query type scans, so the cost difference between equals / prefix / suffix / enum lookups is measurable, not just inferred.

**Architecture:** A counting wrapper around Lucene's reader (`FilterDirectoryReader` → `FilterLeafReader` → counting `Terms`/`TermsEnum`) increments a per-field counter on every `next()`/`seekCeil()`/`seekExact()`. A separate `main` builds the same 1M-series index the benchmark uses, opens its own reader wrapped with the counter, runs each query type once (queries built by the production `LuceneQueryBuilder`), and prints a table. It is NOT a JMH benchmark — counting runs outside any timed path.

**Tech Stack:** Scala 2.13, Lucene 8.8.2 (`lucene-core`), FiloDB `core` + `gateway` modules, sbt.

## Global Constraints

- Lucene version is **8.8.2** — use only APIs present in 8.8.2 (`FilterDirectoryReader`, `FilterLeafReader.FilterTerms`, `FilterLeafReader.FilterTermsEnum`, `Terms.intersect`, `ByteBuffersDirectory`, `TotalHitCountCollector`, `RegexpQuery(Term, int)` all exist).
- All new files declare `package filodb.core.memstore` so they can use the package-protected `LuceneQueryBuilder` and the `PART_ID_FIELD`/`START_TIME` constants — even though they live physically under `jmh/src/main/scala/filodb.jmh/`. Package is set by the `package` line, not the directory.
- **Do NOT modify any existing file.** No edits to `PartKeyLuceneIndex.scala`, `PartKeyIndex.scala`, build files, or `Dependencies.scala`. New files only.
- **Do NOT add anything to `core/src/main`** — this stays benchmark-only. All new code goes in the `jmh` module.
- **Do NOT `git commit` or `git push`.** This repo's `origin` is the public OSS FiloDB repo. Leave changes as working-tree files only.
- Scala style: no `_` wildcard-only lambdas where a named param reads clearer; match surrounding 2-space indentation; `// scalastyle:off` is already used in this module for benchmarks if a rule complains.

---

### Task 1: Counting reader + self-verifying correctness check

**Files:**
- Create: `jmh/src/main/scala/filodb.jmh/CountingDirectoryReader.scala` (package `filodb.core.memstore`)
- Create: `jmh/src/main/scala/filodb.jmh/TermScanSelfCheck.scala` (package `filodb.core.memstore`)

**Interfaces:**
- Produces:
  - `class TermScanCounter` with `add(field: String): Unit`, `get(field: String): Long`, `total(): Long`, `reset(): Unit`.
  - `class CountingDirectoryReader(in: DirectoryReader, counter: TermScanCounter)` — a `DirectoryReader` usable directly in `new IndexSearcher(reader)`.
  - `object TermScanSelfCheck { def main(args: Array[String]): Unit }` — builds a tiny in-memory index and asserts the counts; prints `SELF-CHECK PASSED …` or throws.

- [ ] **Step 1: Write the self-check (the failing test)**

Create `jmh/src/main/scala/filodb.jmh/TermScanSelfCheck.scala`. It references `TermScanCounter` and `CountingDirectoryReader`, which do not exist yet, so the module will not compile — that is the intended "red" state.

```scala
package filodb.core.memstore

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.search.{IndexSearcher, PrefixQuery, Query, RegexpQuery, TermQuery, TotalHitCountCollector}
import org.apache.lucene.store.ByteBuffersDirectory
import org.apache.lucene.util.automaton.RegExp

// Builds a tiny index with 10 distinct terms (v0..v9) on field "k" and verifies
// that CountingDirectoryReader counts term-dictionary advances correctly.
object TermScanSelfCheck {
  def main(args: Array[String]): Unit = {
    val dir = new ByteBuffersDirectory()
    val writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))
    (0 until 10).foreach { i =>
      val doc = new Document()
      doc.add(new StringField("k", s"v$i", Field.Store.NO))
      writer.addDocument(doc)
    }
    writer.commit()
    writer.close()

    val counter = new TermScanCounter
    val reader = new CountingDirectoryReader(DirectoryReader.open(dir), counter)
    val searcher = new IndexSearcher(reader)
    searcher.setQueryCache(null) // otherwise a cached query would skip term enumeration

    def run(q: Query): (Long, Int) = {
      counter.reset()
      val coll = new TotalHitCountCollector()
      searcher.search(q, coll)
      (counter.get("k"), coll.getTotalHits)
    }

    // Exact seek: TermQuery(k=v5) -> at least 1 seek, exactly 1 doc.
    val (eqScan, eqHits) = run(new TermQuery(new Term("k", "v5")))
    require(eqHits == 1, s"equals hits: expected 1, got $eqHits")
    require(eqScan >= 1, s"equals scan: expected >=1, got $eqScan")

    // Prefix: PrefixQuery(k=v) -> walks only the matching prefix range (all 10 here), 10 docs.
    val (pfScan, pfHits) = run(new PrefixQuery(new Term("k", "v")))
    require(pfHits == 10, s"prefix hits: expected 10, got $pfHits")
    require(pfScan >= 10 && pfScan <= 11, s"prefix scan: expected 10-11, got $pfScan")

    // Leading-.* regex: RegexpQuery(k=.*5) -> automaton walks EVERY term (10), matches 1 (v5).
    val (rxScan, rxHits) = run(new RegexpQuery(new Term("k", ".*5"), RegExp.NONE))
    require(rxHits == 1, s"regex hits: expected 1, got $rxHits")
    require(rxScan == 10, s"regex scan: expected 10 (full term dict), got $rxScan")

    reader.close()
    println(s"SELF-CHECK PASSED  equals(scan=$eqScan,hits=$eqHits)  " +
      s"prefix(scan=$pfScan,hits=$pfHits)  regex(scan=$rxScan,hits=$rxHits)")
  }
}
```

- [ ] **Step 2: Compile to verify it fails**

Run: `sbt jmh/compile`
Expected: FAIL — `not found: type TermScanCounter` and `not found: type CountingDirectoryReader`.

- [ ] **Step 3: Implement the counting reader**

Create `jmh/src/main/scala/filodb.jmh/CountingDirectoryReader.scala`:

```scala
package filodb.core.memstore

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

import org.apache.lucene.index.{DirectoryReader, FilterDirectoryReader, FilterLeafReader,
  IndexReader, LeafReader, Terms, TermsEnum}
import org.apache.lucene.util.BytesRef
import org.apache.lucene.util.automaton.CompiledAutomaton

// Thread-safe per-field counter of Lucene TermsEnum advances.
class TermScanCounter {
  private val perField = new ConcurrentHashMap[String, LongAdder]()
  def add(field: String): Unit = perField.computeIfAbsent(field, _ => new LongAdder()).increment()
  def get(field: String): Long = { val a = perField.get(field); if (a == null) 0L else a.sum() }
  def total(): Long = { var sum = 0L; perField.values().forEach(a => sum += a.sum()); sum }
  def reset(): Unit = perField.clear()
}

// Counts every term visited during a scan (next) or a seek (seekCeil/seekExact).
class CountingTermsEnum(in: TermsEnum, field: String, counter: TermScanCounter)
  extends FilterLeafReader.FilterTermsEnum(in) {
  override def next(): BytesRef = {
    val r = super.next()
    if (r != null) counter.add(field)
    r
  }
  override def seekCeil(text: BytesRef): TermsEnum.SeekStatus = {
    counter.add(field)
    super.seekCeil(text)
  }
  override def seekExact(text: BytesRef): Boolean = {
    counter.add(field)
    super.seekExact(text)
  }
}

class CountingTerms(in: Terms, field: String, counter: TermScanCounter)
  extends FilterLeafReader.FilterTerms(in) {
  // iterator() is used by exact-term queries (TermQuery, TermInSetQuery).
  override def iterator(): TermsEnum = new CountingTermsEnum(in.iterator(), field, counter)
  // intersect() is used by automaton queries (PrefixQuery, RegexpQuery).
  override def intersect(compiled: CompiledAutomaton, startTerm: BytesRef): TermsEnum =
    new CountingTermsEnum(in.intersect(compiled, startTerm), field, counter)
}

class CountingLeafReader(in: LeafReader, counter: TermScanCounter) extends FilterLeafReader(in) {
  override def terms(field: String): Terms = {
    val t = super.terms(field)
    if (t == null) null else new CountingTerms(t, field, counter)
  }
  override def getCoreCacheHelper: IndexReader.CacheHelper = in.getCoreCacheHelper
  override def getReaderCacheHelper: IndexReader.CacheHelper = in.getReaderCacheHelper
}

class CountingDirectoryReader(in: DirectoryReader, counter: TermScanCounter)
  extends FilterDirectoryReader(in, new FilterDirectoryReader.SubReaderWrapper {
    override def wrap(reader: LeafReader): LeafReader = new CountingLeafReader(reader, counter)
  }) {
  override protected def doWrapDirectoryReader(reader: DirectoryReader): DirectoryReader =
    new CountingDirectoryReader(reader, counter)
  override def getReaderCacheHelper: IndexReader.CacheHelper = in.getReaderCacheHelper
}
```

- [ ] **Step 4: Compile and run the self-check to verify it passes**

Run: `sbt jmh/compile`
Expected: SUCCESS.

Run: `sbt "jmh/runMain filodb.core.memstore.TermScanSelfCheck"`
Expected: prints a line beginning `SELF-CHECK PASSED` and exits 0. If a `require` fails, the counts are wrong — stop and fix the counting logic before proceeding (do NOT touch the assertion values to make it pass).

- [ ] **Step 5: (No commit — Global Constraints forbid committing to this repo.)**

Leave the two new files in the working tree.

---

### Task 2: The 1M-series term-scan diagnostic

**Files:**
- Create: `jmh/src/main/scala/filodb.jmh/PartKeyIndexTermScanDiagnostic.scala` (package `filodb.core.memstore`)

**Interfaces:**
- Consumes: `TermScanCounter`, `CountingDirectoryReader` (Task 1); `PartKeyLuceneIndex`, `LuceneQueryBuilder` (existing, same package); `TestTimeseriesProducer` (gateway); `RecordBuilder`, `Schemas`, `ColumnFilter`, `Filter` (core).
- Produces: `object PartKeyIndexTermScanDiagnostic { def main(args: Array[String]): Unit }`. Optional arg `args(0)` = series count (default 1000000).

- [ ] **Step 1: Write the diagnostic**

Create `jmh/src/main/scala/filodb.jmh/PartKeyIndexTermScanDiagnostic.scala`. The `buildIndex` block is copied from `PartKeyIndexBenchmark.scala:31-79` (do not import the benchmark — copy the setup so this runs standalone).

```scala
package filodb.core.memstore

import scala.concurrent.duration._

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, Query, TotalHitCountCollector}
import org.apache.lucene.store.MMapDirectory

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.metadata.Schemas.untyped
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.timeseries.TestTimeseriesProducer

// Standalone diagnostic (NOT a JMH benchmark). Reports Lucene terms scanned per query type.
// Run: sbt "jmh/runMain filodb.core.memstore.PartKeyIndexTermScanDiagnostic [numSeries]"
object PartKeyIndexTermScanDiagnostic {

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

  def main(args: Array[String]): Unit = {
    // 0) Prove the counter is correct on known data before trusting the 1M numbers.
    TermScanSelfCheck.main(Array.empty)

    val numSeries = if (args.nonEmpty) args(0).toInt else 1000000
    println(s"Building index with $numSeries series ...")
    val startNs = System.nanoTime()
    val (index, now) = buildIndex(numSeries)
    index.closeIndex() // commits segments to disk so we can open our own reader
    println(s"Index built in ${(System.nanoTime() - startNs) / 1000000000L}s at ${index.indexDiskLocation}")

    val counter = new TermScanCounter
    val baseReader = DirectoryReader.open(new MMapDirectory(index.indexDiskLocation))
    val reader = new CountingDirectoryReader(baseReader, counter)
    val searcher = new IndexSearcher(reader)
    searcher.setQueryCache(null)

    val ws = ColumnFilter("_ws_", Filter.Equals("demo"))
    val ns0 = ColumnFilter("_ns_", Filter.Equals("App-0")) // App-0 is the ONLY populated namespace
    val metric = ColumnFilter("_metric_", Filter.Equals("heap_usage0"))
    val host0 = ColumnFilter("host", Filter.Equals("H0"))
    val enumPattern = (1 to 30).map(i => s"Instance-$i").mkString("|")

    // (name, expected Lucene query type, filters) — pinned to App-0 to measure the real scan.
    val cases = Seq(
      ("equals", "TermQuery",       Seq(ns0, ws, host0, metric)),
      ("empty",  "TermQuery(none)", Seq(ColumnFilter("_ns_", Filter.Equals("App-999")), ws, host0, metric)),
      ("prefix", "PrefixQuery",     Seq(ns0, ws, metric, ColumnFilter("instance", Filter.EqualsRegex("Instance-2.*")))),
      ("suffix", "RegexpQuery",     Seq(ns0, ws, metric, ColumnFilter("instance", Filter.EqualsRegex(".*2")))),
      ("enum",   "TermInSetQuery",  Seq(ns0, ws, metric, ColumnFilter("instance", Filter.EqualsRegex(enumPattern))))
    )

    println()
    println(f"${"query"}%-9s ${"lucene"}%-16s ${"scan(instance)"}%16s ${"scan(total)"}%13s ${"docs"}%10s")
    cases.foreach { case (name, lucene, filters) =>
      counter.reset()
      val coll = new TotalHitCountCollector()
      searcher.search(buildQuery(filters, now, now + 1000), coll)
      println(f"$name%-9s $lucene%-16s ${counter.get("instance")}%16d ${counter.total()}%13d ${coll.getTotalHits}%10d")
    }

    reader.close()
    println("\nNote: only App-0 has data (see TestTimeseriesProducer). Tantivy (Rust) index has no " +
      "TermsEnum hook, so terms-scanned is Lucene-only.")
  }
}
```

- [ ] **Step 2: Compile**

Run: `sbt jmh/compile`
Expected: SUCCESS. If `LuceneQueryBuilder` is reported inaccessible, confirm the file's first line is exactly `package filodb.core.memstore` (not `package filodb.jmh`).

- [ ] **Step 3: Run a fast smoke test with a small series count**

Run: `sbt "jmh/runMain filodb.core.memstore.PartKeyIndexTermScanDiagnostic 10000"`
Expected: prints `SELF-CHECK PASSED …`, then `Building index …`, then a 5-row table. Sanity of the shape (not exact numbers) at 10k series:
- `suffix` `scan(instance)` ≈ 10000 (walks the whole `instance` dictionary).
- `enum` `scan(instance)` == 30.
- `equals` `scan(instance)` is small (single digits), `docs` == 1.
- `empty` `docs` == 0.
- `prefix` `scan(instance)` ≈ `docs` (prefix walks only matches).

If `suffix scan(instance)` is not ≈ the series count, the `intersect()` override is not being exercised — re-check `CountingTerms.intersect`.

- [ ] **Step 4: Run the full 1M diagnostic**

Run: `sbt "jmh/runMain filodb.core.memstore.PartKeyIndexTermScanDiagnostic"`
Expected: after the ~10-15s index build, a table where `suffix scan(instance)` ≈ 1,000,000, `prefix scan(instance)` ≈ 111,111, `enum` == 30, `equals` small. This is the deliverable.

- [ ] **Step 5: (No commit — Global Constraints.)** Leave the file in the working tree and report the printed table.

---

## Self-Review Notes

- **Spec coverage:** counting mechanism (Task 1) + separate diagnostic reporting terms scanned per query type, no production edits, Lucene-only, App-0 pinned (Task 2) — all from the approved design are covered.
- **Type consistency:** `TermScanCounter`/`CountingDirectoryReader` signatures used in Task 2 match Task 1. `buildQueryWithStartAndEnd(Seq[ColumnFilter], Long, Long): Query` matches the real `LuceneQueryBuilder`.
- **Known approximation:** exact `scan(instance)` for prefix may be off by 1 (boundary term); asserted as a range in the self-check, described as `≈` for the 1M run. This is expected, not a bug.
