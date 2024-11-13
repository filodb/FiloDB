package filodb.jmh

import java.io.File
import java.util.Base64
import java.util.concurrent.TimeUnit

import scala.io.Source

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Scope, State, TearDown}
import org.openjdk.jmh.annotations
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import filodb.core.DatasetRef
import filodb.core.memstore.{IndexMetadataStore, IndexState, PartKeyIndexRaw, PartKeyLuceneIndex, PartKeyTantivyIndex}
import filodb.core.metadata.Schemas.untyped
import filodb.core.query.{ColumnFilter, Filter}

/*
  A benchmark that loads data from an external file full of part keys.
  This is meant to be used with real world exported data to simulate / evaluate changes.

  The input file is specified as a file path and should be a csv with the following columns:

  * partKey (base64 encoded)
  * startTime (long)
  * endTime (long)

  A header row can be optionally included.

  To use this with a data set change the file path below and benchmark queries as needed.
 */
// scalastyle:off
@State(Scope.Benchmark)
abstract class PartKeyIndexExternalBenchmark {
  // File path to load from
  final private val inputPath = "partKeys.csv"
  // File path to use for index storage
  final protected val indexPath = "index/path"

  // Filters to create queries below
  private def wsFilter = ColumnFilter("_ws_", Filter.Equals("myws"))
  private def nsFilter = ColumnFilter("_ns_", Filter.Equals("myns"))
  private def narrowFilter = ColumnFilter("hostname", Filter.Equals("example"))

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  protected def computeIndexPath(): Option[File] = {
    val file = new File(s"${indexPath}${this.getClass.getSimpleName}")

    if (!file.exists()) {
      new File(s"${file.toPath}/${ref.dataset}/0").mkdirs()
    }

    Some(file)
  }

  protected def createPartKeyIndex(): PartKeyIndexRaw

  println(s"Building Part Keys")
  val ref = DatasetRef("prometheus")
  val partKeyIndex = createPartKeyIndex()

  var partId = 1

  private def load_data(): Unit = {
    val source = Source.fromFile(inputPath)
    for (line <- source.getLines()) {
      if (!line.startsWith("partkey")) {
        val parts = line.split(',')

        val partKey = Base64.getDecoder.decode(parts(0))
        val startTime = parts(1).toLong
        val endTime = parts(2).toLong

        partKeyIndex.addPartKey(partKey, partId, startTime, endTime)()
        partId += 1
      }
    }
    source.close()
  }

  if (partKeyIndex.indexNumEntries == 0) {
    val start = System.nanoTime()
    println(s"Indexing started at path ${partKeyIndex.indexDiskLocation}")
    load_data()
    partKeyIndex.refreshReadersBlocking()
    val end = System.nanoTime()

    println(s"Indexing finished. Added $partId part keys Took ${(end - start) / 1000000000L}s")
  } else {
    partKeyIndex.refreshReadersBlocking()
    println(s"Loaded existing index with ${partKeyIndex.indexNumEntries} part keys")
  }

  @TearDown(annotations.Level.Trial)
  def teardown2(): Unit = {
    println(s"Ram usage after testing ${partKeyIndex.indexRamBytes}")
    println(s"Mmap usage after testing ${partKeyIndex.indexMmapBytes}")
  }

  private var lookupTime = 1

  @inline
  private def currentLookupTime(): Long = {
    lookupTime += 1

    lookupTime
  }

  // Wide query - matches most documents
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def labelValuesWide(): Unit = {
    partKeyIndex.labelValuesEfficient(Seq(wsFilter),
      currentLookupTime(), Long.MaxValue, "_ns_")
  }

  // Wide query - matches most documents
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partIdsFromFiltersWide(): Unit = {
    partKeyIndex.partIdsFromFilters(Seq(wsFilter,
      nsFilter),
      currentLookupTime(), Long.MaxValue, 10000)
  }

  // Wide query - matches most documents
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partKeysFromFiltersWide(): Unit = {
    partKeyIndex.partKeyRecordsFromFilters(Seq(wsFilter,
      nsFilter),
      currentLookupTime(), Long.MaxValue, 100)
  }

  // Narrow query - matches few (< 10) documents
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def labelValuesNarrow(): Unit = {
    partKeyIndex.labelValuesEfficient(Seq(wsFilter,
      nsFilter,
      narrowFilter),
      currentLookupTime(), Long.MaxValue, "pod")
  }

  // Narrow query - matches few (< 10) documents
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partIdsFromFiltersNarrow(): Unit = {
    partKeyIndex.partIdsFromFilters(Seq(wsFilter,
      nsFilter,
      narrowFilter),
      currentLookupTime(), Long.MaxValue, 10000)
  }

  // Narrow query - matches few (< 10) documents
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partKeysFromFiltersNarrow(): Unit = {
    partKeyIndex.partKeyRecordsFromFilters(Seq(wsFilter,
      nsFilter,
      narrowFilter),
      currentLookupTime(), Long.MaxValue, 100)
  }
}

@State(Scope.Benchmark)
class PartKeyLuceneIndexExternalBenchmark extends PartKeyIndexExternalBenchmark {
  override protected def createPartKeyIndex(): PartKeyIndexRaw = {
    new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis, diskLocation = computeIndexPath(),
      lifecycleManager = Some(new MockLifecycleManager()))
  }

  @TearDown(annotations.Level.Trial)
  def teardown(): Unit = {
    // This is needed to keep data consistent between runs with Lucene
    partKeyIndex.closeIndex()
  }
}

@State(Scope.Benchmark)
class PartKeyTantivyIndexExternalBenchmark extends PartKeyIndexExternalBenchmark {
  override protected def createPartKeyIndex(): PartKeyIndexRaw = {
    PartKeyTantivyIndex.startMemoryProfiling()

    new PartKeyTantivyIndex(ref, untyped.partition, 0, 1.hour.toMillis, diskLocation = computeIndexPath(),
      lifecycleManager = Some(new MockLifecycleManager()))
  }

  @TearDown(annotations.Level.Trial)
  def teardown(): Unit = {
    PartKeyTantivyIndex.stopMemoryProfiling()
    val index = partKeyIndex.asInstanceOf[PartKeyTantivyIndex]

    println(s"\nCache stats:\n${index.dumpCacheStats()}\n")
  }
}

class MockLifecycleManager extends IndexMetadataStore {

  override def initState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) = (IndexState.Synced, None)

  override def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) = (IndexState.Synced, None)

  override def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {}

  override def updateInitState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {}
}