package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration._

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations
import org.openjdk.jmh.annotations._
import spire.syntax.cfor._

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.{PartKeyIndexRaw, PartKeyLuceneIndex, PartKeyTantivyIndex}
import filodb.core.metadata.Schemas
import filodb.core.metadata.Schemas.untyped
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.timeseries.TestTimeseriesProducer

// scalastyle:off
@State(Scope.Benchmark)
abstract class PartKeyIndexBenchmark {

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  protected def createPartKeyIndex(): PartKeyIndexRaw

  println(s"Building Part Keys")
  val ref = DatasetRef("prometheus")
  val partKeyIndex = createPartKeyIndex()
  val numSeries = 1000000
  val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
  val untypedData = TestTimeseriesProducer.timeSeriesData(0, numSeries,
           numMetricNames = 1, publishIntervalSec = 10, Schemas.untyped) take numSeries
  untypedData.foreach(_.addToBuilder(ingestBuilder))

  val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)

  val converter = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = untyped.comparator.buildPartKeyFromIngest(base, offset, partKeyBuilder)
  }
  // Build part keys from the ingestion records
  ingestBuilder.allContainers.foreach(_.consumeRecords(converter))

  var partId = 1
  val now = System.currentTimeMillis()
  val consumer = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = {
      val partKey = untyped.partition.binSchema.asByteArray(base, offset)
      partKeyIndex.addPartKey(partKey, partId, now)()
      partId += 1
    }
  }

  private var lookupTime = now + 1000

  // Adjust the time range for every iteration.  Without this everything ends up fully covered
  // by query caching and you're only testing performance of the cache.
  //
  // In the real world it's very common to run the same query again and again but with a different time range
  // - think cases like a live dashboard or alerting system.
  @inline
  protected def currentLookupTime(): Long = {
    lookupTime += 1

    lookupTime
  }

  val start = System.nanoTime()

  println(s"Indexing started")
  partKeyBuilder.allContainers.foreach(_.consumeRecords(consumer))
  partKeyIndex.refreshReadersBlocking()
  val end = System.nanoTime()

  println(s"Indexing finished. Added $partId part keys Took ${(end-start)/1000000000L}s")

  println(s"Index Memory Map Size: ${partKeyIndex.indexMmapBytes}")

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partIdsLookupWithEqualsFilters(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-$i")),
            ColumnFilter("_ws_", Filter.Equals("demo")),
            ColumnFilter("host", Filter.Equals("H0")),
            ColumnFilter("_metric_", Filter.Equals("heap_usage0"))),
        now,
        currentLookupTime())
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def emptyPartIdsLookupWithEqualsFilters(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-${i + 200}")),
          ColumnFilter("_ws_", Filter.Equals("demo")),
          ColumnFilter("host", Filter.Equals("H0")),
          ColumnFilter("_metric_", Filter.Equals("heap_usage0"))),
        now,
        currentLookupTime())
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def partIdsLookupWithPrefixRegexFilters(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-$i")),
          ColumnFilter("_ws_", Filter.Equals("demo")),
          ColumnFilter("_metric_", Filter.Equals("heap_usage0")),
          ColumnFilter("instance", Filter.EqualsRegex("Instance-2.*"))),
        now,
        currentLookupTime())
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def partIdsLookupWithSuffixRegexFilters(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-$i")),
          ColumnFilter("_ws_", Filter.Equals("demo")),
          ColumnFilter("_metric_", Filter.Equals("heap_usage0")),
          ColumnFilter("instance", Filter.EqualsRegex(".*2"))),
        now,
        currentLookupTime())
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def partIdsLookupWithEnumRegexFilter(): Unit = {
    cforRange(0 until 8) { i =>
      @scala.annotation.unused val c = partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-0")),
          ColumnFilter("_ws_", Filter.Equals("demo")),
          ColumnFilter("_metric_", Filter.Equals("heap_usage0")),
          ColumnFilter("instance",
            Filter.EqualsRegex("Instance-1|Instance-2|Instance-3|Instance-4|Instance-5|Instance-6|Instance-7|Instance-8|Instance-9|Instance-10|" +
              "Instance-11|Instance-12|Instance-13|Instance-14|Instance-15|Instance-16|Instance-17|Instance-18|Instance-19|Instance-20|" +
              "Instance-21|Instance-22|Instance-23|Instance-24|Instance-25|Instance-26|Instance-27|Instance-28|Instance-29|Instance-30"))),
        now,
        currentLookupTime()).length
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def startTimeLookupWithPartId(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      val pIds = scala.collection.mutable.ArrayBuffer.empty[Int]
      cforRange ( i * 1000 to i * 1000 + 1000 ) { j => pIds += j }
      partKeyIndex.startTimeFromPartIds(pIds.iterator)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def indexNames(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.indexNames(10000)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def indexValues(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.indexValues("instance", 10000)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def getLabelNames(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      val filter = Seq(ColumnFilter("_ns_", Filter.Equals(s"App-$i")),
        ColumnFilter("_ws_", Filter.Equals("demo")))
      partKeyIndex.labelNamesEfficient(filter, now, currentLookupTime())
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def getMetricNames(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      val filter = Seq(ColumnFilter("_ns_", Filter.Equals(s"App-$i")),
        ColumnFilter("_ws_", Filter.Equals("demo")))
      partKeyIndex.labelValuesEfficient(filter, now, currentLookupTime(), "_metric_", 10000)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def getMetricNamesSearchAndIterate(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      val filter = Seq(ColumnFilter("_ns_", Filter.Equals(s"App-$i")),
        ColumnFilter("_ws_", Filter.Equals("demo")))
      val res = mutable.HashSet[ZeroCopyUTF8String]()
      partKeyIndex.partIdsFromFilters(filter, now, currentLookupTime()).foreach { pId =>
        val pk = partKeyIndex.partKeyFromPartId(pId)
        Schemas.promCounter.partition.binSchema.singleColValues(pk.get.bytes, UnsafeUtils.arayOffset, "_metric_", res)
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partIdsLookupOverTime(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-0")),
          ColumnFilter("_ws_", Filter.Equals("demo")),
          ColumnFilter("host", Filter.Equals("H0")),
          ColumnFilter("_metric_", Filter.Equals("heap_usage0")),
          ColumnFilter("instance", Filter.Equals("Instance-1"))),
        now,
        currentLookupTime())
    }
  }

}

@State(Scope.Benchmark)
class PartKeyLuceneIndexBenchmark extends PartKeyIndexBenchmark {
  override protected def createPartKeyIndex(): PartKeyIndexRaw = {
    new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis)
  }
}

@State(Scope.Benchmark)
class PartKeyTantivyIndexBenchmark extends PartKeyIndexBenchmark {
  override protected def createPartKeyIndex(): PartKeyIndexRaw = {
    PartKeyTantivyIndex.startMemoryProfiling()

    new PartKeyTantivyIndex(ref, untyped.partition, 0, 1.hour.toMillis)
  }

  @TearDown(annotations.Level.Trial)
  def teardown(): Unit = {
    PartKeyTantivyIndex.stopMemoryProfiling()
    val index = partKeyIndex.asInstanceOf[PartKeyTantivyIndex]

    println(s"\nCache stats:\n${index.dumpCacheStats()}\n")
  }
}