package filodb.jmh

import java.lang.management.{BufferPoolMXBean, ManagementFactory}
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration._

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations._
import spire.syntax.cfor._

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.metadata.Schemas
import filodb.core.metadata.Schemas.untyped
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.timeseries.TestTimeseriesProducer

// scalastyle:off
@State(Scope.Thread)
class PartKeyIndexBenchmark {

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  println(s"Building Part Keys")
  val ref = DatasetRef("prometheus")
  val partKeyIndex = new PartKeyLuceneIndex(ref, untyped.partition, true, true,0, 1.hour.toMillis)
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

  val start = System.nanoTime()

  println(s"Indexing started")
  partKeyBuilder.allContainers.foreach(_.consumeRecords(consumer))
  partKeyIndex.refreshReadersBlocking()
  val end = System.nanoTime()

  println(s"Indexing finished. Added $partId part keys Took ${(end-start)/1000000000L}s")
  import scala.collection.JavaConverters._

  println(s"Index Memory Map Size: " +
    s"${ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala.find(_.getName == "mapped").get.getMemoryUsed}")

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
        now + 1000)
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
        now + 1000)
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
          ColumnFilter("instance", Filter.EqualsRegex("Instance-2.*"))),
        now,
        now + 1000)
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
          ColumnFilter("instance", Filter.EqualsRegex(".*2"))),
        now,
        now + 1000)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(8)
  def startTimeLookupWithPartId(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      val pIds = debox.Buffer.empty[Int]
      cforRange ( i * 1000 to i * 1000 + 1000 ) { j => pIds += j }
      partKeyIndex.startTimeFromPartIds(pIds.iterator())
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
      partKeyIndex.labelValuesEfficient(filter, now, now + 1000, "_metric_", 10000)
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
      partKeyIndex.partIdsFromFilters(filter, now, now + 1000).foreach { pId =>
        val pk = partKeyIndex.partKeyFromPartId(pId)
        Schemas.promCounter.partition.binSchema.singleColValues(pk.get.bytes, UnsafeUtils.arayOffset, "_metric_", res)
      }
    }
  }

}
