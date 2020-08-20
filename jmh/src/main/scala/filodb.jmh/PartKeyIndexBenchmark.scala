package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations._
import spire.syntax.cfor._

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.metadata.Schemas.untyped
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.timeseries.TestTimeseriesProducer

@State(Scope.Thread)
class PartKeyIndexBenchmark {

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  val ref = DatasetRef("prometheus")
  val partKeyIndex = new PartKeyLuceneIndex(ref, untyped.partition, 0, 1.hour)
  val numSeries = 1000000
  val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory)
  val untypedData = TestTimeseriesProducer.timeSeriesData(0, numSeries) take numSeries
  untypedData.foreach(_.addToBuilder(ingestBuilder))

  val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory)

  val converter = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = untyped.comparator.buildPartKeyFromIngest(base, offset, partKeyBuilder)
  }
  // Build part keys from the ingestion records
  ingestBuilder.allContainers.head.consumeRecords(converter)

  var partId = 1
  val now = System.currentTimeMillis()
  val consumer = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = {
      val partKey = untyped.partition.binSchema.asByteArray(base, offset)
      partKeyIndex.addPartKey(partKey, partId, now)()
      partId += 1
    }
  }
  partKeyBuilder.allContainers.foreach(_.consumeRecords(consumer))
  partKeyIndex.refreshReadersBlocking()

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partIdsLookupWithEqualsFilters(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-$i")),
            ColumnFilter("_ws_", Filter.Equals("demo")),
            ColumnFilter("host", Filter.EqualsRegex("H0")),
            ColumnFilter("__name__", Filter.Equals("heap_usage"))),
        now,
        now + 1000)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def emptyPartIdsLookupWithEqualsFilters(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-${i + 200}")),
          ColumnFilter("_ws_", Filter.Equals("demo")),
          ColumnFilter("host", Filter.EqualsRegex("H0")),
          ColumnFilter("__name__", Filter.Equals("heap_usage"))),
        now,
        now + 1000)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partIdsLookupWithSuffixRegexFilters(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-$i")),
          ColumnFilter("_ws_", Filter.Equals("demo")),
          ColumnFilter("__name__", Filter.Equals("heap_usage")),
          ColumnFilter("instance", Filter.EqualsRegex("Instance-2.*"))),
        now,
        now + 1000)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partIdsLookupWithPrefixRegexFilters(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("_ns_", Filter.Equals(s"App-$i")),
          ColumnFilter("_ws_", Filter.Equals("demo")),
          ColumnFilter("__name__", Filter.Equals("heap_usage")),
          ColumnFilter("instance", Filter.EqualsRegex(".*2"))),
        now,
        now + 1000)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def startTimeLookupWithPartId(): Unit = {
    cforRange ( 0 until 8 ) { i =>
      val pIds = debox.Buffer.empty[Int]
      cforRange ( i * 1000 to i * 1000 + 1000 ) { j => pIds += j }
      partKeyIndex.startTimeFromPartIds(pIds.iterator())
    }
  }

}
