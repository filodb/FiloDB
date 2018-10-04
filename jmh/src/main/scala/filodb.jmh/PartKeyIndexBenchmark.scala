package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.language.postfixOps

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations._
import scalaxy.loops._

import filodb.core.TestData
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.prometheus.FormatConversion
import filodb.timeseries.TestTimeseriesProducer

@State(Scope.Thread)
class PartKeyIndexBenchmark {

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  val dataset = FormatConversion.dataset
  val partKeyIndex = new PartKeyLuceneIndex(dataset, 0, TestData.storeConf)
  val numSeries = 1000000
  val partKeyData = TestTimeseriesProducer.timeSeriesData(0, 1, numSeries) take numSeries
  val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, dataset.partKeySchema)
  partKeyData.foreach { s =>
    partKeyBuilder.startNewRecord()
    partKeyBuilder.startMap()
    s.tags.foreach { case (k, v) =>
      partKeyBuilder.addMapKeyValue(k.getBytes(), v.getBytes())
    }
    partKeyBuilder.endMap()
    partKeyBuilder.endRecord()
  }

  var partId = 1
  val now = System.currentTimeMillis()
  val consumer = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = {
      val partKey = dataset.partKeySchema.asByteArray(base, offset)
      partKeyIndex.addPartKey(partKey, partId, now)()
      partId += 1
    }
  }
  partKeyBuilder.allContainers.head.consumeRecords(consumer)
  partKeyIndex.commitBlocking()

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partIdsLookupWithEqualsFilters(): Unit = {
    for ( i <- 0 to 20 optimized) {
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("job", Filter.Equals(s"App-$i")),
            ColumnFilter("__name__", Filter.Equals("heap_usage"))),
        now,
        now + 1000)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def partIdsLookupWithSuffixRegexFilters(): Unit = {
    for ( i <- 0 to 20 optimized) {
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("job", Filter.Equals(s"App-$i")),
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
    for ( i <- 0 to 20 optimized) {
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("job", Filter.Equals(s"App-$i")),
          ColumnFilter("__name__", Filter.Equals("heap_usage")),
          ColumnFilter("instance", Filter.EqualsRegex(".*2"))),
        now,
        now + 1000)
    }
  }

}
