package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.language.postfixOps

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations._
import scalaxy.loops._

import filodb.core.{DatasetRef, TestData}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.metadata.Schemas.promCounter
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.timeseries.TestTimeseriesProducer

@State(Scope.Thread)
class PartKeyIndexBenchmark {

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  val ref = DatasetRef("prometheus")
  val partKeyIndex = new PartKeyLuceneIndex(ref, promCounter.partition, 0, TestData.storeConf)
  val numSeries = 1000000
  val partKeyData = TestTimeseriesProducer.timeSeriesData(0, numSeries) take numSeries
  val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory)
  partKeyData.foreach(_.addToBuilder(partKeyBuilder))

  var partId = 1
  val now = System.currentTimeMillis()
  val consumer = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = {
      val partKey = promCounter.partition.binSchema.asByteArray(base, offset)
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
    for ( i <- 0 to 20 optimized) {
      partKeyIndex.partIdsFromFilters(
        Seq(ColumnFilter("job", Filter.Equals(s"App-$i")),
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

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def startTimeLookupWithPartId(): Unit = {
    for ( i <- 0 to 20 optimized) {
      partKeyIndex.startTimeFromPartId(i)
    }
  }

}
