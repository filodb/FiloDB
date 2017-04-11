package filodb.jmh

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core._
import filodb.core.memstore.{RowWithOffset, TimeSeriesMemStore}
import filodb.core.query._
import filodb.core.store.FilteredPartitionScan

/**
 * Microbenchmark involving TimeSeriesMemStore aggregation using TimeGroupingAggregate
 * 1000 points x 10 time series = 10k points, chunk size=1000 so aggregating over compressed chunks
 */
@State(Scope.Thread)
class AggregationBenchmark {
  import monix.execution.Scheduler.Implicits.global
  import MachineMetricsData._

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  val config = ConfigFactory.parseString("filodb.memstore.max-chunks-size = 1000")
                            .withFallback(ConfigFactory.load("application_test.conf"))
                            .getConfig("filodb")
  val memStore = new TimeSeriesMemStore(config)
  val startTs = 1000000L
  val numPoints = 10000
  val endTs   = startTs + 1000 * numPoints

  // Ingest raw data
  memStore.setup(projection1)
  val data = mapper(linearMultiSeries(startTs)).take(numPoints)
  val rows = data.zipWithIndex.map { case (reader, n) => RowWithOffset(reader, n) }
  memStore.ingest(projection1.datasetRef, rows)
  val split = memStore.getScanSplits(projection1.datasetRef, 1).head

  /**
   * Doing average aggregation on all 10000 points with 100 buckets
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def avgTimeGroupAgg(): Array[Double] = {
    val aggregator = new TimeGroupingAvgDoubleAgg(0, 1, startTs, endTs, 100)
    val fut = memStore.readChunks(projection1, schema take 2, 0, FilteredPartitionScan(split))
                      .foldLeftL(aggregator.asInstanceOf[Aggregate[Double]])(_ add _)
                      .runAsync
    Await.result(fut, 2.second).result
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def sumAgg(): Array[Double] = {
    val aggregator = new SumDoublesAggregate(0)
    val fut = memStore.readChunks(projection1, schema drop 1 take 1, 0, FilteredPartitionScan(split))
                      .foldLeftL(aggregator.asInstanceOf[Aggregate[Double]])(_ add _)
                      .runAsync
    Await.result(fut, 2.second).result
  }
}