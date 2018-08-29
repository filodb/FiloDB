package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import org.openjdk.jmh.annotations._

import filodb.prometheus.FormatConversion
import filodb.timeseries.TestTimeseriesProducer

/**
 * Measures the shard calculation and ingestion record creation logic used in the Gateway
 */
@State(Scope.Thread)
class GatewayBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  val numShards = 2
  val numSamples = 720   // 2 hours * 3600 / 10 sec interval
  val numSeries = 100
  val startTime = System.currentTimeMillis - (3600*1000)
  private val dataset = FormatConversion.dataset

  import monix.execution.Scheduler.Implicits.global

  val batchedData = TestTimeseriesProducer.timeSeriesData(startTime, numShards, numSeries)
                                          .take(numSamples * numSeries)
                                          .grouped(128)
                                          .toBuffer

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def produceShardData(): Long = {
    val fut = TestTimeseriesProducer.batchSingleThreaded(
                      Observable.fromIterable(batchedData), dataset, numShards, Set("__name__", "job"))
                      .groupBy(_._1)
                      .countL.runAsync
    Await.result(fut, 10.seconds)
  }
}