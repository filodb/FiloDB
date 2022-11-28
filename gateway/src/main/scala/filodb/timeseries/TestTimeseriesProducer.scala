package filodb.timeseries

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable

import filodb.coordinator.ShardMapper
import filodb.core.GlobalConfig
import filodb.core.metadata.{Dataset, Schema, Schemas}
import filodb.gateway.GatewayServer
import filodb.gateway.conversion.{InputRecord, MetricTagInputRecord, PrometheusInputRecord}
import filodb.memory.format.{vectors => bv, ZeroCopyUTF8String => ZCUTF8}

/**
  * Utilities to produce time series data into local Kafka for development testing.
  * Please see GatewayServer for the app to run, or README for docs.
  */
object TestTimeseriesProducer extends StrictLogging {
  val dataset = Dataset("prometheus", Schemas.promCounter)

  val oneBitMask = 0x1
  val twoBitMask = 0x3
  val rand = Random
  // start from a random day in the last 5 years

  import scala.concurrent.ExecutionContext.Implicits.global

  /**
    * Produce metrics
    * @param conf the sourceConfig
    * @param numSamples number of samples to produce
    * @param numTimeSeries number of time series partitions to produce
    * @param startMinutesAgo the samples will carry a timestamp starting from these many minutes ago
    * @return
    */
  def produceMetrics(sourceConfig: Config, numMetrics: Int, numSamples: Int,
                     numTimeSeries: Int, startMinutesAgo: Long, publishIntervalSec: Int): Future[Unit] = {
    val startTime = System.currentTimeMillis() - startMinutesAgo.minutes.toMillis
    val numShards = sourceConfig.getInt("num-shards")
    val shardMapper = new ShardMapper(numShards)
    val spread = if (numShards >= 2) { (Math.log10(numShards / 2) / Math.log10(2.0)).toInt } else { 0 }
    val topicName = sourceConfig.getString("sourceconfig.filo-topic-name")

    val (producingFut, containerStream) = metricsToContainerStream(startTime, numShards, numTimeSeries,
                           numMetrics, numSamples, dataset, shardMapper, spread, publishIntervalSec)
    GatewayServer.setupKafkaProducer(sourceConfig, containerStream)

    logger.info(s"Started producing $numSamples messages into topic $topicName with timestamps " +
      s"from about ${(System.currentTimeMillis() - startTime) / 1000 / 60} minutes ago")

    producingFut
  }

  //scalastyle:off method.length parameter.number
  def logQueryHelp(dataset: String, numMetrics: Int, numSamples: Int, numTimeSeries: Int, startTimeMs: Long,
                   genHist: Boolean, genDeltaHist: Boolean, genGauge: Boolean, publishIntervalSec: Int): Unit = {
    val startQuery = startTimeMs / 1000
    val endQuery = startQuery + (numSamples / numMetrics / numTimeSeries) * publishIntervalSec
    logger.info(s"Finished producing $numSamples records for ${(endQuery-startQuery).toDouble/60} minutes")

    val metricName = if (genGauge) "heap_usage0"
                      else if (genHist) "http_request_latency"
                      else if (genDeltaHist) "http_request_latency_delta"

    val promQL = s"""$metricName{_ns_="App-0",_ws_="demo"}"""

    val cliQuery =
      s"""./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset $dataset """ +
        s"""--promql '$promQL' --start $startQuery --end $endQuery --limit 15"""
    logger.info(s"CLI Query : \n$cliQuery")

    val encodedPromQL = URLEncoder.encode(promQL, StandardCharsets.UTF_8.toString)
    val httpQuery = s"http://localhost:8080/promql/$dataset/api/v1/query_range?" +
        s"query=$encodedPromQL&start=$startQuery&end=$endQuery&step=15"
    logger.info(s"HTTP Query : \n$httpQuery")
  }

  //scalastyle:off parameter.number
  def metricsToContainerStream(startTimeMs: Long,
                               numShards: Int,
                               numTimeSeries: Int,
                               numMetricNames: Int,
                               numSamples: Int,
                               dataset: Dataset,
                               shardMapper: ShardMapper,
                               spread: Int,
                               publishIntervalSec: Int): (Future[Unit], Observable[(Int, Seq[Array[Byte]])]) = {
    val (shardQueues, containerStream) = GatewayServer.shardingPipeline(GlobalConfig.systemConfig, numShards, dataset)

    val producingFut = Future {
      timeSeriesData(startTimeMs, numTimeSeries, numMetricNames, publishIntervalSec)
        .take(numSamples)
        .foreach { rec =>
          val shard = shardMapper.ingestionShard(rec.shardKeyHash, rec.partitionKeyHash, spread)
          while (!shardQueues(shard).offer(rec)) { Thread sleep 50 }
        }
    }
    (producingFut, containerStream)
  }

  /**
    * Generate Prometheus-schema time series data.
    *
    * @param startTime    Start time stamp
    * @param numTimeSeries number of instances or time series
    * @return stream of a 2-tuple (kafkaParitionId , sampleData)
    */
  def timeSeriesData(startTime: Long,
                     numTimeSeries: Int,
                     numMetricNames: Int,
                     publishIntervalSec: Int): Stream[InputRecord] = {
    // TODO For now, generating a (sinusoidal + gaussian) time series. Other generators more
    // closer to real world data can be added later.
    Stream.from(0).flatMap { n =>
      val instance = n % numTimeSeries
      val dc = instance & oneBitMask
      val partition = (instance >> 1) & twoBitMask
      val app = 0 // (instance >> 3) & twoBitMask // commented to get high-cardinality in one app
      val host = (instance >> 4) & twoBitMask
      val timestamp = startTime + (n.toLong / numTimeSeries) * publishIntervalSec * 1000
      val value = 15 + Math.sin(n + 1) + rand.nextGaussian()

      val tags = Map("dc"         -> s"DC$dc",
                     "_ws_"       -> "demo",
                     "_ns_"       -> s"App-$app",
                     "partition"  -> s"partition-$partition",
                     "partitionAl"-> s"partition-$partition",
                     "longTag"    -> "AlonglonglonglonglonglonglonglonglonglonglonglonglonglongTag",
                     "host"       -> s"H$host",
                     "hostAlias"  -> s"H$host",
                     "instance"   -> s"Instance-$instance")

      (0 until numMetricNames).map { i =>
        PrometheusInputRecord(tags, "heap_usage" + i, timestamp, value)
      }
    }
  }

  import ZCUTF8._

  val dcUTF8 = "dc".utf8
  val wsUTF8 = "_ws_".utf8
  val nsUTF8 = "_ns_".utf8
  val partUTF8 = "partition".utf8
  val hostUTF8 = "host".utf8
  val instUTF8 = "instance".utf8

  /**
   * Generate a stream of random Histogram data, with the metric name "http_request_latency"
   * Schema:  (timestamp:ts, sum:long, count:long, h:hist) for data, plus (metric:string, tags:map)
   * The dataset must match the above schema
   * Note: the set of "instance" tags is unique for each invocation of genHistogramData.  This helps increase
   * the cardinality of time series for testing purposes.
   */
  def genHistogramData(startTime: Long, numTimeSeries: Int = 16, histSchema: Schema): Stream[InputRecord] = {
    val numBuckets = 10
    val histBucketScheme = bv.GeometricBuckets(2.0, 3.0, numBuckets)
    var buckets = new Array[Long](numBuckets)
    val metric = if (Schemas.deltaHistogram == histSchema) {
                  "http_request_latency_delta"
                 } else {
                  "http_request_latency"
                 }

    def updateBuckets(bucketNo: Int): Unit = {
      for { b <- bucketNo until numBuckets } {
        buckets(b) += 1
      }
    }

    val instanceBase = System.currentTimeMillis
    var prevTimestamp = startTime
    Stream.from(0).map { n =>
      val instance = n % numTimeSeries + instanceBase
      val dc = instance & oneBitMask
      val partition = (instance >> 1) & twoBitMask
      val app = 0 // (instance >> 3) & twoBitMask // commented to get high-cardinality in one app
      val host = (instance >> 4) & twoBitMask
      val timestamp = startTime + (n.toLong / numTimeSeries) * 10000 // generate 1 sample every 10s for each instance

      updateBuckets(n % numBuckets)
      val hist = bv.LongHistogram(histBucketScheme, buckets.map(x => x))
      val count = util.Random.nextInt(100).toDouble
      val sum = buckets.sum.toDouble

      val tags = Map(dcUTF8   -> s"DC$dc".utf8,
                     wsUTF8   -> "demo".utf8,
                     nsUTF8   -> s"App-$app".utf8,
                     partUTF8 -> s"partition-$partition".utf8,
                     hostUTF8 -> s"H$host".utf8,
                     instUTF8 -> s"Instance-$instance".utf8)

      // reset buckets for delta histograms
      if (Schemas.deltaHistogram == histSchema && prevTimestamp != timestamp) {
        prevTimestamp = timestamp
        buckets = new Array[Long](numBuckets)
        updateBuckets(0)
      }

      new MetricTagInputRecord(Seq(timestamp, sum, count, hist), metric, tags, histSchema)
    }
  }
}

