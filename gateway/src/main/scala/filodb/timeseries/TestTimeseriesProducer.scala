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
import filodb.core.metadata.{Column, Dataset, Schemas}
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
  def produceMetrics(sourceConfig: Config, numSamples: Int, numTimeSeries: Int, startMinutesAgo: Long): Future[Unit] = {
    val startTime = System.currentTimeMillis() - startMinutesAgo.minutes.toMillis
    val numShards = sourceConfig.getInt("num-shards")
    val shardMapper = new ShardMapper(numShards)
    val spread = if (numShards >= 2) { (Math.log10(numShards / 2) / Math.log10(2.0)).toInt } else { 0 }
    val topicName = sourceConfig.getString("sourceconfig.filo-topic-name")

    val (producingFut, containerStream) = metricsToContainerStream(startTime, numShards, numTimeSeries,
                                            numSamples, dataset, shardMapper, spread)
    GatewayServer.setupKafkaProducer(sourceConfig, containerStream)

    logger.info(s"Started producing $numSamples messages into topic $topicName with timestamps " +
      s"from about ${(System.currentTimeMillis() - startTime) / 1000 / 60} minutes ago")

    producingFut.map { _ =>
      logQueryHelp(numSamples, numTimeSeries, startTime)
    }
  }


  def logQueryHelp(numSamples: Int, numTimeSeries: Int, startTime: Long): Unit = {
    val samplesDuration = (numSamples.toDouble / numTimeSeries / 6).ceil.toInt * 60L * 1000L

    logger.info(s"Finished producing $numSamples records for ${samplesDuration / 1000} seconds")
    val startQuery = startTime / 1000
    val endQuery = startQuery + (numSamples / numTimeSeries) * 10
    val periodicPromQL = """heap_usage{_ns_="App-0",_ws_="demo"}"""
    val query =
      s"""./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset prometheus """ +
      s"""--promql '$periodicPromQL' --start $startQuery --end $endQuery --limit 15"""
    logger.info(s"Periodic Samples CLI Query : \n$query")

    val periodicSamplesQ = URLEncoder.encode(periodicPromQL, StandardCharsets.UTF_8.toString)
    val periodicSamplesUrl = s"http://localhost:8080/promql/prometheus/api/v1/query_range?" +
      s"query=$periodicSamplesQ&start=$startQuery&end=$endQuery&step=15"
    logger.info(s"Periodic Samples query URL: \n$periodicSamplesUrl")

    val rawSamplesQ = URLEncoder.encode("""heap_usage{_ws_="demo",_ns_="App-0"}[2m]""",
      StandardCharsets.UTF_8.toString)
    val rawSamplesUrl = s"http://localhost:8080/promql/prometheus/api/v1/query?query=$rawSamplesQ&time=$endQuery"
    logger.info(s"Raw Samples query URL: \n$rawSamplesUrl")
  }

  def metricsToContainerStream(startTime: Long,
                               numShards: Int,
                               numTimeSeries: Int,
                               numSamples: Int,
                               dataset: Dataset,
                               shardMapper: ShardMapper,
                               spread: Int): (Future[Unit], Observable[(Int, Seq[Array[Byte]])]) = {
    val (shardQueues, containerStream) = GatewayServer.shardingPipeline(GlobalConfig.systemConfig, numShards, dataset)

    val producingFut = Future {
      timeSeriesData(startTime, numTimeSeries)
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
  def timeSeriesData(startTime: Long, numTimeSeries: Int = 16): Stream[InputRecord] = {
    // TODO For now, generating a (sinusoidal + gaussian) time series. Other generators more
    // closer to real world data can be added later.
    Stream.from(0).map { n =>
      val instance = n % numTimeSeries
      val dc = instance & oneBitMask
      val partition = (instance >> 1) & twoBitMask
      val app = (instance >> 3) & twoBitMask
      val host = (instance >> 4) & twoBitMask
      val timestamp = startTime + (n.toLong / numTimeSeries) * 10000 // generate 1 sample every 10s for each instance
      val value = 15 + Math.sin(n + 1) + rand.nextGaussian()

      val tags = Map("dc"       -> s"DC$dc",
                     "_ws_"      -> "demo",
                     "_ns_"      -> s"App-$app",
                     "partition" -> s"partition-$partition",
                     "host"     -> s"H$host",
                     "instance" -> s"Instance-$instance")

      PrometheusInputRecord(tags, "heap_usage", timestamp, value)
    }
  }

  import ZCUTF8._
  import Column.ColumnType._

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
  def genHistogramData(startTime: Long, dataset: Dataset, numTimeSeries: Int = 16): Stream[InputRecord] = {
    require(dataset.dataColumns.map(_.columnType) == Seq(TimestampColumn, LongColumn, LongColumn, HistogramColumn))
    val numBuckets = 10

    val histBucketScheme = bv.GeometricBuckets(2.0, 3.0, numBuckets)
    val buckets = new Array[Long](numBuckets)
    def updateBuckets(bucketNo: Int): Unit = {
      for { b <- bucketNo until numBuckets } {
        buckets(b) += 1
      }
    }

    val instanceBase = System.currentTimeMillis

    Stream.from(0).map { n =>
      val instance = n % numTimeSeries + instanceBase
      val dc = instance & oneBitMask
      val partition = (instance >> 1) & twoBitMask
      val app = (instance >> 3) & twoBitMask
      val host = (instance >> 4) & twoBitMask
      val timestamp = startTime + (n.toLong / numTimeSeries) * 10000 // generate 1 sample every 10s for each instance

      updateBuckets(n % numBuckets)
      val hist = bv.LongHistogram(histBucketScheme, buckets.map(x => x))
      val count = util.Random.nextInt(100).toLong
      val sum = buckets.sum

      val tags = Map(dcUTF8   -> s"DC$dc".utf8,
                     wsUTF8   -> "demo".utf8,
                     nsUTF8   -> s"App-$app".utf8,
                     partUTF8 -> s"partition-$partition".utf8,
                     hostUTF8 -> s"H$host".utf8,
                     instUTF8 -> s"Instance-$instance".utf8)

      new MetricTagInputRecord(Seq(timestamp, sum, count, hist), "http_request_latency", tags, dataset.schema)
    }
  }
}

