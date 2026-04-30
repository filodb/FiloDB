package filodb.timeseries

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable

import filodb.coordinator.ShardMapper
import filodb.core.GlobalConfig
import filodb.core.metadata.{Dataset, Schema, Schemas}
import filodb.core.metadata.Schemas.{aggregatingDeltaHistogramV2, gauge}
import filodb.gateway.GatewayServer
import filodb.gateway.conversion.{DeltaCounterRecord, InputRecord, MetricTagInputRecord, PrometheusCounterRecord,
                                  PrometheusGaugeRecord}
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
                   genHist: Boolean, genDeltaHist: Boolean, genGauge: Boolean,
                   genPromCounter: Boolean,
                   genOtelCumulativeHistData: Boolean,
                   genOtelDeltaHistData: Boolean, genOtelExpDeltaHistData: Boolean,
                   publishIntervalSec: Int, nameSpace: String, workSpace: String): Unit = {
    val startQuery = startTimeMs / 1000
    val endQuery = startQuery + (numSamples / numMetrics / numTimeSeries) * publishIntervalSec
    logger.info(s"Finished producing $numSamples records for ${(endQuery-startQuery).toDouble/60} minutes")

    val metricName = if (genGauge) "heap_usage0"
                      else if (genHist || genOtelCumulativeHistData) "http_request_latency"
                      else if (genDeltaHist || genOtelDeltaHistData || genOtelExpDeltaHistData)
                        "http_request_latency_delta"
                      else if (genPromCounter) "heap_usage_counter"
                      else "heap_usage_delta0"

    val promQL = s"""$metricName{_ns_="$nameSpace",_ws_="$workSpace"}"""

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
                               publishIntervalSec: Int,
                               expHist: Boolean = false,
                               numBuckets: Int = 20): (Future[Unit], Observable[(Int, Seq[Array[Byte]])]) = {
    val (shardQueues, containerStream) = GatewayServer.shardingPipeline(GlobalConfig.systemConfig, numShards, dataset)

    val producingFut = Future {
      val data = if (expHist) genHistogramData(startTimeMs, numTimeSeries,
                                   Schemas.otelExpDeltaHistogram, numBuckets = numBuckets)
      else timeSeriesData(startTimeMs, numTimeSeries, numMetricNames, publishIntervalSec, gauge)
      data.take(numSamples)
        .foreach { rec =>
          val shard = shardMapper.ingestionShard(rec.shardKeyHash, rec.partitionKeyHash, spread)
          while (!shardQueues(shard).offer(rec)) {  Thread sleep 50 }
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
                     publishIntervalSec: Int,
                     schema: Schema,
                     namespace: String = "App-0",
                     workspace: String = "demo",
                     metricNameOverride: Option[String] = None): Stream[InputRecord] = {
    // TODO For now, generating a (sinusoidal + gaussian) time series. Other generators more
    // closer to real world data can be added later.
    val metricName = metricNameOverride.getOrElse("heap_usage")
    Stream.from(0).flatMap { n =>
      val instance = n % numTimeSeries
      val dc = instance & oneBitMask
      val partition = (instance >> 1) & twoBitMask
      val app = 0 // (instance >> 3) & twoBitMask // commented to get high-cardinality in one app
      val host = (instance >> 4) & twoBitMask
      val timestamp = startTime + (n.toLong / numTimeSeries) * publishIntervalSec * 1000
      val value = 15 + Math.sin(n + 1) + rand.nextGaussian()

      val tags = Map("dc"         -> s"DC$dc",
                     "_ws_" -> workspace,
                     "_ns_" -> namespace,
                     "partition"  -> s"partition-$partition",
                     "partitionAl"-> s"partition-$partition",
                     "longTag"    -> "AlonglonglonglonglonglonglonglonglonglonglonglonglonglongTag",
                     "host"       -> s"H$host",
                     "hostAlias"  -> s"H$host",
                     "instance"   -> s"Instance-$instance")

      (0 until numMetricNames).map { i =>
        if (schema == Schemas.deltaCounter)
          DeltaCounterRecord(tags, metricName + "_delta" + i, timestamp, value)
        else
          PrometheusGaugeRecord(tags, metricName + i, timestamp, value)
      }
    }
  }

  def timeSeriesCounterData(startTime: Long,
                     numTimeSeries: Int,
                     numMetricNames: Int,
                     publishIntervalSec: Int,
                     namespace: String = "App-0",
                     workspace: String = "demo",
                     metricNameOverride: Option[String] = None): Stream[InputRecord] = {
    // TODO For now, generating a (sinusoidal + gaussian) time series. Other generators more
    // closer to real world data can be added later.
    val metricName = metricNameOverride.getOrElse("heap_usage_counter")
    val valMap: mutable.HashMap[Map[String, String], Double] = mutable.HashMap.empty[Map[String, String], Double]
    Stream.from(0).flatMap { n =>
      val instance = n % numTimeSeries
      val dc = instance & oneBitMask
      val partition = (instance >> 1) & twoBitMask
      val app = 0 // (instance >> 3) & twoBitMask // commented to get high-cardinality in one app
      val host = (instance >> 4) & twoBitMask
      val timestamp = startTime + (n.toLong / numTimeSeries) * publishIntervalSec * 1000

      val tags = Map("dc" -> s"DC$dc",
        "_ws_" -> workspace,
        "_ns_" -> namespace,
        "partition" -> s"partition-$partition",
        "partitionAl" -> s"partition-$partition",
        "longTag" -> "AlonglonglonglonglonglonglonglonglonglonglonglonglonglongTag",
        "host" -> s"H$host",
        "hostAlias" -> s"H$host",
        "instance" -> s"Instance-$instance")
      var value: Double = valMap.getOrElse(tags, 0)
      value += (15 + Math.sin(n + 1) + rand.nextGaussian())
      valMap(tags) = value
      (0 until numMetricNames).map { i =>
        PrometheusCounterRecord(tags, metricName + i, timestamp, value)
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
  def genHistogramData(startTime: Long, numTimeSeries: Int = 16, histSchema: Schema,
                       numBuckets : Int = 20, metricNameOverride: Option[String] = None,
                       namespace: String = "App-0", workspace: String = "demo"): Stream[InputRecord] = {

    val metricName: String = metricNameOverride.getOrElse("http_request_latency")
    val histBucketScheme = if (Schemas.otelExpDeltaHistogram == histSchema)
                                bv.Base2ExpHistogramBuckets(3, -numBuckets/2, numBuckets)
                           else {
                             // Create custom buckets that include +Inf as the last bucket
                             val finiteBuckets = (0 until numBuckets-1).map { i =>
                               2.0 * Math.pow(3.0, i)
                             }.toArray
                             val allBuckets = finiteBuckets :+ Double.PositiveInfinity
                             bv.CustomBuckets(allBuckets)
                           }
    var buckets = new Array[Long](histBucketScheme.numBuckets)
    val metric = if (Schemas.deltaHistogram == histSchema || Schemas.otelDeltaHistogram == histSchema
                     || Schemas.otelExpDeltaHistogram == histSchema) {
                  metricName + "_delta"
                 } else {
                  metricName
                 }

    def updateBuckets(bucketNo: Int): Unit = {
      for { b <- bucketNo until histBucketScheme.numBuckets } {
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
      // reset buckets for delta histograms
      if ( (Schemas.deltaHistogram == histSchema || Schemas.otelDeltaHistogram == histSchema
        || Schemas.otelExpDeltaHistogram == histSchema)
          && prevTimestamp != timestamp) {
        prevTimestamp = timestamp
        buckets = new Array[Long](histBucketScheme.numBuckets)
      }
      updateBuckets(n % histBucketScheme.numBuckets)
      val hist = bv.LongHistogram(histBucketScheme, buckets.map(x => x))
      val count = util.Random.nextInt(100).toDouble
      val sum = buckets.sum.toDouble

      val tags = Map(dcUTF8   -> s"DC$dc".utf8,
                     wsUTF8   -> workspace.utf8,
                     nsUTF8   -> namespace.utf8,
                     partUTF8 -> s"partition-$partition".utf8,
                     hostUTF8 -> s"H$host".utf8,
                     instUTF8 -> s"Instance-$instance".utf8)

      if (histSchema == Schemas.otelDeltaHistogram || histSchema == Schemas.otelCumulativeHistogram
                 || Schemas.otelExpDeltaHistogram == histSchema) {
        val minVal = buckets.min.toDouble
        val maxVal = buckets.max.toDouble
        new MetricTagInputRecord(Seq(timestamp, sum, count, hist, minVal, maxVal), metric, tags, histSchema)
      }
      else {
        new MetricTagInputRecord(Seq(timestamp, sum, count, hist), metric, tags, histSchema)
      }
    }
  }

  /**
   * Generate a stream of out-of-order delta-histogram-v2 data for testing the aggregating schema.
   * Produces samples in batches, shuffling a configurable percentage out of chronological order.
   * The generated data uses the aggregating-delta-histogram-v2 schema which has 7 data columns:
   *   (timestamp:ts, sum:double, count:double, h:hist, min:double, max:double, sumLast:double)
   * plus partition key columns (metric:string, tags:map).
   *
   * @param startTime      starting timestamp in millis
   * @param numTimeSeries  number of distinct time series / partitions
   * @param numBuckets     number of histogram buckets
   * @param oooPercent     percentage of samples (0-100) to send out of chronological order
   * @param maxSkewSecs    maximum time skew in seconds for OOO samples (should be within OOO tolerance)
   * @param publishIntervalSec interval in seconds between samples per time series
   * @param namespace      _ns_ tag value
   * @param workspace      _ws_ tag value
   */
  def genOooHistogramData(startTime: Long, numTimeSeries: Int = 16,
                          numBuckets: Int = 20,
                          oooPercent: Int = 30,
                          maxSkewSecs: Int = 90,
                          publishIntervalSec: Int = 10,
                          namespace: String = "App-0",
                          workspace: String = "demo"): Stream[InputRecord] = {
    val metricName = "http_req_latency_ooo_delta"
    val histBucketScheme = {
      val finiteBuckets = (0 until numBuckets - 1).map { i =>
        2.0 * Math.pow(3.0, i)
      }.toArray
      val allBuckets = finiteBuckets :+ Double.PositiveInfinity
      bv.CustomBuckets(allBuckets)
    }

    val batchSize = numTimeSeries * 10 // 10 timestamps worth of samples per batch
    val instanceBase = System.currentTimeMillis

    // Generate samples in batches and shuffle OOO samples within each batch
    Stream.from(0).grouped(batchSize).flatMap { indices =>
      val batch = indices.map { n =>
        val instance = n % numTimeSeries + instanceBase
        val dc = instance & oneBitMask
        val partition = (instance >> 1) & twoBitMask
        val host = (instance >> 4) & twoBitMask
        val timestamp = startTime + (n.toLong / numTimeSeries) * publishIntervalSec * 1000

        // Generate histogram data for this sample
        val buckets = new Array[Long](histBucketScheme.numBuckets)
        val bucketNo = n % histBucketScheme.numBuckets
        for (b <- bucketNo until histBucketScheme.numBuckets) { buckets(b) = (n / numTimeSeries + 1).toLong }
        val hist = bv.LongHistogram(histBucketScheme, buckets.map(x => x))
        val count = rand.nextInt(100).toDouble
        val sum = buckets.sum.toDouble
        val minVal = buckets.min.toDouble
        val maxVal = buckets.max.toDouble
        val sumLast = sum * 0.8 + rand.nextGaussian() * 0.1

        val tags = Map(dcUTF8   -> s"DC$dc".utf8,
                       wsUTF8   -> workspace.utf8,
                       nsUTF8   -> namespace.utf8,
                       partUTF8 -> s"partition-$partition".utf8,
                       hostUTF8 -> s"H$host".utf8,
                       instUTF8 -> s"Instance-$instance".utf8)

        (timestamp, sum, count, hist, minVal, maxVal, sumLast, metricName, tags)
      }

      // Shuffle OOO samples: for ~oooPercent% of samples, skew their timestamp backward
      val shuffled = batch.map { case (ts, sum, count, hist, minVal, maxVal, sumLast, metric, tags) =>
        val isOoo = rand.nextInt(100) < oooPercent
        val actualTs = if (isOoo) {
          // Push timestamp back by a random amount up to maxSkewSecs
          val skewMs = (rand.nextInt(maxSkewSecs) + 1) * 1000L
          ts - skewMs
        } else {
          ts
        }
        new MetricTagInputRecord(
          Seq(actualTs, sum, count, hist, minVal, maxVal, sumLast),
          metric, tags, aggregatingDeltaHistogramV2)
      }

      // Shuffle the batch order so OOO samples are interspersed rather than grouped
      rand.shuffle(shuffled).toStream
    }.toStream
  }
}

