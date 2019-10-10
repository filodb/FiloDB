package filodb.jmh

import java.util.concurrent.TimeUnit

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.StrictLogging
import org.jboss.netty.buffer.ChannelBuffers
import org.openjdk.jmh.annotations._
import remote.RemoteStorage.{LabelPair, Sample, TimeSeries}

import filodb.core.binaryrecord2.RecordBuilder
import filodb.gateway.conversion.{InfluxProtocolParser, PrometheusInputRecord}
import filodb.memory.MemFactory

/**
 * Measures the shard calculation, deserialization, and ingestion record creation logic used in the Gateway
 */
@State(Scope.Thread)
class GatewayBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  val tagMap = Map(
    "__name__" -> "heap_usage",
    "dc"       -> "DC1",
    "_ws_"      -> "demo",
    "_ns_"      -> "App-123",
    "partition" -> "partition-2",
    "host"     -> "abc.xyz.company.com",
    "instance" -> s"Instance-123"
  )
  val influxTags = tagMap.filterKeys(_ != "__name__").toSeq.sortBy(_._1)

  val initTimestamp = System.currentTimeMillis
  val value: Double = 2.5

  def timeseries(tags: Map[String, String], dblValue: Double = value): TimeSeries = {
    val builder = TimeSeries.newBuilder
      .addSamples(Sample.newBuilder.setTimestampMs(initTimestamp).setValue(dblValue).build)
    tags.foreach { case (k, v) => builder.addLabels(LabelPair.newBuilder.setName(k).setValue(v).build) }
    builder.build
  }

  val singlePromTSBytes = timeseries(tagMap).toByteArray

  val singleInfluxRec = s"${tagMap("__name__")},${influxTags.map{case (k, v) => s"$k=$v"}.mkString(",")} " +
                        s"counter=$value ${initTimestamp}000000"
  val singleInfluxBuf = ChannelBuffers.buffer(1024)
  singleInfluxBuf.writeBytes(singleInfluxRec.getBytes)


  // Histogram containing 8 buckets + sum and count
  val histBuckets = Map("0.025" -> 0, "0.05" -> 0, "0.1" -> 2, "0.25" -> 2,
                        "0.5" -> 5, "1.0" -> 9, "2.5" -> 11, "+Inf" -> 11)
  val histSum = histBuckets.values.sum

  val histPromSeries =
    histBuckets.map { case (bucket, count) =>
      timeseries(tagMap ++ Map("__name__" -> "heap_usage_bucket", "le" -> bucket), count)
    } ++ Seq(timeseries(tagMap ++ Map("__name__" -> "heap_usage_sum"), histSum),
             timeseries(tagMap ++ Map("__name__" -> "heap_usage_count"), histBuckets.size))
  val histPromBytes = histPromSeries.map(_.toByteArray)

  val histInfluxRec = s"${tagMap("__name__")},${influxTags.map{case (k, v) => s"$k=$v"}.mkString(",")} " +
                      s"${histBuckets.map { case (k, v) => s"$k=$v"}.mkString(",") },sum=$histSum,count=8 " +
                      s"${initTimestamp}000000"
  val histInfluxBuf = ChannelBuffers.buffer(1024)
  histInfluxBuf.writeBytes(histInfluxRec.getBytes)

  val builder = new RecordBuilder(MemFactory.onHeapFactory, reuseOneContainer = true)

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def promCounterProtoConversion(): Int = {
    val record = PrometheusInputRecord(TimeSeries.parseFrom(singlePromTSBytes)).head
    val partHash = record.partitionKeyHash
    val shardHash = record.shardKeyHash
    record.getMetric
    record.nonMetricShardValues.length
    record.addToBuilder(builder)
    partHash | shardHash
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def influxCounterConversion(): Int = {
    // reset the ChannelBuffer so it can be read every timeseries
    singleInfluxBuf.resetReaderIndex()
    val record = InfluxProtocolParser.parse(singleInfluxBuf).get
    val partHash = record.partitionKeyHash
    val shardHash = record.shardKeyHash
    record.getMetric
    record.nonMetricShardValues.length
    record.addToBuilder(builder)
    partHash | shardHash
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def promHistogramProtoConversion(): Int = {
    var overallHash = 7
    histPromBytes.foreach { tsBytes =>
      val record = PrometheusInputRecord(TimeSeries.parseFrom(tsBytes)).head
      val partHash = record.partitionKeyHash
      val shardHash = record.shardKeyHash
      record.getMetric
      record.nonMetricShardValues.length
      record.addToBuilder(builder)
      overallHash |= partHash | shardHash
    }
    overallHash
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def influxHistogramConversion(): Int = {
    // reset the ChannelBuffer so it can be read every timeseries
    histInfluxBuf.resetReaderIndex()
    val record = InfluxProtocolParser.parse(histInfluxBuf).get
    val partHash = record.partitionKeyHash
    val shardHash = record.shardKeyHash
    record.getMetric
    record.nonMetricShardValues.length
    record.addToBuilder(builder)
    partHash | shardHash
  }
}