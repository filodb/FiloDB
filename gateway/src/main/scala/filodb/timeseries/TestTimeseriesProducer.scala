package filodb.timeseries

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import org.rogach.scallop._

import filodb.coordinator.{GlobalConfig, ShardMapper}
import filodb.core.metadata.Dataset
import filodb.gateway.conversion.PrometheusInputRecord
import filodb.gateway.GatewayServer
import filodb.prometheus.FormatConversion

sealed trait DataOrCommand
final case class DataSample(tags: Map[String, String],
                            metric: String,
                            timestamp: Long,
                            value: Double) extends DataOrCommand
case object FlushCommand extends DataOrCommand

/**
  * Simple driver to produce time series data into local kafka similar. Data format is similar to
  * prometheus metric sample.
  * This is for development testing purposes only. TODO: Later evolve this to accept prometheus formats.
  *
  * Run as `java -cp classpath filodb.timeseries.TestTimeseriesProducer --help`
  *
  */
object TestTimeseriesProducer extends StrictLogging {
  val dataset = FormatConversion.dataset

  class ProducerOptions(args: Seq[String]) extends ScallopConf(args) {
    val samplesPerSeries = opt[Int](short = 'n', default = Some(100),
                                    descr="# of samples per time series")
    val startMinutesAgo = opt[Int](short='t')
    val numSeries = opt[Int](short='p', default = Some(20), descr="# of total time series")
    val sourceConfigPath = opt[String](required = true, short = 'c',
                                       descr="Path to source conf file eg conf/timeseries-dev-source.conf")
    verify()
  }

  val oneBitMask = 0x1
  val twoBitMask = 0x3
  val rand = Random
  // start from a random day in the last 5 years

  def main(args: Array[String]): Unit = {
    val conf = new ProducerOptions(args)
    val sourceConfig = ConfigFactory.parseFile(new java.io.File(conf.sourceConfigPath()))
    val numSamples = conf.samplesPerSeries() * conf.numSeries()
    val numTimeSeries = conf.numSeries()
    // to get default start time, look at numSamples and calculate a startTime that ends generation at current time
    val startMinutesAgo = conf.startMinutesAgo.toOption
      .getOrElse((numSamples.toDouble / numTimeSeries / 6).ceil.toInt )  // at 6 samples per minute

    Await.result(produceMetrics(sourceConfig, numSamples, numTimeSeries, startMinutesAgo), 1.hour)
    sys.exit(0)
  }

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
      // Give enough time for the last containers to be sent off successfully to Kafka
      Thread sleep 2000
      logger.info(s"Finished producing $numSamples messages into topic $topicName with timestamps " +
        s"from about ${(System.currentTimeMillis() - startTime) / 1000 / 60} minutes ago at $startTime")
      val startQuery = startTime / 1000
      val endQuery = startQuery + 300
      val query =
        s"""./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset prometheus """ +
        s"""--promql 'heap_usage{dc="DC0",app="App-0"}' --start $startQuery --end $endQuery --limit 15"""
      logger.info(s"Sample Query you can use: \n$query")
      val q = URLEncoder.encode("heap_usage{dc=\"DC0\",app=\"App-0\"}", StandardCharsets.UTF_8.toString)
      val url = s"http://localhost:8080/promql/prometheus/api/v1/query_range?" +
        s"query=$q&start=$startQuery&end=$endQuery&step=15"
      logger.info(s"Sample URL you can use to query: \n$url")
    }
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
      timeSeriesData(startTime, numShards, numTimeSeries)
        .take(numSamples)
        .foreach { sample =>
          val rec = PrometheusInputRecord(sample.tags, sample.metric, dataset, sample.timestamp, sample.value)
          val shard = shardMapper.ingestionShard(rec.shardKeyHash, rec.partitionKeyHash, spread)
          while (!shardQueues(shard).offer(rec)) { Thread sleep 50 }
        }
    }
    (producingFut, containerStream)
  }

  /**
    * Generate time series data.
    *
    * @param startTime    Start time stamp
    * @param numShards the number of shards or Kafka partitions
    * @param numTimeSeries number of instances or time series
    * @return stream of a 2-tuple (kafkaParitionId , sampleData)
    */
  def timeSeriesData(startTime: Long, numShards: Int, numTimeSeries: Int = 16): Stream[DataSample] = {
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
                     "app"      -> s"App-$app",
                     "partition" -> s"partition-$partition",
                     "host"     -> s"H$host",
                     "instance" -> s"Instance-$instance")
      DataSample(tags, "heap_usage", timestamp, value)
    }
  }
}

