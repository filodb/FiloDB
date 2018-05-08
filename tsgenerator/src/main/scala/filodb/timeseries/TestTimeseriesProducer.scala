package filodb.timeseries

import java.lang.{Long => JLong}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.kafka._
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.rogach.scallop._

import filodb.coordinator.{ShardKeyGenerator, ShardMapper}

/**
  * Simple driver to produce time series data into local kafka similar. Data format is similar to
  * prometheus metric sample.
  * This is for development testing purposes only. TODO: Later evolve this to accept prometheus formats.
  *
  * Run as `java -cp classpath filodb.timeseries.TestTimeseriesProducer --help`
  *
  */
object TestTimeseriesProducer extends StrictLogging {
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
  }

  /**
    * Produce metrics
    * @param conf the sourceConfig
    * @param numSamples number of samples to produce
    * @param numTimeSeries number of time series partitions to produce
    * @param startMinutesAgo the samples will carry a timestamp starting from these many minutes ago
    * @return
    */
  def produceMetrics(conf: Config, numSamples: Int, numTimeSeries: Int, startMinutesAgo: Long): Future[Unit] = {
    val startTime = System.currentTimeMillis() - startMinutesAgo.minutes.toMillis
    val numShards = conf.getInt("num-shards")

    // TODO: use the official KafkaIngestionStream stuff to parse the file.  This is just faster for now.
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = conf.getString("sourceconfig.bootstrap.servers").split(',').toList
    )
    val topicName = conf.getString("sourceconfig.filo-topic-name")

    implicit val io = Scheduler.io("kafka-producer")
    logger.info(s"Started producing $numSamples messages into topic $topicName with timestamps " +
      s"from about ${(System.currentTimeMillis() - startTime) / 1000 / 60} minutes ago")
    val stream = timeSeriesData(startTime, numShards, numTimeSeries).take(numSamples)
    val producer = KafkaProducerSink[JLong, String](producerCfg, io)
    Observable.fromIterable(stream)
      //.dump("Produced: ")
      .map { case (partition, value) =>
        new ProducerRecord[JLong, String](topicName, partition.toInt, partition, value)
      }
      .bufferIntrospective(1024)
      .consumeWith(producer)
      .runAsync
      .map { _ =>
        logger.info(s"Finished producing $numSamples messages into topic $topicName with timestamps " +
          s"from about ${(System.currentTimeMillis() - startTime) / 1000 / 60} minutes ago at $startTime")
      }
      .recover { case NonFatal(e) =>
        logger.error("Error occurred while producing messages to Kafka", e)
      }
  }

  /**
    * Generate time series data.
    *
    * @param startTime    Start time stamp
    * @param numShards the number of shards or Kafka partitions
    * @param numTimeSeries number of instances or time series
    * @return stream of a 2-tuple (kafkaParitionId , sampleData)
    */
  def timeSeriesData(startTime: Long, numShards: Int, numTimeSeries: Int = 16): Stream[(JLong, String)] = {
    val shardMapper = new ShardMapper(numShards)
    val spread = (Math.log10(numShards / 2) / Math.log10(2.0)).toInt
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

      //scalastyle:off line.size.limit
      val shardKeyHash = ShardKeyGenerator.shardKeyHash(Seq("heap_usage", s"A$app"))
      val tagHash = s"dc=DC$dc,partition=P$partition,host=H$host,instance=I$instance".hashCode
      val kafkaParitionId: JLong = shardMapper.ingestionShard(shardKeyHash, tagHash, spread).toLong

      val sample = s"__name__=heap_usage,dc=DC$dc,job=A$app,partition=P$partition,host=H$host,instance=I$instance   $timestamp   $value"
      logger.trace(s"Producing $sample")
      (kafkaParitionId, s"__name__=heap_usage,dc=DC$dc,job=A$app,partition=P$partition,host=H$host,instance=I$instance   $timestamp   $value")
    }
  }
}

