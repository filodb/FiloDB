package filodb.timeseries

import java.lang.{Long => JLong}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.kafka._
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.rogach.scallop._

import filodb.coordinator.ShardMapper

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
    val numSamples = opt[Int](required = true, short = 'n')
    val startMinutesAgo = opt[Int](short='t')
    val numPartitions = opt[Int](short='p', default = Some(20))
    verify()
  }

  val numKafkaPartitions = 4 // this should match the numshards value specified in the dataset source configuration
  val topicName = "timeseries-dev"
  val oneBitMask = 0x1
  val twoBitMask = 0x3
  val rand = Random
  // start from a random day in the last 5 years
  val kafkaServer = "localhost:9092"

  def main(args: Array[String]): Unit = {

    val conf = new ProducerOptions(args)
    val numSamples = conf.numSamples()
    val numTimeSeries = conf.numPartitions()
    // to get default start time, look at numSamples and calculate a startTime that ends generation at current time
    val startMinutesAgo = conf.startMinutesAgo.toOption
      .getOrElse((numSamples.toDouble / numTimeSeries / 6).ceil.toInt )  // at 6 samples per minute

    Await.result(produceMetrics(numSamples, numTimeSeries, startMinutesAgo), 1.hour)
  }

  /**
    * Produce metrics
    * @param numSamples number of samples to produce
    * @param numTimeSeries number of time series partitions to produce
    * @param startMinutesAgo the samples will carry a timestamp starting from these many minutes ago
    * @return
    */
  def produceMetrics(numSamples: Int, numTimeSeries: Int, startMinutesAgo: Long): Future[Unit] = {
    val startTime = System.currentTimeMillis() - startMinutesAgo * 60 * 1000

    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List(kafkaServer)
    )
    implicit val io = Scheduler.io("kafka-producer")
    logger.info(s"Started producing $numSamples messages into topic $topicName with timestamps " +
      s"from about ${(System.currentTimeMillis() - startTime) / 1000 / 60} minutes ago")
    val stream = timeSeriesData(startTime, numTimeSeries).take(numSamples)
    val producer = KafkaProducerSink[JLong, String](producerCfg, io)
    Observable.fromIterable(stream)
      .map { case (partition, value) =>
        new ProducerRecord[JLong, String](topicName, partition.toInt, partition, value)
      }
      .bufferIntrospective(1024)
      .consumeWith(producer)
      .runAsync
      .map { _ =>
        logger.info(s"Finished producing $numSamples messages into topic $topicName with timestamps " +
          s"from about ${(System.currentTimeMillis() - startTime) / 1000 / 60} minutes ago")
      }
      .recover { case NonFatal(e) =>
        logger.error("Error occurred while producing messages to Kafka", e)
      }
  }

  /**
    * Generate time series data.
    *
    * @param startTime    Start time stamp
    * @param numTimeSeries number of instances or time series
    * @return stream of a 2-tuple (kafkaParitionId , sampleData)
    */
  def timeSeriesData(startTime: Long, numTimeSeries: Int = 16): Stream[(JLong, String)] = {

    val shardMapper = new ShardMapper(numKafkaPartitions)
    // TODO For now, generating a (sinusoidal + gaussian) time series. Other generators more
    // closer to real world data can be added later.
    Stream.from(0).map { n =>
      val instance = n % numTimeSeries
      val dc = instance & oneBitMask
      val partition = (instance >> 1) & twoBitMask
      val app = (instance >> 3) & oneBitMask
      val host = (instance >> 4) & twoBitMask
      val timestamp = startTime + (n.toLong / numTimeSeries) * 10000 // generate 1 sample every 10s for each instance
      val value = 15 + Math.sin(n + 1) + rand.nextGaussian()

      //scalastyle:off line.size.limit
      val appNameMetricNameHash = s"__name__=heap_usage;app=A$app".hashCode
      val tagHash = s"__name__=heap_usage,dc=DC$dc,app=A$app,partition=P$partition,host=H$host,instance=I$instance".hashCode
      val kafkaParitionId: JLong = shardMapper.ingestionShard(appNameMetricNameHash, tagHash, 2).toLong

      val sample = s"__name__=heap_usage,dc=DC$dc,app=A$app,partition=P$partition,host=H$host,instance=I$instance   $timestamp   $value"
      logger.trace(s"Producing $sample")
      (kafkaParitionId, s"__name__=heap_usage,dc=DC$dc,app=A$app,partition=P$partition,host=H$host,instance=I$instance   $timestamp   $value")
    }
  }
}

