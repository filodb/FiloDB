package filodb.timeseries

import java.lang.{Long => JLong}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Random, Try}
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.kafka._
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord

import filodb.coordinator.ShardMapper


/**
  * Simple driver to produce time series data into local kafka similar. Data format is similar to
  * prometheus metric sample.
  * This is for development testing purposes only. TODO: Later evolve this to accept prometheus formats.
  *
  * Run as `java -cp classpath filodb.timeseries.TestTimeseriesProducer numMessages`
  *
  */
object TestTimeseriesProducer extends StrictLogging {

  val numKafkaPartitions = 4 // this should match the numshards value specified in the dataset source configuration
  val topicName = "timeseries-dev"
  val oneBitMask = 0x1
  val twoBitMask = 0x3
  val rand = Random
  // start from a random day in the last 5 years
  val defaultStartTime = System.currentTimeMillis() - 1000L * 60 * 60 * 24 * rand.nextInt(365 * 5)
  val kafkaServer = "localhost:9092"

  def main(args: Array[String]): Unit = {

    val numSamples = Try(args(0).toInt).toOption

    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List(kafkaServer)
    )

    numSamples match {
      case None =>
        logger.info("Provide valid number of samples as an argument")
      case Some(n) =>
        logger.info(s"Started producing $n messages into topic $topicName")
        implicit val io = Scheduler.io("kafka-producer")
        val stream = timeSeriesData().take(n)
        val producer = KafkaProducerSink[JLong, String](producerCfg, io)
        val sinkT = Observable.fromIterable(stream)
          .map { case (partition, value) =>
            new ProducerRecord[JLong, String](topicName, partition.toInt, partition, value)
          }
          .bufferIntrospective(1024)
          .consumeWith(producer)
          .runAsync
          .recover { case NonFatal(e) =>
            logger.error("Error occcured while producing messages to Kafka", e)
          }
        Await.result(sinkT, 1.hour)
        logger.info(s"Finished producing $n messages")
    }
  }

  /**
    * Generate time series data.
    *
    * @param startTime    Start time stamp
    * @param numInstances number of instances or time series
    * @return stream of a 2-tuple (kafkaParitionId , sampleData)
    */
  def timeSeriesData(startTime: Long = defaultStartTime, numInstances: Int = 16): Stream[(JLong, String)] = {

    val shardMapper = new ShardMapper(numKafkaPartitions)
    // TODO For now, generating a (sinusoidal + gaussian) time series. Other generators more
    // closer to real world data can be added later.
    Stream.from(0).map { n =>
      val dc = n & oneBitMask
      val partition = (n >> 1) & twoBitMask
      val app = (n >> 3) & oneBitMask
      val host = (n >> 4) & twoBitMask
      val instance = n % numInstances
      val timestamp = startTime + (n / numInstances) * 10000 // generate 1 sample every 10s for each instance
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
