package filodb.kafka

import java.lang.{Long => JLong}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{CancelableFuture, Scheduler}
import monix.kafka.{KafkaProducerConfig, KafkaProducerSink}
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.jctools.queues.{MessagePassingQueue, MpscGrowableArrayQueue}

import filodb.core.{Response, Success}
import filodb.core.downsample.DownsamplePublisher

class KafkaDownsamplePublisher(downsampleConfig: Config) extends DownsamplePublisher with StrictLogging {

  private val kafkaConfig = KafkaProducerConfig(downsampleConfig.getConfig("publisher-config.kafka"))
  private implicit val sched = Scheduler.computation(name = "downsample")

  private val topics: Map[Int, String] = downsampleConfig.getConfig("publisher-config.topics")
    .entrySet().asScala.map { e => e.getKey.toInt -> e.getValue.unwrapped().toString }.toMap

  private val consumeSize = downsampleConfig.getInt("publisher-config.consume-batch-size")
  private val minQueueSize = downsampleConfig.getInt("publisher-config.min-queue-size")
  private val maxQueueSize = downsampleConfig.getInt("publisher-config.max-queue-size")
  private val queue = new MpscGrowableArrayQueue[ProducerRecord[JLong, Array[Byte]]](minQueueSize, maxQueueSize)
  private var future: CancelableFuture[Unit] = _

  override def publish(shardNum: Int, resolution: Int, records: Seq[Array[Byte]]): Future[Response] = {
    logger.debug(s"Got request to publish ${records.size} record containers " +
      s"to shard $shardNum for resolution $resolution")
    topics.get(resolution) match {
      case Some(topic) =>
        records.foreach { bytes =>
          queue.offer(new ProducerRecord[JLong, Array[Byte]](topic, shardNum, shardNum.toLong: JLong, bytes))
        }
        Future.successful(Success)
      case None =>
        Future.failed(new IllegalArgumentException(s"Unregistered resolution $resolution"))
    }
  }

  def start(): Unit = {
    logger.info(s"Starting Kafka Downsampling Publisher. Will be publishing to $topics ")
    val producer = KafkaProducerSink[JLong, Array[Byte]](kafkaConfig, sched)
    future = Observable.repeat(0).map { _: Int =>
      val records = new ArrayBuffer[ProducerRecord[JLong, Array[Byte]]](consumeSize)
      val consumer = new MessagePassingQueue.Consumer[ProducerRecord[JLong, Array[Byte]]] {
        override def accept(el: ProducerRecord[JLong, Array[Byte]]): Unit = {
          records += el
        }
      }
      queue.drain(consumer, consumeSize)
      records
    }.consumeWith(producer)
     .onErrorRestartIf { case t => logger.error("Error in kafka downsample publisher stream", t); true }
     .runAsync
  }

  def stop(): Unit = {
    logger.info("Stopping Kafka Downsampling Publisher")
    future.cancel()
  }
}
