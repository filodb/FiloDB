package filodb.kafka

import java.lang.{Long => JLong}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka.{KafkaProducerConfig, KafkaProducerSink}
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.jctools.queues.{MessagePassingQueue, MpscGrowableArrayQueue}

import filodb.core.{Response, Success}
import filodb.core.downsample.DownsamplePublisher

class KafkaDownsamplePublisher(sourceConf: Config, topics: Map[Int, String])
                              (implicit io: Scheduler) extends DownsamplePublisher with StrictLogging {

  val producerCfg = KafkaProducerConfig.default.copy(
    bootstrapServers = sourceConf.getString("sourceconfig.bootstrap.servers").split(',').toList
  )

  private val producer = KafkaProducerSink[JLong, Array[Byte]](producerCfg, io)
  val consumeSize = 1000
  val queue = new MpscGrowableArrayQueue[ProducerRecord[JLong, Array[Byte]]](100, 1000) // TODO get sizes from config

  override def publish(shardNum: Int, resolution: Int, records: Seq[Array[Byte]]): Future[Response] = {
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

  def makeDownsamplePublishStream(): Task[Unit] = {
    Observable.repeat(0).map { _: Int =>
      val records = new ArrayBuffer[ProducerRecord[JLong, Array[Byte]]](consumeSize)
      val consumer = new MessagePassingQueue.Consumer[ProducerRecord[JLong, Array[Byte]]] {
        override def accept(el: ProducerRecord[JLong, Array[Byte]]): Unit = {
          records += el
        }
      }
      queue.drain(consumer, consumeSize)
      records
    }.consumeWith(producer)
  }
}
