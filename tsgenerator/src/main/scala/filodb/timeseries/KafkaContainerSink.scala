package filodb.timeseries

import java.lang.{Long => JLong}

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.schedulers.SchedulerService
import monix.kafka._
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord


/**
 * This class takes an Observable of containers and writes them to Kafka
 * @param producerCfg the KafkaProducerConfig to use for writing to Kafka
 */
class KafkaContainerSink(producerCfg: KafkaProducerConfig, topicName: String) extends StrictLogging {
  /**
   * Write stream of containers, returning a Task which can be hooked into when the stream completes.
   * Currently flushes happen at a regular interval, with an extra flush happening when the item stream ends.
   * @param containerStream an Observable of containers
   */
  // TODO: abstract out some of this stuff so we can accept records of different schemas
  def writeTask(containerStream: Observable[(Int, Array[Byte])])
               (implicit io: SchedulerService): Task[Unit] = {
    val producer = KafkaProducerSink[JLong, Array[Byte]](producerCfg, io)

    containerStream.map { case (shard, bytes) =>
                     new ProducerRecord[JLong, Array[Byte]](topicName, shard, shard.toLong: JLong, bytes)
                   }.bufferIntrospective(5)
                   .consumeWith(producer)
  }
}