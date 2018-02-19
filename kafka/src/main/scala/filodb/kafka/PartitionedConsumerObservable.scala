package filodb.kafka

import java.lang.{Long => JLong}

import scala.concurrent.blocking

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

object PartitionedConsumerObservable extends StrictLogging {
  import collection.JavaConverters._

  /** Creates a `KafkaConsumerObservable` instance.
    *
    * @param sourceConfig the `SourceConfig` needed for initializing the consumer
    * @param topicPartition the Kafka ingestion topic-partition(s) to assign the new consumer to
    * @param offset Some(longOffset) to seek to a certain offset when the consumer starts
    */
  def create(sourceConfig: SourceConfig,
             topicPartition: TopicPartition,
             offset: Option[Long]): KafkaConsumerObservable[JLong, Any] = {

    val consumer = createConsumer(sourceConfig, topicPartition, offset)
    val cfg = consumerConfig(sourceConfig)

    KafkaConsumerObservable[JLong, Any](cfg, consumer)
  }

  private[filodb] def createConsumer(sourceConfig: SourceConfig,
                                     topicPartition: TopicPartition,
                                     offset: Option[Long]): Task[KafkaConsumer[JLong, Any]] =
    Task {
      val props = sourceConfig.asProps
      if (sourceConfig.LogConfig) logger.info(s"Consumer properties: $props")

      blocking {
        val consumer = new KafkaConsumer(props)
        consumer.assign(List(topicPartition).asJava)
        offset.foreach { off => consumer.seek(topicPartition, off) }
        consumer.asInstanceOf[KafkaConsumer[JLong, Any]]
      }
    }

  private[filodb] def consumerConfig(sourceConfig: SourceConfig) =
    KafkaConsumerConfig(sourceConfig.asConfig)

}

object PartitionedProducerSink {

  /** Convenience function for creating a Monix producer:
    * {{{
    *     val producer = PartitionedProducer.create[K, V](sinkConfig, scheduler)
    * }}}
    *
    * @param sinkConfig the kafka settings
    *
    * @param io the monix scheduler
    */
  def create[K, V](sinkConfig: SinkConfig, io: Scheduler)
                  (implicit K: Serializer[K], V: Serializer[V]): KafkaProducerSink[K, V] = {
    val cfg = KafkaProducerConfig(sinkConfig.asConfig)
    KafkaProducerSink[K, V](cfg, io)
  }
}
