package filodb.kafka

import java.lang.{Long => JLong}

import scala.concurrent.blocking

import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

object PartitionedConsumerObservable {
  import collection.JavaConverters._

  /** Creates a `KafkaConsumerObservable` instance.
    *
    * @param settings the `KafkaSettings` needed for initializing the consumer
    *
    * @param topicPartition the Kafka ingestion topic-partition(s) to assign the new consumer to
    */
  def create(settings: KafkaSettings, topicPartition: TopicPartition): KafkaConsumerObservable[JLong, Any] = {

    val consumer = createConsumer(settings, topicPartition)
    val cfg = consumerConfig(settings)

    KafkaConsumerObservable[JLong, Any](cfg, consumer)
  }

  private[filodb] def createConsumer(settings: KafkaSettings,
                                     topicPartition: TopicPartition): Task[KafkaConsumer[JLong, Any]] =
    Task {
      val props = settings.sourceConfig.kafkaConfig.asProps
      blocking {
        val consumer = new KafkaConsumer(props)
        consumer.assign(List(topicPartition).asJava)
        consumer.asInstanceOf[KafkaConsumer[JLong, Any]]
      }
    }

  private[filodb] def consumerConfig(settings: KafkaSettings) =
    KafkaConsumerConfig(settings.sourceConfig.asConfig)

}


object PartitionedProducerSink {

  /** Convenience function for creating a Monix producer:
    * {{{
    *     val producer = PartitionedProducer.create[K, V](settings, scheduler)
    * }}}
    *
    * @param settings the kafka settings
    *
    * @param io the monix scheduler
    */
  def create[K, V](settings: KafkaSettings, io: Scheduler)
                  (implicit K: Serializer[K], V: Serializer[V]): KafkaProducerSink[K, V] = {
    val cfg = KafkaProducerConfig(settings.sinkConfig.asConfig)
    KafkaProducerSink[K, V](cfg, io)
  }
}
