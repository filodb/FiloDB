package filodb.kafka

import scala.concurrent.blocking

import monix.eval.Task
import monix.kafka.{Deserializer, KafkaConsumerConfig, KafkaConsumerObservable}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

object PartitionedConsumerObservable {

  /** Creates a `KafkaConsumerObservable` instance.
    *
    * @param cfg is the `KafkaConsumerConfig` needed for initializing the consumer
    *
    * @param topics the list of Kafka topics to subscribe to.
    */
  def apply[K,V](cfg: KafkaConsumerConfig, topics: List[TopicPartition])
                (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] = {

    val consumer = createConsumer[K,V](cfg, topics)
    KafkaConsumerObservable[K,V](cfg, consumer)
  }

  /** Returns a `Task` for creating a consumer instance, assigned to the
    * specified topic-partition(s).
    */
  def createConsumer[K,V](config: KafkaConsumerConfig, topics: List[TopicPartition])
                         (implicit K: Deserializer[K], V: Deserializer[V]): Task[KafkaConsumer[K,V]] = {

    import collection.JavaConverters._
    Task {
      val props = config.toProperties
      blocking {
        val consumer = new KafkaConsumer[K,V](props, K.create, V.create)
        consumer.assign(topics.asJava)
        consumer
      }
    }
  }
}