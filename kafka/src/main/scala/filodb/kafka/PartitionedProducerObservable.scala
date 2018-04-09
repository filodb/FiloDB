package filodb.kafka

import monix.execution.Scheduler
import monix.kafka.{KafkaProducerConfig, KafkaProducerSink, Serializer}

object PartitionedProducerObservable {

  /** Convenience function for creating a Monix producer:
    * {{{
    *     val producer = PartitionedProducerObservable.create[K, V](sinkConfig, scheduler)
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

