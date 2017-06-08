package filodb.kafka

import java.util.{Properties => JProperties}

import akka.actor.ActorRef
import filodb.routing.WriteProtocol.{Publish, PublishFailureAck, PublishFailureStatus, PublishSuccessStatus}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import filodb.routing.UtcTime

/** Handles one topic vs multiple to more easily compartmentalize the typed schema-to-topic and
  * per-topic/partition Kafka offsets and topology for LocalNodePartitionAwareActor.
  * @param producerConfig the producer configuration
  * @param topic          the topic to use
  */
abstract class Publisher[K, V](topic: String, producerConfig: JProperties) {

  import filodb.routing.WriteProtocol.Publish

  protected val producer = new KafkaProducer[K, V](producerConfig)

  /** IO side effects of writing to Kafka, etc. Non-blocking async. */
  protected def send(e: Publish, origin: ActorRef): Unit =
    producer.send(
      new ProducerRecord[K, V](topic, e.key.asInstanceOf[K], e.value.asInstanceOf[V]),
      new Callback {
        override def onCompletion(m: RecordMetadata, t: Exception): Unit =
          Option(t).map(onError(_, origin)).getOrElse(onSuccess(m))
      })

  /** Retriable exceptions (transient, may be covered by increasing #.retries):
    * CorruptRecordException InvalidMetadataException NotEnoughReplicasAfterAppendException
    * NotEnoughReplicasException OffsetOutOfRangeException TimeoutException UnknownTopicOrPartitionException
    */
  protected def onError(e: Exception, origin: ActorRef): Unit = {}

  protected def onSuccess(m: RecordMetadata): Unit = {}

}

final class ShardAwarePublisher[K, V](topic: String,
                                      producerConfig: JProperties,
                                      listener: Option[ActorRef]
                                     ) extends Publisher[K, V](topic, producerConfig) {



  override protected def send(e: Publish, origin: ActorRef): Unit = {
    require(Option(e.key).isDefined, "'A")
    import filodb.implicits._
    producer.send(
      new ProducerRecord[K, V](topic, e.key.asInstanceOf[K], e.value.asInstanceOf[V]),
      new Callback {
        override def onCompletion(m: RecordMetadata, t: Exception): Unit =
          Option(t).map(onError(_, origin)).getOrElse(onSuccess(m))
      })
  }

  override protected def onError(e: Exception, origin: ActorRef): Exception = {
    //log.error("Failure on publish, {}", e)
    val status = PublishFailureStatus(UtcTime.now, topic, e)
    listener foreach (_ ! status)
    origin ! PublishFailureAck(status.eventTime, topic, e, None)
    e
  }

  override protected def onSuccess(m: RecordMetadata): RecordMetadata = {
    listener foreach ( _ ! PublishSuccessStatus(m.timestamp, m.topic, m.partition, m.offset))
    m
  }
}