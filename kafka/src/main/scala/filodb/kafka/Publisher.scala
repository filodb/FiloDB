package filodb.kafka

import java.util.{Properties => JProperties}

import akka.actor.ActorRef
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

/** Handles one topic vs multiple to more easily compartmentalize the typed schema-to-topic and
  * per-topic/partition Kafka offsets and topology for LocalNodePartitionAwareActor.
  *
  * @param producerConfig the producer configuration
  * @param topic          the topic to use
  */
abstract class Publisher(topic: String, producerConfig: JProperties) {

  import Protocol._

  protected val producer = new KafkaProducer[Long, Array[Byte]](producerConfig)

  /** IO side effects of writing to Kafka, etc. Non-blocking async. */
  protected def send(e: Publish, origin: ActorRef): Unit = {
    require(Option(e.key).isDefined, "A key is required but was not provided.")

    // TODO for hackathon, the key is disabled!!!
    producer.send(
      new ProducerRecord[Long, Array[Byte]](topic, e.value),
      //new ProducerRecord[String, Array[Byte]](topic, /*e.key, */e.value),
      new Callback {
        override def onCompletion(m: RecordMetadata, t: Exception): Unit =
          Option(t).map(onError(_, origin)).getOrElse(onSuccess(m))
      })
  }

  /** Retriable exceptions (transient, may be covered by increasing #.retries):
    * CorruptRecordException InvalidMetadataException NotEnoughReplicasAfterAppendException
    * NotEnoughReplicasException OffsetOutOfRangeException TimeoutException UnknownTopicOrPartitionException
    */
  protected def onError(e: Exception, origin: ActorRef): Exception

  protected def onSuccess(m: RecordMetadata): RecordMetadata

}
