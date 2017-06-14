package filodb.kafka

import java.util.{Properties => JProperties}

import akka.actor._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

import scala.util.control.NonFatal

object TopicPublisher {

  def props(topic: String, producerConfig: JProperties, listener: ActorRef): Props = Props(
    new TopicPublisher(topic, producerConfig, listener))
}

/** Handles one topic vs multiple to more easily compartmentalize the typed schema-to-topic and
  * per-topic/partition Kafka offsets and topology for LocalNodePartitionAwareActor. Additionally creates a
  * PieKafkaProducer with an internal `PieKafkaEnvelopeProducer` in the PIE Kafka Client.
  *
  * @param producerConfig the producer configuration
  * @param topic          the topic to use
  */
final class TopicPublisher(topic: String, producerConfig: JProperties, listener: ActorRef)
  extends Publisher(topic, producerConfig) with KafkaActor {

  import Protocol._
  import scala.collection.JavaConverters._

  override def preStart(): Unit = {
    require(producer.partitionsFor(topic).asScala.nonEmpty, "Unable to connect to cluster")
    context.parent ! Protocol.ChildConnected
  }

  override def postStop(): Unit = {
    try producer.close() catch { case NonFatal(e) => }
    super.postStop()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.parent ! WorkerFailure(UtcTime.now, topic, reason, message.toString)// TODO msg type
    super.preRestart(reason, message)
  }

  def receive: Actor.Receive = {
    case e: Publish => send(e, sender())
  }

  /** Retriable exceptions (transient, may be covered by increasing #.retries):
    * CorruptRecordException InvalidMetadataException NotEnoughReplicasAfterAppendException
    * NotEnoughReplicasException OffsetOutOfRangeException TimeoutException UnknownTopicOrPartitionException
    */
  override protected def onError(e: Exception, origin: ActorRef): Exception = {
    log.error("Failure on publish, {}", e)
    val status = PublishFailureStatus(UtcTime.now, topic, e)
    context.parent ! status
    listener ! status

    origin ! PublishFailureAck(status.eventTime, topic, e, None)
    e
  }

  override protected def onSuccess(m: RecordMetadata): RecordMetadata = {
    listener ! PublishSuccessStatus(m.timestamp, m.topic, m.partition, m.offset)
    m
  }
}
