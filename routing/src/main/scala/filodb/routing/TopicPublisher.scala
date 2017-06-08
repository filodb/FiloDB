package filodb.routing

import java.util.{Properties => JProperties}

import akka.actor._
import filodb.kafka.Publisher
import filodb.routing.PlatformProtocol.WorkerFailure
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.util.control.NonFatal

object TopicPublisher {

  def props[K, V](producerConfig: JProperties, topic: String, listener: ActorRef): Props = Props(
    new TopicPublisher[K, V](producerConfig, topic, listener))
}

/** Handles one topic vs multiple to more easily compartmentalize the typed schema-to-topic and
  * per-topic/partition Kafka offsets and topology for LocalNodePartitionAwareActor. Additionally creates a
  * PieKafkaProducer with an internal `PieKafkaEnvelopeProducer` in the PIE Kafka Client.
  *
  * @param producerConfig the producer configuration
  * @param topic          the topic to use
  */
final class TopicPublisher[K, V](producerConfig: JProperties, topic: String, listener: ActorRef)
  extends KafkaActor with Publisher[K, V] {

  import WriteProtocol._

  import scala.collection.JavaConverters._

  protected val producer = new KafkaProducer[K, V](producerConfig)

  // throws WrappedClusterResolverException if this fails
  require(producer.partitionsFor(topic).asScala.nonEmpty, "Unable to connect to cluster")
  context.parent ! PlatformProtocol.ChildConnected

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

  protected def record(e: Publish): ProducerRecord[K, V] =
    Option(e.key) match {
      case Some(key) =>
        new ProducerRecord[K, V](topic, e.key.asInstanceOf[K], e.value.asInstanceOf[V])
      case _ =>
        new ProducerRecord[K, V](topic, e.value.asInstanceOf[V])
    }

  /** Retriable exceptions (transient, may be covered by increasing #.retries):
    * CorruptRecordException InvalidMetadataException NotEnoughReplicasAfterAppendException
    * NotEnoughReplicasException OffsetOutOfRangeException TimeoutException UnknownTopicOrPartitionException
    */
  private def onError(e: Exception, origin: ActorRef): Exception = {
    log.error("Failure on publish, {}", e)
    val status = PublishFailureStatus(UtcTime.now, topic, e)
    context.parent ! status
    listener ! status

    origin ! PublishFailureAck(status.eventTime, topic, e, None)
    e
  }

  private def onSuccess(m: RecordMetadata): RecordMetadata = {
    listener ! PublishSuccessStatus(m.timestamp, m.topic, m.partition, m.offset)
    m
  }
}