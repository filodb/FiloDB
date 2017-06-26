package filodb.kafka

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

import akka.actor._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

/** Handles one topic vs multiple to more easily compartmentalize the typed schema-to-topic and
  * per-topic/partition Kafka offsets and topology.
  */
trait Publisher[K,V] extends StrictLogging {
  import Sink._

  protected def topic: String

  /** Allows implementing class to use any of the producer constructors. */
  protected def producer: KafkaProducer[K,V]

  protected def send(e: Publish[K,V]): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K,V](topic, e.key, e.value)).asScala

  protected def complete(outcome: Either[Exception, RecordMetadata])(implicit key: Option[Any]): PublishContext =
    outcome match {
      case Left(thrown) =>
        logger.error(s"Failure on publish", thrown)
        PublishFailure(UtcTime.now, topic, thrown, key)
      case Right(s) =>
        PublishSuccess(s.timestamp, s.topic, s.partition, s.offset)
    }
}

/** Actor implementation of a Publisher, handling connection status on start and managing lifecyce.
  * Has status listener tracking success, failure and report respond to requests for cumulative status
  *
  * Exposed to users.
  */
final class PublisherLifecycle[K, V](settings: SinkSettings, val topic: String) extends Publisher[K, V] with KafkaActor {

  import akka.pattern.{gracefulStop, pipe}
  import Protocol._, Sink._
  import settings._

  protected val producer = new KafkaProducer[K, V](producerConfig.asProps)

  private val status = context.actorOf(TopicPublisherStats.props(settings, topic), s"$topic-stats")
  context watch status

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    producer.flush()
    super.preRestart(reason, message)
  }

  override def postStop(): Unit = {
    Try(producer.close())
    super.postStop()
  }

  override def receive: Actor.Receive = {
    case GetConnected           => connected(sender())
    case e: Publish[K, V]       => send(e)
    case e: PublishAndAck[K, V] => sendC(e, sender())
    case e: Terminated          => terminated(e)
    case e: GetStatus           => snapshot(sender())
    case GracefulShutdown       => gracefulShutdown(sender())
  }

  private def connected(origin: ActorRef): Unit =
    Try(producer.partitionsFor(topic)) match {
      case Success(v) => origin ! Connected(topic)
      case Failure(t) => origin ! NotConnected(topic)
    }

  /** IO side effects of writing to Kafka, etc. Non-blocking async. */
  protected def sendC(e: PublishAndAck[K,V], origin: ActorRef): Unit = {
    producer.send(new ProducerRecord[K,V](topic, e.key, e.value), new Callback {
      override def onCompletion(success: RecordMetadata, failure: Exception): Unit = {
        val result = Option(failure)
          .map(t => PublishFailure(UtcTime.now, topic, failure, Some(e.key)))
          .getOrElse(PublishSuccess(success.timestamp, success.topic, success.partition, success.offset))
        origin ! result
        status ! result
      }
    })}

  private def snapshot(origin: ActorRef): Unit =
    status forward GetStatus

  private def terminated(e: Terminated): Unit = {
    logger.info("Actor for topic '{}' terminated: {}", e.actor.path.name, e)
    context unwatch e.actor
  }

  protected def gracefulShutdown(sender: ActorRef): Unit = {
    import scala.concurrent.duration.MILLISECONDS
    import context.dispatcher

    try {
      producer.flush()
      producer.close(4000, MILLISECONDS)
      logger.debug("Producer closed")
    }
    catch { case NonFatal(e) =>
      logger.error(s"Problem occurred during producer close: $e")
    }
    gracefulStop(status, GracefulStopTimeout, PoisonPill) pipeTo sender
  }
}

object PublisherLifecycle {
  def props[K, V](settings: SinkSettings, topic: String): Props =
    Props(new PublisherLifecycle[K, V](settings, topic))
}