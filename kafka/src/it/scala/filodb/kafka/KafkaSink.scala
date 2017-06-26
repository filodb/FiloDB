package filodb.kafka

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{Future, Await}
import scala.util.control.NonFatal
import scala.concurrent.duration.{Duration, MILLISECONDS}

import akka.actor._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.LongSerializer

/** Experimental: Standalone Kafka publisher. Publish events to FiloDB via Kafka.
  * Shuts itself down gracefully on ActorSystem.shutdown/terminate.
  *
  * {{{
  *   val sink = KafkaSink(system)
  *   sink.publish(hashKey, bytesValue)
  * }}}
  *
  * Or send with exposed Future ack:
  * {{{
  *   val taskCtx = sink.publishWithAck(hashKey, bytesValue)
  * }}}
  */
object KafkaSink extends ExtensionId[KafkaSink] with ExtensionIdProvider {
  override def lookup: ExtensionId[_ <: Extension] = KafkaSink
  override def createExtension(system: ExtendedActorSystem) = new KafkaSink(system)
  override def get(system: ActorSystem): KafkaSink = super.get(system)
}

/** Experimental.
  * INTERNAL API.
  */
private[kafka] final class KafkaSink(override val system: ExtendedActorSystem) extends Kafka(system) {

  import Sink._, Protocol._
  import akka.pattern.ask

  override val settings = new SinkSettings(system.settings.config)
  import settings._

  protected val _isConnected = new AtomicBoolean(false)

  def isConnected: Boolean = _isConnected.get

  /** Creates the supervisor, gets status of connection to Kafka cluster.
    * INTERNAL API
    */
  private[kafka] val publisher: ActorRef = {
    implicit val timeout: Timeout = settings.ConnectedTimeout
    try {
      val actor = system.actorOf(PublisherLifecycle.props(settings, IngestionTopic))
      Await.result((actor ? Protocol.GetConnected).mapTo[Connected], timeout.duration)
      actor
    } catch { case NonFatal(e) =>
      logger.error(s"Failed to start Sink, check your cluster and settings", e)
      shutdown() // don't re-throw, that would cause the extension to be re-recreated
      system.deadLetters
    }
  }

  /** Publish event stream to FiloDB. */
  def publish[A](key: Long, value: A): Unit =
    publisher ! Publish(key, value)

  /** Publish event stream to FiloDB. Returns the context of the task on complete. */
  def publishWithAck[A](key: Long, value: A): Future[PublishContext] = {
    implicit val timeout: Timeout = settings.PublishTimeout
    (publisher ? PublishAndAck(key, value)).mapTo[PublishContext]
  }

  /** Returns the current totals for the given topic of
    * successful sends by partition, and total failures by exception type.
    */
  def getStatus(topic: String): Future[PublishContextStatus] = {
    implicit val timeout: Timeout = StatusTimeout
    (publisher ? GetStatus(topic)).mapTo[PublishContextStatus]
  }

  override protected def shutdown(): Unit = {
    if (_isTerminated.compareAndSet(false, true)) {
      logger.info("Starting graceful shutdown.")
      implicit val timeout: Timeout = GracefulStopTimeout
      val status = scala.concurrent.Await.result((publisher ? GracefulShutdown).mapTo[Boolean], timeout.duration)
      system stop publisher
      logger.info(s"Shutdown of children completed with status $status")
    }
  }
}

final class SinkSettings(conf: Config) extends KafkaSettings(conf) {
  def this() = this(ConfigFactory.load)

  val StatusLogInterval = Duration(kafka.getDuration(
    "tasks.status.log-interval", MILLISECONDS), MILLISECONDS)

  val PublishTimeout = Duration(kafka.getDuration(
    "tasks.publish-timeout", MILLISECONDS), MILLISECONDS)

  val EnableFailureChannel = kafka.getBoolean("failures.channel-enabled")

  def producerConfig: Map[String, AnyRef] = {
    val props = Loaded.producer
    props ++ Map(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
      ProducerConfig.CLIENT_ID_CONFIG ->
        props.getOrElse(ProducerConfig.CLIENT_ID_CONFIG, createSelfId),
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[LongSerializer].getName)
  }
}