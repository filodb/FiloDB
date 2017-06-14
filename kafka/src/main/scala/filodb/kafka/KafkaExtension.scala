package filodb.kafka

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props, _}
import akka.event.Logging
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import monix.eval.Task

/** Basic Usage:
  *
  * {{{
  *  val system = ActorSystem()
  *  val kafka = KafkaExtension(system)
  *
  *  val config = Map()
  *  kafka.provision(config)
  *
  *  while(events stream in) {
  *    kafka.publish(hashKey, protobufBytes)
  *  }
  *
  *  Runtime.getRuntime().addShutdownHook(new Thread() {
  *    override def run(): Unit = kafka.shutdown()
  *  })
  * }}}
  */
object KafkaExtension extends ExtensionId[KafkaExtension] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] = KafkaExtension

  override def createExtension(system: ExtendedActorSystem) = new KafkaExtension(system)

  /** Java API. */
  override def get(system: ActorSystem): KafkaExtension = super.get(system)

}

class KafkaExtension(val system: ExtendedActorSystem) extends Extension {

  import Protocol._
  import akka.pattern.ask
  import system.dispatcher

  val settings = new NodeSettings(system.settings.config)
  import settings._

  val kafkaSettings = new KafkaSettings()

  private val _isConnected = new AtomicBoolean(false)

  private val log = Logging.getLogger(system, this)

  private val listenAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

  private val guardian: ActorRef = system.actorOf(Props(new NodeGuardian(kafkaSettings, this)), "node-guardian")

  def isConnected: Boolean = _isConnected.get

  /** Java API. Blocking call which times out after `NodeSettings.ProvisionTimeout` duration. */
  def provision(config: java.util.Map[String, Object]): Boolean = {
    import scala.collection.JavaConverters._
    Await.result(provision(config.asScala.toMap), ProvisionTimeout)
  }

  /** Scala API. Async which times out after `NodeSettings.ProvisionTimeout` duration. */
  def provision(config: immutable.Map[String, AnyRef]): Future[Boolean] = {
    implicit val timeout: Timeout = Timeout(ProvisionTimeout)
    (guardian ? Provision(config)) map {
      case e: Provisioned =>
        log.info("Node Provisioned.")
        _isConnected.set(true)
        isConnected
      case other =>
        isConnected
    }
  }

  /** Publish event stream to FiloDB. */
  def publish(key: Long, value: Array[Byte]): Unit =
    guardian ! Publish(key, value, UtcTime.now)

  /** in progress, and will add a java call */
  def publishAck(key: Int, value: Array[Byte]): Task[PublishTaskAck] = {
    implicit val timeout: Timeout = Timeout(RouteTimeout)
    Task.fromFuture((guardian ? Publish(key, value, UtcTime.now)).mapTo[PublishTaskAck])
  }

  def getPublishStatus(topic: String): Future[TopicPartitionPublishStatus] = {
    implicit val timeout: Timeout = StatusTimeout
    (guardian ? GetPublishStatus(topic)).mapTo[TopicPartitionPublishStatus]
  }

  /** TODO untested */
  def newProducer(topic: String, config: immutable.Map[String, AnyRef]): Future[PublisherCreated] = {
    implicit val timeout: Timeout = Timeout(ProvisionTimeout)
    (guardian ? CreatePublisher(topic, config)).mapTo[PublisherCreated]
  }

  private[filodb] def shutdown(): Unit = {
    if (!system.isTerminated) {
      import scala.concurrent.Await

      log.info("Starting graceful shutdown.")
      implicit val timeout: Timeout = Timeout(GracefulStopTimeout)
      Await.result(guardian ? GracefulShutdown, GracefulStopTimeout)
      system.shutdown()
      system.awaitTermination(GracefulStopTimeout)
      log.info("Async shutdown completed.")
    }
  }
}

class NodeSettings(conf: Config) {

  ConfigFactory.invalidateCaches()

  protected val config = conf.withFallback(ConfigFactory.load())
  protected val filodb = config.getConfig("filodb")

  val selfAddress = InetAddress.getLocalHost.getHostAddress

  def selfAddressId: String = selfAddress + "-" + System.currentTimeMillis()

  val ConnectedTimeout = Duration(filodb.getDuration(
    "tasks.connect-timeout", MILLISECONDS), MILLISECONDS)

  val PublishInterval = Duration(filodb.getDuration(
    "tasks.publish-interval", MILLISECONDS), MILLISECONDS)

  val TaskLogInterval = Duration(filodb.getDuration(
    "tasks.log-interval", MILLISECONDS), MILLISECONDS)

  val PublishTimeout = Duration(filodb.getDuration(
    "tasks.publish-timeout", MILLISECONDS), MILLISECONDS)

  val StatusTimeout = Duration(filodb.getDuration(
    "tasks.publish-timeout", MILLISECONDS), MILLISECONDS)

  val GracefulStopTimeout = Duration(filodb.getDuration(
    "tasks.shutdown-timeout", SECONDS), SECONDS)

  val RouteTimeout = Duration(filodb.getDuration(
    "tasks.route-timeout", MILLISECONDS), MILLISECONDS)

  val ProvisionTimeout = Duration(filodb.getDuration(
    "tasks.provision-timeout", MILLISECONDS), MILLISECONDS)
}