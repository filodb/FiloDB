package filodb.routing

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{Await, Future}
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props, _}
import akka.event.Logging
import akka.util.Timeout
import monix.eval.Task

object NodeRouter extends ExtensionId[NodeRouter] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] = NodeRouter

  override def createExtension(system: ExtendedActorSystem) = new NodeRouter(system)

  /** Java API. */
  override def get(system: ActorSystem): NodeRouter = super.get(system)

}

class NodeRouter(val system: ExtendedActorSystem) extends Extension {

  import PlatformProtocol._
  import ReadProtocol._
  import WriteProtocol._
  import akka.pattern.ask
  import system.dispatcher

  private val settings = NodeSettings(system.settings.config)

  import settings._

  private val _isRunning = new AtomicBoolean(false)

  private val _isTerminating = new AtomicBoolean(false)

  private val logger = Logging.getLogger(system, this)

  private val log = Logging.getLogger(system, this)

  private val listenAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

  private val guardian: ActorRef = system.actorOf(Props(new NodeGuardian(this)), "node-guardian")

  (guardian ? IsInitialized) (Timeout(ConnectedTimeout))
    .foreach { case Connected =>
      _isRunning.set(true)
      log.info(s"Started listening on {}", listenAddress)
    }

  def isRunning: Boolean = _isRunning.get

  def isTerminating: Boolean = _isTerminating.get

  def publish(e: WriteEvent): Unit = guardian ! e

  def publishAck(e: WriteEvent): Task[PublishTaskAck] = {
    implicit val timeout: Timeout = Timeout(RouteTimeout)
    Task.fromFuture((guardian ? Publish(e.identity, e)).mapTo[PublishTaskAck])
  }

  def getPublishStatus(topic: String): Future[TopicPartitionPublishStatus] = {
    implicit val timeout: Timeout = StatusTimeout
    (guardian ? GetPublishStatus(topic)).mapTo[TopicPartitionPublishStatus]
  }

  private[filodb] def gracefulShutdown(): Unit = {
    if (_isTerminating.compareAndSet(false, true)) {
      import scala.concurrent.Await

      log.info("Starting graceful shutdown.")
      implicit val timeout: Timeout = Timeout(GracefulStopTimeout)
      // block on shutdown to allow mailbox drain
      Await.result(guardian ? GracefulShutdown, GracefulStopTimeout)
      //system.s(GracefulStopTimeout)
      log.info("Async shutdown.")
    }
  }

  private[filodb] def shutdown(): Unit = {
    if (!_isTerminating.get) {
      _isTerminating.set(true)
      log.info("Shutting down due to node failure.")
      system.terminate()
      try Await.ready(system.whenTerminated, GracefulStopTimeout) catch {
        case _: TimeoutException =>
          val msg = "Failed to stop [%s] within [%s] \n%s".format(system.name, GracefulStopTimeout, system)
          system.log.warning(msg)
      }
    }
  }
}