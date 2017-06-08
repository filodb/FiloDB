package filodb.routing

import scala.concurrent.duration._
import akka.actor._
import akka.kafka.ProducerSettings
import filodb.kafka.{KafkaSettings, PublisherSettings}

/** Guards the health, performance and state of all workers on the node, and manages
  * the node lifecycle.
  *
  * Throttles inbound events to handle backpressure, avoid data loss and failure
  * from the data emitters/producers.
  *
  * WIP to remove later:
  * private val resizer = Some(DefaultResizer(lowerBound = 1, upperBound = 1)) // TODO lowerBound = 2, upperBound = 15
  *   val pool = RoundRobinPool(1, resizer, supervisorStrategy = this.supervisorStrategy)
  *   val router = context.actorOf(pool.props(worker), name = topic)
  */
class NodeGuardian(parent: NodeRouter) extends NodeActor with DownAware {

  import PlatformProtocol._
  import ReadProtocol._
  import WriteProtocol._
  import context.dispatcher

  private val nodeSettings = NodeSettings(context.system.settings.config)

  private val producerSettings = new PublisherSettings

  import nodeSettings._, producerSettings._

  protected val timeout: FiniteDuration = GracefulStopTimeout

  private val failures = context.actorOf(
    TopicPublisher.props[String, String](producerConfig, FailureTopic, self), FailureTopic)

  private val publisher = {
    val listener = context.actorOf(TopicStatsPublisher.props(IngestionTopic, TaskLogInterval), s"$IngestionTopic-stats")
    val props = TopicPublisher.props[String, String](producerConfig, IngestionTopic, listener)
    val actor = context.actorOf(props, IngestionTopic)
    context watch actor
    actor
  }

  log.info("Created topic publisher {}", publisher)

  private var connectCount = 0

  // WIP
  private val task = context.system.scheduler.scheduleOnce(ConnectedTimeout) {
    if (!isConnected) {
      log.error(s"Unable to connect to Kafka cluster, system shutting down. Brokers: \n[$BootstrapServers].")
      failures ! Publish(self.path, ConnectTimeout)
      parent.shutdown()
    }
  }

  private def isConnected: Boolean =
    context.children.nonEmpty && connectCount == context.children.size

  override def postStop(): Unit = {
    task.cancel()
    super.postStop()
  }

  override def receive: Actor.Receive = {
    case ChildConnected => connected()
    case e: Publish => route(e, sender())
    case e: PublishFailure => failed(e, sender())
    case e: Terminated => terminated(e)
    case GetNodeStatus(t) => status(t, sender())
    case GracefulShutdown => gracefulShutdown(sender())
  }

  private def connected(): Unit = {
    connectCount += 1
    if (isConnected) {
      log.info("Connected to cluster.")
      context.parent ! Connected
      // context become initialized
    }
  }

  private def route(e: Publish, origin: ActorRef): Unit = {
    publisher forward e
    //failed(NoRouteExists(UtcTime.now, other.getClass), origin)
  }

  private def status(topic: String, origin: ActorRef): Unit = {
    context.child(s"$topic-stats") foreach { _ forward GetPublishStatus }
  }

  private def failed(e: TaskFailure, origin: ActorRef): Unit =
    e match {
      case ctx@NoRouteExists(time, tpe) =>
        log.error(s"No route exists for event type ${ctx.eventType}.")
        failures ! Publish(None, ctx)

      case ctx@WorkerFailure(_, tp, _, _) =>
        failures ! Publish(tp, ctx)

      case ctx@PublishFailureStatus(_, tp, _) =>
        failures ! Publish(tp, ctx)

      case _ =>
    }

  private def terminated(e: Terminated): Unit = {
    log.info("Actor for topic '{}' terminated: {}", e.actor.path.name, e)
    /* if the producer crashed but didn't complete publishing, don't loose data...
       Can spin up a new topic actor, etc. */
  }
}
