package filodb.kafka

import akka.actor._

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

/** Guards the health, performance and state of all workers on the node, and manages
  * the node lifecycle.
  *
  * Can throttle inbound events to handle backpressure, avoid data loss and failure
  * from the data emitters/producers.
  *
  * WIP to remove later:
  * private val resizer = Some(DefaultResizer(lowerBound = 1, upperBound = 1)) // TODO lowerBound = 2, upperBound = 15
  * val pool = RoundRobinPool(1, resizer, supervisorStrategy = this.supervisorStrategy)
  * val router = context.actorOf(pool.props(worker), name = topic)
  */
class NodeGuardian(settings: KafkaSettings, parent: KafkaExtension)
  extends NodeActor with KafkaActor {

  import Protocol._
  import settings._
  import context.dispatcher

  protected val timeout: FiniteDuration = GracefulStopTimeout

  /*private val task = context.system.scheduler.scheduleOnce(ConnectedTimeout) {
    if (!isConnected) {
      log.error(s"Unable to connect to Kafka cluster, system shutting down. Brokers: \n[$BootstrapServers].")
      // TODO failures ! FailureContext(self.path, 1, ConnectTimeout)
      parent.shutdown()
    }
  }*/

  private var events: ActorRef = _

  private var failures: ActorRef = _

  override def postStop(): Unit = {
   // task.cancel()
    super.postStop()
  }

  override def receive: Actor.Receive = {
    case e: Provision         => onProvision(e, sender())
    //case ChildConnected     => connected()
    case e: Publish           => events forward e
    case e: PublishFailure    => failed(e, sender())
    case e: Terminated        => terminated(e)
    case GetPublishStatus(t)  => status(t, sender())
    case GracefulShutdown     => gracefulShutdown(sender())
  }

  /** Creates a new topic publisher, there will be one failure publisher, one ingestion. */
  private def onProvision(e: Provision, origin: ActorRef): Unit = {
    events = provision(IngestionTopic, e.config)
    failures = provision(FailureTopic, e.config)
    origin ! Provisioned(self)
  }

  private def provision(topic: String, config: Map[String, AnyRef]): ActorRef = {
    val producerConfig = settings.producerConfig(config)
    val stats = context.actorOf(TopicStatsPublisher.props(topic, TaskLogInterval), s"$topic-stats")
    val publisher = context.actorOf(TopicPublisher.props(topic, producerConfig, stats), topic)

    context watch stats
    context watch publisher
    log.info("Provisioned publisher {}", publisher)
    publisher
  }

  private def status(topic: String, origin: ActorRef): Unit =
    context.child(s"$topic-stats") foreach {_ forward GetPublishStatus }

  /** These behaviors are not yet hooked up yet. */
  private def failed(e: TaskFailure, origin: ActorRef): Unit =
    e match {
      case ctx: NoRouteExists =>
        log.error(s"No route exists.")
        failures ! FailureContext(ctx)

      case ctx@WorkerFailure(_, tp, _, _) =>
        failures ! FailureContext(ctx)

      case ctx@PublishFailureStatus(_, tp, _) =>
        failures ! FailureContext(ctx)

      case _ =>
    }

  private def terminated(e: Terminated): Unit = {
    log.info("Actor for topic '{}' terminated: {}", e.actor.path.name, e)
    /* if the producer crashed but didn't complete publishing, don't loose data...
       Can spin up a new topic actor, etc. */
  }

  protected def gracefulShutdown(sender: ActorRef): Unit = {
    import akka.pattern.{gracefulStop, pipe}
    import context.dispatcher

    log.info("Starting graceful shutdown on node.")

    val future = try
      Future.sequence(context.children.map { child =>
        Promise.successful(true).future
        // FIX: gracefulStop(self, timeout, PoisonPill)
      })
    catch {
      case e: akka.pattern.AskTimeoutException =>
        log.warning("Error shutting down all workers", e)
        Promise.failed(e).future
    }

    scala.concurrent.Await.result(future, timeout)
    future pipeTo sender
    ()
  }
}
