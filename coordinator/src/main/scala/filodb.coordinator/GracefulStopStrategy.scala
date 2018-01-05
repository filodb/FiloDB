package filodb.coordinator

import scala.concurrent.Future

import akka.actor.{ActorRef, PoisonPill, Terminated}
import akka.actor.{Actor, DeadLetter}
import akka.cluster.{Cluster, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive

/** For Deathwatch actors, handles `akka.actor.Terminated` events,
  * and Supervisors wanting graceful shutdown of supervised children
  * for no data loss or clean behavior on shutdown, sends a
  * `filodb.coordinator.NodeProtocol.ShutdownComplete` to the
  * `filodb.coordinator.NodeProtocol.GracefulShutdown` sender.
  */
trait GracefulStopStrategy extends BaseActor {

  import NodeProtocol.{GracefulShutdown, ShutdownComplete}

  protected var gracefulShutdownStarted = false

  def settings: FilodbSettings

  override def receive: Actor.Receive = LoggingReceive {
    case Terminated(actor) => terminated(actor)
    case GracefulShutdown  => gracefulShutdown(sender())
  }

  protected def terminated(actor: ActorRef): Unit = {
    val message = s"$actor terminated."
    if (gracefulShutdownStarted) logger.info(message) else logger.warn(message)
  }

  protected def gracefulShutdown(requester: ActorRef): Unit = {
    import akka.pattern.{gracefulStop, pipe}
    import context.dispatcher

    logger.info("Starting graceful shutdown.")

    gracefulShutdownStarted = true

    Future
      .sequence(context.children.map(gracefulStop(_, settings.GracefulStopTimeout, PoisonPill)))
      .map(f => ShutdownComplete(self))
      .pipeTo(requester)
  }
}

/** TODO handle when singleton is in handover. */
private[coordinator] final class NodeLifecycleStrategy(settings: FilodbSettings) extends BaseActor {

  val cluster = Cluster(context.system)

  val failureDetector = cluster.failureDetector

  override def preStart(): Unit = {
    super.preStart()
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[ReachabilityEvent], ClusterShuttingDown.getClass)
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster unsubscribe self
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    // TODO
  }

  override def receive: Actor.Receive = LoggingReceive {
    case e: MemberUp          => onUp(e)
    case e: UnreachableMember => onUnreachable(e)
    case e: ReachableMember   => onReachable(e)
    case e: MemberRemoved     => onRemoved(e)
    case e: MemberEvent       => // ignore others
    case e: DeadLetter        => onDeadLetter(e)
  }

  private def state = cluster.state

  private def onUp(e: MemberUp): Unit = {}

  private def onUnreachable(e: UnreachableMember): Unit = {}

  private def onReachable(e: ReachableMember): Unit = {}

  private def onRemoved(e: MemberRemoved): Unit =
    e.previousStatus match {
      case MemberStatus.Down =>
      case MemberStatus.Exiting =>
      case other =>
    }

  private def onDeadLetter(e: DeadLetter): Unit = {
    logger.warn(s"Received $e") // TODO in a worker, handle no data loss etc
  }
}
