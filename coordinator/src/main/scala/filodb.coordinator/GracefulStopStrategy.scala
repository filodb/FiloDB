package filodb.coordinator

import scala.concurrent.Future

import akka.actor.{ActorRef, PoisonPill, Terminated}
import akka.actor.{Actor, DeadLetter}
import akka.cluster.{Cluster, Member, MemberStatus}
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

// Impl and usage coming next commit
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
    case MemberJoined(member) =>
    case MemberUp(member) =>
    case MemberLeft(member) =>
    case MemberRemoved(member, previous) =>
    case UnreachableMember(member) =>
      logger.info(s"Member detected as unreachable: $member")
    case ReachableMember(member) =>
    case LeaderChanged(leader) =>
    case e: DeadLetter => onDeadLetter(e)
    case e: MemberEvent => // ignore others
    case ClusterShuttingDown =>
  }

  private def state = cluster.state

  private def onMemberRemoved(member: Member): Unit =
    member.status match {
      case MemberStatus.Down =>
      case MemberStatus.Exiting =>
      case other =>
    }

  private def onDeadLetter(e: DeadLetter): Unit = {
    logger.warn(s"Received $e") // TODO in a worker, handle no data loss etc
  }
}
