package filodb.coordinator

import akka.actor.{Actor, DeadLetter}
import akka.cluster.{Cluster, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive

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
