package filodb.coordinator

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import scala.collection.mutable.HashMap
import scala.concurrent.duration._

import filodb.core.ErrorResponse

object NodeClusterActor {
  // Forwards message to one random recipient that has the given role.  Any replies go back to originator.
  case class ForwardToOne(role: String, msg: Any)
  case object NoSuchRole extends ErrorResponse

  // Gets all the ActorRefs for a specific role.  Returns a Set[ActorRef].
  case class GetRefs(role: String)

  // Forwards message to all recipients with given role.  Sending actor must handle separate replies.
  case class ForwardToAll(role: String, msg: Any)

  // Internal message
  case class AddCoordActor(roles: Set[String], ref: ActorRef)
  case object EverybodyLeave  // Make every member leave, should be used only for testing

  def props(cluster: Cluster,
            resolveActorTimeout: FiniteDuration = 10.seconds): Props =
    Props(classOf[NodeClusterActor], cluster, resolveActorTimeout)

  class RemoteAddressExtensionImpl(system: ExtendedActorSystem) extends Extension {
    def address: Address = system.provider.getDefaultAddress
  }

  object RemoteAddressExtension extends ExtensionKey[RemoteAddressExtensionImpl]
}

/**
 * An actor that subscribes to membership events for a FiloDB cluster, updating a state of coordinators.
 * It also can send messages to one or all coordinators of a given role.
 *
 * Compared to the standard cluster aware routers, it has a couple advantages:
 * - It tracks all roles, not just one, and can send messages to a specific role per invocation
 * - It has much lower latency - direct sending to ActorRefs, no need to resolve ActorSelections
 *   (but of course it assumes the destination actor is always up... TODO watch for other actor dying)
 * - It broadcasts messages to all members of a role and sends back a collected response in one message,
 *   making it very easy to handle for non-actors.
 * - It can notify when some address joins
 */
class NodeClusterActor(cluster: Cluster,
                       resolveActorTimeout: FiniteDuration) extends BaseActor {
  import NodeClusterActor._

  val roleToCoords = new HashMap[String, Set[ActorRef]]().withDefaultValue(Set.empty[ActorRef])

  import context.dispatcher

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  // TODO: leave cluster?
  override def postStop(): Unit = cluster.unsubscribe(self)

  private def withRole(role: String)(f: Set[ActorRef] => Unit): Unit = roleToCoords.get(role) match {
    case None       => sender ! NoSuchRole
    case Some(refs) => f(refs)
  }

  val localRemoteAddr = RemoteAddressExtension(context.system).address
  var everybodyLeftSender: Option[ActorRef] = None

  def membershipHandler: Receive = LoggingReceive {
    case MemberUp(member) =>
      logger.info(s"Member is Up: ${member.address} with roles ${member.roles}")
      val memberCoordActor = RootActorPath(member.address) / "user" / "coordinator"
      val fut = context.actorSelection(memberCoordActor).resolveOne(resolveActorTimeout)
      fut.foreach { ref => self ! AddCoordActor(member.roles, ref) }
      fut.recover {
        case e: Exception =>
          logger.warn(s"Unable to resolve coordinator at $memberCoordActor, ignoring. " +
                      "Maybe NodeCoordinatorActor did not start up before node joined cluster.", e)
      }

    case UnreachableMember(member) =>
      logger.info(s"Member detected as unreachable: $member")

    case MemberRemoved(member, previousStatus) =>
      logger.info(s"Member is Removed: ${member.address} after $previousStatus")
      roleToCoords.keys.foreach { role =>
        roleToCoords(role) = roleToCoords(role).filterNot { ref =>
          // if we don't do this cannot properly match when self node is asked to leave
          val addr = if (ref.path.address.hasLocalScope) localRemoteAddr else ref.path.address
          addr == member.address
        }
      }
      roleToCoords.retain { case (role, refs) => refs.nonEmpty }
      if (roleToCoords.isEmpty) {
        logger.info("All members removed!")
        everybodyLeftSender.foreach { ref => ref ! EverybodyLeave }
        everybodyLeftSender = None
      }

    case _: MemberEvent => // ignore
  }

  def routerEvents: Receive = LoggingReceive {
    case ForwardToOne(role, msg) =>
      withRole(role) { refs => refs.toSeq.apply(util.Random.nextInt(refs.size)).forward(msg) }

    case GetRefs(role) =>
      withRole(role) { refs => sender ! refs }

    case ForwardToAll(role, msg) =>
      withRole(role) { refs => refs.foreach(_.forward(msg)) }

    case AddCoordActor(roles, coordRef) =>
      roles.foreach { role => roleToCoords(role) += coordRef }
      logger.debug(s"Updated roleToCoords: $roleToCoords")

    case EverybodyLeave =>
      if (roleToCoords.isEmpty) { sender ! EverybodyLeave }
      else if (everybodyLeftSender.isEmpty) {
        logger.info(s"Removing all members from cluster...")
        cluster.state.members.map(_.address).foreach(cluster.leave)
        everybodyLeftSender = Some(sender)
      } else {
        logger.warn(s"Ignoring EverybodyLeave, somebody already sent it")
      }
  }

  def receive: Receive = membershipHandler orElse routerEvents
}