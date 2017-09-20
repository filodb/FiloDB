package filodb.akkabootstrapper

import akka.actor.{Actor, ActorLogging, Address}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable.HashSet

class ClusterMembershipTracker extends Actor with StrictLogging {

  val cluster = Cluster(context.system)
  val members: HashSet[Address] = HashSet.empty

  import AkkaBootstrapperMessages._

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {
    case MemberUp(member) =>
      members += member.address
      logger.info("Member is Up: {}. New member list {}", member.address, members)
    case UnreachableMember(member) =>
      logger.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      members -= member.address
      logger.info("Member is Removed: {} after {}. New member list {}", member.address, previousStatus, members)
    case _: MemberEvent => // ignore
    case ClusterMembershipRequest =>
      sender ! ClusterMembershipResponse(members.toList)
  }
}