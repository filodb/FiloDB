package filodb.coordinator

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.ExecutionContext
import scala.util.Random

import akka.actor.{ActorContext, ActorRef, Address, PoisonPill}
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp}
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.ActorName.nodeCoordinatorPath
import filodb.coordinator.client.IngestionCommands.UnknownDataset
import filodb.core.DatasetRef

final case class NodeCoordinatorActorDiscovered(addr: Address, coordinator: ActorRef)

/**
  * To handle query routing from a NodeCoordActor.
  * @param settings the filodb settings.
  */
class TempQueryRouter(settings: FilodbSettings) extends StrictLogging {

  val queryActors = new HashMap[DatasetRef, ActorRef]
  val nodeCoordinatorActorsMap = new HashMap[Address, ActorRef]
  val nodeCoordinatorActors = new ArrayBuffer[ActorRef]
  val random = new Random()


  /**
    * Attempts to forward the query to the right node that has this dataset setup.
    * Note: a query to a dataset that doesn't exist would keep bouncing between nodes
    *
    *
    * @param originator   the originating actor
    * @param dataset      the dataset queried against
    */
  // TODO: if we ever support query API against cold (not in memory) datasets, change this
  // TODO: Enable ignore test for "should return UnknownDataset if attempting to query before ingestion set up"
  def withQueryActor(originator: ActorRef, dataset: DatasetRef)(func: ActorRef => Unit): Unit = {
    logger.info(s"finding query actor for dataset: $dataset from queryActors: $queryActors")
    queryActors.get(dataset) match {
      case Some(queryActor) => func(queryActor)
      case None => if (nodeCoordinatorActors.size > 1) { // forward only when there are other NCAs to forward to.
        func(getRandomActor(nodeCoordinatorActors, random))
      } else UnknownDataset
    }
  }

  def resetState(origin: ActorRef): Unit = {
    queryActors.values.foreach(_ ! PoisonPill)
    queryActors.clear()
    nodeCoordinatorActors.clear()
    nodeCoordinatorActorsMap.clear()
  }

  private def getRandomActor(list: ArrayBuffer[ActorRef], random: Random): ActorRef = {
    val randomActor: ActorRef = list(random.nextInt(list.size))
    logger.info(s"returning random: $randomActor")
    return randomActor
  }

  def receiveMemberEvent(memberEvent: MemberEvent, context: ActorContext, coord: ActorRef)
                        (implicit execContext: ExecutionContext): Unit = {
    memberEvent match {
      case MemberUp(member) =>
        logger.debug(s"Member up received: $member")
        val memberCoordActorPath = nodeCoordinatorPath(member.address)
        context.actorSelection(memberCoordActorPath).resolveOne(settings.ResolveActorTimeout)
          .map(ref => coord ! NodeCoordinatorActorDiscovered(memberCoordActorPath.address, ref))
          .recover {
            case e: Exception =>
              logger.warn(s"Unable to resolve coordinator at $memberCoordActorPath, ignoring. ", e)
          }

      case MemberRemoved(member, _) => {
        val memberCoordActorPath = nodeCoordinatorPath(member.address)
        nodeCoordinatorActorsMap.get(memberCoordActorPath.address) match {
          case Some(x) => {
            nodeCoordinatorActorsMap.remove(memberCoordActorPath.address)
            nodeCoordinatorActors -= x
          }
          case None => logger.warn(s"Member not in coordinatorsMap: $memberCoordActorPath.address")
        }
        logger.debug(s"Member down updated map: $nodeCoordinatorActorsMap")
        logger.debug(s"Member down updated list: $nodeCoordinatorActors")
      }

      case _: MemberEvent => // ignore
    }
  }

  def receiveNodeCoordDiscoveryEvent(ncaDiscoveredEvent: NodeCoordinatorActorDiscovered)
                                    (implicit executionContext: ExecutionContext): Unit = {
    ncaDiscoveredEvent match {
      case NodeCoordinatorActorDiscovered(addr, coordRef) => {
        logger.debug(s"Received NodeCoordDiscoveryEvent $addr  $coordRef")
        nodeCoordinatorActorsMap(addr) = coordRef
        nodeCoordinatorActors += coordRef
        logger.debug(s"Member up updated map: $nodeCoordinatorActorsMap")
        logger.debug(s"Member up updated list: $nodeCoordinatorActors")
      }
    }
  }

}
