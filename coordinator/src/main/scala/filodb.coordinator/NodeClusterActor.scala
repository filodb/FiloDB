package filodb.coordinator

import scala.collection.mutable.HashMap
import scala.concurrent.duration._

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import com.typesafe.config.{Config, ConfigFactory}

import filodb.core._
import filodb.core.metadata.RichProjection
import filodb.core.store.MetaStore

object NodeClusterActor {

  sealed trait ClusterActorEvent

  // Forwards message to one random recipient that has the given role.  Any replies go back to originator.
  final case class ForwardToOne(role: String, msg: Any) extends ClusterActorEvent

  // Gets all the ActorRefs for a specific role.  Returns a Set[ActorRef].
  final case class GetRefs(role: String) extends ClusterActorEvent

  // Forwards message to all recipients with given role.  Sending actor must handle separate replies.
  final case class Broadcast(role: String, msg: Any) extends ClusterActorEvent

  case object NoSuchRole extends ErrorResponse

  final case class DatasetResourceSpec(numShards: Int, minNumNodes: Int)

  final case class IngestionSource(streamFactoryClass: String, config: Config = ConfigFactory.empty)

  /**
   * Sets up a dataset for streaming ingestion and querying, with specs for sharding.
   * Results in a state change and shard assignment to existing and new nodes.
   * Initiates ingestion on each node.
   *
   * @param ref the DatasetRef for the dataset defined in MetaStore to start ingesting
   * @param columns the schema/columns to ingest
   * @param resources the sharding and number of nodes for ingestion and querying
   * @param source the IngestionSource on each node.  Use noOpSource to not start ingestion and
   *               manually push records into NodeCoordinator.
   * @return DatasetVerified - meaning the dataset and columns are valid.  Does not mean ingestion is
   *                           setup on all nodes - for that, subscribe to ShardMapUpdate's
   */
  final case class SetupDataset(ref: DatasetRef,
                                columns: Seq[String],
                                resources: DatasetResourceSpec,
                                source: IngestionSource)

  // A dummy source to use for tests and when you just want to push new records in
  val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)

  case object DatasetVerified
  final case class DatasetAlreadySetup(ref: DatasetRef) extends ErrorResponse
  final case class UnknownDataset(ref: DatasetRef) extends ErrorResponse
  final case class UndefinedColumns(undefined: Set[String]) extends ErrorResponse
  final case class BadSchema(message: String) extends ErrorResponse

  // Registers sending actor to receive ShardMapUpdate whenever it changes.  DeathWatch will be used
  // on the sending actors to watch for updates.  Also, will immediately send back the current state
  // via a ShardMapUpdate message.  NodeCoordinators are automatically subscribed to every dataset.
  // This should be sent after RegisterDataset.
  final case class SubscribeShardUpdates(ref: DatasetRef)
  final case class ShardMapUpdate(dataset: DatasetRef, map: ShardMapper)

  // Obtains a copy of the current cluster state
  case object GetState

  // Internal message
  final case class RegisterDataset(proj: RichProjection,
                                   resources: DatasetResourceSpec,
                                   source: IngestionSource)
  final case class AddCoordActor(roles: Set[String], addr: Address, ref: ActorRef)
  case object Reset           // Only use for testing, in before {} blocks
  case object EverybodyLeave  // Make every member leave, should be used only for testing

  /**
   * Creates a new NodeClusterActor.
   * @param cluster the Cluster to subscribe to for membership messages
   * @param nodeCoordRole String, for the role containing the NodeCoordinatorActor or ingestion nodes
   * @param resolveActorDuration the timeout to use to resolve NodeCoordinatorActor refs for new nodes
   */
  def props(cluster: Cluster,
            nodeCoordRole: String,
            metaStore: MetaStore,
            assignmentStrategy: ShardAssignmentStrategy,
            resolveActorDuration: FiniteDuration = 10.seconds): Props =
    Props(classOf[NodeClusterActor], cluster, nodeCoordRole, metaStore,
                                     assignmentStrategy, resolveActorDuration)

  class RemoteAddressExtensionImpl(system: ExtendedActorSystem) extends Extension {
    def address: Address = system.provider.getDefaultAddress
  }

  object RemoteAddressExtension extends ExtensionKey[RemoteAddressExtensionImpl]
}

/**
 * An actor that subscribes to membership events for a FiloDB cluster and maintains assignments
 * for dataset shards to nodes. It also can send messages to one or all coordinators of a given role,
 * and helps coordinate dataset setup and teardown -- for both ingestion and querying.
 *
 * The state consists of the node membership as well as a ShardMapper for each registered dataset.
 * Membership changes as well as dataset registrations cause a ShardAssignmentStrategy to be consulted
 * for changes to shard mapping to happen.  Any changes to shard mapping are then distributed to all
 * coordinators as well as subscribers (usually RowSources).
 * The state includes the state of each node - is it ready for ingestion?  recovering?  etc.  These
 * state changes are watched by for example RowSources to determine when to send data.
 *
 * TODO: implement UnregisterDataset?  Any state change -> reassign shards?
 * TODO: implement get cluster state
 * TODO: what to preserve to restore properly on cluster exit/rollover? datasets being ingested?
 *
 * There should only be ONE of these for a given cluster.
 *
 * Compared to the standard cluster aware routers, it has a couple advantages:
 * - It tracks all roles, not just one, and can send messages to a specific role per invocation
 * - It has much lower latency - direct sending to ActorRefs, no need to resolve ActorSelections
 *   (but of course it assumes the destination actor is always up... TODO watch for other actor dying)
 * - It broadcasts messages to all members of a role and sends back a collected response in one message,
 *   making it very easy to handle for non-actors.
 * - It can notify when some address joins
 * - It tracks dataset shard assignments and coordinates new dataset setup
 */
private[filodb] class NodeClusterActor(cluster: Cluster,
                                       nodeCoordRole: String,
                                       metaStore: MetaStore,
                                       assignmentStrategy: ShardAssignmentStrategy,
                                       resolveActorDuration: FiniteDuration) extends BaseActor {
  import NodeClusterActor._, NodeGuardian._

  val memberRefs = new HashMap[Address, ActorRef]
  val roleToCoords = new HashMap[String, Set[ActorRef]]().withDefaultValue(Set.empty[ActorRef])
  val shardMappers = new HashMap[DatasetRef, ShardMapper]
  val subscribers = new HashMap[DatasetRef, Set[ActorRef]].withDefaultValue(Set.empty[ActorRef])
  val projections = new HashMap[DatasetRef, RichProjection]
  val sources = new HashMap[DatasetRef, IngestionSource]

  import context.dispatcher

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  private def withRole(role: String)(f: Set[ActorRef] => Unit): Unit = roleToCoords.get(role) match {
    case None       => sender() ! NoSuchRole
    case Some(refs) => f(refs)
  }

  // Update shard maps and send out updates to subcribers
  private def stateChanged(updatedMaps: Seq[(DatasetRef, ShardMapper)]): Unit = {
    updatedMaps.foreach { case (ref, newMap) =>
      shardMappers(ref) = newMap
      val update = ShardMapUpdate(ref, newMap)
      subscribers(ref).foreach(_ ! update)
    }
  }

  private def setupDataset(originator: ActorRef, setup: SetupDataset): Unit = {
    (for { datasetObj <- metaStore.getDataset(setup.ref)
           columnObjects <- metaStore.getColumns(setup.ref, 0, setup.columns) }
     yield {
       val proj = RichProjection(datasetObj, columnObjects)
       self ! RegisterDataset(proj, setup.resources, setup.source)
       originator ! DatasetVerified
    }).recover {
      case MetaStore.UndefinedColumns(cols) => originator ! UndefinedColumns(cols)
      case err: RichProjection.BadSchema => originator ! BadSchema(err.toString)
      case NotFoundError(what) => originator ! UnknownDataset(setup.ref)
      case t: Throwable        => originator ! MetadataException(t)
    }
  }

  val localRemoteAddr = RemoteAddressExtension(context.system).address
  var everybodyLeftSender: Option[ActorRef] = None

  def membershipHandler: Receive = LoggingReceive {
    case MemberUp(member) =>
      logger.info(s"Member ${member.status}: ${member.address} with roles ${member.roles}")
      val memberCoordActor = nodeCoordinatorPath(member.address)
      context.actorSelection(memberCoordActor).resolveOne(resolveActorDuration)
        .map { ref => self ! AddCoordActor(member.roles, member.address, ref) }
        .recover {
          case e: Exception =>
            logger.warn(s"Unable to resolve coordinator at $memberCoordActor, ignoring. " +
                        "Maybe NodeCoordinatorActor did not start up before node joined cluster.", e)
        }

    case UnreachableMember(member) =>
      logger.info(s"Member detected as unreachable: $member")

    case MemberRemoved(member, previousStatus) =>
      logger.info(s"Member is Removed: ${member.address} after $previousStatus")
      memberRefs.remove(member.address).map { removedCoordinator =>
          // roleToCoords(role) = roleToCoords(role).filterNot { ref =>
          //   // if we don't do this cannot properly match when self node is asked to leave
          //   val addr = if (ref.path.address.hasLocalScope) localRemoteAddr else ref.path.address
          //   addr == member.address
          // }
        roleToCoords.transform { case (_, refs) => refs - removedCoordinator }
        roleToCoords.retain { case (role, refs) => refs.nonEmpty }
        subscribers.transform { case (_, refs) => refs - removedCoordinator }
        val updates = assignmentStrategy.nodeRemoved(removedCoordinator, shardMappers)
        stateChanged(updates)
      }.getOrElse { logger.warn(s"UNABLE TO REMOVE ${member.address} FROM memberRefs!!") }
      if (roleToCoords.isEmpty) {
        logger.info("All members removed!")
        everybodyLeftSender.foreach { ref => ref ! EverybodyLeave }
        everybodyLeftSender = None
      }

    case _: MemberEvent => // ignore
  }

  private def sendDatasetSetup(coords: Set[ActorRef], proj: RichProjection, source: IngestionSource): Unit = {
    logger.info(s"Sending setup message for ${proj.datasetRef} to coordinators $coords...")
    val colStrs = proj.dataColumns.map(_.toString)
    val setupMsg = IngestionCommands.DatasetSetup(proj.dataset, colStrs, 0, source)
    coords.foreach(_ ! setupMsg)
  }

  private def addNodeCoord(addr: Address, coordRef: ActorRef): Unit = {
    logger.info(s"Adding node with address $addr, coordinator $coordRef")
    memberRefs(addr) = coordRef
    subscribers.transform { case (_, refs) => refs + coordRef }

    // Let each NodeCoordinator know about us so it can send us updates
    coordRef ! NodeCoordinatorActor.ClusterHello(self)

    val updates = assignmentStrategy.nodeAdded(coordRef, shardMappers)
    logger.debug(s"Updated shard maps: $updates")
    stateChanged(updates)

    updates.foreach { case (ref, _) => sendDatasetSetup(Set(coordRef), projections(ref), sources(ref)) }
  }

  def shardMapHandler: Receive = LoggingReceive {
    case Terminated(ref) =>
      subscribers.foreach { case (ds, subs) => subscribers(ds) = subscribers(ds) - ref }

    case SubscribeShardUpdates(ref) =>
      // Send an immediate current snapshot of partition state
      // (as ingestion will subscribe usually when cluster is already stable)
      shardMappers.get(ref) match {
        case Some(map) =>
          sender() ! ShardMapUpdate(ref, map)
          subscribers(ref) = subscribers(ref) + sender()
          logger.debug(s"Registered $sender to receive shard map updates for $ref")
          context.watch(sender())
        case None =>
          logger.info(s"No such dataset $ref set up yet")
          sender() ! UnknownDataset(ref)
      }

    case s: SetupDataset =>
      if (subscribers contains s.ref) { sender() ! DatasetAlreadySetup(s.ref) }
      else { setupDataset(sender(), s) }

    case RegisterDataset(proj, spec, source) =>
      val ref = proj.datasetRef
      if (subscribers contains ref) {
        logger.info(s"Dataset $ref already registered, ignoring...")
      } else {
        logger.info(s"Registering dataset $ref with resources $spec and ingestion source $source...")
        val allCoordinators = memberRefs.values.toSet
        val updates = assignmentStrategy.datasetAdded(ref, spec, allCoordinators, shardMappers)
        logger.debug(s"Updated shard maps: $updates")

        projections(ref) = proj
        sources(ref) = source
        subscribers(ref) = allCoordinators
        stateChanged(updates)

        sendDatasetSetup(allCoordinators, proj, source)
      }

    case AddCoordActor(roles, addr, coordRef) =>
      roles.foreach { role => roleToCoords(role) += coordRef }
      if (roles contains nodeCoordRole) addNodeCoord(addr, coordRef)
      logger.debug(s"Updated roleToCoords: $roleToCoords")
  }

  def routerEvents: Receive = LoggingReceive {
    case ForwardToOne(role, msg) =>
      withRole(role) { refs => refs.toSeq.apply(util.Random.nextInt(refs.size)).forward(msg) }

    case GetRefs(role) =>
      withRole(role) { refs => sender() ! refs }

    case Broadcast(role, msg) =>
      withRole(role) { refs => refs.foreach(_.forward(msg)) }

    case EverybodyLeave =>
      if (roleToCoords.isEmpty) { sender() ! EverybodyLeave }
      else if (everybodyLeftSender.isEmpty) {
        logger.info(s"Removing all members from cluster...")
        cluster.state.members.map(_.address).foreach(cluster.leave)
        everybodyLeftSender = Some(sender())
      } else {
        logger.warn(s"Ignoring EverybodyLeave, somebody already sent it")
      }

    case Reset =>
      logger.info(s"Resetting all dataset state except membership....")
      shardMappers.clear()
      subscribers.clear()
      projections.clear()
      sources.clear()
  }

  def receive: Receive = membershipHandler orElse shardMapHandler orElse routerEvents
}
