package filodb.coordinator

import scala.collection.mutable.{HashMap, HashSet}

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import filodb.core._
import filodb.core.metadata.Dataset
import filodb.core.store.{IngestionConfig, MetaStore}

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
   * @param resources the sharding and number of nodes for ingestion and querying
   * @param source the IngestionSource on each node.  Use noOpSource to not start ingestion and
   *               manually push records into NodeCoordinator.
   * @return DatasetVerified - meaning the dataset and columns are valid.  Does not mean ingestion is
   *                           setup on all nodes - for that, subscribe to ShardMapUpdate's
   */
  final case class SetupDataset(ref: DatasetRef,
                                resources: DatasetResourceSpec,
                                source: IngestionSource) {
    import collection.JavaConverters._
    val resourceConfig = ConfigFactory.parseMap(
      Map("num-shards" -> resources.numShards, "min-num-nodes" -> resources.minNumNodes).asJava)
    val config = IngestionConfig(ref, resourceConfig,
                                 source.streamFactoryClass,
                                 source.config)
  }

  object SetupDataset {
    def apply(source: IngestionConfig): SetupDataset =
      SetupDataset(source.ref,
                   DatasetResourceSpec(source.resources.getInt("num-shards"),
                                       source.resources.getInt("min-num-nodes")),
                   IngestionSource(source.streamFactoryClass, source.streamConfig))
  }

  // A dummy source to use for tests and when you just want to push new records in
  val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)

  case object DatasetVerified
  private[coordinator] final case class DatasetExists(ref: DatasetRef) extends ErrorResponse
  final case class DatasetUnknown(ref: DatasetRef) extends ErrorResponse
  final case class BadSchema(message: String) extends ErrorResponse

  // Cluste state info commands
  // Returns a Seq[DatasetRef]
  case object ListRegisteredDatasets
  // Returns CurrentShardSnapshot or DatasetUnknown
  final case class GetShardMap(ref: DatasetRef)

  /** Registers sending actor to receive `ShardMapUpdate` whenever it changes. DeathWatch
    * will be used on the sending actors to watch for updates. On subscribe, will
    * immediately send back the current state via a `ShardMapUpdate` message.
    * NodeCoordinators are automatically subscribed to every dataset. This should
    * be sent after RegisterDataset.
    */
  final case class SubscribeShardUpdates(ref: DatasetRef)

  /**
   * INTERNAL MESSAGES
   */
  private[coordinator] final case class AddCoordinator(roles: Set[String], addr: Address, coordinator: ActorRef)
  /** Lets each NodeCoordinator know about the `clusterActor` so it can send it updates.
    * Would not be necessary except coordinator currently is created before the clusterActor.
    */
  private[coordinator] final case class CoordinatorRegistered(clusterActor: ActorRef)


  // Only use for testing, in before {} blocks
  private[coordinator] case object EverybodyLeave
  private[coordinator] case object RecoverShardStates

  private[coordinator] final case class ActorArgs(singletonProxy: ActorRef,
                                                             guardian: ActorRef,
                                                             watcher: Option[ActorRef])
  /**
    * Creates a new NodeClusterActor.
    *
    * @param nodeCoordRole String, for the role containing the NodeCoordinatorActor or ingestion nodes
    */
  def props(settings: FilodbSettings,
            nodeCoordRole: String,
            metaStore: MetaStore,
            assignmentStrategy: ShardAssignmentStrategy,
            actors: ActorArgs): Props =
    Props(new NodeClusterActor(settings, nodeCoordRole, metaStore, assignmentStrategy, actors))

  class RemoteAddressExtension(system: ExtendedActorSystem) extends Extension {
    def address: Address = system.provider.getDefaultAddress
  }

  object RemoteAddressExtension extends ExtensionId[RemoteAddressExtension] with ExtensionIdProvider {
    override def lookup: ExtensionId[RemoteAddressExtension]= RemoteAddressExtension
    override def createExtension(system: ExtendedActorSystem): RemoteAddressExtension =
      new RemoteAddressExtension(system)
    override def get(system: ActorSystem): RemoteAddressExtension = super.get(system)
  }

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
 * When DatasetSetup is received, the setup info is persisted to the MetaStore.  On startup, this actor
 * will restore previously persisted setup state so that DatasetSetup is not needed and streaming/querying
 * can restart automatically for datasets previously set up.
 *
 * TODO: implement UnregisterDataset?  Any state change -> reassign shards?
 * TODO: implement get cluster state
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
private[filodb] class NodeClusterActor(settings: FilodbSettings,
                                       nodeCoordRole: String,
                                       metaStore: MetaStore,
                                       assignmentStrategy: ShardAssignmentStrategy,
                                       actors: NodeClusterActor.ActorArgs
                                      ) extends NamingAwareBaseActor {

  import akka.pattern.{ask, pipe}
  import ActorName._, NodeClusterActor._, ShardSubscriptions._
  import ShardAssignmentStrategy.DatasetShards
  import actors._
  import settings._

  val cluster = Cluster(context.system)

  val memberRefs = new HashMap[Address, ActorRef]
  val roleToCoords = new HashMap[String, Set[ActorRef]]().withDefaultValue(Set.empty[ActorRef])
  val datasets = new HashMap[DatasetRef, Dataset]
  val sources = new HashMap[DatasetRef, IngestionSource]

  private val initDatasets = new HashSet[DatasetRef]

  /* TODO run on its own dispatcher .withDispatcher("akka.shard-status-dispatcher") */
  val shardActor = context.actorOf(Props(new ShardCoordinatorActor(assignmentStrategy)), ShardName)

  import context.dispatcher

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    super.preStart()
    actors.watcher foreach(_ ! NodeProtocol.PreStart(self.path))
    // Restore previously set up datasets and shards.  This happens in a very specific order so that
    // shard and dataset state can be recovered correctly.  First all the datasets are set up.
    // Then shard state is recovered, and finally cluster membership events are replayed.
    logger.info(s"Attempting to restore previous ingestion state...")
    metaStore.readIngestionConfigs()
             .map { configs =>
               initDatasets ++= configs.map(_.ref)
               configs.foreach { config => self ! SetupDataset(config) }
               if (configs.isEmpty) self ! RecoverShardStates
             }.recover {
               case e: Exception =>
                 logger.error(s"Unable to restore ingestion state: $e\nTry manually setting up ingestion again", e)
             }
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster.unsubscribe(self)
    watcher foreach(_ ! NodeProtocol.PostStop(self.path))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    watcher foreach(_ ! NodeProtocol.PreRestart(self.path, reason))
  }

  private def withRole(role: String, requester: ActorRef)(f: Set[ActorRef] => Unit): Unit =
    roleToCoords.get(role) match {
      case None       => requester ! NoSuchRole
      case Some(refs) => f(refs)
    }

  val localRemoteAddr = RemoteAddressExtension(context.system).address
  var everybodyLeftSender: Option[ActorRef] = None

  def membershipHandler: Receive = LoggingReceive {
    case MemberUp(member) =>
      logger.info(s"Member ${member.status}: ${member.address} with roles ${member.roles}")
      val memberCoordActor = nodeCoordinatorPath(member.address)
      context.actorSelection(memberCoordActor).resolveOne(ResolveActorTimeout)
        .map { ref => self ! AddCoordinator(member.roles, member.address, ref) }
        .recover {
          case e: Exception =>
            logger.warn(s"Unable to resolve coordinator at $memberCoordActor, ignoring. " +
                         "Maybe NodeCoordinatorActor did not start up before node joined cluster.", e)
        }

    case UnreachableMember(member) =>
      logger.info(s"Member detected as unreachable: $member")

    case MemberRemoved(member, previousStatus) =>
      logger.info(s"Member is Removed: ${member.address} after $previousStatus")
      memberRefs.remove(member.address) match {
        case Some(removedCoordinator) =>
        // roleToCoords(role) = roleToCoords(role).filterNot { ref =>
        //   // if we don't do this cannot properly match when self node is asked to leave
        //   val addr = if (ref.path.address.hasLocalScope) localRemoteAddr else ref.path.address
        //   addr == member.address
        // }
        roleToCoords.transform { case (_, refs) => refs - removedCoordinator }
        roleToCoords.retain { case (role, refs) => refs.nonEmpty }

        shardActor ! ShardSubscriptions.RemoveMember(removedCoordinator)
        case _ =>
          logger.warn(s"UNABLE TO REMOVE ${member.address} FROM memberRefs")
      }

      if (roleToCoords.isEmpty) {
        logger.info("All members removed!")
        everybodyLeftSender.foreach { ref => ref ! EverybodyLeave }
        everybodyLeftSender = None
      }

    case _: MemberEvent => // ignore
  }

  def infoHandler: Receive = LoggingReceive {
    case ListRegisteredDatasets => sender() ! datasets.keys.toSeq
    case GetShardMap(ref)       => shardActor.forward(GetSnapshot(ref))
  }

  // The initial recovery handler: recover dataset setup/ingestion config first
  def datasetInitHandler: Receive = LoggingReceive {
    case e: SetupDataset    => setupDataset(e, sender())
    case e: DatasetAdded    => datasetSetup(e)
                               initDatasets -= e.dataset.ref
                               if (initDatasets.isEmpty) self ! RecoverShardStates
    case RecoverShardStates => logger.info(s"Recovery of ingestion configs/datasets complete")
                               sendShardStateRecoveryMessage()
                               context.become(shardMapRecoveryHandler orElse subscriptionHandler)
  }

  def shardMapRecoveryHandler: Receive = LoggingReceive {
    case ms: NodeProtocol.MapsAndSubscriptions =>
      ms.shardMaps foreach { case (ref, map) => shardActor ! RecoverShardState(ref, map) }
      shardActor ! RecoverSubscriptions(ms.subscriptions)
      // NOW, subscribe to cluster membership state and then switch to normal receiver
      logger.info("Now subscribing to cluster events and switching to normalReceive")
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
                                classOf[MemberEvent])
      context.become(normalReceive)
  }

  def shardMapHandler: Receive = LoggingReceive {
    case e: AddCoordinator        => addCoordinator(e)
    case e: SetupDataset          => setupDataset(e, sender())
    case e: DatasetAdded          => datasetSetup(e)
    case e: CoordinatorAdded      => coordinatorAdded(e)
    case e: CoordinatorRemoved    => coordinatorRemoved(e)
  }

  def subscriptionHandler: Receive = LoggingReceive {
    case e: ShardEvent            => shardActor.forward(e)
    case e: SubscribeShardUpdates => subscribe(e.ref, sender())
    case SubscribeAll             => shardActor.forward(SubscribeAll)
  }

  /** Send a message to recover the current shard maps and subscriptions from the Guardian of local node. */
  private def sendShardStateRecoveryMessage(): Unit =
    guardian ! NodeProtocol.GetShardMapsSubscriptions

  /** If the dataset is registered as a subscription, a `CurrentShardSnapshot` is sent
    * to the subscriber, otherwise a `DatasetUnknown` is returned.
    *
    * It is on the subscriber/client and NodeClusterActor to know you can't subscribe
    * unless the dataset has first been fully set up. Subscription datasets are only
    * set up by the NodeClusterActor.
    *
    * Idempotent.
    */
  private def subscribe(dataset: DatasetRef, subscriber: ActorRef): Unit =
    shardActor ! ShardSubscriptions.Subscribe(subscriber, dataset)

  /** Sets up the new coordinator, forwards to shard status actor to complete
    * its subscription setup. The coordinator is sent an ack.
    * The shard stats actor responds with a `SendDatasetSetup` to handle the updates.
    */
  private def addCoordinator(e: AddCoordinator): Unit = {
    e.roles.foreach { role => roleToCoords(role) += e.coordinator }
    logger.debug(s"Updated roleToCoords: $roleToCoords")

    if (e.roles contains nodeCoordRole) {
      shardActor ! ShardSubscriptions.AddMember(e.coordinator, e.addr)
    }
  }

  /** Initiated by Client and Spark FiloDriver setup. */
  private def setupDataset(setup: SetupDataset, origin: ActorRef): Unit =
    (for { datasetObj    <- metaStore.getDataset(setup.ref)
           resp1         <- metaStore.writeIngestionConfig(setup.config) }
      yield {
        shardActor ! AddDataset(setup, datasetObj, memberRefs.values.toSet, origin)
      }).recover {
      case err: Dataset.BadSchema           => origin ! BadSchema(err.toString)
      case NotFoundError(what)              => origin ! DatasetUnknown(setup.ref)
      case t: Throwable                     => origin ! MetadataException(t)
    }

  private def datasetSetup(e: DatasetAdded): Unit = {
    val ref = e.dataset.ref
    logger.info(s"Registering dataset $ref with ingestion source ${e.source}")
    datasets(ref) = e.dataset
    sources(ref) = e.source

    // TODO inspect diff for new members not known at time of initial ds setup
    sendDatasetSetup(memberRefs.values.toSet, e.dataset, e.source)
    e.ackTo ! DatasetVerified
    sendStartCommand(e.shards, ref)
  }

  /** Called after a new coordinator is added and shard assignment strategy has
    * added the node and returns updates.
    *
    * NOTE: we send not our own ActorRef, but rather that of the cluster singleton proxy.
    * This enables coordinator/IngestionActor shard event updates to be more resilient.
    * The proxy buffers messages if the singleton is not available, and is a stable ref if the singleton moves.
    *
    * INTERNAL API. Idempotent.
    */
  private def coordinatorAdded(e: CoordinatorAdded): Unit = {
    e.coordinator ! CoordinatorRegistered(singletonProxy)

    // NOTE: it's important that memberRefs happens here, after CoordinatorRegistered is sent out, and not before
    // otherwise a race condition happens where SetupDataset could be sent to coords before it gets hello, and then
    // the SetupDataset will fail
    memberRefs(e.addr) = e.coordinator
    val oneAdded = Set(e.coordinator)
    for {
      DatasetShards(ref, _, shards) <- e.shards
    } {
      sendDatasetSetup(oneAdded, datasets(ref), sources(ref))
      sendStartCommand(Map(e.coordinator -> shards), ref)
    }
  }

  /** Called after an existing coordinator is removed, shard assignment
    * strategy has removed the node and returns updates.
    */
  private def coordinatorRemoved(e: CoordinatorRemoved): Unit =
    for {
      dss   <- e.shards
      shard <- dss.shards
    } e.coordinator ! StopShardIngestion(dss.ref, shard)

  /** Called on successful AddNodeCoordinator and SetupDataset protocols.
    *
    * @param coords the current cluster members
    * @param dataset the Dataset object
    * @param source the ingestion source type to use
    */
  private def sendDatasetSetup(coords: Set[ActorRef], dataset: Dataset, source: IngestionSource): Unit = {
    logger.info(s"Sending setup message for ${dataset.ref} to coordinators $coords.")
    val setupMsg = IngestionCommands.DatasetSetup(dataset.asCompactString, source)
    coords.foreach(_ ! setupMsg)
  }

  /** If no shards are assigned to a coordinator, no commands are sent. */
  private def sendStartCommand(coords: Map[ActorRef, Seq[Int]], ref: DatasetRef): Unit =
    for {
      (coord, shards) <- coords
      shard           <- shards.toList
    } coord ! StartShardIngestion(ref, shard, None)

  def routerEvents: Receive = LoggingReceive {
    case ForwardToOne(role, msg) =>
      withRole(role, sender()) { refs => refs.toSeq.apply(util.Random.nextInt(refs.size)).forward(msg) }

    case GetRefs(role) =>
      val requestor = sender()
      withRole(role, requestor) { refs => requestor ! refs }

    case Broadcast(role, msg) =>
      withRole(role, sender()) { refs => refs.foreach(_.forward(msg)) }

    case EverybodyLeave =>
      val requestor = sender()
      if (roleToCoords.isEmpty) {
        requestor ! EverybodyLeave
      }
      else if (everybodyLeftSender.isEmpty) {
        logger.info(s"Removing all members from cluster...")
        cluster.state.members.map(_.address).foreach(cluster.leave)
        everybodyLeftSender = Some(requestor)
      } else {
        logger.warn(s"Ignoring EverybodyLeave, somebody already sent it")
      }

    case NodeProtocol.ResetState =>
      val origin = sender()
      logger.info("Resetting all dataset state except membership.")
      datasets.clear()
      sources.clear()

      implicit val timeout: Timeout = DefaultTaskTimeout
      (shardActor ? NodeProtocol.ResetState) pipeTo origin
  }

  def normalReceive: Receive = membershipHandler orElse shardMapHandler orElse infoHandler orElse
                                  routerEvents orElse subscriptionHandler
  def receive: Receive = datasetInitHandler orElse subscriptionHandler
}
