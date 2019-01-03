package filodb.coordinator

import scala.collection.mutable.{HashMap => MutableHashMap, HashSet => MutableHashSet}
import scala.concurrent.duration._
import scala.concurrent.Future

import akka.actor._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.event.LoggingReceive
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import filodb.core._
import filodb.core.metadata.Dataset
import filodb.core.store.{AssignShardConfig, IngestionConfig, MetaStore, StoreConfig, UnassignShardConfig}

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

  case class GetDatasetFromRef(datasetRef: DatasetRef)

  final case class StopShards(unassignmentConfig: UnassignShardConfig, datasetRef: DatasetRef)

  final case class StartShards(assignmentConfig: AssignShardConfig, datasetRef: DatasetRef)

  /**
   * Sets up a dataset for streaming ingestion and querying, with specs for sharding.
   * Results in a state change and shard assignment to existing and new nodes.
   * Initiates ingestion on each node.
   *
   * @param ref the DatasetRef for the dataset defined in MetaStore to start ingesting
   * @param resources the sharding and number of nodes for ingestion and querying
   * @param source the IngestionSource on each node.  Use noOpSource to not start ingestion and
   *               manually push records into NodeCoordinator.
   * @param storeConfig a StoreConfig for the MemStore.
   * @return DatasetVerified - meaning the dataset and columns are valid.  Does not mean ingestion is
   *                           setup on all nodes - for that, subscribe to ShardMapUpdate's
   */
  final case class SetupDataset(ref: DatasetRef,
                                resources: DatasetResourceSpec,
                                source: IngestionSource,
                                storeConfig: StoreConfig) {
    import collection.JavaConverters._
    val resourceConfig = ConfigFactory.parseMap(
      Map("num-shards" -> resources.numShards, "min-num-nodes" -> resources.minNumNodes).asJava)
    val config = IngestionConfig(ref, resourceConfig,
                                 source.streamFactoryClass,
                                 source.config,
                                 storeConfig)
  }

  object SetupDataset {
    def apply(source: IngestionConfig): SetupDataset =
      SetupDataset(source.ref,
                   DatasetResourceSpec(source.numShards, source.minNumNodes),
                   IngestionSource(source.streamFactoryClass, source.streamConfig),
                   source.storeConfig)
  }

  // A dummy source to use for tests and when you just want to push new records in
  val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)

  case object DatasetVerified
  private[coordinator] final case class DatasetExists(ref: DatasetRef) extends ErrorResponse
  final case class DatasetUnknown(ref: DatasetRef) extends ErrorResponse
  final case class BadSchema(message: String) extends ErrorResponse
  final case class BadData(message: String) extends ErrorResponse

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

  // TODO Important Bug: Unsubscribe was not handled here from earlier. Needs to be addressed in a new PR.

  /**
   * INTERNAL MESSAGES
   */
  private[coordinator] final case class AddCoordinator(roles: Set[String], addr: Address, coordinator: ActorRef)
  private case object RemoveStaleCoordinators

  /** Lets each NodeCoordinator know about the `clusterActor` so it can send it updates.
    * Would not be necessary except coordinator currently is created before the clusterActor.
    */
  private[coordinator] final case class CoordinatorRegistered(clusterActor: ActorRef)

  // Only use for testing, in before {} blocks
  private[coordinator] case object EverybodyLeave

  private[coordinator] final case class ActorArgs(singletonProxy: ActorRef,
                                                  guardian: ActorRef,
                                                  watcher: ActorRef)

  private[coordinator] case object PublishSnapshot

  /**
    * Creates a new NodeClusterActor.
    *
    * @param localRole String, for the role containing the NodeCoordinatorActor or ingestion nodes
    */
  def props(settings: FilodbSettings,
            localRole: String,
            metaStore: MetaStore,
            assignmentStrategy: ShardAssignmentStrategy,
            actors: ActorArgs): Props =
    Props(new NodeClusterActor(settings, localRole, metaStore, assignmentStrategy, actors))

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
                                       localRole: String,
                                       metaStore: MetaStore,
                                       assignmentStrategy: ShardAssignmentStrategy,
                                       actors: NodeClusterActor.ActorArgs
                                      ) extends NamingAwareBaseActor {

  import ActorName._, NodeClusterActor._, ShardSubscriptions._
  import actors._
  import settings._
  import context.dispatcher

  val cluster = Cluster(context.system)

  val initDatasets = new MutableHashSet[DatasetRef]
  val roleToCoords = new MutableHashMap[String, Set[ActorRef]]().withDefaultValue(Set.empty[ActorRef])
  val datasets = new MutableHashMap[DatasetRef, Dataset]
  val sources = new MutableHashMap[DatasetRef, IngestionSource]
  val shardManager = new ShardManager(settings, assignmentStrategy)
  val localRemoteAddr = RemoteAddressExtension(context.system).address
  var everybodyLeftSender: Option[ActorRef] = None
  val shardUpdates = new MutableHashSet[DatasetRef]

  val publishInterval = settings.ShardMapPublishFrequency

  var pubTask: Option[Cancellable] = None

  override def preStart(): Unit = {
    super.preStart()
    watcher ! NodeProtocol.PreStart(singletonProxy, cluster.selfAddress)

    // Restore previously set up datasets and shards.  This happens in a very specific order so that
    // shard and dataset state can be recovered correctly.  First all the datasets are set up.
    // Then shard state is recovered, and finally cluster membership events are replayed.
    logger.info(s"Attempting to restore previous ingestion state...")
    metaStore.readIngestionConfigs()
             .map { configs =>
               initDatasets ++= configs.map(_.ref)
               configs.foreach { config => self ! SetupDataset(config) }
               if (configs.isEmpty) initiateShardStateRecovery()
             }.recover {
               case e: Exception =>
                 logger.error(s"Unable to restore ingestion state: $e\nTry manually setting up ingestion again", e)
                 // Continue the protocol, so that the singleton can come up
                 initiateShardStateRecovery()
             }
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster.unsubscribe(self)
    pubTask foreach (_.cancel)
    // Publish one last update
    datasets.keys.foreach(shardManager.publishSnapshot)
    watcher ! NodeProtocol.PostStop(singletonProxy, cluster.selfAddress)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    watcher ! NodeProtocol.PreRestart(singletonProxy, cluster.selfAddress, reason)
  }

  private def withRole(role: String, requester: ActorRef)(f: Set[ActorRef] => Unit): Unit =
    roleToCoords.get(role) match {
      case None       => requester ! NoSuchRole
      case Some(refs) => f(refs)
    }

  private def memberUp(member: Member): Future[Unit] = {
    logger.info(s"Member ${member.status}: ${member.address} with roles ${member.roles}")
    val memberCoordActor = nodeCoordinatorPath(member.address)
    context.actorSelection(memberCoordActor).resolveOne(ResolveActorTimeout)
      .map { ref => self ! AddCoordinator(member.roles, member.address, ref) }
      .recover {
        case e: Exception =>
          logger.warn(s"Unable to resolve coordinator at $memberCoordActor, ignoring. " +
            "Maybe NodeCoordinatorActor did not start up before node joined cluster.", e)
      }
  }

  def membershipHandler: Receive = LoggingReceive {
    case s: CurrentClusterState =>
      logger.info(s"Initial Cluster State was: $s")
      val memberUpFutures = s.members.filter(_.status == MemberStatus.Up).map(memberUp(_))
      Future.sequence(memberUpFutures.toSeq).onComplete { _ =>
        self ! RemoveStaleCoordinators
      }

    case MemberUp(member) =>
      memberUp(member)

    case UnreachableMember(member) =>
      logger.info(s"Member detected as unreachable: $member")

    case e @ MemberRemoved(member, previousStatus) =>
      logger.info(s"Member is Removed: ${member.address} after $previousStatus")

      shardManager.removeMember(member.address) match {
        case Some(ref) =>
          // roleToCoords(role) = roleToCoords(role).filterNot { ref =>
          //   // if we don't do this cannot properly match when self node is asked to leave
          //   val addr = if (ref.path.address.hasLocalScope) localRemoteAddr else ref.path.address
          //   addr == member.address
          // }
          roleToCoords.transform { case (_, refs) => refs - ref }
          roleToCoords.retain { case (role, refs) => refs.nonEmpty }

        case None =>
          logger.info(s"Received MemberRemoved for stale member ${member.address}. It " +
            s" will be (or already has been) removed on RemoveStaleCoordinators event")
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
    case GetShardMap(ref)       => shardManager.sendSnapshot(ref, sender())
  }

  // The initial recovery handler: recover dataset setup/ingestion config first
  def datasetHandler: Receive = LoggingReceive {
    case e: SetupDataset =>
      setupDataset(e, sender()) map { _ =>
        initDatasets -= e.ref
        if (initDatasets.isEmpty) initiateShardStateRecovery()
      }
    case GetDatasetFromRef(r) => sender() ! datasets(r)
  }

  private def initiateShardStateRecovery(): Unit = {
    logger.info("Recovery of ingestion configs/datasets complete")
    sendShardStateRecoveryMessage()
    context.become(shardMapRecoveryHandler orElse subscriptionHandler)
  }

  def shardMapRecoveryHandler: Receive = LoggingReceive {
    case NodeProtocol.ClusterState(mappers, subscriptions) =>
      // never includes downed nodes, which come through cluster.subscribe event replay
      mappers foreach { case (ref, map) => shardManager.recoverShards(ref, map) }
      shardManager.recoverSubscriptions(subscriptions)

      // NOW, subscribe to cluster membership state and then switch to normal receiver
      logger.info("Subscribing to cluster events and switching to normalReceive")
      cluster.subscribe(self, initialStateMode = InitialStateAsSnapshot, classOf[MemberEvent])
      scheduleSnapshotPublishes()
      context.become(normalReceive)
  }

  // The idea behind state updates is that they are cached and published as an entire ShardMapper snapshot periodically
  // This way it is much less likely for subscribers to miss individual events.
  // handleEventEnvelope() currently acks right away, so there is a chance that this actor dies between receiving
  // a new event and the new snapshot is published.
  private def scheduleSnapshotPublishes() = {
    pubTask = Some(context.system.scheduler.schedule(1.second, publishInterval, self, PublishSnapshot))
  }

  def shardMapHandler: Receive = LoggingReceive {
    case e: AddCoordinator        => addCoordinator(e)
    case RemoveStaleCoordinators  => shardManager.removeStaleCoordinators()
    case e: SetupDataset          => setupDataset(e, sender())
    case e: StartShards           => shardManager.startShards(e, sender())
    case e: StopShards            => shardManager.stopShards(e, sender())
  }

  def subscriptionHandler: Receive = LoggingReceive {
    case e: ShardEvent            => handleShardEvent(e)
    case e: StatusActor.EventEnvelope => handleEventEnvelope(e, sender())
    case PublishSnapshot          => shardUpdates.foreach(shardManager.publishSnapshot)
                                     shardUpdates.clear()
    case e: SubscribeShardUpdates => subscribe(e.ref, sender())
    case SubscribeAll             => subscribeAll(sender())
    case Terminated(subscriber)   => context unwatch subscriber
  }

  private def handleShardEvent(e: ShardEvent) = {
    logger.debug(s"Received ShardEvent $e from $sender")
    shardUpdates += e.ref
    shardManager.updateFromExternalShardEvent(sender(), e)
  }

  // TODO: Save acks for when snapshots are published?
  private def handleEventEnvelope(e: StatusActor.EventEnvelope, ackTo: ActorRef) = {
    e.events.foreach(handleShardEvent)
    ackTo ! StatusActor.StatusAck(e.sequence)
  }

  /** Send a message to recover the current shard maps and subscriptions from the Guardian of local node. */
  private def sendShardStateRecoveryMessage(): Unit =
    guardian ! NodeProtocol.GetClusterState

  /** If the dataset is registered as a subscription, a `CurrentShardSnapshot` is sent
    * to the subscriber, otherwise a `DatasetUnknown` is returned.
    *
    * It is on the subscriber/client and NodeClusterActor to know you can't subscribe
    * unless the dataset has first been fully set up. Subscription datasets are only
    * set up by the NodeClusterActor.
    *
    * Idempotent.
    */
  private def subscribe(dataset: DatasetRef, subscriber: ActorRef): Unit = {
    shardManager.subscribe(subscriber, dataset)
    context watch subscriber
  }

  private def subscribeAll(subscriber: ActorRef): Unit = {
    shardManager.subscribeAll(subscriber)
    context watch subscriber
  }

  /** Sets up the new coordinator which is sent an ack. */
  private def addCoordinator(e: AddCoordinator): Unit = {
    e.roles.foreach { role => roleToCoords(role) += e.coordinator }
    logger.debug(s"Updated roleToCoords: $roleToCoords")

    if (e.roles contains localRole) {
      e.coordinator ! CoordinatorRegistered(singletonProxy)
      shardManager.addMember(e.addr, e.coordinator)
    }
  }

  /** Initiated by Client and Spark FiloDriver setup. */
  private def setupDataset(setup: SetupDataset, origin: ActorRef): Future[Unit] = {
    logger.info(s"Registering dataset ${setup.ref} with ingestion source ${setup.source}")
    (for { datasetObj    <- metaStore.getDataset(setup.ref)
           resp1         <- metaStore.writeIngestionConfig(setup.config) }
      yield {
        // Add the dataset first so once DatasetVerified is sent back any queries will actually list the dataset
        datasets(setup.ref) = datasetObj
        shardManager.addDataset(setup, datasetObj, origin)
        sources(setup.ref) = setup.source
      }).recover {
      case err: Dataset.BadSchema           => origin ! BadSchema(err.toString)
      case NotFoundError(what)              => origin ! DatasetUnknown(setup.ref)
      case t: Throwable                     => origin ! MetadataException(t)
    }
  }

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
      shardUpdates.clear()

      implicit val timeout: Timeout = DefaultTaskTimeout
      shardManager.reset()
      origin ! NodeProtocol.StateReset
  }

  def normalReceive: Receive =
    membershipHandler orElse
    shardMapHandler orElse
    infoHandler orElse
    routerEvents orElse
    subscriptionHandler

  def receive: Receive =
    datasetHandler orElse
    subscriptionHandler
}
