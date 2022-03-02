package filodb.coordinator

import scala.collection.mutable.{HashMap => MutableHashMap, Map => MMap}

import akka.actor._
import akka.cluster.singleton._

import filodb.core.DatasetRef
import filodb.core.memstore.TimeSeriesStore
import filodb.core.store.MetaStore

/** Supervisor for all child actors and their actors on the node. */
final class NodeGuardian(val settings: FilodbSettings,
                         metaStore: MetaStore,
                         memStore: TimeSeriesStore,
                         assignmentStrategy: ShardAssignmentStrategy
                        ) extends BaseActor {

  import ActorName._
  import NodeProtocol._

  val shardMappers = new MutableHashMap[DatasetRef, ShardMapper]

  /** For tracking state when the singleton goes down and restarts on a new node.  */
  var subscriptions = ShardSubscriptions.Empty

  val failureAware = context.actorOf(Props(new NodeLifecycleStrategy(settings)), "failure-aware")

  override def postStop(): Unit = {
    super.postStop()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preStart()
    // coming soon
  }

  def guardianReceive: Actor.Receive = {
    case CreateCoordinator         => createCoordinator(sender())
    case e: CreateClusterSingleton => createSingleton(e, sender())
    case e: ShardEvent             => shardEvent(e)
    case s: CurrentShardSnapshot   => shardSnapshot(s)
    case e: ShardSubscriptions     => subscriptions = e
    case GetClusterState           => state(sender())
    case e: ListenerRef            => failureAware ! e
  }

  override def receive: Actor.Receive = guardianReceive

  /** Sends the current state of local `ShardMapper`
    * and `ShardSubscription` collections to the `requestor`.
    */
  private def state(requestor: ActorRef): Unit =
    requestor ! ClusterState(shardMappers, subscriptions)

  private def shardSnapshot(s: CurrentShardSnapshot): Unit = {
    logger.trace(s"Updating shardMappers for ref ${s.ref}")
    shardMappers(s.ref) = s.map
  }

  private def shardEvent(e: ShardEvent): Unit = {
    logger.trace(s"Updating shard mapper for ref ${e.ref} with event $e")
    for {
      map <- shardMappers.get(e.ref)
      if map.updateFromEvent(e).isSuccess
    } shardMappers(e.ref) = map
  }

  /** The NodeClusterActor's [[filodb.coordinator.ShardSubscriptions]]
    * does the DeathWatch on  [[filodb.coordinator.NodeCoordinatorActor]]
    * instances in order to update shard status, so this actor does not
    * watch it here.
    *
    * Idempotent.
    */
  private def createCoordinator(requester: ActorRef): Unit = {
    val actor = context.child(CoordinatorName) getOrElse {
      val props = NodeCoordinatorActor.props(metaStore, memStore, settings)
      context.actorOf(props, CoordinatorName) }

    requester ! CoordinatorIdentity(actor)
  }

  /** Creates a singleton NodeClusterActor and returns a proxy ActorRef to it.
    * This should be called on every FiloDB Coordinator/ingestion
    * node. There is only ONE instance per cluster.
    */
  private def createSingleton(e: CreateClusterSingleton, requester: ActorRef): Unit = {
    val proxy = clusterActor(e.role)

    if (context.child(ClusterSingletonName).isEmpty) {
      val watcher = e.watcher.getOrElse(self)
      val mgr = context.actorOf(
        ClusterSingletonManager.props(
          singletonProps = NodeClusterActor.props(
            settings, e.role, metaStore, assignmentStrategy, NodeClusterActor.ActorArgs(proxy, self, watcher)),
          terminationMessage = PoisonPill,
          settings = ClusterSingletonManagerSettings(context.system)
            .withRole(e.role)
            .withSingletonName(ClusterSingletonName)),
          name = ClusterSingletonManagerName)

      logger.info(s"Created ClusterSingletonManager for NodeClusterActor [mgr=$mgr, role=${e.role}]")
    }

    requester ! ClusterSingletonIdentity(proxy)
  }

  /** Returns reference to the cluster actor. The proxy
    * can be started on every node where the singleton needs to be reached.
    *
    * @param role the cluster role
    */
  private def clusterActor(role: String): ActorRef = {
    val proxy = context.child(ClusterSingletonProxyName).getOrElse {
      context.actorOf(ClusterSingletonProxy.props(
        singletonManagerPath = s"/user/$NodeGuardianName/$ClusterSingletonManagerName",
        settings = ClusterSingletonProxySettings(context.system).withRole(role)),
        name = ClusterSingletonProxyName)
    }

    // Subscribe myself to all shard updates and subscriber additions. NOTE: this code needs to be
    // run on every node so that any node that the singleton fails over to will have the backup in the Guardian.
    proxy ! ShardSubscriptions.SubscribeAll

    logger.info(s"Created ClusterSingletonProxy [proxy=$proxy, role=$role]")
    proxy
  }
}

private[filodb] object NodeGuardian {

  def props(settings: FilodbSettings,
            metaStore: MetaStore,
            memStore: TimeSeriesStore,
            assignmentStrategy: ShardAssignmentStrategy): Props =
    Props(new NodeGuardian(settings, metaStore, memStore, assignmentStrategy))
}

/** Management and task actions on the local node.
  * INTERNAL API.
  */
object NodeProtocol {

  /** Commands to start a task. */
  @SerialVersionUID(1)
  sealed trait TaskCommand extends Serializable
  /* Acked on task complete */
  @SerialVersionUID(1)
  sealed trait TaskAck extends Serializable

  sealed trait LifecycleCommand extends TaskCommand
  sealed trait StateCommand extends TaskCommand

  sealed trait LifecycleAck extends TaskAck
  sealed trait StateTaskAck extends TaskAck

  /**
    * @param role the role to assign
    * @param watcher the guardian actor. In Test this can include a probe.
    */
  private[coordinator] final case class CreateClusterSingleton(role: String,
                                                               watcher: Option[ActorRef]
                                                              ) extends LifecycleCommand

  private[coordinator] case object CreateCoordinator extends LifecycleCommand

  private[coordinator] final case class ShutdownComplete(ref: ActorRef) extends LifecycleAck

  /** Identity ACK. */
  @SerialVersionUID(1)
  sealed trait ActorIdentity extends Serializable {
    def identity: ActorRef
  }

  /* Specific load-time 'create' task completion ACKs: */
  private[coordinator] final case class CoordinatorIdentity(identity: ActorRef) extends ActorIdentity
  private[coordinator] final case class ClusterSingletonIdentity(identity: ActorRef) extends ActorIdentity
  private[coordinator] final case class TraceLoggerRef(identity: ActorRef) extends ActorIdentity
  private[coordinator] final case class ListenerRef(identity: ActorRef) extends ActorIdentity

  private[filodb] case object ResetState extends StateCommand
  private[coordinator] case object GetClusterState extends StateCommand

  private[filodb] case object StateReset extends StateTaskAck

  private[coordinator] final case class ClusterState(shardMaps: MMap[DatasetRef, ShardMapper],
                                                     subscriptions: ShardSubscriptions) extends StateTaskAck

  /** Self-generated events for watchers of specific actor lifecycle transitions such as
    * restarts of an actor on error, or restarts of one actor on a different node than
    * it was first created on, for host or new instance verification.
    */
  sealed trait ActorLifecycleEvent extends ActorIdentity
  final case class PreStart(identity: ActorRef, address: Address) extends ActorLifecycleEvent
  final case class PreRestart(identity: ActorRef, address: Address, reason: Throwable) extends ActorLifecycleEvent
  final case class PostStop(identity: ActorRef, address: Address) extends ActorLifecycleEvent

}
