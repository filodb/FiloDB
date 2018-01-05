package filodb.coordinator

import scala.collection.mutable.{HashMap => MutableHashMap, Map => MMap}

import akka.actor._
import akka.cluster.singleton._

import filodb.core.DatasetRef
import filodb.core.memstore.MemStore
import filodb.core.store.MetaStore

/** Supervisor for all child actors and their actors on the node. */
final class NodeGuardian(val settings: FilodbSettings,
                         metaStore: MetaStore,
                         memStore: MemStore,
                         assignmentStrategy: ShardAssignmentStrategy
                        ) extends GracefulStopAwareSupervisor {

  import ActorName._
  import NodeProtocol._

  val shardMappers = new MutableHashMap[DatasetRef, ShardMapper]

  /** For tracking state when the singleton goes down and restarts on a new node.  */
  var subscriptions = ShardSubscriptions.Empty

  val failureAware = context.actorOf(Props(new NodeLifecycleStrategy(settings)), "failure-aware")

  override def postStop(): Unit = {
    super.postStop()
    context.child(TraceLoggerName) foreach {
      actor => kamon.Kamon.shutdown()
    }
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preStart()
    // coming soon
  }

  def guardianReceive: Actor.Receive = {
    case CreateTraceLogger(role)   => startKamon(role, sender())
    case CreateCoordinator         => createCoordinator(sender())
    case e: CreateClusterSingleton => createSingleton(e, sender())
    case e: ShardEvent             => shardEvent(e)
    case s: CurrentShardSnapshot   => setShardMap(s)
    case e: ShardSubscriptions     => subscriptions = e
    case GetShardMapsSubscriptions => getMapsSubscriptions(sender())
    case e: ListenerRef            => failureAware ! e
  }

  override def receive: Actor.Receive = guardianReceive orElse super.receive

  private def setShardMap(s: CurrentShardSnapshot): Unit = {
    logger.debug(s"Guardian setting shardmap for ref ${s.ref}")
    shardMappers(s.ref) = s.map
  }

  private def getMapsSubscriptions(requestor: ActorRef): Unit =
    requestor ! MapsAndSubscriptions(shardMappers, subscriptions)

  private def shardEvent(e: ShardEvent): Unit = {
    logger.debug(s"Updating shard mapper for ref ${e.ref} with event $e")
    for {
      map <- shardMappers.get(e.ref)
      if map.updateFromEvent(e).isSuccess
    } shardMappers(e.ref) = map
  }

  /** Idempotent. Cli does not start metrics. */
  private def startKamon(role: ClusterRole, requester: ActorRef): Unit = {
    role match {
      case ClusterRole.Cli =>
      case _ =>
        context.child(TraceLoggerName) getOrElse {
          kamon.Kamon.start()
          val actor = context.actorOf(KamonTraceLogger.props(settings.metrics), TraceLoggerName)
          context watch actor
          kamon.Kamon.tracer.subscribe(actor)
          KamonLogger.start(context.system, settings.metrics)
          requester ! TraceLoggerRef(actor)
        }
    }
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
      val props = NodeCoordinatorActor.props(metaStore, memStore, settings.config)
      context.actorOf(props, CoordinatorName) }

    requester ! CoordinatorRef(actor)
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

    requester ! ClusterSingletonRef(proxy)
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
            memStore: MemStore,
            assignmentStrategy: ShardAssignmentStrategy): Props =
    Props(new NodeGuardian(settings, metaStore, memStore, assignmentStrategy))
}

/** Management and task actions on the local node.
  * INTERNAL API.
  */
object NodeProtocol {

  /** Commands to start a task. */
  sealed trait TaskCommand
  /* Acked on task complete */
  sealed trait TaskAck

  sealed trait CreationCommand extends TaskCommand

  /**
    * @param role the role to assign
    * @param watcher the guardian actor. In Test this can include a probe.
    */
  private[coordinator] final case class CreateClusterSingleton(role: String,
                                                               watcher: Option[ActorRef]
                                                              ) extends CreationCommand

  private[coordinator] final case class CreateTraceLogger(role: ClusterRole) extends CreationCommand
  private[coordinator] case object CreateCoordinator extends CreationCommand

  sealed trait CreationAck extends TaskAck
  private[coordinator] final case class CoordinatorRef(ref: ActorRef) extends CreationAck
  private[coordinator] final case class ClusterSingletonRef(ref: ActorRef) extends CreationAck
  private[coordinator] final case class TraceLoggerRef(ref: ActorRef) extends CreationAck
  private[coordinator] final case class ListenerRef(ref: ActorRef) extends CreationAck

  sealed trait RecoveryCommand extends TaskCommand
  private[coordinator] case object GetShardMapsSubscriptions extends RecoveryCommand

  sealed trait RecoveryAck extends TaskAck
  private[coordinator] final case class MapsAndSubscriptions(shardMaps: MMap[DatasetRef, ShardMapper],
                                                             subscriptions: ShardSubscriptions) extends RecoveryAck

  sealed trait LifecycleCommand
  private[coordinator] case object GracefulShutdown extends LifecycleCommand

  sealed trait LifecycleAck extends TaskAck
  private[coordinator] final case class ShutdownComplete(ref: ActorRef) extends LifecycleAck

  sealed trait StateCommand
  private[filodb] case object ResetState extends StateCommand

  sealed trait StateTaskAck extends TaskAck
  private[filodb] case object StateReset extends StateTaskAck

  /** For watchers aware of specific actor transitions in lifecycle. */
  sealed trait ActorLifecycle extends LifecycleCommand {
    def identity: ActorPath
  }
  private[coordinator] final case class PreStart(identity: ActorPath) extends ActorLifecycle
  private[coordinator] final case class PreRestart(identity: ActorPath, reason: Throwable) extends ActorLifecycle
  private[coordinator] final case class PostStop(identity: ActorPath) extends ActorLifecycle

}
