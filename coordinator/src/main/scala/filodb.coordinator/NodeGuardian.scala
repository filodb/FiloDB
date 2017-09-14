package filodb.coordinator

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.pattern.{ClusterSingletonManager, ClusterSingletonProxy}

import filodb.core.memstore.MemStore
import filodb.core.store.{ColumnStore, MetaStore}

/** Supervisor for all child actors and their actors on the node. */
final class NodeGuardian(val settings: FilodbSettings,
                         cluster: Cluster,
                         metaStore: MetaStore,
                         memStore: MemStore,
                         columnStore: ColumnStore,
                         assignmentStrategy: ShardAssignmentStrategy
                        ) extends GracefulStopAwareSupervisor {

  import NodeProtocol._, ActorName._
  import context.dispatcher

  private val failureDetector = cluster.failureDetector

  override def preStart(): Unit = {
    //context.system.eventStream.subscribe(self, classOf[DeadLetter]) // todo in separate worker
  }

  override def postStop(): Unit = {
    super.postStop()
    context.child(TraceLoggerName) foreach {
      actor => kamon.Kamon.shutdown()
    }
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preStart()
  }

  def guardianReceive: Actor.Receive = {
    case CreateTraceLogger(role)   => startKamon(role, sender())
    case CreateCoordinator         => createCoordinator(sender())
    case e: CreateClusterSingleton => createProxy(e, sender())
    case e: DeadLetter             =>
  }

  override def receive: Actor.Receive = guardianReceive orElse super.receive

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
      val props = NodeCoordinatorActor.props(metaStore, memStore, columnStore, cluster.selfAddress, settings.config)
      context.actorOf(props, CoordinatorName) }

    requester ! CoordinatorRef(actor)
  }

  /**
    * Creates a singleton NodeClusterActor and returns a proxy ActorRef to it.
    * This should be called on every FiloDB Coordinator/ingestion
    * node. There is only ONE instance per cluster.
    */
  private def createProxy(e: CreateClusterSingleton, requester: ActorRef): Unit = {
    if (e.withManager && context.child(SingletonMgrName).isEmpty) {
      val mgr = context.actorOf(ClusterSingletonManager.props(
        singletonProps = NodeClusterActor.props(settings, cluster, e.role, metaStore, assignmentStrategy),
        singletonName = NodeClusterName,
        terminationMessage = PoisonPill,
        role = Some(e.role)),
        name = SingletonMgrName)

      context watch mgr
      logger.info(s"Created ClusterSingletonManager for NodeClusterActor [mgr=$mgr, role=${e.role}]")
    }

    requester ! ClusterSingletonRef(clusterActor(e.role))
  }

  /** Returns reference to the cluster actor. The proxy
    * can be started on every node where the singleton needs to be reached.
    *
    * @param role the cluster role
    */
  private def clusterActor(role: String): ActorRef = {
    val proxy = context.actorOf(ClusterSingletonProxy.props(
      singletonPath = ClusterSingletonProxyPath,
      role = Some(role)),
      name = NodeClusterProxyName)

    context watch proxy
    logger.info(s"Created ClusterSingletonProxy $proxy for [role=$role, path=$ClusterSingletonProxyPath")
    proxy
  }

  private def onDeadLetter(e: DeadLetter): Unit = {
    logger.warn(s"Received $e") // TODO in a worker, handle no data loss etc
  }
}

private[filodb] object NodeGuardian {

  def props(settings: FilodbSettings,
            cluster: Cluster,
            metaStore: MetaStore,
            memStore: MemStore,
            columnStore: ColumnStore,
            assignmentStrategy: ShardAssignmentStrategy): Props =
    Props(new NodeGuardian(settings, cluster, metaStore, memStore, columnStore, assignmentStrategy))
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
    * @param withManager if `true` creates the ClusterSingletonManager as well, if `false` only creates the proxy
    */
  private[coordinator] final case class CreateClusterSingleton(role: String, withManager: Boolean)
      extends CreationCommand
  private[coordinator] final case class CreateTraceLogger(role: ClusterRole) extends CreationCommand
  private[coordinator] case object CreateCoordinator extends CreationCommand

  sealed trait CreationAck extends TaskAck
  private[coordinator] final case class CoordinatorRef(ref: ActorRef) extends CreationAck
  private[coordinator] final case class ClusterSingletonRef(ref: ActorRef) extends CreationAck
  private[coordinator] final case class TraceLoggerRef(ref: ActorRef) extends CreationAck


  sealed trait LifecycleCommand
  private[coordinator] case object GracefulShutdown extends LifecycleCommand

  sealed trait LifecycleAck extends TaskAck
  private[coordinator] final case class ShutdownComplete(ref: ActorRef) extends LifecycleAck

  sealed trait StateCommand
  private[filodb] case object ResetState extends LifecycleCommand

  sealed trait StateTaskAck extends TaskAck
  private[filodb] case object StateReset extends StateTaskAck
}
