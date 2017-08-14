package filodb.coordinator

import scala.concurrent.Future

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.pattern.{ClusterSingletonManager, ClusterSingletonProxy}

import filodb.core.memstore.MemStore
import filodb.core.store.{ColumnStore, MetaStore}

/** Supervisor for all child actors and their actors on the node. */
final class NodeGuardian(settings: FilodbSettings,
                         cluster: Cluster,
                         metaStore: MetaStore,
                         memStore: MemStore,
                         columnStore: ColumnStore,
                         assignmentStrategy: ShardAssignmentStrategy
                        ) extends BaseActor {

  import NodeGuardian._, NodeProtocol._
  import context.dispatcher

  private var gracefulShutdownStarted = false

  override def preStart(): Unit = {
    //context.system.eventStream.subscribe(self, classOf[DeadLetter]) // todo in separate worker
  }

  override def postStop(): Unit = {
    super.postStop()
    // shut down kamon if started
    context.child(TraceLoggerName) foreach {
      actor => kamon.Kamon.shutdown()
    }
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preStart()
  }

  override def receive: Actor.Receive = {
    case CreateTraceLogger(role)   => startKamon(role, sender())
    case CreateCoordinator         => createCoordinator(sender())
    case e: CreateClusterSingleton => createProxy(e, sender())
    case Terminated(actor)         => terminated(actor)
    case e: DeadLetter             =>
    case GracefulShutdown          => gracefulShutdown(sender())
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

  /** Idempotent. */
  private def createCoordinator(requester: ActorRef): Unit = {
    val ref = context.child(CoordinatorName) getOrElse {
      val actor = context.actorOf(NodeCoordinatorActor.props(
        metaStore, memStore, columnStore, cluster.selfAddress, settings.config), CoordinatorName)
      context watch actor
      actor
    }

    requester ! CoordinatorRef(ref)
  }

  /**
    * Creates a singleton NodeClusterActor and returns a proxy ActorRef to it.
    * This should be called on every FiloDB Coordinator/ingestion
    * node. There is only ONE instance per cluster.
    */
  private def createProxy(e: CreateClusterSingleton, requester: ActorRef): Unit = {
    if (e.withManager && context.child(SingletonMgrName).isEmpty) {
      val mgr = context.actorOf(ClusterSingletonManager.props(
        singletonProps = NodeClusterActor.props(cluster, e.role, metaStore, assignmentStrategy),
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

  private def terminated(actor: ActorRef): Unit = {
    val message = s"$actor terminated."
    if (gracefulShutdownStarted) logger.info(message) else logger.warn(message)
  }

  private def gracefulShutdown(requester: ActorRef): Unit = {
    import akka.pattern.gracefulStop
    import akka.pattern.pipe
    import settings.GracefulStopTimeout

    gracefulShutdownStarted = true
    logger.info("Starting graceful shutdown.")

    Future
      .sequence(context.children.map(gracefulStop(_, GracefulStopTimeout, PoisonPill)))
      .map(f => ShutdownComplete(self))
      .pipeTo(requester)
  }
}

private[filodb] object NodeGuardian {

  /* Actor Path names */
  val NodeGuardianName = "node"
  val CoordinatorName = "coordinator"
  val TraceLoggerName = "trace-logger"
  /* The actor name of the child singleton actor */
  val NodeClusterName = "nodecluster"
  val SingletonMgrName = "singleton"
  val NodeClusterProxyName = "nodeClusterProxy"

  /* Actor Paths */
  val ClusterSingletonProxyPath = s"/user/$NodeGuardianName/$SingletonMgrName/$NodeClusterName"

  def nodeCoordinatorPath(addr: Address): ActorPath =
    RootActorPath(addr) / "user" / NodeGuardianName / CoordinatorName

  def props(settings: FilodbSettings,
            cluster: Cluster,
            metaStore: MetaStore,
            memStore: MemStore,
            columnStore: ColumnStore,
            assignmentStrategy: ShardAssignmentStrategy): Props =
    Props(new NodeGuardian(settings, cluster, metaStore, memStore, columnStore, assignmentStrategy))
}

private[coordinator] object NodeProtocol {

  sealed trait TaskCommand
  final case class CreateTraceLogger(role: ClusterRole) extends TaskCommand
  case object CreateCoordinator extends TaskCommand

  /**
    * @param role the role to assign
    * @param withManager if `true` creates the ClusterSingletonManager as well, if `false` only creates the proxy
    */
  final case class CreateClusterSingleton(role: String, withManager: Boolean) extends TaskCommand

  case object GracefulShutdown extends TaskCommand

  sealed trait NodeAck
  final case class CoordinatorRef(ref: ActorRef) extends NodeAck
  final case class ClusterSingletonRef(ref: ActorRef) extends NodeAck
  final case class TraceLoggerRef(ref: ActorRef) extends NodeAck
  final case class ShutdownComplete(ref: ActorRef) extends NodeAck
}
