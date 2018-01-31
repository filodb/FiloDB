package filodb.coordinator

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.immutable
import scala.concurrent.Await

import akka.actor._
import akka.cluster._
import akka.cluster.ClusterEvent._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.execution.misc.NonFatal

import filodb.core.store.MetaStore

/** The base Coordinator Extension implementation providing standard ActorSystem startup.
  * The coordinator module is responsible for cluster coordination and node membership information.
  * Changes to the cluster are events that can be subscribed to.
  * Commands to operate the cluster for managmement are provided based on role/authorization.
  *
  * Provides a compute scheduler (ec) and one for I/O operations.
  */
object FilodbCluster extends ExtensionId[FilodbCluster] with ExtensionIdProvider {
  override def get(system: ActorSystem): FilodbCluster = super.get(system)
  override def lookup: ExtensionId[_ <: Extension] = FilodbCluster
  override def createExtension(system: ExtendedActorSystem): FilodbCluster = new FilodbCluster(system)
}

/**
  * Coordinator Extension Id and factory for creating a basic Coordinator extension.
  */
final class FilodbCluster(val system: ExtendedActorSystem) extends Extension with StrictLogging {

  import ActorName.{NodeGuardianName => guardianName}
  import NodeProtocol._
  import akka.pattern.ask

  val settings = new FilodbSettings(system.settings.config)
  import settings._

  implicit lazy val timeout: Timeout = DefaultTaskTimeout

  private[coordinator] val _isInitialized = new AtomicBoolean(false)

  private val _isJoined = new AtomicBoolean(false)

  private val _isTerminated = new AtomicBoolean(false)

  private val _cluster = new AtomicReference[Option[Cluster]](None)

  private val _coordinatorActor = new AtomicReference[Option[ActorRef]](None)

  private val _clusterActor = new AtomicReference[Option[ActorRef]](None)

  implicit lazy val ec = Scheduler.Implicits.global
  lazy val ioPool = Scheduler.io(IOPoolName)

  /** Initializes columnStore and metaStore using the factory setting from config. */
  private lazy val factory = StoreFactory(settings, ioPool)

  lazy val metaStore: MetaStore = factory.metaStore

  lazy val memStore = factory.memStore

  /** The supervisor creates nothing unless specific tasks are requested of it.
    * All actions are idempotent. It manages the underlying lifecycle of all node actors.
    */
  private[coordinator] lazy val guardian = system.actorOf(NodeGuardian.props(
    settings, metaStore, memStore, DefaultShardAssignmentStrategy), guardianName)

  def isInitialized: Boolean = _isInitialized.get

  def isTerminated: Boolean = _isTerminated.get

  /** Idempotent. */
  def kamonInit(role: ClusterRole): ActorRef =
    Await.result((guardian ? CreateTraceLogger(role)).mapTo[TraceLoggerRef], DefaultTaskTimeout).identity

  def coordinatorActor: ActorRef = _coordinatorActor.get.getOrElse {
    val actor = Await.result((guardian ? CreateCoordinator).mapTo[CoordinatorIdentity], DefaultTaskTimeout).identity
    logger.info(s"NodeCoordinatorActor created: $actor")
    actor
  }

  def cluster: Cluster = _cluster.get.getOrElse {
    val c = Cluster(system)
    _cluster.set(Some(c))
    c.registerOnMemberUp(startListener())
    c
  }

  def selfAddress: Address = cluster.selfAddress

  /** The address including a `uid` of this cluster member. */
  def selfUniqueAddress: UniqueAddress = cluster.selfUniqueAddress

  logger.info(s"Filodb cluster node starting on $selfAddress")

  /** Current snapshot state of the cluster. */
  def state: ClusterEvent.CurrentClusterState = cluster.state

  /** Join the cluster using the cluster selfAddress. Idempotent.
    * INTERNAL API.
    */
  def join(): Unit = join(selfAddress)

  /** Join the cluster using the provided address. Idempotent.
    * Used by drivers or other users.
    * INTERNAL API.
    *
    * @param address the address from a driver to use for joining the cluster.
    *                The driver joins using cluster.selfAddress, executors join
    *                using `spark-driver-addr` configured dynamically during
    *                a driver's initialization.
    */
  def join(address: Address): Unit = cluster join address

  /** Join the cluster using the configured seed nodes. Idempotent.
    * This action ensures the cluster is joined only after the `NodeCoordinatorActor` is created.
    * This is so that when the NodeClusterActor discovers the joined node, it can find the coordinator right away.
    * Used by FiloDB server.
    *
    * This is a static way to join the cluster. For a more dynamic way to join the cluster,
    * see the akka-bootstrapper module.
    *
    * INTERNAL API.
    */
  def joinSeedNodes(providerSeeds: immutable.Seq[Address]): Unit = {
    val seeds = if (providerSeeds.nonEmpty) providerSeeds else SeedNodes.map(AddressFromURIString.apply)
    logger.info(s"Attempting to join cluster with seed nodes $seeds")
    cluster.joinSeedNodes(seeds)
  }

  /** Returns true if self-node has joined the cluster and is MemberStatus.Up.
    * Returns false if local node is removed from the cluster, by graceful leave
    * or failure/unreachable downing.
    */
  def isJoined: Boolean = _isJoined.get

  /** Returns true if the node for the given `address` is unreachable and `Down`. */
  def isUnreachable(address: Address): Boolean = state.unreachable.exists(m =>
    m.address == address && m.status == MemberStatus.Down)

  /** All roles but the `Cli` create this actor. `Server` creates
    * it as a val. `Executor` creates it after calling join on cluster.
    * `Driver` creates it after initializing metaStore and all executors.
    */
  def clusterActor: Option[ActorRef] = _clusterActor.get

  /** Returns a singleton proxy reference to the NodeClusterActor.
    * Only one will exist per cluster. This should be called on every FiloDB
    * Coordinator/ingestion node. The proxy can be started on every node where
    * the singleton needs to be reached. If withManager is true, additionally
    * creates a ClusterSingletonManager.
    *
    * Idempotent.
    *
    * @param role    the FilodbClusterNode.role
    *
    * @param watcher an optional Test watcher
    */
  def clusterSingleton(role: ClusterRole, watcher: Option[ActorRef]): ActorRef =
    _clusterActor.get.getOrElse {
      val e = CreateClusterSingleton(role.roleName, watcher)
      val actor = Await.result((guardian ? e).mapTo[ClusterSingletonIdentity], DefaultTaskTimeout).identity
      _clusterActor.set(Some(actor))
      _isInitialized.set(true)
      actor
    }

  /** Begins node graceful shutdown:
    *   A. Sets `isTerminating` to true and `isJoined` and `isInitialized` to false,
    *   B. Calls `akka.cluster.Cluster.leave(selfAddress)`
    *   C. Awaits the async graceful shutdown of the `filodb.coordinator.NodeGuardian`
    *      supervised child actors via `filodb.coordinator.GracefulStopStrategy`
    *   D. Shuts down `monix.execution.Scheduler.io` and `filodb.core.store.StoreFactory`'s MetaStore and MemStore
    *   E. Calls the async terminate on `akka.actor.ActorSystem`
    *
    * Idempotent.
    */
  protected[coordinator] def shutdown(): Unit =
    if (!isTerminated) {
      import akka.pattern.gracefulStop
      import NodeProtocol.GracefulShutdown

      logger.info("Leaving the cluster.")
      _cluster.get foreach (_.leave(selfAddress))

      _isInitialized.set(false)
      logger.info("Terminating: starting shutdown")

      try {
        Await.result(gracefulStop(guardian, GracefulStopTimeout, GracefulShutdown), GracefulStopTimeout)
        system.terminate foreach { _ =>
          logger.info("Actor system was shut down")
        }
        metaStore.shutdown()
        memStore.shutdown()
        ioPool.shutdown()
      } catch {
        case NonFatal(e) =>
          system.terminate()
          ioPool.shutdown()
      }
      finally _isTerminated.set(true)
    }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = shutdown()
  })

  /** For usage when the `akka.cluster.Member` is needed in a non-actor.
    * Creates a temporary actor which subscribes to `akka.cluster.MemberRemoved`.
    * for local node to privately set the appropriate node flag internally.
    * Upon receiving sets self-node flag, stops the listener.
    */
  private def startListener(): Unit = {
    cluster.subscribe(system.actorOf(Props(new Actor {
      guardian ! NodeProtocol.ListenerRef(self)

      def receive: Actor.Receive = {
        case e: MemberUp if e.member.address == selfAddress =>
          _isJoined.set(true)
        case e: MemberRemoved if e.member.address == selfAddress =>
          _isJoined.set(false)
          cluster unsubscribe self
          context stop self
      }
    })), InitialStateAsEvents, classOf[MemberUp], classOf[MemberRemoved])
  }

}

/** Mixin for easy usage of the FiloDBCluster Extension.
  * Used by all `ClusterRole` nodes starting an ActorSystem and FiloDB Cluster nodes.
  */
private[filodb] trait FilodbClusterNode extends NodeConfiguration with StrictLogging {

  def role: ClusterRole

  /** Override to pass in additional module config. */
  protected lazy val roleConfig: Config = ConfigFactory.empty

  /** The `ActorSystem` used to create the FilodbCluster Akka Extension. */
  final lazy val system = {
    val allConfig = roleConfig.withFallback(role match {
      case ClusterRole.Cli => ConfigFactory.empty
      case _ => ConfigFactory.parseString(s"""akka.cluster.roles=["${role.roleName}"]""")
    }).withFallback(systemConfig)

    ActorSystem(role.systemName, allConfig)
  }

  lazy val cluster = FilodbCluster(system)

  implicit lazy val ec = cluster.ec

  lazy val metaStore: MetaStore = cluster.metaStore

  /** If `role` is `ClusterRole.Cli`, the `FilodbCluster` `isInitialized`
    * flag is set here, on creation of the `NodeCoordinatorActor`. All other
    * roles are marked as initialized after `NodeClusterActor` is created.
    */
  lazy val coordinatorActor: ActorRef = {
    val actor = cluster.coordinatorActor
    role match {
      case ClusterRole.Cli if actor != Actor.noSender =>
        cluster._isInitialized.set(true)
      case _ =>
    }
    actor
  }

  /** Returns a singleton proxy reference to the `NodeClusterActor`. */
  def clusterSingleton(role: ClusterRole, watcher: Option[ActorRef]): ActorRef =
    cluster.clusterSingleton(role, watcher)

  /** Only calls `akka.cluster.leave` akka.cluster.Cluster was called by user. */
  def shutdown(): Unit = cluster.shutdown()

}
