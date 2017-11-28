package filodb.coordinator

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.Await

import akka.actor._
import akka.cluster.{Cluster, ClusterEvent}
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import monix.execution.misc.NonFatal
import monix.execution.Scheduler

import filodb.core.store.MetaStore

/** The base Coordinator Extension implementation providing standard ActorSystem startup.
  * The coordinator module is responsible cluster coordination and node membership information.
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
final class FilodbCluster(system: ExtendedActorSystem) extends Extension with StrictLogging {

  import NodeProtocol._
  import ActorName.{NodeGuardianName => guardianName}
  import akka.pattern.ask

  val settings = new FilodbSettings(system.settings.config)
  import settings._

  implicit lazy val timeout: Timeout = DefaultTaskTimeout

  private val _isTerminated = new AtomicBoolean(false)

  private val _isTerminating = new AtomicBoolean(false)

  private[filodb] val _isInitialized = new AtomicBoolean(false)

  private[filodb] val _isJoined = new AtomicBoolean(false)

  private val _coordinatorActor = new AtomicReference[Option[ActorRef]](None)

  private val _clusterActor = new AtomicReference[Option[ActorRef]](None)

  private[filodb] lazy val cluster = {
    val _cluster = Cluster(system)
    logger.info(s"Cluster node starting on ${_cluster.selfAddress}")
    _cluster
  }

  lazy val selfAddress = cluster.selfAddress

  /** The address including a `uid` of this cluster member. */
  lazy val selfUniqueAddress = cluster.selfUniqueAddress

  implicit lazy val ec = Scheduler.Implicits.global
  lazy val ioPool = Scheduler.io(IOPoolName)

  /** Initializes columnStore and metaStore using the factory setting from config. */
  private lazy val factory = StoreFactory(settings, ioPool)

  lazy val metaStore: MetaStore = factory.metaStore

  lazy val memStore = factory.memStore

  lazy val assignmentStrategy = new DefaultShardAssignmentStrategy

  /** The supervisor creates nothing unless specific tasks are requested of it.
    * All actions are idempotent. It manages the underlying lifecycle of all node actors.
    */
  private lazy val guardian = system.actorOf(NodeGuardian.props(
    this, cluster, metaStore, memStore, assignmentStrategy), guardianName)

  /** Idempotent. */
  def kamonInit(role: ClusterRole): ActorRef =
    Await.result((guardian ? CreateTraceLogger(role)).mapTo[TraceLoggerRef], DefaultTaskTimeout).ref

  /** Join the cluster using the cluster selfAddress. Idempotent.
    * INTERNAL API.
    */
  def join(): Unit = cluster join selfAddress

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
  def joinSeedNodes(): Unit = {
    val address = SeedNodes.map(AddressFromURIString.apply)
    logger.debug(s"Attempting to join cluster with address $address")
    cluster.joinSeedNodes(address)
  }

  def coordinatorActor: ActorRef = _coordinatorActor.get.getOrElse {
    val actor = Await.result((guardian ? CreateCoordinator).mapTo[CoordinatorRef], DefaultTaskTimeout).ref
    logger.info(s"NodeCoordinatorActor created: $actor")
    actor
  }

  /** All roles but the `Cli` create this actor. `Server` creates
    * it as a val. `Executor` creates it after calling join on cluster.
    * `Driver` creates it after initializing metaStore and all executors.
    */
  def clusterActor: Option[ActorRef] = _clusterActor.get

  /** Returns a singleton proxy reference to the `NodeClusterActor`.
    * Only one will exist per cluster. This should be called on every FiloDB
    * Coordinator/ingestion node. The proxy can be started on every node where
    * the singleton needs to be reached. If `withManager` is true, additionally
    * creates a ClusterSingletonManager.
    *
    * Idempotent.
    *
    * @param role the [[NodeRoleAwareConfiguration.roleName]]
    *
    * @param withManager depending on the [[ClusterRole]], whether or not to create
    *                    the [[akka.cluster.singleton.ClusterSingletonManager]]
    *                    when creating the [[akka.cluster.singleton.ClusterSingletonProxy]]
    */
  private[filodb] def clusterSingleton(role: String, withManager: Boolean, watcher: Option[ActorRef] = None): ActorRef =
    _clusterActor.get.getOrElse {
      logger.info(s"Creating clusterActor for role '$role'")
      val e = CreateClusterSingleton(role, withManager, watcher)
      val actor = Await.result((guardian ? e).mapTo[ClusterSingletonRef], DefaultTaskTimeout).ref
      _clusterActor.set(Some(actor))
      actor
    }

  /** Current snapshot state of the cluster. */
  def state: ClusterEvent.CurrentClusterState = cluster.state

  def isInitialized: Boolean = _isInitialized.get

  def isJoined: Boolean = _isJoined.get

  def isTerminated: Boolean = _isTerminated.get

  /** Returns true if the node termination is in progress. This is
    * used during evaluation of DeathWatch Terminated events and actions
    * based on that state.
    */
  def isTerminating: Boolean = _isTerminating.get

  /** Idempotent. */
  def shutdown(): Unit = {
    if (_isTerminated.compareAndSet(false, true)) {
      import NodeProtocol.GracefulShutdown
      import akka.pattern.gracefulStop


      _isTerminating.set(true)
      logger.info("Starting shutdown")
      _isJoined.set(false)
      _isInitialized.set(false)

      try {
        cluster.leave(selfAddress)
        Await.result(gracefulStop(guardian, GracefulStopTimeout, GracefulShutdown), GracefulStopTimeout)
        system.terminate foreach { _ =>
          logger.info("Actor system was shut down")
        }

        metaStore.shutdown()
        memStore.shutdown()
        ioPool.shutdown()
      } catch { case NonFatal(e) =>
        system.terminate()
        ioPool.shutdown()
      } finally {
        _isJoined.set(false)
        _isTerminated.set(true)
      }
    }
  }
}
