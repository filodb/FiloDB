package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.cluster.Cluster
import akka.contrib.pattern.{ClusterSingletonManager, ClusterSingletonProxy}
import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.Scheduler
import scala.concurrent.ExecutionContext

import filodb.core.FutureUtils
import filodb.core.reprojector._
import filodb.core.store.{ColumnStore, ColumnStoreScanner, InMemoryMetaStore, InMemoryColumnStore, MetaStore}
import filodb.core.memstore.TimeSeriesMemStore

object GlobalConfig {
  // Loads the overall configuration in a specific order:
  //  - System properties
  //  - Config file in location specified by filodb.config.file
  //  - filodb-defaults.conf (resource / in jar)
  //  - cluster-reference.conf
  //  - all other reference.conf's
  val systemConfig: Config = {
    val customConfig = sys.props.get("filodb.config.file").orElse(sys.props.get("config.file"))
                                .map { path => ConfigFactory.parseFile(new java.io.File(path)) }
                                .getOrElse(ConfigFactory.empty)
    // Don't ask me why, but ConfigFactory.parseResources() does NOT work in Spark 1.4.1 executors
    // and only the below works.
    val defaultsFromUrl = ConfigFactory.parseURL(getClass.getResource("/filodb-defaults.conf"))
    val clusterFromUrl = ConfigFactory.parseURL(getClass.getResource("/cluster-reference.conf"))
    ConfigFactory.defaultOverrides.withFallback(customConfig)
                 .withFallback(defaultsFromUrl)
                 .withFallback(clusterFromUrl)
                 .withFallback(ConfigFactory.defaultReference)
                 .resolve()
  }
}

/**
 * A trait to make setup of the `NodeCoordinatorActor` stack a bit easier.
 * Mixed in for tests as well as the main FiloDB app and anywhere else the stack needs to be spun up.
 */
trait CoordinatorSetup {
  def system: ActorSystem

  // The global Filo configuration object.  Should be systemConfig.getConfig("filodb")
  def config: Config

  // NOTE: should only be called once.  It's up to the classes inheriting this to protect themselves.
  // This is automatically done by FiloSetup, for example.
  def kamonInit(): Unit = {
    kamon.Kamon.start()
    val traceLogger = system.actorOf(KamonTraceLogger.props(config))
    kamon.Kamon.tracer.subscribe(traceLogger)
    KamonLogger.start(system, config.getConfig("metrics-logger"))
  }

  lazy val systemConfig: Config = GlobalConfig.systemConfig

  lazy val threadPool = FutureUtils.getBoundedTPE(config.getInt("core-futures-queue-length"),
                                                  "filodb.core",
                                                  config.getInt("core-futures-pool-size"),
                                                  config.getInt("core-futures-max-pool-size"))

  implicit lazy val ec = Scheduler(ExecutionContext.fromExecutorService(threadPool) : ExecutionContext)

  // A separate ExecutionContext can optionally be used for reads, to control read task queue length
  // separately perhaps.  I have found this is not really necessary.  This was originally created to
  // help decongest heavy write workloads, but a better design has been the throttling of reprojection
  // by limiting number of segments flushed at once (see the use of foldLeftSequentially in Reprojector).
  lazy val readEc = ec

  // These should be implemented as lazy val's, though tests might want to reset them
  def columnStore: ColumnStore with ColumnStoreScanner
  def metaStore: MetaStore
  lazy val stateCache = new SegmentStateCache(config, columnStore)
  lazy val memStore = new TimeSeriesMemStore(config)

  // TODO: consider having a root actor supervising everything
  lazy val coordinatorActor =
    system.actorOf(NodeCoordinatorActor.props(metaStore, memStore, columnStore, config),
                   "coordinator")

  lazy val cluster = Cluster(system)
  lazy val assignmentStrategy = new DefaultShardAssignmentStrategy

  // Creates a singleton NodeClusterActor via ClusterSingletonManager and returns a proxy ActorRef to it
  // This should be called on every FiloDB Coordinator/ingestion node
  def singletonClusterActor(role: String): ActorRef = {
    val mgr = system.actorOf(ClusterSingletonManager.props(
                singletonProps = NodeClusterActor.props(cluster, role, metaStore, assignmentStrategy),
                singletonName = "nodecluster",
                terminationMessage = PoisonPill,
                role = Some(role)),
                name = "singleton")
    getClusterActor(role)
  }

  // This can be called by clients to get the handle to the cluster actor
  def getClusterActor(role: String): ActorRef =
    system.actorOf(ClusterSingletonProxy.props(
      singletonPath = "/user/singleton/nodecluster",
      role = Some(role)),
      name = "nodeClusterProxy")

  def shutdown(): Unit = {
    try {
      system.shutdown()
      columnStore.shutdown()
      metaStore.shutdown()
      // Important: shut down executioncontext as well
      threadPool.shutdown()
      kamon.Kamon.shutdown()
    } catch {
      case e: Exception =>
        system.shutdown()
        threadPool.shutdown()
    }
  }
}

/**
 * This is an optional method for Spark and other FiloDB apps to reuse the same config loading
 * mechanism provided by FiloDB, instead of using the default Typesafe config loading.  To use this
 * pass -Dkamon.config-provider=filodb.coordinator.KamonConfigProvider
 */
class KamonConfigProvider extends kamon.ConfigProvider {
  def config: Config = GlobalConfig.systemConfig
}

/**
 * A StoreFactory creates instances of ColumnStore and MetaStore one time.  The columnStore and metaStore
 * methods should return that created instance every time, not create a new instance.  The implementation
 * should be a class that is passed a single parameter, the CoordinatorSetup instance with config,
 * ExecutionContext, etc.
 */
trait StoreFactory {
  def columnStore: ColumnStore with ColumnStoreScanner
  def metaStore: MetaStore
}

class InMemoryStoreFactory(setup: CoordinatorSetup) extends StoreFactory {
  import setup.ec

  val columnStore = new InMemoryColumnStore(setup.readEc)
  val metaStore = SingleJvmInMemoryStore.metaStore
}

// TODO: make the InMemoryMetaStore either distributed (using clustering to forward and distribute updates)
// or, perhaps modify NodeCoordinator to not need metastore.
object SingleJvmInMemoryStore {
  import scala.concurrent.ExecutionContext.Implicits.global
  lazy val metaStore = new InMemoryMetaStore
}

/**
 * A CoordinatorSetup which initializes columnStore and metaStore using the factory setting from config
 */
trait CoordinatorSetupWithFactory extends CoordinatorSetup {
  lazy val factory = config.getString("store") match {
    case "in-memory" => new InMemoryStoreFactory(this)
    case className: String =>
      val ctor = Class.forName(className).getConstructors.head
      ctor.newInstance(this).asInstanceOf[StoreFactory]
  }

  def columnStore: ColumnStore with ColumnStoreScanner = factory.columnStore
  def metaStore: MetaStore = factory.metaStore
}