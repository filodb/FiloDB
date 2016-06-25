package filodb.coordinator

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.ExecutionContext

import filodb.core.FutureUtils
import filodb.core.reprojector._
import filodb.core.store.{ColumnStore, MetaStore}

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
  }
}

/**
 * A trait to make setup of the [[NodeCoordinatorActor]] stack a bit easier.
 * Mixed in for tests as well as the main FiloDB app and anywhere else the stack needs to be spun up.
 */
trait CoordinatorSetup {
  def system: ActorSystem

  // The global Filo configuration object.  Should be systemConfig.getConfig("filodb")
  def config: Config

  def kamonInit(): Unit = {
    kamon.Kamon.start()
    val traceLogger = system.actorOf(KamonTraceLogger.props(config))
    kamon.Kamon.tracer.subscribe(traceLogger)
  }

  lazy val systemConfig: Config = GlobalConfig.systemConfig

  lazy val threadPool = FutureUtils.getBoundedTPE(config.getInt("core-futures-queue-length"),
                                                  "filodb.core",
                                                  config.getInt("core-futures-pool-size"),
                                                  config.getInt("core-futures-max-pool-size"))

  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

  // A separate ExecutionContext can optionally be used for reads, to control read task queue length
  // separately perhaps.  I have found this is not really necessary.  This was originally created to
  // help decongest heavy write workloads, but a better design has been the throttling of reprojection
  // by limiting number of segments flushed at once (see the use of foldLeftSequentially in Reprojector).
  lazy val readEc = ec

  // These should be implemented as lazy val's, though tests might want to reset them
  val columnStore: ColumnStore
  val metaStore: MetaStore
  lazy val reprojector = new DefaultReprojector(config, columnStore)

  // TODO: consider having a root actor supervising everything
  lazy val coordinatorActor =
    system.actorOf(NodeCoordinatorActor.props(metaStore, reprojector, columnStore, config),
                   "coordinator")

  lazy val cluster = Cluster(system)

  def shutdown(): Unit = {
    system.shutdown()
    columnStore.shutdown()
    metaStore.shutdown()
    // Important: shut down executioncontext as well
    threadPool.shutdown()
    kamon.Kamon.shutdown()
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
