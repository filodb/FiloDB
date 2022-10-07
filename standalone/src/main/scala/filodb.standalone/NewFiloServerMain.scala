package filodb.standalone

import scala.concurrent.duration.FiniteDuration

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import net.ceedubs.ficus.Ficus._

import filodb.coordinator._
import filodb.coordinator.v2.{FiloDbClusterDiscovery, NewNodeCoordinatorActor}
import filodb.core.GlobalConfig
import filodb.core.memstore.FiloSchedulers
import filodb.http.FiloHttpServer

object NewFiloServerMain extends StrictLogging {

  def start(): Unit = {
    try {

      val allConfig = GlobalConfig.configToDisableAkkaCluster.withFallback(GlobalConfig.systemConfig)
      val settings = FilodbSettings.initialize(allConfig)

      Kamon.init()

      val system = ActorSystemHolder.createActorSystem("filo-standalone", allConfig)

      lazy val ioPool = Scheduler.io(name = FiloSchedulers.IOSchedName,
        reporter = UncaughtExceptionReporter(
          logger.error("Uncaught Exception in FilodbCluster.ioPool", _)))

      /* Initializes columnStore and metaStore using the factory setting from config. */
      val factory = StoreFactory(settings, ioPool)

      val memStore = factory.memStore

      implicit val discoveryScheduler = Scheduler.computation(name = "cluster-ops")
      val failureDetectInterval = allConfig.as[FiniteDuration]("filodb.cluster-discovery.failure-detection-interval")
      val clusterDiscovery = new FiloDbClusterDiscovery(settings, system, failureDetectInterval)

      val nodeCoordinatorActor = system.actorOf(NewNodeCoordinatorActor.props(memStore,
        clusterDiscovery, settings), "coordinator")

      nodeCoordinatorActor ! NewNodeCoordinatorActor.InitNewNodeCoordinatorActor

      val filoHttpServer = new FiloHttpServer(system, settings)
      filoHttpServer.start(nodeCoordinatorActor, nodeCoordinatorActor, true)

      SimpleProfiler.launch(allConfig.getConfig("filodb.profiler"))
      KamonShutdownHook.registerShutdownHook()

    } catch {
      case e: Exception =>
        logger.error("Error occurred when initializing FiloDB server", e)
    }
  }
}
