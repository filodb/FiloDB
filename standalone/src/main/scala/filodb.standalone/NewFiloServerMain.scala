package filodb.standalone

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Scheduler, UncaughtExceptionReporter}

import filodb.coordinator._
import filodb.coordinator.v2.{FiloDbClusterDiscovery, NewNodeCoordinatorActor}
import filodb.core.{GlobalConfig, GlobalScheduler}
import filodb.core.memstore.FiloSchedulers
import filodb.core.store.MetaStore

object NewFiloServerMain extends App with StrictLogging {

  try {
    val allConfig = GlobalConfig.systemConfig
    val settings = FilodbSettings.initialize(allConfig)

    val system = ActorSystem("FiloDB", allConfig)

    implicit lazy val ec = GlobalScheduler.globalImplicitScheduler

    lazy val ioPool = Scheduler.io(name = FiloSchedulers.IOSchedName,
      reporter = UncaughtExceptionReporter(
        logger.error("Uncaught Exception in FilodbCluster.ioPool", _)))

    /** Initializes columnStore and metaStore using the factory setting from config. */
    val factory = StoreFactory(settings, ioPool)

    val metaStore: MetaStore = factory.metaStore

    val memStore = factory.memStore

    val clusterDiscovery = new FiloDbClusterDiscovery(settings, system)

    val nodeCoordinatorActor = system.actorOf(NewNodeCoordinatorActor.props(memStore,
      clusterDiscovery, settings), "NodeCoordinatorActor")

    //  val filoHttpServer = new FiloHttpServer(system, settings)
    //  filoHttpServer.start(nodeCoordinatorActor, singleton, bootstrapper.getAkkaHttpRoute())

    SimpleProfiler.launch(allConfig.getConfig("filodb.profiler"))
    KamonShutdownHook.registerShutdownHook()
  } catch { case e: Exception =>
    logger.error("Error occurred when initializing FiloDB server", e)
  }
}
