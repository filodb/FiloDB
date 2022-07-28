package filodb.standalone

import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Scheduler, UncaughtExceptionReporter}

import filodb.coordinator._
import filodb.coordinator.v2.{FiloDbClusterDiscovery, NewNodeCoordinatorActor}
import filodb.core.GlobalConfig
import filodb.core.memstore.FiloSchedulers
import filodb.core.store.MetaStore
import filodb.http.FiloHttpServer

object NewFiloServerMain extends App with StrictLogging {

  try {
    val allConfig = GlobalConfig.systemConfig
    val settings = FilodbSettings.initialize(allConfig)

    val system = ActorSystem("FiloDB", allConfig)

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

    nodeCoordinatorActor ! NewNodeCoordinatorActor.InitNewNodeCoordinatorActor

    val filoHttpServer = new FiloHttpServer(system, settings)
    filoHttpServer.start(nodeCoordinatorActor, nodeCoordinatorActor)

    SimpleProfiler.launch(allConfig.getConfig("filodb.profiler"))
    KamonShutdownHook.registerShutdownHook()

//    implicit val ec = system.dispatcher
//    system.actorSelection("akka.tcp://FiloDB@127.0.0.1:2552/user/NodeCoordinatorActor")
//      .resolveOne(10.seconds).foreach { ref =>
//        val t = Timeout(60.seconds)
//        val v = Await.result((ref ? GetShardMapScatter(DatasetRef("prometheus")))(t), settings.ResolveActorTimeout)
//        println(v)
//      }
  } catch { case e: Exception =>
    logger.error("Error occurred when initializing FiloDB server", e)
  }
}
