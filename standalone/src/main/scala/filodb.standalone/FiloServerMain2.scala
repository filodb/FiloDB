package filodb.standalone

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Scheduler, UncaughtExceptionReporter}

import filodb.coordinator._
import filodb.core.{GlobalConfig, GlobalScheduler}
import filodb.core.memstore.FiloSchedulers
import filodb.core.store.MetaStore
import filodb.http.FiloHttpServer

object FiloServerMain2 extends App with StrictLogging {
  val allConfig = GlobalConfig.systemConfig
  val settings = FilodbSettings.initialize(allConfig)

  val system = ActorSystem("System", allConfig)

  implicit lazy val ec = GlobalScheduler.globalImplicitScheduler

  lazy val ioPool = Scheduler.io(name = FiloSchedulers.IOSchedName,
    reporter = UncaughtExceptionReporter(
      logger.error("Uncaught Exception in FilodbCluster.ioPool", _)))

  /** Initializes columnStore and metaStore using the factory setting from config. */
  private val factory = StoreFactory(settings, ioPool)

  val metaStore: MetaStore = factory.metaStore

  val memStore = factory.memStore

  val shardAssignmentStrategy = if (settings.config.getBoolean("shard-manager.enable-k8s-stateful-shard-strategy"))
    new K8sStatefulSetShardAssignmentStrategy else new FixedShardAssignmentStrategy(settings.config)

  val nodeCoordinatorActor = system.actorOf(NewNodeCoordinatorActor.props(memStore, shardAssignmentStrategy, settings),
                                "NodeCoordinatorActor")

//  val filoHttpServer = new FiloHttpServer(system, settings)
//  filoHttpServer.start(nodeCoordinatorActor, singleton, bootstrapper.getAkkaHttpRoute())

  SimpleProfiler.launch(allConfig.getConfig("filodb.profiler"))
  KamonShutdownHook.registerShutdownHook()

}
