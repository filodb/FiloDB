package filodb.standalone

import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import net.ceedubs.ficus.Ficus._

import filodb.coordinator._
import filodb.coordinator.client.LocalClient
import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.coordinator.v2.{FiloDbClusterDiscovery, NewNodeCoordinatorActor}
import filodb.core.{DatasetRef, GlobalConfig, GlobalScheduler}
import filodb.core.memstore.FiloSchedulers
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.QueryConfig
import filodb.http.{FiloHttpServer, PromQLGrpcServer}

object NewFiloServerMain extends StrictLogging {

  def start(): Unit = {
    try {

      val allConfig = GlobalConfig.systemConfig
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
      startGrpcServer(settings, nodeCoordinatorActor)
      SimpleProfiler.launch(allConfig.getConfig("filodb.profiler"))
      KamonShutdownHook.registerShutdownHook()

    } catch {
      case e: Exception =>
        logger.error("Error occurred when initializing FiloDB server", e)
    }
  }

  private def startGrpcServer(settings: FilodbSettings, coordinatorActor: ActorRef): Unit = {
      if (settings.config.getBoolean("grpc.start-grpc-service")) {
        val client = new LocalClient(coordinatorActor)
        // TODO: Remove hardcoding
        val dsRef = DatasetRef("prometheus")
        val queryConfig = QueryConfig(settings.config.getConfig("query"))
        def shardMapper = client.getShardMapper(dsRef, true).get
        client.getShardMapper(dsRef, true) match {
          case Some(_) =>
            val planner = new SingleClusterPlanner(new Dataset(dsRef.dataset, Schemas.promCounter), Schemas.global,
              shardMapper,
              earliestRetainedTimestampFn = 0, queryConfig, "raw")
            val promQLGrpcServer = new PromQLGrpcServer(_ => planner, settings,
              GlobalScheduler.globalImplicitScheduler)
            Runtime.getRuntime.addShutdownHook(new Thread() {
              override def run(): Unit = {
                promQLGrpcServer.stop()
              }
            })
            promQLGrpcServer.start()
          case None              =>
            logger.warn("Unable to get shardMapper, not starting gRPC service")
        }
      }
  }
}
