package filodb.standalone

import akka.actor.ActorRef
import akka.cluster.Cluster
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration._

import filodb.akkabootstrapper.AkkaBootstrapper
import filodb.coordinator._
import filodb.coordinator.client.LocalClient
import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core.{DatasetRef, GlobalConfig, GlobalScheduler}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.QueryConfig
import filodb.http.{FiloHttpServer, PromQLGrpcServer}

/**
 * FiloServer starts a "standalone" FiloDB server which can ingest and support queries through the Akka
 * API.  It is meant to be used in a cluster.
 *
 * - The servers connect to each other setting up an Akka Cluster.  Seed nodes must be configured.
 * - Ingestion must be started using the CLI and the source configured.  When it starts it does nothing
 *   at the beginning.
 *
 * ## Configuration ##
 * {{{
 *   seed-nodes = ["akka://filo-standalone@hostname_or_ip:2552"]
 *   dataset-definitions {
 *     sample-timeseries {
 *       partition-columns = ["metricName:string", "tags:map"]
 *       data-columns = ["timestamp:long", "value:double"]
 *       row-keys = ["timestamp"]
 *     }
 *   }
 * }}}
 *
 * @param watcher optionally register a watcher `ActorRef` with the `NodeClusterActor` cluster
 *                singleton. Primarily for Multi-JVM tests, but this strategy is used in the
 *                coordinator module in all test types.
 */
class FiloServer(watcher: Option[ActorRef]) extends FilodbClusterNode {

  def this() = this(None)
  def this(watcher: ActorRef) = this(Some(watcher))

  override val role = ClusterRole.Server

  lazy val config = cluster.settings.config

  var filoHttpServer: FiloHttpServer = _
  var promQLGrpcServer: PromQLGrpcServer = _

  // Now, initialize any datasets using in memory MetaStore.
  // This is a hack until we are able to use CassandraMetaStore for standalone.  It is also a
  // convenience for users to get up and running quickly without setting up cassandra.
  val client = new LocalClient(coordinatorActor)

  def bootstrap(akkaCluster: Cluster): AkkaBootstrapper = {
    val bootstrapper = AkkaBootstrapper(akkaCluster)
    bootstrapper.bootstrap()
    bootstrapper
  }

  def start(): Unit = {
    coordinatorActor
    scala.concurrent.Await.result(metaStore.initialize(), cluster.settings.InitializationTimeout)
    val bootstrapper = bootstrap(cluster.cluster)
    val singleton = cluster.clusterSingleton(role, watcher)
    filoHttpServer = new FiloHttpServer(cluster.system, cluster.settings)
    filoHttpServer.start(coordinatorActor, singleton, false, bootstrapper.getAkkaHttpRoute())
    if (config.getBoolean("grpc.start-grpc-service")) {
      // TODO: Remove hardcoding
      val dsRef = DatasetRef("prometheus")
      val queryConfig = QueryConfig(config.getConfig("query"))
      def shardMapper = client.getShardMapper(dsRef, false).get
      client.getShardMapper(dsRef, false) match {
        case Some(_) =>
          val dataset = new Dataset(dsRef.dataset, Schemas.promCounter)
          val planner = new SingleClusterPlanner(dataset, Schemas.global,
            shardMapper,
            earliestRetainedTimestampFn = 0, queryConfig, "raw")
          promQLGrpcServer = new PromQLGrpcServer(_ => planner,
            cluster.settings, GlobalScheduler.globalImplicitScheduler)
          promQLGrpcServer.start()
        case None              =>
          logger.warn("Unable to get shardMapper, not starting gRPC service")
      }
    }

    // Launch the profiler after startup, if configured.
    SimpleProfiler.launch(systemConfig.getConfig("filodb.profiler"))
    KamonShutdownHook.registerShutdownHook()
  }

  override def shutdown(): Unit = {
    if (filoHttpServer != null) {
      filoHttpServer.shutdown(5.seconds) // TODO configure
    }
    if (promQLGrpcServer != null) {
      promQLGrpcServer.stop()
    }
    super.shutdown()
  }

  def shutdownAndExit(code: Int): Unit = {
    shutdown()
    sys.exit(code)
  }
}

object FiloServer extends StrictLogging {
  def main(args: Array[String]): Unit = {
    val allConfig = GlobalConfig.systemConfig
    if (allConfig.getBoolean("filodb.v2-cluster-enabled")) {
      NewFiloServerMain.start()
    } else {
      // TODO can remove once new-startup is tested and proven
      var filoServer: Option[FiloServer] = None
      try {
        filoServer = Some(new FiloServer())
        filoServer.get.start()
      } catch {
        case e: Exception =>
          // if there is an error in the initialization, we need to fail fast so that the process can be rescheduled
          logger.error("Could not start FiloDB server", e)
          if (filoServer.isDefined) {
            filoServer.get.shutdown()
          }
          sys.exit(1)
      }
    }
  }
}
