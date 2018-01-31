package filodb.standalone

import scala.util.control.NonFatal

import akka.actor.ActorRef
import akka.cluster.Cluster
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import filodb.akkabootstrapper.AkkaBootstrapper
import filodb.coordinator._
import filodb.coordinator.client.LocalClient
import filodb.core.metadata.Dataset
import filodb.http.FiloHttpServer

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
 *   seed-nodes = ["akka.tcp://filo-standalone@hostname_or_ip:2552"]
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

  // Now, initialize any datasets using in memory MetaStore.
  // This is a hack until we are able to use CassandraMetaStore for standalone.  It is also a
  // convenience for users to get up and running quickly without setting up cassandra.
  val client = new LocalClient(coordinatorActor)

  def bootstrap(akkaCluster: Cluster): AkkaBootstrapper = {
    val bootstrapper = AkkaBootstrapper(akkaCluster)
    val seeds = bootstrapper.bootstrap()
    bootstrapper
  }

  def start(): Unit = {
    try {
      cluster.kamonInit(role)
      coordinatorActor
      scala.concurrent.Await.result(metaStore.initialize(), cluster.settings.InitializationTimeout)
      val bootstrapper = bootstrap(cluster.cluster)
      val singleton = cluster.clusterSingleton(role, watcher)
      val filoHttpServer = new FiloHttpServer(cluster.system)
      filoHttpServer.start(coordinatorActor, singleton, bootstrapper.getAkkaHttpRoute())
    } catch {
      // if there is an error in the initialization, we need to fail fast so that the process can be rescheduled
      case NonFatal(e) =>
        logger.error("Could not initialize server", e)
        shutdown()
    }
  }

  // Design: unused - why is it here still? why was FiloServer made a class and main separate?
  def createDatasetFromConfig(datasetName: String, config: Config): Unit = {
    val partColumns = config.as[Seq[String]]("partition-columns")
    val dataColumns = config.as[Seq[String]]("data-columns")
    val rowKeys = config.as[Seq[String]]("row-keys")

    val dataset = Dataset(datasetName, partColumns, dataColumns, rowKeys)
    logger.info(s"Created dataset $dataset...")
    client.createNewDataset(dataset)
  }

  // NOTE: user must watch for ingestion manually using CLI and logs

  def shutdownAndExit(code: Int): Unit = {
    shutdown()
    sys.exit(code)
  }
}

object FiloServer {
  def main(args: Array[String]): Unit =
    new FiloServer().start()
}