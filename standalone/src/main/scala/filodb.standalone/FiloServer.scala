package filodb.standalone

import scala.util.control.NonFatal

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
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
 */
object FiloServer extends FilodbClusterNode with StrictLogging {
  override val role = ClusterRole.Server

  val settings = new FilodbSettings()

  override lazy val system = ActorSystem(systemName, settings.allConfig)

  override lazy val cluster = FilodbCluster(system)

  // Now, initialize any datasets using in memory MetaStore.
  // This is a hack until we are able to use CassandraMetaStore for standalone.  It is also a
  // convenience for users to get up and running quickly without setting up cassandra.
  val client = new LocalClient(coordinatorActor)

  val config = settings.config

  def bootstrap(akkaCluster: Cluster): Unit = {
    val bootstrapper = AkkaBootstrapper(akkaCluster)
    bootstrapper.bootstrap()
    val filoHttpServer = new FiloHttpServer(akkaCluster.system)
    filoHttpServer.start(bootstrapper.getAkkaHttpRoute())
  }

  def main(args: Array[String]): Unit = {
    try {
      import settings._
      cluster.kamonInit(role)
      coordinatorActor
      scala.concurrent.Await.result(metaStore.initialize(), InitializationTimeout)
      bootstrap(cluster.cluster)
      cluster.clusterSingleton(roleName, withManager = true)
      cluster._isInitialized.set(true)

//      settings.DatasetDefinitions.foreach { case (datasetName, datasetConf) =>
//        createDatasetFromConfig(datasetName, datasetConf)
//      }
    } catch {
      // if there is an error in the initialization, we need to fail fast so that the process can be rescheduled
      case NonFatal(e) =>
        logger.error("Could not initialize server", e)
        cluster.cluster.system.terminate()
    }
  }

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

  /** To ensure proper shutdown in case `shutdownAndExit` is not called. */
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = shutdown()
  })
}