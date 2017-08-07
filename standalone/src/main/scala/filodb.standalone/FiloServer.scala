package filodb.standalone

import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._

import filodb.coordinator._
import filodb.coordinator.client.LocalClient
import filodb.core.metadata.{Column, DataColumn, Dataset}

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
 *       string-columns = ["metric-name"]
 *       double-columns = ["metric-value"]
 *       long-columns = ["timestamp"]
 *       int-columns = []
 *
 *       partition-keys = ["metric-name"]
 *       row-keys = ["timestamp"]
 *     }
 *   }
 * }}}
 */
object FiloServer extends FilodbClusterNode with StrictLogging {
  import Column.ColumnType._

  override val role = ClusterRole.Server

  val settings = new FilodbSettings()

  override lazy val system = ActorSystem(systemName, settings.allConfig)

  override lazy val cluster = FilodbCluster(system)

  // Now, initialize any datasets using in memory MetaStore.
  // This is a hack until we are able to use CassandraMetaStore for standalone.  It is also a
  // convenience for users to get up and running quickly without setting up cassandra.
  val client = new LocalClient(coordinatorActor)

  val config = settings.config

  def main(args: Array[String]): Unit = {
    import settings._
    cluster.kamonInit(role)
    coordinatorActor
    scala.concurrent.Await.result(metaStore.initialize(), InitializationTimeout)
    cluster.joinSeedNodes()
    cluster.clusterSingletonProxy(roleName, withManager = true)
    cluster._isInitialized.set(true)

    settings.DatasetDefinitions.foreach { case (datasetName, datasetConf) =>
      createDatasetFromConfig(datasetName, datasetConf)
    }
  }

  def createDatasetFromConfig(datasetName: String, config: Config): Unit = {
    val partKeys = config.as[Seq[String]]("partition-keys")
    val rowKeys = config.as[Seq[String]]("row-keys")

    val columns =
      config.as[Seq[String]]("string-columns").map(n => DataColumn(0, n, datasetName, 0, StringColumn)) ++
      config.as[Seq[String]]("double-columns").map(n => DataColumn(0, n, datasetName, 0, DoubleColumn)) ++
      config.as[Seq[String]]("long-columns").map(n => DataColumn(0, n, datasetName, 0, LongColumn)) ++
      config.as[Seq[String]]("int-columns").map(n => DataColumn(0, n, datasetName, 0, IntColumn)) ++
      config.as[Seq[String]]("map-columns").map(n => DataColumn(0, n, datasetName, 0, MapColumn))

    val dataset = Dataset(datasetName, rowKeys, ":string 0", partKeys)
    logger.info(s"Creating dataset $dataset with columns $columns...")
    client.createNewDataset(dataset, columns)
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