package filodb.standalone

import akka.actor.{ActorSystem, AddressFromURIString}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.coordinator.client.LocalClient
import filodb.coordinator.CoordinatorSetupWithFactory
import filodb.core.metadata.{Dataset, DataColumn, Column, RichProjection}

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
object FiloServer extends App with CoordinatorSetupWithFactory with StrictLogging {
  import Column.ColumnType._

  val config = systemConfig.getConfig("filodb")
  lazy val system = ActorSystem("filo-standalone", systemConfig)

  kamonInit()
  coordinatorActor
  Await.result(metaStore.initialize(), 60.seconds)

  // initialize Akka Cluster
  // We must join the cluster after the coordinator starts up, so that when the NodeClusterActor discovers
  // the joined node, it can find the coordinator right away
  val seedNodes: collection.immutable.Seq[String] = config.as[Seq[String]]("seed-nodes").toList
  cluster.joinSeedNodes(seedNodes.map(AddressFromURIString.apply))

  // get cluster actor
  val clusterActor = singletonClusterActor("worker")

  logger.info(s"Cluster initialized.")

  // Now, initialize any datasets using in memory MetaStore.
  // This is a hack until we are able to use CassandraMetaStore for standalone.  It is also a
  // convenience for users to get up and running quickly without setting up cassandra.
  val client = new LocalClient(coordinatorActor)
  config.as[Map[String, Config]]("dataset-definitions").foreach { case (datasetName, datasetConf) =>
    createDatasetFromConfig(datasetName, datasetConf)
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
}