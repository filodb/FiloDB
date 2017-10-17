package filodb.cassandra.metastore

import com.typesafe.config.Config
import scala.concurrent.{ExecutionContext, Future}

import filodb.cassandra.{DefaultFiloSessionProvider, FiloSessionProvider}
import filodb.core._
import filodb.core.metadata.Dataset
import filodb.core.store.MetaStore

/**
 * A class for Cassandra implementation of the MetaStore.
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 * @param filoSessionProvider if provided, a session provider provides a session for the configuration
 */
class CassandraMetaStore(config: Config, filoSessionProvider: Option[FiloSessionProvider] = None)
                        (implicit val ec: ExecutionContext) extends MetaStore {
  private val sessionProvider = filoSessionProvider.getOrElse(new DefaultFiloSessionProvider(config))
  val datasetTable = new DatasetTable(config, sessionProvider)

  val defaultKeySpace = config.getString("keyspace")

  def initialize(): Future[Response] = {
    datasetTable.createKeyspace(datasetTable.keyspace)
    datasetTable.initialize()
  }

  def clearAllData(): Future[Response] =
    datasetTable.clearAll()

  def newDataset(dataset: Dataset): Future[Response] =
    datasetTable.createNewDataset(dataset)

  def getDataset(ref: DatasetRef): Future[Dataset] =
    datasetTable.getDataset(ref)

  def getAllDatasets(database: Option[String]): Future[Seq[DatasetRef]] =
    datasetTable.getAllDatasets(database)

  def deleteDataset(ref: DatasetRef): Future[Response] =
    datasetTable.deleteDataset(ref)

  def shutdown(): Unit = {
    datasetTable.shutdown()
  }
}