package filodb.cassandra.metastore

import com.typesafe.config.Config
import scala.concurrent.{ExecutionContext, Future}

import filodb.cassandra.{DefaultFiloSessionProvider, FiloSessionProvider}
import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, IngestionStateData}
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
  val columnTable = new ColumnTable(config, sessionProvider)
  val ingestionStateTable = new IngestionStateTable(config, sessionProvider)

  val defaultKeySpace = config.getString("keyspace")

  def initialize(): Future[Response] = {
    datasetTable.createKeyspace(datasetTable.keyspace)
    for { dtResp <- datasetTable.initialize()
          ctResp <- columnTable.initialize()
          istResp <- ingestionStateTable.initialize() }
    yield { ctResp }
  }

  def clearAllData(): Future[Response] =
    for { dtResp <- datasetTable.clearAll()
          ctResp <- columnTable.clearAll()
          istResp <- ingestionStateTable.clearAll() }
    yield { ctResp }

  def newDataset(dataset: Dataset): Future[Response] =
    datasetTable.createNewDataset(dataset)

  def getDataset(ref: DatasetRef): Future[Dataset] =
    datasetTable.getDataset(ref)

  def getAllDatasets(database: Option[String]): Future[Seq[DatasetRef]] =
    datasetTable.getAllDatasets(database)

  def deleteDataset(ref: DatasetRef): Future[Response] =
    for { dtResp <- datasetTable.deleteDataset(ref)
          ctResp <- columnTable.deleteDataset(ref) }
    yield { ctResp }

  def insertColumns(columns: Seq[DataColumn], ref: DatasetRef): Future[Response] =
    columnTable.insertColumns(columns, ref)

  def getSchema(ref: DatasetRef, version: Int): Future[Column.Schema] =
    columnTable.getSchema(ref, version)

  def shutdown(): Unit = {
    datasetTable.shutdown()
    columnTable.shutdown()
  }

  def insertIngestionState(actorAddress: String, dataset: DatasetRef, columns: String,
                           state: String, version: Int, exceptions: String = ""): Future[Response] =
    ingestionStateTable.insertIngestionState(actorAddress,
                                            dataset.database.getOrElse(defaultKeySpace),
                                            dataset.dataset,
                                            version,
                                            columns,
                                            state)

  def getAllIngestionEntries(actorPath: String): Future[Seq[IngestionStateData]] =
    ingestionStateTable.getIngestionStateByNodeActor(actorPath)

  def updateIngestionState(actorAddress: String, dataset: DatasetRef,
                           state: String, exceptions: String, version: Int ): Future[Response] =
    ingestionStateTable.updateIngestionState(actorAddress,
                                            dataset.database.getOrElse(defaultKeySpace),
                                            dataset.dataset,
                                            state,
                                            exceptions,
                                            version)
}