package filodb.cassandra.metastore

import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}
import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, IngestionStateData}
import filodb.core.store.MetaStore

/**
 * A class for Cassandra implementation of the MetaStore.
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
class CassandraMetaStore(config: Config)
                        (implicit val ec: ExecutionContext) extends MetaStore {
  val datasetTable = new DatasetTable(config)
  val columnTable = new ColumnTable(config)
  val ingestionStateTable = new IngestionStateTable(config)

  def initialize(): Future[Response] = {
    datasetTable.createKeyspace(datasetTable.keyspace)
    for { dtResp <- datasetTable.initialize()
          ctResp <- columnTable.initialize()
          istResp <- ingestionStateTable.initialize()}
    yield { istResp }
  }

  def clearAllData(): Future[Response] =
    for { dtResp <- datasetTable.clearAll()
          ctResp <- columnTable.clearAll()
          istResp <- ingestionStateTable.clearAll()}
    yield { istResp }

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
    ingestionStateTable.shutdown()
  }

  def insertIngestionState(actorAddress: String, dataset: DatasetRef,
                                    state: String, version: Int): Future[Response] =
    ingestionStateTable.insertIngestionState(actorAddress,
                                            dataset.database.getOrElse(""),
                                            dataset.dataset,
                                            version,
                                            state)

  def getAllIngestionEntries(actorPath: String): Future[Seq[IngestionStateData]] = {
    ingestionStateTable.getIngestionStateByNodeActor(actorPath)
  }
}