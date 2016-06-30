package filodb.cassandra.metastore

import com.typesafe.config.Config
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset}
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

  def initialize(): Future[Response] = {
    datasetTable.createKeyspace(datasetTable.keyspace)
    for { dtResp <- datasetTable.initialize()
          ctResp <- columnTable.initialize() }
    yield { ctResp }
  }

  def clearAllData(): Future[Response] =
    for { dtResp <- datasetTable.clearAll()
          ctResp <- columnTable.clearAll() }
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
}