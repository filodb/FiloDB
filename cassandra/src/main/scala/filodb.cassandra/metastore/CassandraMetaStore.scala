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

  def initialize(): Future[Response] =
    for { dtResp <- datasetTable.initialize()
          ctResp <- columnTable.initialize() }
    yield { ctResp }

  def clearAllData(): Future[Response] =
    for { dtResp <- datasetTable.clearAll()
          ctResp <- columnTable.clearAll() }
    yield { ctResp }

  def newDataset(dataset: Dataset): Future[Response] =
    datasetTable.createNewDataset(dataset)

  def getDataset(name: String): Future[Dataset] =
    datasetTable.getDataset(name)

  def getAllDatasets(): Future[Seq[String]] =
    datasetTable.getAllDatasets

  def deleteDataset(name: String): Future[Response] =
    datasetTable.deleteDataset(name)

  def insertColumn(column: DataColumn): Future[Response] =
    columnTable.insertColumn(column)

  def getSchema(dataset: String, version: Int): Future[Column.Schema] =
    columnTable.getSchema(dataset, version)

  def shutdown(): Unit = {
    datasetTable.shutdown()
    columnTable.shutdown()
  }
}