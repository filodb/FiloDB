package filodb.cassandra.metastore

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.Config

import filodb.cassandra.{DefaultFiloSessionProvider, FiloSessionProvider}
import filodb.core._
import filodb.core.metadata.Dataset
import filodb.core.store.{IngestionConfig, MetaStore}

/**
 * A class for Cassandra implementation of the MetaStore.
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 * @param filoSessionProvider if provided, a session provider provides a session for the configuration
 */
class CassandraMetaStore(config: Config, filoSessionProvider: Option[FiloSessionProvider] = None)
                        (implicit val ec: ExecutionContext) extends MetaStore {
  private val ingestionConsistencyLevel = ConsistencyLevel.valueOf(config.getString("ingestion-consistency-level"))
  private val sessionProvider = filoSessionProvider.getOrElse(new DefaultFiloSessionProvider(config))
  val datasetTable = new DatasetTable(config, sessionProvider)
  val checkpointTable = new CheckpointTable(config, sessionProvider, ingestionConsistencyLevel)
  val sourcesTable = new IngestionConfigTable(config, sessionProvider)

  val defaultKeySpace = config.getString("keyspace")

  def initialize(): Future[Response] = {
    datasetTable.createKeyspace(datasetTable.keyspace)
    datasetTable.initialize()
    checkpointTable.initialize()
    sourcesTable.initialize()
  }

  def clearAllData(): Future[Response] = {
    datasetTable.clearAll()
    checkpointTable.clearAll()
    sourcesTable.clearAll()
  }

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

  def writeCheckpoint(dataset: DatasetRef, shardNum: Int, groupNum: Int, offset: Long): Future[Response] = {
    checkpointTable.writeCheckpoint(dataset, shardNum, groupNum, offset)
  }

  def readCheckpoints(dataset: DatasetRef, shardNum: Int): Future[Map[Int, Long]] = {
    checkpointTable.readCheckpoints(dataset, shardNum)
  }

  def readEarliestCheckpoint(dataset: DatasetRef, shardNum: Int): Future[Long] = {
    readCheckpoints(dataset, shardNum) map { m =>
      if (m.values.isEmpty) Long.MinValue else m.values.min
    }
  }

  def writeIngestionConfig(source: IngestionConfig): Future[Response] =
    sourcesTable.insertIngestionConfig(source)

  def readIngestionConfigs(): Future[Seq[IngestionConfig]] =
    sourcesTable.readAllConfigs()

  def deleteIngestionConfig(ref: DatasetRef): Future[Response] =
    sourcesTable.deleteIngestionConfig(ref)

  /**
    * Record highest time bucket for part key indexable data in meta store
    */
  def writeHighestIndexTimeBucket(dataset: DatasetRef, shardNum: Int,
                                           highestTimeBucket: Int): Future[Response] = {
    checkpointTable.writeHighestIndexTimeBucket(dataset, shardNum, highestTimeBucket)
  }

  /**
    * Read highest time bucket for part key indexable data in meta store
    */
  def readHighestIndexTimeBucket(dataset: DatasetRef,
                                          shardNum: Int): Future[Option[Int]] = {
    checkpointTable.readHighestIndexTimeBucket(dataset, shardNum)
  }
}