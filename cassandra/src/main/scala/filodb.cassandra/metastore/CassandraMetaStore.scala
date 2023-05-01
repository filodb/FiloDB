package filodb.cassandra.metastore

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{ConsistencyLevel, Session}
import com.typesafe.config.Config

import filodb.core._
import filodb.core.store.MetaStore

/**
 * A class for Cassandra implementation of the MetaStore.
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
class CassandraMetaStore(config: Config, session: Session)
                        (implicit val ec: ExecutionContext) extends MetaStore {
  private val ingestionConsistencyLevel = ConsistencyLevel.valueOf(config.getString("ingestion-consistency-level"))
  private val checkpointReadConsistencyLevel = ConsistencyLevel.valueOf(
    config.getString("checkpoint-read-consistency-level"))

  val checkpointTable = new CheckpointTable(config, session, ingestionConsistencyLevel, checkpointReadConsistencyLevel)
  private val createTablesEnabled = config.getBoolean("create-tables-enabled")

  val defaultKeySpace = config.getString("keyspace")

  def initialize(): Future[Response] = {
    checkpointTable.createKeyspace(checkpointTable.keyspace)
    if (createTablesEnabled) checkpointTable.initialize() else Future.successful(Success)
  }

  def clearAllData(): Future[Response] = {
    checkpointTable.clearAll()
  }

  def shutdown(): Unit = {
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

}
