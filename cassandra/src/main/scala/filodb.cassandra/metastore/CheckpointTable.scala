package filodb.cassandra.metastore

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.Config

import filodb.cassandra.{FiloCassandraConnector, FiloSessionProvider}

/**
  * Represents the "checkpoint" Cassandra table tracking each dataset and its column definitions
  *
  * @param config          a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
  * @param sessionProvider if provided, a session provider provides a session for the configuration
  */
sealed class CheckpointTable(val config: Config, val sessionProvider: FiloSessionProvider)
                            (implicit val ec: ExecutionContext) extends FiloCassandraConnector {
  val keyspace = config.getString("admin-keyspace")
  val tableString = s"${keyspace}.checkpoints"

  val createCql =
    s"""CREATE TABLE IF NOT EXISTS $tableString (
       | databasename text,
       | datasetname text,
       | shardnum int,
       | groupnum int,
       | offset bigint,
       | PRIMARY KEY ((databasename, datasetname, shardNum), groupNum)
       |)""".stripMargin

  lazy val readCheckpointCql = {
    val statement = session.prepare(
      s"""SELECT groupnum, offset FROM $tableString WHERE
         | databasename = ? AND
         | datasetname = ? AND
         | shardnum = ? """.stripMargin)
    statement.setConsistencyLevel(ConsistencyLevel.QUORUM) // we want consistent reads during recovery
    statement
  }

  lazy val writeCheckpointCql = {
    val statement = session.prepare(
      s"""INSERT INTO $tableString (databasename, datasetname, shardnum, groupnum, offset)
         | VALUES (?, ?, ?, ?, ?)""".stripMargin
    )
    statement.setConsistencyLevel(ConsistencyLevel.ONE) // we want fast writes during ingestion
    statement
  }

  import filodb.cassandra.Util._
  import filodb.core._

  def initialize(): Future[Response] = execCql(createCql)

  def clearAll(): Future[Response] = execCql(s"TRUNCATE $tableString")

  def writeCheckpoint(dataset: DatasetRef, shardNum: Int, groupNum: Int, offset: Long): Future[Response] = {
    // TODO database name should not be an optional in internally since there is a default value. Punted for later.
    execStmt(writeCheckpointCql.bind(dataset.database.getOrElse(""),
      dataset.dataset, Int.box(shardNum), Int.box(groupNum), Long.box(offset)))
  }

  def readCheckpoints(dataset: DatasetRef, shardNum: Int): Future[Map[Int,Long]] = {
    session.executeAsync(readCheckpointCql.bind(dataset.database.getOrElse(""),
            dataset.dataset, Int.box(shardNum)))
      .toIterator // future of Iterator
      .map { it => it.map(r => r.getInt(0) -> r.getLong(1)).toMap }
  }

}
