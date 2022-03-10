package filodb.cassandra.metastore

import java.lang.{Integer => JInt, Long => JLong}

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{ConsistencyLevel, Session}
import com.typesafe.config.Config

import filodb.cassandra.FiloCassandraConnector

/**
  * Represents the "checkpoint" Cassandra table tracking each dataset and its column definitions
  *
  * @param config          a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
  */
sealed class CheckpointTable(val config: Config,
                             val session: Session,
                             writeConsistencyLevel: ConsistencyLevel)
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
       | PRIMARY KEY ((databasename, datasetname, shardnum), groupnum)
       |)""".stripMargin

  lazy val readCheckpointCql =
    session.prepare(
      s"""SELECT groupnum, offset FROM $tableString WHERE
         | databasename = ? AND
         | datasetname = ? AND
         | shardnum = ? """.stripMargin).setConsistencyLevel(ConsistencyLevel.ONE)
    // we want consistent reads during recovery

  lazy val writeCheckpointCql = {
    val statement = session.prepare(
      s"""INSERT INTO $tableString (databasename, datasetname, shardnum, groupnum, offset)
         | VALUES (?, ?, ?, ?, ?)""".stripMargin
    )
    statement.setConsistencyLevel(writeConsistencyLevel)
    statement
  }

  import filodb.cassandra.Util._
  import filodb.core._

  def initialize(): Future[Response] = execCql(createCql)

  def clearAll(): Future[Response] = execCql(s"TRUNCATE $tableString")

  def writeCheckpoint(dataset: DatasetRef, shardNum: Int, groupNum: Int, offset: Long): Future[Response] = {
    // TODO database name should not be an optional in internally since there is a default value. Punted for later.
    execStmt(writeCheckpointCql.bind(dataset.database.getOrElse(""),
      dataset.dataset, shardNum: JInt, groupNum: JInt, offset: JLong))
  }

  def readCheckpoints(dataset: DatasetRef, shardNum: Int): Future[Map[Int, Long]] = {
    session.executeAsync(readCheckpointCql.bind(dataset.database.getOrElse(""),
            dataset.dataset, shardNum: JInt))
      .toIterator // future of Iterator
      .map { it => it.map(r => r.getInt(0) -> r.getLong(1)).toMap }
  }
}
