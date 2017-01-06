package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import filodb.cassandra.{FiloCassandraConnector, FiloSessionProvider}
import filodb.core.metadata.IngestionStateData
import filodb.core.{AlreadyExists, Response}

import scala.concurrent.{ExecutionContext, Future}
/**
  * Represents the "ingestion_state" Cassandra table tracking ingestion progress of each dataset
  *
  * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
  */
sealed class IngestionStateTable(val config: Config, val sessionProvider: FiloSessionProvider)
                                (implicit val ec: ExecutionContext) extends FiloCassandraConnector {

  val keyspace = config.getString("admin-keyspace")
  val tableString = s"${keyspace}.ingestion_state"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                      |nodeactor text,
                      |database text,
                      |dataset text,
                      |version int,
                      |columns text,
                      |state text,
                      |exceptions text,
                      |PRIMARY KEY (nodeactor, database, dataset, version)
                      |)""".stripMargin

  import filodb.cassandra.Util._

  lazy val ingestionStateInsertCql = session.prepare(
    s"""INSERT INTO $tableString (nodeactor, database, dataset, version, columns, state)
        | VALUES (?, ?, ?, ?,?, ?) IF NOT EXISTS""".stripMargin
  )

  lazy val ingestionStSelectByDsCql = session.prepare(
    s"""SELECT nodeActor, database, dataset, version, columns, state, exceptions
        | FROM $tableString WHERE nodeactor = ? AND database = ? AND dataset = ? AND version = ?""".stripMargin
  )

  lazy val ingestionStSelectByActorCql = session.prepare(
    s"""SELECT nodeActor, database, dataset, version, columns, state, exceptions
        | FROM $tableString WHERE nodeactor = ? """.stripMargin
  )

  def initialize(): Future[Response] = execCql(createCql)

  def fromRow(row: Row): IngestionStateData =
    IngestionStateData(row.getString("nodeActor"),
      row.getString("database"),
      row.getString("dataset"),
      row.getInt("version"),
      row.getString("columns"),
      row.getString("state"),
      row.getString("exceptions"))

  def clearAll(): Future[Response] = execCql(s"TRUNCATE $tableString")

  def dropTable(): Future[Response] = execCql(s"DROP TABLE $tableString")

  def insertIngestionState(nodeActor: String, database: String, name: String,
                           version: Int, columns: String, state: String): Future[Response] = {
    execStmt(ingestionStateInsertCql.bind(
      nodeActor, database, name, version: java.lang.Integer, columns, state),
      AlreadyExists
    )
  }

  def getIngestionStateByDataset(nodeActor: String, database: String,
                                 name: String, version: Int) : Future[Seq[Row]] =
    session.executeAsync(ingestionStSelectByDsCql.bind(
      nodeActor,
      database,
      name,
      version: java.lang.Integer)
    ).toIterator.map(_.toSeq)

  def getIngestionStateByNodeActor(nodeActor: String) : Future[Seq[IngestionStateData]] =
    session.executeAsync(ingestionStSelectByActorCql.bind(
      nodeActor)
    ).toIterator.map(_.map(fromRow).toSeq)

  def deleteIngestionStateByNodeActor(nodeActor: String): Future[Response] =
    execCql(s"DELETE FROM $tableString WHERE nodeactor = '${nodeActor}'")

  def deleteIngestionStateByDataset(nodeActor: String, keyspace: String, name: String,
                                    version: Int): Future[Response] =
    execCql(s"DELETE FROM $tableString WHERE nodeactor = '${nodeActor}' AND " +
      s"database = '${keyspace}' AND dataset = '${name}' AND version = ${version}")

  def updateIngestionState(nodeActor: String, keyspace: String, name: String,
                           state: String, exceptions: String, version: Int = 0): Future[Response] =
    execCql(s"UPDATE $tableString SET state = '${state}', exceptions = '${exceptions}' " +
      s"WHERE nodeactor = '${nodeActor}' AND database = '${keyspace}' AND dataset = '${name}'" +
      s"AND version = $version")

}
