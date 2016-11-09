package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}
import filodb.cassandra.{FiloCassandraConnector, FiloSessionProvider}
import filodb.core.DatasetRef
import filodb.core.metadata.{Column, DataColumn}

/**
 * Represents the "columns" Cassandra table tracking column and schema changes for a dataset
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 * @param sessionProvider if provided, a session provider provides a session for the configuration
 */
sealed class ColumnTable(val config: Config, val sessionProvider: FiloSessionProvider)
                        (implicit val ec: ExecutionContext) extends FiloCassandraConnector {
  val keyspace = config.getString("admin-keyspace")
  val tableString = s"${keyspace}.columns"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                     |database text,
                     |dataset text,
                     |version int,
                     |name text,
                     |columntype text,
                     |id int,
                     |isdeleted boolean,
                     |PRIMARY KEY ((database, dataset), version, name)
                     |)""".stripMargin

  import filodb.cassandra.Util._
  import filodb.core._

  // May throw IllegalArgumentException if cannot convert one of the string types to one of the Enums
  def fromRow(row: Row): DataColumn =
    DataColumn(row.getInt("id"),
               row.getString("name"),
               row.getString("dataset"),
               row.getInt("version"),
               Column.ColumnType.withName(row.getString("columnType")),
               row.getBool("isdeleted"))

  def initialize(): Future[Response] = execCql(createCql)

  def clearAll(): Future[Response] = execCql(s"TRUNCATE $tableString")

  lazy val schemaCql = session.prepare(s"SELECT * FROM $tableString WHERE dataset = ? AND " +
                                  s"database = ? AND version <= ?")

  def getSchema(dataset: DatasetRef, version: Int): Future[Column.Schema] = {
    session.executeAsync(schemaCql.bind(dataset.dataset,
                                        dataset.database.getOrElse(defaultKeySpace),
                                        version: java.lang.Integer)).toIterator.map { it =>
      it.map(fromRow).foldLeft(Column.EmptySchema)(Column.schemaFold)
    }.handleErrors
  }

  lazy val columnInsertCql = session.prepare(
    s"""INSERT INTO $tableString (dataset, database, name, version, id, columntype, isdeleted
       |) VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS""".stripMargin
  )

  def insertColumns(columns: Seq[DataColumn],
                    dataset: DatasetRef): Future[Response] = {
    val db = dataset.database.getOrElse(defaultKeySpace)
    val statements = columns.map { column =>
      columnInsertCql.bind(dataset.dataset, db,
                           column.name, column.version: java.lang.Integer,
                           column.id: java.lang.Integer,
                           column.columnType.toString,
                           column.isDeleted: java.lang.Boolean)
    }
    execStmt(unloggedBatch(statements), AlreadyExists)
  }

  def deleteDataset(dataset: DatasetRef): Future[Response] =
    execCql(s"DELETE FROM $tableString WHERE dataset = '${dataset.dataset}' AND " +
            s"database = '${dataset.database.getOrElse(defaultKeySpace)}'")
}