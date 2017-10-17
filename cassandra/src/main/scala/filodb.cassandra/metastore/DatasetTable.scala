package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}
import filodb.cassandra.{FiloCassandraConnector, FiloSessionProvider}
import filodb.core.DatasetRef
import filodb.core.metadata.{Dataset, DatasetOptions}

/**
 * Represents the "dataset" Cassandra table tracking each dataset and its column definitions
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 * @param sessionProvider if provided, a session provider provides a session for the configuration
 */
sealed class DatasetTable(val config: Config, val sessionProvider: FiloSessionProvider)
                         (implicit val ec: ExecutionContext) extends FiloCassandraConnector {
  val keyspace = config.getString("admin-keyspace")
  val tableString = s"${keyspace}.datasets"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                    |database text,
                    |name text,
                    |datasetstring text,
                    |options text,
                    |PRIMARY KEY ((database, name))
                    |)""".stripMargin

  import filodb.cassandra.Util._
  import filodb.core._

  def fromRow(row: Row): Dataset =
    Dataset.fromCompactString(row.getString("datasetstring"))
           .copy(database = Some(row.getString("database")),
                 options = DatasetOptions.fromString(row.getString("options")))

  def initialize(): Future[Response] = execCql(createCql)

  def clearAll(): Future[Response] = execCql(s"TRUNCATE $tableString")

  lazy val datasetInsertCql = session.prepare(
    s"""INSERT INTO $tableString (name, database, datasetstring, options
       |) VALUES (?, ?, ?, ?) IF NOT EXISTS""".stripMargin
  )

  def createNewDataset(dataset: Dataset): Future[Response] =
    execStmt(datasetInsertCql.bind(dataset.name,
                                   dataset.database.getOrElse(defaultKeySpace),
                                   dataset.asCompactString,
                                   dataset.options.toString
                                  ), AlreadyExists)

  lazy val datasetSelectCql = session.prepare(
    s"SELECT database, datasetstring, options FROM $tableString WHERE name = ? AND database = ?")

  def getDataset(dataset: DatasetRef): Future[Dataset] =
    session.executeAsync(datasetSelectCql.bind(
                         dataset.dataset, dataset.database.getOrElse(defaultKeySpace)))
           .toOne
           .map { _.map(fromRow).getOrElse(throw NotFoundError(s"Dataset $dataset")) }

  // Return data filtered by database or all datasets
  def getAllDatasets(database: Option[String]): Future[Seq[DatasetRef]] = {
    val filterFunc: DatasetRef => Boolean =
      database.map { db => (ref: DatasetRef) => ref.database.get == db }.getOrElse { ref => true }
    session.executeAsync(s"SELECT database, name FROM $tableString")
           .toIterator.map { it => it.toSeq
        .map { row => DatasetRef(row.getString(1), Some(row.getString(0))) }.filter(filterFunc).distinct
    }
  }

  // NOTE: CQL does not return any error if you DELETE FROM datasets WHERE name = ...
  def deleteDataset(dataset: DatasetRef): Future[Response] =
    execCql(s"DELETE FROM $tableString WHERE name = '${dataset.dataset}' AND " +
            s"database = '${dataset.database.getOrElse(defaultKeySpace)}'")
}
