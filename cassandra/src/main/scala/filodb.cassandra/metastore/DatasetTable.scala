package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import filodb.cassandra.{FiloCassandraConnector, FiloSessionProvider}
import filodb.core.DatasetRef
import filodb.core.metadata.{Dataset, DatasetOptions, Projection}

/**
 * Represents the "dataset" Cassandra table tracking each dataset and its partitions
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
                    |projectionid int,
                    |keycolumns text,
                    |options text static,
                    |partitioncolumns text static,
                    |projectioncolumns text,
                    |projectionreverse boolean,
                    |segmentcolumns text,
                    |PRIMARY KEY ((database, name), projectionid)
                    |)""".stripMargin

  import filodb.cassandra.Util._
  import filodb.core._
  import filodb.core.Types._

  def fromRow(row: Row): Projection =
    Projection(row.getInt("projectionid"),
               DatasetRef(row.getString("name"), Some(row.getString("database"))),
               splitCString(row.getString("keycolumns")),
               row.getString("segmentcolumns"),
               row.getBool("projectionreverse"),
               splitCString(Try(row.getString("projectioncolumns")).getOrElse("")))

  // We use \u0001 to split and demarcate column name strings, because
  // 1) this char is not allowed,
  // 2) spaces, : () are used in function definitions
  // 3) Cassandra CQLSH will highlight weird unicode chars in diff color so it's easy to see :)
  private def stringsToStr(strings: Seq[String]): String = strings.mkString("\u0001")
  private def splitCString(string: String): Seq[String] =
    if (string.isEmpty) Nil else string.split('\u0001').toSeq

  def initialize(): Future[Response] = execCql(createCql)

  def clearAll(): Future[Response] = execCql(s"TRUNCATE $tableString")

  lazy val projInsertCql = session.prepare(
    s"""INSERT INTO $tableString (name, database, projectionid, keycolumns, segmentcolumns,
       |projectionreverse, projectioncolumns) VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin
  )

  def insertProjection(projection: Projection): Future[Response] =
    execStmt(projInsertCql.bind(projection.dataset.dataset,
                                projection.dataset.database.getOrElse(defaultKeySpace),
                                projection.id: java.lang.Integer,
                                stringsToStr(projection.keyColIds),
                                projection.segmentColId,
                                projection.reverse: java.lang.Boolean,
                                stringsToStr(projection.columns)))

  lazy val datasetInsertCql = session.prepare(
    s"""INSERT INTO $tableString (name, database, partitioncolumns, options
       |) VALUES (?, ?, ?, ?) IF NOT EXISTS""".stripMargin
  )

  def createNewDataset(dataset: Dataset): Future[Response] =
    (for { createResp <- execStmt(datasetInsertCql.bind(
                                    dataset.name,
                                    dataset.projections.head.dataset.database.getOrElse(defaultKeySpace),
                                    stringsToStr(dataset.partitionColumns),
                                    dataset.options.toString
                                  ), AlreadyExists)
          insertProj <- insertProjection(dataset.projections.head)
             if (dataset.projections.nonEmpty) && createResp == Success }
    yield { insertProj }).recover {
      case e: NoSuchElementException => AlreadyExists
    }

  def getProjection(dataset: DatasetRef, id: Int): Future[Projection] =
    session.executeAsync(s"""
      SELECT name, database, projectionid, keycolumns, segmentcolumns, projectionreverse, projectioncolumns
      FROM $tableString WHERE name = '${dataset.dataset}' AND projectionid = $id
      AND database = '${dataset.database.getOrElse(defaultKeySpace)}'
      """).toOne.map { _.map(fromRow).getOrElse(throw NotFoundError(s"Dataset $dataset")) }

  lazy val datasetSelectCql = session.prepare(s"SELECT partitioncolumns, options FROM $tableString WHERE " +
                                          "name = ? AND database = ?")

  def getDataset(dataset: DatasetRef): Future[Dataset] =
    for { proj <- getProjection(dataset, 0)
          Some(row) <- session.executeAsync(datasetSelectCql.bind(
                         dataset.dataset, dataset.database.getOrElse(defaultKeySpace))).toOne }
    yield { Dataset(dataset.dataset,
                    Seq(proj),
                    splitCString(row.getString("partitioncolumns")),
                    DatasetOptions.fromString(row.getString("options"))) }

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
