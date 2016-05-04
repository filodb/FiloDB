package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import scala.concurrent.Future
import scala.util.Try

import filodb.cassandra.FiloCassandraConnector
import filodb.core.DatasetRef
import filodb.core.metadata.{Dataset, DatasetOptions, Projection}

/**
 * Represents the "dataset" Cassandra table tracking each dataset and its partitions
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class DatasetTable(val config: Config) extends CassandraTable[DatasetTable, Projection]
with FiloCassandraConnector {
  override val tableName = "datasets"
  val keyspace = config.getString("admin-keyspace")
  implicit val ks = KeySpace(keyspace)

  // scalastyle:off
  object database extends StringColumn(this) with PartitionKey[String]
  object name extends StringColumn(this) with PartitionKey[String]
  object partitionColumns extends StringColumn(this) with StaticColumn[String]
  object options extends StringColumn(this) with StaticColumn[String]
  object projectionId extends IntColumn(this) with PrimaryKey[Int]
  object keyColumns extends StringColumn(this)
  object segmentColumns extends StringColumn(this)
  object projectionColumns extends StringColumn(this)
  object projectionReverse extends BooleanColumn(this)
  // scalastyle:on

  import filodb.cassandra.Util._
  import filodb.core._
  import filodb.core.Types._

  override def fromRow(row: Row): Projection =
    Projection(projectionId(row),
               DatasetRef(name(row), Some(database(row))),
               splitCString(keyColumns(row)),
               segmentColumns(row),
               projectionReverse(row),
               splitCString(Try(projectionColumns(row)).getOrElse("")))

  // We use \001 to split and demarcate column name strings, because
  // 1) this char is not allowed,
  // 2) spaces, : () are used in function definitions
  // 3) Cassandra CQLSH will highlight weird unicode chars in diff color so it's easy to see :)
  private def stringsToStr(strings: Seq[String]): String = strings.mkString("\001")
  private def splitCString(string: String): Seq[String] =
    if (string.isEmpty) Nil else string.split('\001').toSeq

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def insertProjection(projection: Projection): Future[Response] =
    insert.value(_.name, projection.dataset.dataset)
          .value(_.database, projection.dataset.database.getOrElse(defaultKeySpace))
          .value(_.projectionId, projection.id)
          .value(_.keyColumns, stringsToStr(projection.keyColIds))
          .value(_.segmentColumns, projection.segmentColId)
          .value(_.projectionReverse, projection.reverse)
          .value(_.projectionColumns, stringsToStr(projection.columns))
          .future.toResponse()

  def createNewDataset(dataset: Dataset): Future[Response] =
    (for { createResp <- insert.value(_.name, dataset.name)
                               .value(_.database, dataset.projections.head.
                                                    dataset.database.getOrElse(defaultKeySpace))
                               .value(_.partitionColumns, stringsToStr(dataset.partitionColumns))
                               .value(_.options, dataset.options.toString)
                               .ifNotExists.future().toResponse(AlreadyExists)
          insertProj <- insertProjection(dataset.projections.head)
             if (dataset.projections.nonEmpty) && createResp == Success }
    yield { insertProj }).recover {
      case e: NoSuchElementException => AlreadyExists
    }

  def getProjection(dataset: DatasetRef, id: Int): Future[Projection] =
    select.where(_.name eqs dataset.dataset)
          .and(_.database eqs dataset.database.getOrElse(defaultKeySpace))
          .and(_.projectionId eqs id)
          .one().map(_.getOrElse(throw NotFoundError(s"Dataset $dataset")))

  def getDataset(dataset: DatasetRef): Future[Dataset] =
    for { proj <- getProjection(dataset, 0)
          Some((partCols, options)) <- select(_.partitionColumns, _.options)
                                         .where(_.name eqs dataset.dataset)
                                         .and(_.database eqs dataset.database.getOrElse(defaultKeySpace))
                                         .one() }
    yield { Dataset(dataset.dataset,
                    Seq(proj),
                    splitCString(partCols),
                    DatasetOptions.fromString(options)) }

  // Return data filtered by database or all datasets
  def getAllDatasets(database: Option[String]): Future[Seq[DatasetRef]] = {
    val filterFunc: DatasetRef => Boolean =
      database.map { db => (ref: DatasetRef) => ref.database.get == db }.getOrElse { ref => true }
    select(_.database, _.name).fetch.map { data =>
      data.map { case (db, name) => DatasetRef(name, Some(db)) }.filter(filterFunc).distinct
    }
  }

  // NOTE: CQL does not return any error if you DELETE FROM datasets WHERE name = ...
  def deleteDataset(dataset: DatasetRef): Future[Response] =
    delete.where(_.name eqs dataset.dataset)
          .and(_.database eqs dataset.database.getOrElse(defaultKeySpace))
          .future().toResponse()
}
