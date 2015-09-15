package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.websudos.phantom.dsl._
import scala.concurrent.Future

import filodb.core.metadata.{Dataset, DatasetOptions, Projection}

/**
 * Represents the "dataset" Cassandra table tracking each dataset and its partitions
 */
sealed class DatasetTable extends CassandraTable[DatasetTable, Projection] {
  // scalastyle:off
  object name extends StringColumn(this) with PartitionKey[String]
  object partitionColumn extends StringColumn(this) with StaticColumn[String]
  object options extends StringColumn(this) with StaticColumn[String]
  object projectionId extends IntColumn(this) with PrimaryKey[Int]
  object projectionSortColumn extends StringColumn(this)
  object projectionReverse extends BooleanColumn(this)
  object projectionSegmentSize extends StringColumn(this)
  // scalastyle:on

  override def fromRow(row: Row): Projection =
    Projection(projectionId(row),
               name(row),
               projectionSortColumn(row),
               projectionReverse(row),
               segmentSize = projectionSegmentSize(row))
}

/**
 * Asynchronous methods to operate on datasets.  All normal errors and exceptions are returned
 * through ErrorResponse types.
 */
object DatasetTable extends DatasetTable with SimpleCassandraConnector {
  override val tableName = "datasets"

  // TODO: add in Config-based initialization code to find the keyspace, cluster, etc.
  implicit val keySpace = KeySpace("unittest")

  import filodb.cassandra.Util._
  import filodb.core._
  import filodb.core.Types._

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def insertProjection(projection: Projection): Future[Response] =
    insert.value(_.name, projection.dataset)
          .value(_.projectionId, projection.id)
          .value(_.projectionSortColumn, projection.sortColumn)
          .value(_.projectionReverse, projection.reverse)
          .value(_.projectionSegmentSize, projection.segmentSize)
          .future.toResponse()

  def createNewDataset(dataset: Dataset): Future[Response] =
    (for { createResp <- insert.value(_.name, dataset.name)
                               .value(_.partitionColumn, dataset.partitionColumn)
                               .value(_.options, dataset.options.toString)
                               .ifNotExists.future().toResponse(AlreadyExists)
          insertProj <- insertProjection(dataset.projections.head)
             if (dataset.projections.nonEmpty) && createResp == Success }
    yield { insertProj }).recover {
      case e: NoSuchElementException => AlreadyExists
    }

  def getProjection(dataset: TableName, id: Int): Future[Projection] =
    select.where(_.name eqs dataset).and(_.projectionId eqs id)
          .one().map(_.getOrElse(throw NotFoundError(s"Dataset $dataset")))

  def getDataset(dataset: TableName): Future[Dataset] =
    for { proj <- getProjection(dataset, 0)
          Some((partCol, options)) <- select(_.partitionColumn, _.options).where(_.name eqs dataset).one() }
    yield { Dataset(dataset, Seq(proj), partCol, DatasetOptions.fromString(options)) }

  // NOTE: CQL does not return any error if you DELETE FROM datasets WHERE name = ...
  def deleteDataset(name: String): Future[Response] =
    delete.where(_.name eqs name).future().toResponse()
}
