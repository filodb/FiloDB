package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import filodb.coordinator.{NotFoundError, Success, AlreadyExists, Response}
import filodb.core.store.{ProjectionInfo, DatasetOptions, Dataset}
import scala.concurrent.Future

import filodb.cassandra.FiloCassandraConnector
import filodb.core.metadata.Projection

/**
 * Represents the "dataset" Cassandra table tracking each dataset and its partitions
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class DatasetTable(val config: Config) extends CassandraTable[DatasetTable, ProjectionInfo]
with FiloCassandraConnector {
  override val tableName = "datasets"

  // scalastyle:off
  object name extends StringColumn(this) with PartitionKey[String]
  object partitionColumn extends StringColumn(this) with StaticColumn[String]
  object options extends StringColumn(this) with StaticColumn[String]
  object projectionId extends IntColumn(this) with PrimaryKey[Int]
  object projectionSortColumn extends StringColumn(this)
  object projectionReverse extends BooleanColumn(this)
  object projectionSegmentSize extends StringColumn(this)
  // scalastyle:on

  import filodb.cassandra.Util._
  import filodb.core._
  import filodb.core.Types._

  override def fromRow(row: Row): ProjectionInfo =
    ProjectionInfo(projectionId(row),
               name(row),
               projectionSortColumn(row),
               projectionReverse(row),
               segmentSize = projectionSegmentSize(row))

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def insertProjection(projection: ProjectionInfo): Future[Response] =
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

  def getProjection(dataset: TableName, id: Int): Future[ProjectionInfo] =
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
