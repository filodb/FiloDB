package filodb.cassandra.metastore


import com.datastax.driver.core.Session
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import filodb.core.Messages._
import filodb.core.Types._
import filodb.core.metadata.Column
import filodb.core.store.ProjectionInfo

import scala.concurrent.Future

/**
 * Represents partitions in a dataset
 */
sealed class ProjectionTable(ks: KeySpace, _session: Session)
  extends CassandraTable[ProjectionTable, ProjectionInfo] {

  import filodb.cassandra.Util._

  implicit val keySpace = ks
  implicit val session = _session

  // scalastyle:off
  object dataset extends StringColumn(this) with PartitionKey[String]

  object projectionId extends IntColumn(this) with PrimaryKey[Int]

  object datasetSchema extends BlobColumn(this)

  object partitionKeyColumns extends StringColumn(this)

  object keyColumns extends StringColumn(this)

  object segmentColumns extends StringColumn(this)

  object projectionReverse extends BooleanColumn(this)

  // scalastyle:on

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  override def fromRow(row: Row): ProjectionInfo = {
    val bb = datasetSchema(row)
    val schemaObj = Column.readSchema(bb)
    ProjectionInfo(projectionId(row),
      dataset(row),
      schemaObj,
      columns(partitionKeyColumns(row)),
      columns(keyColumns(row)),
      columns(segmentColumns(row)),
      projectionReverse(row)
    )
  }

  private def columns(str: String) = {
    str.split(",")
  }

  private def str(cols: Seq[ColumnId]) = cols.mkString(",")


  def insertProjection(projection: ProjectionInfo): Future[Response] =
    insert.value(_.dataset, projection.dataset)
      .value(_.projectionId, projection.id)
      .value(_.datasetSchema, Column.schemaAsByteBuffer(projection.schema))
      .value(_.partitionKeyColumns, str(projection.partitionColumns))
      .value(_.keyColumns, str(projection.keyColumns))
      .value(_.segmentColumns, str(projection.segmentColumns))
      .value(_.projectionReverse, projection.reverse)
      .future.toResponse()


  def getProjection(dataset: TableName, id: Int): Future[ProjectionInfo] =
    select.where(_.dataset eqs dataset).and(_.projectionId eqs id)
      .one().map(_.getOrElse(throw NotFoundError(s"Dataset $dataset")))

  def getSuperProjection(dataset: TableName): Future[ProjectionInfo] =
    select.where(_.dataset eqs dataset).and(_.projectionId eqs 0)
      .one().map(_.getOrElse(throw NotFoundError(s"Dataset $dataset")))

  def getProjections(dataset: TableName): Future[Seq[ProjectionInfo]] = {
    select.where(_.dataset eqs dataset).fetch()
  }

  def getAllSuperProjectionNames: Future[Seq[String]] ={
    select.where(_.projectionId eqs 0).allowFiltering().fetch().map(list => list map(info => info.dataset))
  }

}
