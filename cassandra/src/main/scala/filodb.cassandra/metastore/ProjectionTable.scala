package filodb.cassandra.metastore


import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import filodb.cassandra.FiloCassandraConnector
import filodb.core.Messages._
import filodb.core.Types._
import filodb.core.metadata.Column
import filodb.core.store.ProjectionInfo

import scala.concurrent.Future

/**
 * Represents partitions in a dataset
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class ProjectionTable(val config: Config) extends CassandraTable[ProjectionTable, ProjectionInfo]
with FiloCassandraConnector {

  import filodb.cassandra.Util._
  override val tableName = "projections"

  // scalastyle:off
  object name extends StringColumn(this) with PartitionKey[String]

  object options extends StringColumn(this)

  object schema extends BlobColumn(this)

  object projectionId extends IntColumn(this) with PrimaryKey[Int]

  object partitionKeyColumns extends StringColumn(this)

  object keyColumns extends StringColumn(this)

  object sortColumns extends StringColumn(this)

  object segmentColumns extends StringColumn(this)

  object projectionReverse extends BooleanColumn(this)

  // scalastyle:on

  override def fromRow(row: Row): ProjectionInfo = {
    val bb = schema(row)
    val schemaObj = Column.readSchema(bb)
    ProjectionInfo(projectionId(row),
      name(row),
      schemaObj,
      columns(partitionKeyColumns(row)),
      columns(keyColumns(row)),
      columns(sortColumns(row)),
      columns(segmentColumns(row)),
      projectionReverse(row)
    )
  }

  def columns(str: String) = {
    str.split(",")
  }

  def str(cols: Seq[ColumnId]) = cols.mkString(",")

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def insertProjection(projection: ProjectionInfo): Future[Response] =
    insert.value(_.name, projection.dataset)
      .value(_.projectionId, projection.id)
      .value(_.schema, Column.schemaAsByteBuffer(projection.schema))
      .value(_.partitionKeyColumns, str(projection.partitionColumns))
      .value(_.keyColumns, str(projection.keyColumns))
      .value(_.sortColumns, str(projection.sortColumns))
      .value(_.segmentColumns, str(projection.segmentColumns))
      .future.toResponse()


  def getProjection(dataset: TableName, id: Int): Future[ProjectionInfo] =
    select.where(_.name eqs dataset).and(_.projectionId eqs id)
      .one().map(_.getOrElse(throw NotFoundError(s"Dataset $dataset")))

  def getSuperProjection(dataset: TableName): Future[ProjectionInfo] =
    select.where(_.name eqs dataset).and(_.projectionId eqs 0)
      .one().map(_.getOrElse(throw NotFoundError(s"Dataset $dataset")))

  def getProjections(dataset:TableName):Future[Seq[ProjectionInfo]] ={
    select.where(_.name eqs dataset).fetch()
  }

}
