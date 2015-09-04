package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.websudos.phantom.dsl._
import scala.concurrent.Future

import filodb.core.metadata.Dataset

/**
 * Represents the "dataset" Cassandra table tracking each dataset and its partitions
 */
sealed class DatasetTable extends CassandraTable[DatasetTable, Dataset] {
  // scalastyle:off
  object name extends StringColumn(this) with PartitionKey[String]
  object partitionColumn extends StringColumn(this) with StaticColumn[String]
  object options extends StringColumn(this) with StaticColumn[String]
  // scalastyle:on

  override def fromRow(row: Row): Dataset =
    // TODO: implement projections storage
    Dataset(name(row), Nil, partitionColumn(row))
}

/**
 * Asynchronous methods to operate on datasets.  All normal errors and exceptions are returned
 * through ErrorResponse types.
 */
object DatasetTableOps extends DatasetTable with SimpleCassandraConnector {
  override val tableName = "datasets"

  // TODO: add in Config-based initialization code to find the keyspace, cluster, etc.
  implicit val keySpace = KeySpace("unittest")

  import Util._
  import filodb.core.messages._

  def createNewDataset(name: String): Future[Response] =
    insert.value(_.name, name).ifNotExists.future().toResponse(AlreadyExists)

  def getDataset(name: String): Future[Dataset] =
    select.where(_.name eqs name).one()
      .map(_.getOrElse(throw NotFoundError(s"Dataset $name")))

  def deleteDataset(name: String): Future[Response] =
    delete.where(_.name eqs name).future().toResponse()
}
