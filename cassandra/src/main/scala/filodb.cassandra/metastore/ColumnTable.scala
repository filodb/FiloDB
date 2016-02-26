package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import play.api.libs.iteratee.Iteratee
import scala.concurrent.Future

import filodb.cassandra.FiloCassandraConnector
import filodb.core.metadata.{Column, DataColumn}

/**
 * Represents the "columns" Cassandra table tracking column and schema changes for a dataset
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class ColumnTable(val config: Config) extends CassandraTable[ColumnTable, DataColumn]
with FiloCassandraConnector {
  override val tableName = "columns"

  // scalastyle:off
  object dataset extends StringColumn(this) with PartitionKey[String]
  object version extends IntColumn(this) with PrimaryKey[Int]
  object name extends StringColumn(this) with PrimaryKey[String]
  object id extends IntColumn(this)
  object columnType extends StringColumn(this)
  object isDeleted extends BooleanColumn(this)
  // scalastyle:on

  import filodb.cassandra.Util._
  import filodb.core._

  // May throw IllegalArgumentException if cannot convert one of the string types to one of the Enums
  override def fromRow(row: Row): DataColumn =
    DataColumn(id(row),
               name(row),
               dataset(row),
               version(row),
               Column.ColumnType.withName(columnType(row)),
               isDeleted(row))

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def getSchema(dataset: String, version: Int): Future[Column.Schema] = {
    val enum = select.where(_.dataset eqs dataset).and(_.version lte version)
                     .fetchEnumerator()
    (enum run Iteratee.fold(Column.EmptySchema)(Column.schemaFold)).handleErrors
  }

  def insertColumn(column: DataColumn): Future[Response] = {
    insert.value(_.dataset, column.dataset)
          .value(_.name,    column.name)
          .value(_.version, column.version)
          .value(_.id,      column.id)
          .value(_.columnType, column.columnType.toString)
          .value(_.isDeleted, column.isDeleted)
          .ifNotExists
          .future().toResponse(AlreadyExists)
  }

  def deleteDataset(dataset: String): Future[Response] =
    delete.where(_.dataset eqs dataset).future().toResponse()
}