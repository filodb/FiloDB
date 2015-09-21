package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import play.api.libs.iteratee.Iteratee
import scala.concurrent.Future

import filodb.cassandra.FiloCassandraConnector
import filodb.core.metadata.Column

/**
 * Represents the "columns" Cassandra table tracking column and schema changes for a dataset
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class ColumnTable(val config: Config) extends CassandraTable[ColumnTable, Column]
with FiloCassandraConnector {
  override val tableName = "columns"

  // scalastyle:off
  object dataset extends StringColumn(this) with PartitionKey[String]
  object version extends IntColumn(this) with PrimaryKey[Int]
  object name extends StringColumn(this) with PrimaryKey[String]
  object columnType extends StringColumn(this)
  object serializer extends StringColumn(this)
  object isDeleted extends BooleanColumn(this)
  object isSystem extends BooleanColumn(this)
  // scalastyle:on

  import filodb.cassandra.Util._
  import filodb.core._

  // May throw IllegalArgumentException if cannot convert one of the string types to one of the Enums
  override def fromRow(row: Row): Column =
    Column(name(row),
           dataset(row),
           version(row),
           Column.ColumnType.withName(columnType(row)),
           Column.Serializer.withName(serializer(row)),
           isDeleted(row),
           isSystem(row))

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def getSchema(dataset: String, version: Int): Future[Column.Schema] = {
    val enum = select.where(_.dataset eqs dataset).and(_.version lte version)
                     .fetchEnumerator()
    (enum run Iteratee.fold(Column.EmptySchema)(Column.schemaFold)).handleErrors
  }

  def insertColumn(column: Column): Future[Response] = {
    insert.value(_.dataset, column.dataset)
          .value(_.name,    column.name)
          .value(_.version, column.version)
          .value(_.columnType, column.columnType.toString)
          .value(_.serializer, column.serializer.toString)
          .value(_.isDeleted, column.isDeleted)
          .value(_.isSystem, column.isSystem)
          .ifNotExists
          .future().toResponse(AlreadyExists)
  }
}