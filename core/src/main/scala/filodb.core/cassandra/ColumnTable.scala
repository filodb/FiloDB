package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.websudos.phantom.dsl._
import play.api.libs.iteratee.Iteratee
import scala.concurrent.Future

import filodb.core.metadata.Column

/**
 * Represents the "columns" Cassandra table tracking column and schema changes for a dataset
 */
sealed class ColumnTable extends CassandraTable[ColumnTable, Column] {
  // scalastyle:off
  object dataset extends StringColumn(this) with PartitionKey[String]
  object version extends IntColumn(this) with PrimaryKey[Int]
  object name extends StringColumn(this) with PrimaryKey[String]
  object columnType extends StringColumn(this)
  object serializer extends StringColumn(this)
  object isDeleted extends BooleanColumn(this)
  object isSystem extends BooleanColumn(this)
  // scalastyle:on

  // May throw IllegalArgumentException if cannot convert one of the string types to one of the Enums
  override def fromRow(row: Row): Column =
    Column(name(row),
           dataset(row),
           version(row),
           Column.ColumnType.withName(columnType(row)),
           Column.Serializer.withName(serializer(row)),
           isDeleted(row),
           isSystem(row))
}

/**
 * Asynchronous methods to operate on columns.  All normal errors and exceptions are returned
 * through ErrorResponse types.
 */
object ColumnTable extends ColumnTable with SimpleCassandraConnector {
  override val tableName = "columns"

  // TODO: add in Config-based initialization code to find the keyspace, cluster, etc.
  implicit val keySpace = KeySpace("unittest")

  import Util._
  import filodb.core.messages._

  def getSchema(dataset: String, version: Int): Future[Column.Schema] = {
    val enum = select.where(_.dataset eqs dataset).and(_.version lte version)
                     .fetchEnumerator()
    enum run Iteratee.fold(Column.EmptySchema)(Column.schemaFold)
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