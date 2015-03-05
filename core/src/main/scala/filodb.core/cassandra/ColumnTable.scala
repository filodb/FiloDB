package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
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
  val keySpace = "test"

  import Util._
  import filodb.core.messages._

  private def getSchemaNoErrorHandling(dataset: String, version: Int): Future[Column.Schema] = {
    val enum = select.where(_.dataset eqs dataset).and(_.version lte version)
                     .fetchEnumerator()
    enum run Iteratee.fold(Column.EmptySchema)(Column.schemaFold)
  }

  private def getSchemaAllVersions(dataset: String): Future[Column.Schema] =
    getSchemaNoErrorHandling(dataset, Int.MaxValue)

  /**
   * Returns a schema of columns for a given dataset version.
   * NOTE: If there is no dataset row found, it is returned as an empty schema ... C* cannot tell the
   * difference.
   * @return TheSchema(schema), or some ErrorResponse
   */
  def getSchema(dataset: String, version: Int): Future[Response] = {
    getSchemaNoErrorHandling(dataset, version)
      .map { schema => Column.TheSchema(schema) }
      .handleErrors
  }

  /**
   * Creates a new column if a definition for that dataset, version, and name doesn't exist already.
   * This does no validation, so please use it with care!  Should be used for testing only.
   */
  def newColumnPrettyPlease(column: Column): Future[Response] = {
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

  /**
   * Creates a new column definition (or updated column definition).
   * First it retrieves the current schema and checks against the validator to make
   * sure the change/addition is allowed.
   * @param column the Column to add/update
   * @return Success, or NotFound (dataset not found), or IllegalColumnChange, or ErrorResponse
   */
  def newColumn(column: Column): Future[Response] = {
    getSchemaAllVersions(column.dataset).flatMap { schema =>
      val invalidReasons = Column.invalidateNewColumn(column.dataset, schema, column)
      if (invalidReasons.nonEmpty) { Future(Column.IllegalColumnChange(invalidReasons)) }
      else                         { newColumnPrettyPlease(column) }
    }.handleErrors
  }

  // Partial function mapping commands to functions executing them
  val commandMapper: PartialFunction[Command, Future[Response]] = {
    case Column.NewColumn(column) => newColumn(column)
    case Column.GetSchema(dataset, version) => getSchema(dataset, version)
    // case Column.DeleteColumn(dataset, version, name) => ???
  }
}