package filodb.cassandra.metastore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import play.api.libs.iteratee.Iteratee
import scala.concurrent.Future

import filodb.cassandra.FiloCassandraConnector
import filodb.core.DatasetRef
import filodb.core.metadata.{Column, DataColumn}

/**
 * Represents the "columns" Cassandra table tracking column and schema changes for a dataset
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class ColumnTable(val config: Config) extends CassandraTable[ColumnTable, DataColumn]
with FiloCassandraConnector {
  override val tableName = "columns"
  val keyspace = config.getString("admin-keyspace")
  implicit val ks = KeySpace(keyspace)

  // scalastyle:off
  object database extends StringColumn(this) with PartitionKey[String]
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

  def getSchema(dataset: DatasetRef, version: Int): Future[Column.Schema] = {
    val enum = select.where(_.dataset eqs dataset.dataset)
                     .and(_.database eqs dataset.database.getOrElse(defaultKeySpace))
                     .and(_.version lte version)
                     .fetchEnumerator()
    (enum run Iteratee.fold(Column.EmptySchema)(Column.schemaFold)).handleErrors
  }

  def insertColumns(columns: Seq[DataColumn],
                    dataset: DatasetRef): Future[Response] = {
    val batch = columns.foldLeft(Batch.unlogged) { case (batch, column) =>
      batch.add(insert.value(_.dataset, dataset.dataset)
                      .value(_.database, dataset.database.getOrElse(defaultKeySpace))
                      .value(_.name,    column.name)
                      .value(_.version, column.version)
                      .value(_.id,      column.id)
                      .value(_.columnType, column.columnType.toString)
                      .value(_.isDeleted, column.isDeleted)
                      .ifNotExists)
    }
    batch.future().toResponse(AlreadyExists)
  }

  def deleteDataset(dataset: DatasetRef): Future[Response] =
    delete.where(_.dataset eqs dataset.dataset)
          .and(_.database eqs dataset.database.getOrElse(defaultKeySpace))
          .future().toResponse()
}