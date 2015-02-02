package filodb.core.metadata

import com.typesafe.scalalogging.slf4j.StrictLogging

import filodb.core.messages.{Command, Response}

/**
 * Defines a column of data and its properties.
 *
 * ==Columns and versions==
 * It is uncommon for the schema to change much between versions.
 * A DDL might change the type of a column; a column might be deleted;
 *
 * 1. A Column def remains in effect for subsequent versions unless;
 * 2. It has been marked deleted in subsequent versions, or
 * 3. Its columnType or serializer has been changed, in which case data
 *    from previous versions are discarded (due to incompatibility)
 *
 * ==System Columns and Names==
 * Column names starting with a colon (':') are reserved for "system" columns:
 * - ':deleted' - special bitmap column marking deleted rows
 * - ':inherited' - special bitmap column, 1 means this row was inherited from
 *   previous versions due to a chunk operation, but doesn't belong to this version
 *
 */
case class Column(name: String,
                  dataset: String,
                  version: Int,
                  columnType: Column.ColumnType,
                  serializer: Column.Serializer = Column.FiloSerializer,
                  isDeleted: Boolean = false,
                  isSystem: Boolean = false) {
  /**
   * Has one of the properties other than name, dataset, version changed?
   * (Name and dataset have to be constant for comparison to even be valid)
   * (Since name has to be constant, can't change from system to nonsystem)
   */
  def propertyChanged(other: Column): Boolean =
    (columnType != other.columnType) ||
    (serializer != other.serializer) ||
    (isDeleted != other.isDeleted)
}

object Column extends StrictLogging {
  sealed trait ColumnType
  case object IntColumn extends ColumnType
  case object LongColumn extends ColumnType
  case object DoubleColumn extends ColumnType
  case object StringColumn extends ColumnType
  case object BitmapColumn extends ColumnType

  sealed trait Serializer
  case object FiloSerializer extends Serializer

  type Schema = Map[String, Column]

  /**
   * Fold function used to compute a schema up from a list of Column instances.
   * Assumes the list of columns is sorted in increasing version.
   * Contains the business rules above.
   */
  def schemaFold(schema: Schema, newColumn: Column): Schema = {
    if (newColumn.isDeleted) { schema - newColumn.name }
    else if (schema.contains(newColumn.name)) {
      // See if newColumn changed anything from older definition
      if (newColumn.propertyChanged(schema(newColumn.name))) {
        schema ++ Map(newColumn.name -> newColumn)
      } else {
        logger.warn(s"Skipping illegal or redundant column definition: $newColumn")
        schema
      }
    } else {
      // really a new column definition, just add it
      schema ++ Map(newColumn.name -> newColumn)
    }
  }

  /**
   * Checks for any problems with a column about to be "created" or changed.
   * @param dataset the name of the dataset for which this column change applies
   * @param schema the latest schema from scanning all versions
   * @param column the Column to be validated
   * @return A Seq[String] of every reason the column is not valid
   */
  def invalidateNewColumn(dataset: String, schema: Schema, column: Column): Seq[String] = {
    def check(requirement: => Boolean, failMessage: String): Option[String] =
      if (requirement) None else Some(failMessage)

    val startsWithColon = column.name.startsWith(":")
    val alreadyHaveIt = schema contains column.name
    Seq(
      check((column.isSystem && startsWithColon) || (!column.isSystem && !startsWithColon),
            "Only system columns can start with a colon"),
      check(!alreadyHaveIt || (alreadyHaveIt && column.version > schema(column.name).version),
            "Cannot add column at version lower than latest definition"),
      check(!alreadyHaveIt || (alreadyHaveIt && column.propertyChanged(schema(column.name))),
            "Nothing changed from previous definition"),
      check(alreadyHaveIt || (!alreadyHaveIt && !column.isDeleted), "New column cannot be deleted")
    ).flatten
  }

  /**
   * Set of column commands to send to a datastore actor for I/O
   */

  /**
   * Creates a new column for a particular dataset and effective version.
   * Can also be used to change the column type by "creating" the same column with changes for a higher
   * version.  Note that changes for a column must have an effective version higher than the last change.
   * See the notes above regarding columns and versioning.
   * @param column the new Column to create.
   * @return Created if succeeds, or AlreadyExists, or IllegalColumnChange
   */
  case class NewColumn(column: Column) extends Command

  /**
   * Get the schema for a version of a dataset.  This scans all defined columns from the first version
   * on up to figure out the changes. Deleted columns are not returned.
   * @param dataset the name of the dataset to return the schema for
   * @param version the version of the dataset to return the schema for
   * @return a Schema, column name -> Column definition
   */
  case class GetSchema(dataset: String, version: Int) extends Command

  /**
   * Marks a column as deleted.  This is more like "hiding" a column and does not actually delete the data --
   * rather it marks a column as deleted starting at version Y, and reads will no longer return data for that
   * column.
   * Also, this cannot be done to the current / latest version, it must be a new version.
   */
  case class DeleteColumn(dataset: String, version: Int, name: String) extends Command

  /**
   * Set of responses from dataset commands
   */
  case class IllegalColumnChange(reason: String) extends Response
  case class TheSchema(schema: Schema) extends Response
}