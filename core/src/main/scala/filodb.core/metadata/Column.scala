package filodb.core.metadata

/**
 * Identifies a Column of data for a particular version of a dataset.
 */
case class Column(name: String,
                  dataset: String,
                  version: Int,
                  columnType: Column.ColumnType,
                  serializer: Column.Serializer = Column.FiloSerializer,
                  isDeleted: Boolean = false,
                  isSystem: Boolean = false)

object Column {
  sealed trait ColumnType
  case object IntColumn extends ColumnType
  case object LongColumn extends ColumnType
  case object DoubleColumn extends ColumnType
  case object StringColumn extends ColumnType

  sealed trait Serializer
  case object FiloSerializer extends Serializer

  /**
   * Set of low-level column commands to send to a datastore actor for I/O
   */
  object Command {
    /**
     * Creates a new column for a particular dataset and version.
     * @param column the new Column to create.
     * @returns Created if succeeds, or AlreadyExists
     */
    case class NewColumn(column: Column)

    /**
     * Get all the Columns for a range of versions of a dataset.
     * Deleted columns are not returned.
     * @param dataset the name of the dataset to return column info for
     * @param minVersion the starting version number to query from, must be >= 0
     * @param maxVersion the ending version number to query from
     * @returns a ColumnMap, column name -> Column's in order of asc version #
     */
    case class GetColumns(dataset: String, minVersion: Int, maxVersion: Int)

    /**
     * Marks a column as deleted.  TODO: does this also delete the data?
     */
    case class DeleteColumn(dataset: String, version: Int, name: String)
  }

  /**
   * Set of responses from dataset commands
   */
  object Response {
    case object AlreadyExists
    case object NotFound
    case object Created
    case object Deleted
    case class ColumnMap(columnMap: Map[String, Seq[Column]])
  }
}