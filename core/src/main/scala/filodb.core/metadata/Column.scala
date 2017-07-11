package filodb.core.metadata

import com.typesafe.scalalogging.StrictLogging
import enumeratum.{Enum, EnumEntry}
import org.velvia.filo.{VectorInfo, ZeroCopyUTF8String}
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.reflect.ClassTag

import filodb.core.{CompositeKeyType, KeyType, SingleKeyType, SingleKeyTypeBase}
import filodb.core.Types._

/**
 * Defines a column of data and its properties.
 *
 * ==Columns and versions==
 * It is uncommon for the schema to change much between versions.
 * A DDL might change the type of a column; a column might be deleted
 *
 * 1. A Column def remains in effect for subsequent versions unless
 * 2. It has been marked deleted in subsequent versions, or
 * 3. Its columnType or serializer has been changed, in which case data
 *    from previous versions are discarded (due to incompatibility)
 *
 * ==System Columns and Names==
 * Column names starting with a colon (':') are reserved for "system" and computed columns:
 * - ':deleted' - special bitmap column marking deleted rows
 * - other columns are computed columns whose values are generated from other columns or a function
 *
 * System columns are not actually stored.
 */
trait Column {
  def id: Int         // Every column must have a unique ID
  def name: String
  def dataset: String
  def columnType: Column.ColumnType
  def extractor: TypedFieldExtractor[_]

  // More type safe than just using ==, if we ever change the type of ColumnId
  // TODO(velvia): remove this and just use id
  def hasId(_id: ColumnId): Boolean = name == _id
}

/**
 * A Column that holds real data.
 */
case class DataColumn(id: Int,
                      name: String,
                      dataset: String,
                      version: Int,
                      columnType: Column.ColumnType,
                      isDeleted: Boolean = false) extends Column {
  /**
   * Has one of the properties other than name, dataset, version changed?
   * (Name and dataset have to be constant for comparison to even be valid)
   * (Since name has to be constant, can't change from system to nonsystem)
   */
  def propertyChanged(other: DataColumn): Boolean =
    (columnType != other.columnType) ||
    (isDeleted != other.isDeleted)

  // Use this for efficient serialization over the wire.
  // We leave out the dataset because that is almost always inferred from context.
  // NOTE: this is one reason why column names cannot have commas
  override def toString: String =
    s"[$id,$name,$version,$columnType${if (isDeleted) ",t" else ""}]"

  def extractor: TypedFieldExtractor[_] = columnType.keyType.extractor
}

object DataColumn {
  /**
   * Recreates a DataColumn from its toString output
   */
  def fromString(str: String, dataset: String): DataColumn = {
    val parts = str.drop(1).dropRight(1).split(',')
    DataColumn(parts(0).toInt, parts(1), dataset, parts(2).toInt,
               Column.ColumnType.withName(parts(3)),
               parts.size > 4)
  }
}

object Column extends StrictLogging {
  sealed trait ColumnType extends EnumEntry {
    // NOTE: due to a Spark serialization bug, this cannot be a val
    // (https://github.com/apache/spark/pull/7122)
    def clazz: Class[_]
    def keyType: SingleKeyTypeBase[_]
  }

  sealed abstract class RichColumnType[T : ClassTag : SingleKeyTypeBase] extends ColumnType {
    def clazz: Class[_] = implicitly[ClassTag[T]].runtimeClass
    def keyType: SingleKeyTypeBase[_] = implicitly[SingleKeyTypeBase[T]]
  }

  object ColumnType extends Enum[ColumnType] {
    val values = findValues

    import filodb.core.SingleKeyTypes._
    case object IntColumn extends RichColumnType[Int]
    case object LongColumn extends RichColumnType[Long]
    case object DoubleColumn extends RichColumnType[Double]
    case object StringColumn extends RichColumnType[ZeroCopyUTF8String]
    case object BitmapColumn extends RichColumnType[Boolean]
    case object TimestampColumn extends RichColumnType[Long]
  }

  type Schema = Map[String, DataColumn]
  val EmptySchema = Map.empty[String, DataColumn]

  /**
   * Converts a list of columns to the appropriate KeyType.
   * @return a KeyType
   */
  def columnsToKeyType(columns: Seq[Column]): KeyType = columns match {
    case Nil      => throw new IllegalArgumentException("Empty columns supplied")
    case Seq(DataColumn(_, _, _, _, columnType, _))  => columnType.keyType
    case Seq(ComputedColumn(_, _, _, columnType, _, _)) => columnType.keyType
    case cols: Seq[Column] =>
      val keyTypes = cols.map { col => columnsToKeyType(Seq(col)).asInstanceOf[SingleKeyType] }
      CompositeKeyType(keyTypes)
  }

  /**
   * Converts a list of data columns to Filo VectorInfos for building Filo vectors
   */
  def toFiloSchema(columns: Seq[Column]): Seq[VectorInfo] = columns.collect {
    case DataColumn(_, name, _, _, colType, false) => VectorInfo(name, colType.clazz)
  }

  /**
   * Fold function used to compute a schema up from a list of DataColumn instances.
   * Assumes the list of columns is sorted in increasing version.
   * Contains the business rules above.
   */
  def schemaFold(schema: Schema, newColumn: DataColumn): Schema = {
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
   * @param schema the latest schema from scanning all versions
   * @param column the DataColumn to be validated
   * @return A Seq[String] of every reason the column is not valid
   */
  def invalidateNewColumn(schema: Schema, column: DataColumn): Seq[String] = {
    def check(requirement: => Boolean, failMessage: String): Option[String] =
      if (requirement) None else Some(failMessage)

    import scala.language.postfixOps
    val illicitCharsRegex = "[:() ,\u0001]+"r
    val alreadyHaveIt = schema contains column.name
    Seq(
      // check(!startsWithColon, "Data columns cannot start with a colon"),
      illicitCharsRegex.findFirstMatchIn(column.name).map(x => s"Illegal char $x found in column name"),
      check(!alreadyHaveIt || (alreadyHaveIt && column.version > schema(column.name).version),
            "Cannot add column at version lower than latest definition"),
      check(!alreadyHaveIt || (alreadyHaveIt && column.propertyChanged(schema(column.name))),
            "Nothing changed from previous definition"),
      check(alreadyHaveIt || (!alreadyHaveIt && !column.isDeleted), "New column cannot be deleted")
    ).flatten
  }
}