package filodb.core.metadata

import scala.reflect.ClassTag

import com.typesafe.scalalogging.StrictLogging
import enumeratum.{Enum, EnumEntry}
import org.scalactic._

import filodb.core._
import filodb.core.SingleKeyTypes._
import filodb.core.Types._
import filodb.memory.format.{VectorInfo, ZeroCopyUTF8String}
import filodb.memory.format.RowReader.TypedFieldExtractor

/**
 * Defines a column of data and its type and other properties
 */
trait Column {
  def id: Int         // Every column must have a unique ID
  def name: String
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
                      columnType: Column.ColumnType) extends Column {
  // Use this for efficient serialization over the wire.
  // We leave out the dataset because that is almost always inferred from context.
  // NOTE: this is one reason why column names cannot have commas
  override def toString: String =
    s"[$id,$name,$columnType]"

  def extractor: TypedFieldExtractor[_] = columnType.keyType.extractor
}

object DataColumn {
  /**
   * Recreates a DataColumn from its toString output
   */
  def fromString(str: String): DataColumn = {
    val parts = str.drop(1).dropRight(1).split(',')
    DataColumn(parts(0).toInt, parts(1), Column.ColumnType.withName(parts(2)))
  }
}

object Column extends StrictLogging {
  sealed trait ColumnType extends EnumEntry {
    def typeName: String
    // NOTE: due to a Spark serialization bug, this cannot be a val
    // (https://github.com/apache/spark/pull/7122)
    def clazz: Class[_]
    def keyType: SingleKeyTypeBase[_]
  }

  sealed abstract class RichColumnType[T : ClassTag : SingleKeyTypeBase](val typeName: String)
  extends ColumnType {
    def clazz: Class[_] = implicitly[ClassTag[T]].runtimeClass
    def keyType: SingleKeyTypeBase[_] = implicitly[SingleKeyTypeBase[T]]
  }

  object ColumnType extends Enum[ColumnType] {
    val values = findValues

    case object IntColumn extends RichColumnType[Int]("int")
    case object LongColumn extends RichColumnType[Long]("long")
    case object DoubleColumn extends RichColumnType[Double]("double")
    case object StringColumn extends RichColumnType[ZeroCopyUTF8String]("string")
    case object BitmapColumn extends RichColumnType[Boolean]("bitmap")
    case object TimestampColumn extends RichColumnType[Long]("ts")
    case object MapColumn extends RichColumnType[UTF8Map]("map")
    case object BinaryRecordColumn extends RichColumnType[ZeroCopyUTF8String]("br")
  }

  val typeNameToColType = ColumnType.values.map { colType => colType.typeName -> colType }.toMap

  val clazzToColType: Map[Class[_], ColumnType] = ColumnType.values.map { colType => colType.getClass -> colType }.toMap

  /**
   * Converts a list of columns to the appropriate KeyType.
   * @return a KeyType
   */
  def columnsToKeyType(columns: Seq[Column]): KeyType = columns match {
    case Nil      => throw new IllegalArgumentException("Empty columns supplied")
    case Seq(DataColumn(_, _, columnType))  => columnType.keyType
    case Seq(ComputedColumn(_, _, _, columnType, _, _)) => columnType.keyType
    case cols: Seq[Column] =>
      val keyTypes = cols.map { col => columnsToKeyType(Seq(col)).asInstanceOf[SingleKeyType] }
      CompositeKeyType(keyTypes)
  }

  /**
   * Converts a list of data columns to Filo VectorInfos for building Filo vectors
   */
  def toFiloSchema(columns: Seq[Column]): Seq[VectorInfo] = columns.collect {
    case DataColumn(_, name, colType) => VectorInfo(name, colType.clazz)
  }

  import OptionSugar._

  import Dataset._
  val illicitCharsRegex = "[:() ,\u0001]+"r

  /**
   * Validates the column name -- make sure it doesn't have any illegal chars
   */
  def validateColumnName(colName: String): Unit Or One[BadSchema] =
    illicitCharsRegex.findFirstMatchIn(colName)
      .toOr(()).swap
      .badMap(x => One(BadColumnName(colName, s"Illegal char $x found")))

  /**
   * Validates the column name and type and returns a DataColumn instance
   * @param name the column name
   * @param typeName the column type string, see ColumnType for valid values
   * @param nextId the next column ID to use
   * @return Good(DataColumn) or Bad(BadSchema)
   */
  def validateColumn(name: String, typeName: String, nextId: Int): DataColumn Or One[BadSchema] =
    for { nothing <- validateColumnName(name)
          colType <- typeNameToColType.get(typeName).toOr(One(BadColumnType(typeName))) }
    yield { DataColumn(nextId, name, colType) }

  import Accumulation._

  /**
   * Creates and validates a set of DataColumns from a list of "columnName:columnType" strings
   * @param nameTypeList a Seq of "columnName:columnType" strings. Valid types are in ColumnType
   * @param startingId   column IDs are assigned starting with startingId incrementally
   */
  def makeColumnsFromNameTypeList(nameTypeList: Seq[String], startingId: Int = 0): Seq[Column] Or BadSchema =
    nameTypeList.zipWithIndex.map { case (nameType, idx) =>
      val parts = nameType.split(':')
      for { nameAndType <- if (parts.size == 2) Good((parts(0), parts(1))) else Bad(One(NotNameColonType(nameType)))
            (name, colType) = nameAndType
            col         <- validateColumn(name, colType, startingId + idx) }
      yield { col }
    }.combined.badMap { errs => ColumnErrors(errs.toSeq) }
}