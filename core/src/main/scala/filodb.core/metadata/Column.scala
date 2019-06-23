package filodb.core.metadata

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import enumeratum.{Enum, EnumEntry}
import org.scalactic._

import filodb.core._
import filodb.core.SingleKeyTypes._
import filodb.core.Types._
import filodb.memory.format.{vectors => bv, VectorInfo, ZeroCopyUTF8String}
import filodb.memory.format.RowReader.TypedFieldExtractor

/**
 * Defines a column of data and its type and other properties
 */
trait Column {
  def id: Int         // Every column must have a unique ID
  def name: String
  def columnType: Column.ColumnType
  def extractor: TypedFieldExtractor[_]
  def params: Config

  // More type safe than just using ==, if we ever change the type of ColumnId
  // TODO(velvia): remove this and just use id
  def hasId(_id: ColumnId): Boolean = name == _id

  def toStringNotation: String = {
    val paramStrs = params.entrySet.asScala.map { e => s"${e.getKey}=${e.getValue.render}" }
    s"$name:${columnType.typeName}:${paramStrs.mkString(":")}"
  }
}

/**
 * A Column that holds real data.
 */
case class DataColumn(id: Int,
                      name: String,
                      columnType: Column.ColumnType,
                      params: Config = ConfigFactory.empty) extends Column {

  // Use this for efficient serialization over the wire.
  // We leave out the dataset because that is almost always inferred from context.
  // NOTE: this is one reason why column names cannot have commas
  override def toString: String = {
    val paramStrs = params.entrySet.asScala.map { e => s"${e.getKey}=${e.getValue.render}" }
    (Seq(id, name, columnType).map(_.toString) ++ paramStrs).mkString("[", ",", "]")
  }

  def extractor: TypedFieldExtractor[_] = columnType.keyType.extractor
}

object DataColumn {
  /**
   * Recreates a DataColumn from its toString output
   */
  def fromString(str: String): DataColumn = {
    val parts = str.drop(1).dropRight(1).split(',')
    val params = ConfigFactory.parseString(parts.drop(3).mkString("\n"))
    DataColumn(parts(0).toInt, parts(1), Column.ColumnType.withName(parts(2)), params)
  }
}

object Column extends StrictLogging {
  import Dataset._
  import TrySugar._

  sealed trait ColumnType extends EnumEntry {
    def typeName: String
    // NOTE: due to a Spark serialization bug, this cannot be a val
    // (https://github.com/apache/spark/pull/7122)
    def clazz: Class[_]
    def keyType: SingleKeyTypeBase[_]

    /**
     * Validates the params found in the column definition.  By default no checking is done.
     */
    def validateParams(params: Config): Unit Or One[BadSchema] = Good(())
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
    case object TimestampColumn extends RichColumnType[Long]("ts")
    case object MapColumn extends RichColumnType[UTF8Map]("map")
    case object BinaryRecordColumn extends RichColumnType[ZeroCopyUTF8String]("br")
    // HistogramColumn requires the following params:
    //    counter=[true,false]    # If true, histogram values increase over time (eg Prometheus histograms)
    case object HistogramColumn extends RichColumnType[bv.Histogram]("hist") {
      override def validateParams(params: Config): Unit Or One[BadSchema] =
        Try({
          params.getBoolean("counter");
          ()    // needed to explicitly return Unit
        }).toOr.badMap(x => One(BadColumnParams(x.toString)))
    }
  }

  val typeNameToColType = ColumnType.values.map { colType => colType.typeName -> colType }.toMap

  val clazzToColType: Map[Class[_], ColumnType] = ColumnType.values.map { colType => colType.getClass -> colType }.toMap

  /**
   * Converts a list of columns to the appropriate KeyType.
   * @return a KeyType
   */
  def columnsToKeyType(columns: Seq[Column]): KeyType = columns match {
    case Nil      => throw new IllegalArgumentException("Empty columns supplied")
    case Seq(DataColumn(_, _, columnType, _)         )  => columnType.keyType
    case Seq(ComputedColumn(_, _, _, columnType, _, _)) => columnType.keyType
    case cols: Seq[Column] =>
      val keyTypes = cols.map { col => columnsToKeyType(Seq(col)).asInstanceOf[SingleKeyType] }
      CompositeKeyType(keyTypes)
  }

  /**
   * Converts a list of data columns to Filo VectorInfos for building Filo vectors
   */
  def toFiloSchema(columns: Seq[Column]): Seq[VectorInfo] = columns.collect {
    case DataColumn(_, name, colType, _) => VectorInfo(name, colType.clazz)
  }

  import OptionSugar._

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
  def validateColumn(name: String, typeName: String, nextId: Int, params: Config): DataColumn Or One[BadSchema] =
    for { nothing <- validateColumnName(name)
          colType <- typeNameToColType.get(typeName).toOr(One(BadColumnType(typeName))) }
    yield { DataColumn(nextId, name, colType, params) }

  import Accumulation._

  /**
   * Creates and validates a set of DataColumns from a list of "columnName:columnType:params" strings.
   * Validation errors (in the form of BadSchema) are returned for malformed input, illegal column names,
   * unknown types, or params that don't parse correctly.  Some column types (such as histogram) have required params.
   * @param nameTypeList a Seq of "columnName:columnType:params" strings. Valid types are in ColumnType
   * @param startingId   column IDs are assigned starting with startingId incrementally
   */
  def makeColumnsFromNameTypeList(nameTypeList: Seq[String], startingId: Int = 0): Seq[Column] Or BadSchema =
    nameTypeList.zipWithIndex.map { case (nameType, idx) =>
      val parts = nameType.split(':')
      for { nameAndType <- if (parts.size >= 2) Good((parts(0), parts(1))) else Bad(One(NotNameColonType(nameType)))
            params      <- Try(ConfigFactory.parseString(parts.drop(2).mkString("\n"))).toOr
                                            .badMap(x => One(BadColumnParams(x.toString)))
            (name, colType) = nameAndType
            col         <- validateColumn(name, colType, startingId + idx, params)
            _           <- col.columnType.validateParams(params) }
      yield { col }
    }.combined.badMap { errs => ColumnErrors(errs.toSeq) }
}