package filodb.core.query

import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.Snakecase
import org.scalactic._
import org.velvia.filo.{FiloVector, BinaryVector, ArrayStringRowReader, RowReader}
import scala.reflect.ClassTag

import filodb.core.metadata._
import filodb.core.store.ChunkScanMethod

/**
 * An Aggregate is a class that can do aggregations by folding on ChunkSetReader's
 */
abstract class Aggregate[R: ClassTag] {
  def add(reader: ChunkSetReader): Aggregate[R]
  def result: Array[R]
  def clazz: Class[_] = implicitly[ClassTag[R]].runtimeClass

  /**
   * If an aggregate can provide an appropriate scan method to aid in pushdown filtering, then it should
   * return something other than None here.
   */
  def chunkScan(projection: RichProjection): Option[ChunkScanMethod] = None
}

class PartitionKeysAggregate extends Aggregate[String] {
  private final val partkeys = new collection.mutable.HashSet[String]
  def add(reader: ChunkSetReader): Aggregate[String] = {
    partkeys += reader.partition.toString
    this
  }
  def result: Array[String] = partkeys.toArray
}

/**
 * An AggregationFunction validates input arguments and produces an Aggregate for computation
 * It is also an EnumEntry and the name of the function is the class name in "snake_case" ie underscores
 */
sealed trait AggregationFunction extends FunctionValidationHelpers with EnumEntry with Snakecase {
  // validate the arguments against the projection and produce an Aggregate of the right type,
  // as well as the indices of the columns to read from the projection
  def validate(args: Seq[String], proj: RichProjection): (Aggregate[_], Seq[Int]) Or InvalidFunctionSpec

  def parseInt(reader: RowReader, colNo: Int): Int Or InvalidFunctionSpec =
    try { Good(reader.getInt(colNo)) }
    catch {
      case t: Throwable => Bad(BadArgument(s"Could not parse [${reader.getString(colNo)}]: ${t.getMessage}"))
    }

  def parseLong(reader: RowReader, colNo: Int): Long Or InvalidFunctionSpec =
    try { Good(reader.getLong(colNo)) }
    catch {
      case t: Throwable => Bad(BadArgument(s"Could not parse [${reader.getString(colNo)}]: ${t.getMessage}"))
    }
}

trait SingleColumnAggFunction extends AggregationFunction {
  def allowedTypes: Set[Column.ColumnType]
  def makeAggregate(colIndex: Int, colType: Column.ColumnType): Aggregate[_]

  def validate(args: Seq[String], proj: RichProjection): (Aggregate[_], Seq[Int]) Or InvalidFunctionSpec = {
    for { args <- validateNumArgs(args, 1)
          sourceColIndex <- columnIndex(proj.columns, args(0))
          sourceColType <- validatedColumnType(proj.columns, sourceColIndex, allowedTypes) }
    yield {
      // the aggregate always uses column index 0 because we will request only one column on read.
      // This logic will work so long as we are not doing compound reads
      (makeAggregate(0, sourceColType), Seq(sourceColIndex))
    }
  }
}

/**
 * Time grouping aggregates take 5 arguments:
 *   <timeColumn> <valueColumn> <startTs> <endTs> <numBuckets>
 *   <startTs> and <endTs> may either be Longs representing millis since epoch, or ISO8601 formatted datetimes.
 */
trait TimeGroupingAggFunction extends AggregationFunction {
  import Column.ColumnType._

  def allowedTypes: Set[Column.ColumnType]
  def makeAggregate(colType: Column.ColumnType, startTs: Long, endTs: Long, numBuckets: Int): Aggregate[_]

  def validate(args: Seq[String], proj: RichProjection): (Aggregate[_], Seq[Int]) Or InvalidFunctionSpec = {
    for { args <- validateNumArgs(args, 5)
          timeColIndex <- columnIndex(proj.columns, args(0))
          timeColType  <- validatedColumnType(proj.columns, timeColIndex, Set(LongColumn, TimestampColumn))
          valueColIndex <- columnIndex(proj.columns, args(1))
          valueColType  <- validatedColumnType(proj.columns, valueColIndex, allowedTypes)
          arrayReader = ArrayStringRowReader(args.toArray)
          startTs      <- parseLong(arrayReader, 2)
          endTs        <- parseLong(arrayReader, 3)
          numBuckets   <- parseInt(arrayReader, 4) }
    yield {
      // Assumption: we only read time and value columns
      (makeAggregate(valueColType, startTs, endTs, numBuckets), Seq(timeColIndex, valueColIndex))
    }
  }
}

/**
 * Defines all of the valid aggregation functions.  The function name is the case object in snake_case
 */
object AggregationFunction extends Enum[AggregationFunction] {
  import Column.ColumnType._
  val values = findValues

  // partition_keys returns all the partition keys (or time series) in a query
  case object PartitionKeys extends AggregationFunction {
    def validate(args: Seq[String], proj: RichProjection): (Aggregate[_], Seq[Int]) Or InvalidFunctionSpec =
      Good((new PartitionKeysAggregate, Nil))
  }

  case object Sum extends SingleColumnAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregate(colIndex: Int, colType: Column.ColumnType): Aggregate[_] = colType match {
      case DoubleColumn => new SumDoublesAggregate(colIndex)
      case o: Any       => ???
    }
  }

  case object Count extends SingleColumnAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Column.ColumnType.values.toSet
    def makeAggregate(colIndex: Int, colType: Column.ColumnType): Aggregate[_] =
      new CountingAggregate(colIndex)
  }

  case object TimeGroupMin extends TimeGroupingAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregate(colType: Column.ColumnType,
                      startTs: Long, endTs: Long, buckets: Int): Aggregate[_] = colType match {
      case DoubleColumn => new TimeGroupingMinDoubleAgg(0, 1, startTs, endTs, buckets)
      case o: Any       => ???
    }
  }

  case object TimeGroupMax extends TimeGroupingAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregate(colType: Column.ColumnType,
                      startTs: Long, endTs: Long, buckets: Int): Aggregate[_] = colType match {
      case DoubleColumn => new TimeGroupingMaxDoubleAgg(0, 1, startTs, endTs, buckets)
      case o: Any       => ???
    }
  }

  case object TimeGroupAvg extends TimeGroupingAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregate(colType: Column.ColumnType,
                      startTs: Long, endTs: Long, buckets: Int): Aggregate[_] = colType match {
      case DoubleColumn => new TimeGroupingAvgDoubleAgg(0, 1, startTs, endTs, buckets)
      case o: Any       => ???
    }
  }
}
