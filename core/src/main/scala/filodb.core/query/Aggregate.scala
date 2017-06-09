package filodb.core.query

import enumeratum.EnumEntry.Snakecase
import enumeratum.{Enum, EnumEntry}
import monix.eval.Task
import org.scalactic._
import org.velvia.filo.{FiloVector, BinaryVector, ArrayStringRowReader, RowReader}
import scala.reflect.{classTag, ClassTag}
import scalaxy.loops._

import filodb.core.metadata._
import filodb.core.store.{ChunkScanMethod, ChunkSetInfo}
import filodb.core.binaryrecord.BinaryRecord

/**
 * An Aggregate stores intermediate results from Aggregators, which can later be combined using
 * Combinators or Operators
 */
abstract class Aggregate[R: ClassTag] {
  def result: Array[R]
  def clazz: Class[_] = implicitly[ClassTag[R]].runtimeClass
  override def toString: String = s"${getClass.getName}[${result.toList.mkString(", ")}]"
}

// Immutable aggregate for simple values
class PrimitiveSimpleAggregate[@specialized R: ClassTag](val data: R) extends Aggregate[R] {
  def result: Array[R] = Array(data)
}

trait NumericAggregate {
  def doubleValue: Double
}

final case class DoubleAggregate(value: Double)
extends PrimitiveSimpleAggregate(value) with NumericAggregate {
  val doubleValue = value
}

final case class IntAggregate(value: Int)
extends PrimitiveSimpleAggregate(value) with NumericAggregate {
  val doubleValue = value.toDouble
}

final case class DoubleSeriesPoint(timestamp: Long, value: Double)
final case class DoubleSeriesValues(partitionName: String, points: Seq[DoubleSeriesPoint])

// This Aggregate is designed to be mutable for high performance and low allocation cost
class ArrayAggregate[@specialized(Int, Long, Double) R: ClassTag](size: Int,
                                                                  value: R) extends Aggregate[R] {
  val result = Array.fill(size)(value)
}

final case class ListAggregate[R: ClassTag](values: Seq[R] = Nil) extends Aggregate[R] {
  val result = values.toArray
  def add(other: ListAggregate[R]): ListAggregate[R] = ListAggregate(values ++ other.values)
}

/**
 * An Aggregator knows how to compute Aggregates from raw data/chunks, and combine them,
 * for a specific query.
 */
trait Aggregator {
  type A <: Aggregate[_]

  def aggPartition(infosSkips: ChunkSetInfo.InfosSkipsIt, partition: FiloPartition): A

  // NOTE: This is called at the beginning of every new partition.  Make sure you return a NEW instance
  // every time, especially if that aggregate is mutable, such as the ArrayAggregate -- unless you are
  // sure that the aggregate and aggregation logic is immutable, in which case using a val is fine.
  def emptyAggregate: A

  // Combines aggregates from individual chunksets. Also may be used by some combiners to combine aggregates
  // from individual partitions.
  def combine(first: A, second: A): A

  // If false, then combine() cannot be used and one should default to accumulating individual results
  // (or possibly things like topK)
  def isCombinable: Boolean = true

  /**
   * If an aggregate can provide an appropriate scan method to aid in pushdown filtering, then it should
   * return something other than None here.
   */
  def chunkScan(projection: RichProjection): Option[ChunkScanMethod] = None
}

// An Aggregator that produces a single value that cannot be combined.
trait OneValueAggregator extends Aggregator {
  type R   // the inner value
  type A = PrimitiveSimpleAggregate[R]
  def tag: ClassTag[R]
  override def isCombinable: Boolean = false
  def emptyAggregate: A = ???
  def combine(first: A, second: A): A = ???
}

trait ChunkAggregator extends Aggregator {
  def add(orig: A, reader: ChunkSetReader): A
  def positions: Array[Int]

  def aggPartition(infosSkips: ChunkSetInfo.InfosSkipsIt, partition: FiloPartition): A =
    partition.readers(infosSkips, positions).foldLeft(emptyAggregate) {
      case (agg, reader) => add(agg, reader)
    }
}

class PartitionKeysAggregator extends OneValueAggregator {
  type R = String
  val tag = classTag[String]
  def aggPartition(infosSkips: ChunkSetInfo.InfosSkipsIt, partition: FiloPartition):
      PrimitiveSimpleAggregate[String] = new PrimitiveSimpleAggregate(partition.stringPartition)
}

class LastDoubleValueAggregator(timestampIndex: Int, doubleColIndex: Int) extends OneValueAggregator {
  type R = DoubleSeriesValues
  val tag = classTag[DoubleSeriesValues]
  def aggPartition(infosSkips: ChunkSetInfo.InfosSkipsIt, partition: FiloPartition):
      PrimitiveSimpleAggregate[DoubleSeriesValues] = {
    val lastVectors = partition.lastVectors
    val timestampVector = lastVectors(timestampIndex).asInstanceOf[FiloVector[Long]]
    val doubleVector = lastVectors(doubleColIndex).asInstanceOf[FiloVector[Double]]
    val point = DoubleSeriesPoint(timestampVector(timestampVector.length - 1),
                                  doubleVector(doubleVector.length - 1))
    new PrimitiveSimpleAggregate(DoubleSeriesValues(partition.stringPartition, Seq(point)))
  }
}

/**
 * An AggregationFunction validates input arguments and produces an Aggregate for computation
 * It is also an EnumEntry and the name of the function is the class name in "snake_case" ie underscores
 */
sealed trait AggregationFunction extends FunctionValidationHelpers with EnumEntry with Snakecase {
  // validate the arguments against the projection and produce an Aggregate of the right type,
  // as well as the indices of the columns to read from the projection
  def validate(args: Seq[String], proj: RichProjection): Aggregator Or InvalidFunctionSpec
}

trait SingleColumnAggFunction extends AggregationFunction {
  def allowedTypes: Set[Column.ColumnType]
  def makeAggregator(colIndex: Int, colType: Column.ColumnType): Aggregator

  def validate(args: Seq[String], proj: RichProjection): Aggregator Or InvalidFunctionSpec = {
    for { args <- validateNumArgs(args, 1)
          sourceColIndex <- columnIndex(proj.nonPartitionColumns, args(0))
          sourceColType <- validatedColumnType(proj.nonPartitionColumns, sourceColIndex, allowedTypes) }
    yield {
      makeAggregator(sourceColIndex, sourceColType)
    }
  }
}

trait TimeAggFunction extends AggregationFunction {
  import Column.ColumnType._
  import filodb.core.SingleKeyTypes._

  def allowedTypes: Set[Column.ColumnType]
  def makeAggregator(timeColIndex: Int, valueColIndex: Int, colType: Column.ColumnType): Aggregator

  def validate(args: Seq[String], proj: RichProjection): Aggregator Or InvalidFunctionSpec = {
    for { args <- validateNumArgs(args, 2)
          timeColIndex <- columnIndex(proj.nonPartitionColumns, args(0))
          timeColType  <- validatedColumnType(proj.nonPartitionColumns, timeColIndex, Set(LongColumn, TimestampColumn))
          valueColIndex <- columnIndex(proj.nonPartitionColumns, args(1))
          valueColType  <- validatedColumnType(proj.nonPartitionColumns, valueColIndex, allowedTypes) }
    yield { makeAggregator(timeColIndex, valueColIndex, valueColType) }
  }
}

/**
 * Time grouping aggregates take 5 arguments:
 *   <timeColumn> <valueColumn> <startTs> <endTs> <numBuckets>
 *   <startTs> and <endTs> may either be Longs representing millis since epoch, or ISO8601 formatted datetimes.
 */
trait TimeGroupingAggFunction extends AggregationFunction {
  import Column.ColumnType._
  import filodb.core.SingleKeyTypes._

  def allowedTypes: Set[Column.ColumnType]
  def makeAggregator(timeColIndex: Int, valueColIndex: Int,
                     colType: Column.ColumnType, startTs: Long, endTs: Long, numBuckets: Int): Aggregator

  def validate(args: Seq[String], proj: RichProjection): Aggregator Or InvalidFunctionSpec = {
    for { args <- validateNumArgs(args, 5)
          timeColIndex <- columnIndex(proj.nonPartitionColumns, args(0))
          timeColType  <- validatedColumnType(proj.nonPartitionColumns, timeColIndex, Set(LongColumn, TimestampColumn))
          valueColIndex <- columnIndex(proj.nonPartitionColumns, args(1))
          valueColType  <- validatedColumnType(proj.nonPartitionColumns, valueColIndex, allowedTypes)
          startTs      <- parseParam(LongKeyType, args(2))
          endTs        <- parseParam(LongKeyType, args(3))
          numBuckets   <- parseParam(IntKeyType, args(4)) }
    yield {
      // Assumption: we only read time and value columns
      makeAggregator(timeColIndex, valueColIndex, valueColType, startTs, endTs, numBuckets)
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
    def validate(args: Seq[String], proj: RichProjection): Aggregator Or InvalidFunctionSpec =
      Good(new PartitionKeysAggregator)
  }

  case object Sum extends SingleColumnAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregator(colIndex: Int, colType: Column.ColumnType): Aggregator =
    colType match {
      case DoubleColumn => new SumDoublesAggregator(colIndex)
      case o: Any       => ???
    }
  }

  case object Count extends SingleColumnAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Column.ColumnType.values.toSet
    def makeAggregator(colIndex: Int, colType: Column.ColumnType): Aggregator =
      new CountingAggregator(colIndex)
  }

  case object Last extends TimeAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregator(timeColIndex: Int, valueColIndex: Int, colType: Column.ColumnType): Aggregator =
    colType match {
      case DoubleColumn => new LastDoubleValueAggregator(timeColIndex, valueColIndex)
      case o: Any       => ???
    }
  }

  case object TimeGroupMin extends TimeGroupingAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregator(timeColIndex: Int, valueColIndex: Int,
                       colType: Column.ColumnType,
                       startTs: Long, endTs: Long, buckets: Int): Aggregator = colType match {
      case DoubleColumn => new TimeGroupingMinDoubleAgg(timeColIndex, valueColIndex, startTs, endTs, buckets)
      case o: Any       => ???
    }
  }

  case object TimeGroupMax extends TimeGroupingAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregator(timeColIndex: Int, valueColIndex: Int,
                       colType: Column.ColumnType,
                       startTs: Long, endTs: Long, buckets: Int): Aggregator = colType match {
      case DoubleColumn => new TimeGroupingMaxDoubleAgg(timeColIndex, valueColIndex, startTs, endTs, buckets)
      case o: Any       => ???
    }
  }

  case object TimeGroupAvg extends TimeGroupingAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregator(timeColIndex: Int, valueColIndex: Int,
                       colType: Column.ColumnType,
                       startTs: Long, endTs: Long, buckets: Int): Aggregator = colType match {
      case DoubleColumn => new TimeGroupingAvgDoubleAgg(timeColIndex, valueColIndex, startTs, endTs, buckets)
      case o: Any       => ???
    }
  }
}
