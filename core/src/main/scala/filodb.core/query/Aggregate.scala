package filodb.core.query

import enumeratum.EnumEntry.Snakecase
import enumeratum.{Enum, EnumEntry}
import monix.eval.Task
import org.scalactic._
import org.velvia.filo.{FiloVector, BinaryVector, ArrayStringRowReader, RowReader}
import scala.reflect.{classTag, ClassTag}
import scalaxy.loops._

import filodb.core.metadata._
import filodb.core.store.{ChunkScanMethod, ChunkSetInfo, RowKeyChunkScan}
import filodb.core.binaryrecord.BinaryRecord

/**
 * An Aggregate stores intermediate results from Aggregators, which can later be combined using
 * Combinators or Operators
 * NOTE: if you add a new type of Aggregate, be sure to extend the serialization test case in
 *       SerializationSpec.scala
 */
abstract class Aggregate[R: ClassTag] extends Serializable {
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

final case class DoubleSeriesPoint(timestamp: Long, value: Double) {
  override def toString: String =
    s"$timestamp (${(System.currentTimeMillis - timestamp)/1000}s ago) - $value"
}
final case class DoubleSeriesValues(shard: Int, partitionName: String, points: Seq[DoubleSeriesPoint]) {
  override def toString: String =
    s"[Shard $shard] $partitionName\n  ${points.mkString("\n  ")}"
}

// This Aggregate is designed to be mutable for high performance and low allocation cost
class ArrayAggregate[@specialized(Int, Long, Double) R: ClassTag](size: Int,
                                                                  value: R) extends Aggregate[R] {
  val result = Array.fill(size)(value)
}

final case class ListAggregate[R: ClassTag](values: Seq[R] = Nil) extends Aggregate[R] {
  val result = values.toArray
  def add(other: ListAggregate[R]): ListAggregate[R] = ListAggregate(values ++ other.values)
  def addWithMax(other: ListAggregate[R], maxSize: Int): ListAggregate[R] =
    if (values.length >= maxSize) { this }
    else { ListAggregate(values ++ other.values.take(maxSize - values.length)) }
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

class NumBytesAggregator(vectorPos: Int = 0) extends ChunkAggregator {
  type A = IntAggregate
  val emptyAggregate = IntAggregate(0)
  val positions = Array(vectorPos)

  def add(orig: A, reader: ChunkSetReader): A = reader.vectors(0) match {
    case v: BinaryVector[_] => IntAggregate(orig.value + v.numBytes)
  }

  def combine(first: A, second: A): A = IntAggregate(first.value + second.value)
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
    new PrimitiveSimpleAggregate(DoubleSeriesValues(partition.shard, partition.stringPartition, Seq(point)))
  }
}

/**
 * An AggregationFunction validates input arguments and produces an Aggregate for computation
 * It is also an EnumEntry and the name of the function is the class name in "snake_case" ie underscores
 */
sealed trait AggregationFunction extends FunctionValidationHelpers with EnumEntry with Snakecase {
  def allowedTypes: Set[Column.ColumnType]

  def validate(dataColumn: String,
               timestampColumn: Option[String],
               scanMethod: ChunkScanMethod,
               args: Seq[String],
               proj: RichProjection): Aggregator Or InvalidFunctionSpec =
    for { dataColIndex <- columnIndex(proj.nonPartitionColumns, dataColumn)
          dataColType <- validatedColumnType(proj.nonPartitionColumns, dataColIndex, allowedTypes)
          agg <- validate(dataColIndex, dataColType, timestampColumn, scanMethod, args, proj) }
    yield { agg }

  // validate the arguments against the projection and produce an Aggregate of the right type,
  def validate(dataColIndex: Int, dataColType: Column.ColumnType,
               timestampColumn: Option[String],
               scanMethod: ChunkScanMethod,
               args: Seq[String],
               proj: RichProjection): Aggregator Or InvalidFunctionSpec
}

trait SingleColumnAggFunction extends AggregationFunction {
  def makeAggregator(colIndex: Int, colType: Column.ColumnType, scanMethod: ChunkScanMethod): Aggregator

  def validate(dataColIndex: Int, dataColType: Column.ColumnType,
               timestampColumn: Option[String],
               scanMethod: ChunkScanMethod,
               args: Seq[String],
               proj: RichProjection): Aggregator Or InvalidFunctionSpec =
    Good(makeAggregator(dataColIndex, dataColType, scanMethod))
}

// Using a time-based function on a non-time-series schema or when no timestamp column is supplied
case object NoTimestampColumn extends InvalidFunctionSpec

trait TimeAggFunction[A] extends AggregationFunction {
  import Column.ColumnType._
  import filodb.core.SingleKeyTypes._

  def makeAggregator(timeColIndex: Int, valueColIndex: Int, colType: Column.ColumnType,
                     scanMethod: ChunkScanMethod, arg: A): Aggregator

  // Used to validate and transform the input args for the aggregator
  def validateArgs(args: Seq[String]): A Or InvalidFunctionSpec

  def validate(dataColIndex: Int, dataColType: Column.ColumnType,
               timestampColumn: Option[String],
               scanMethod: ChunkScanMethod,
               args: Seq[String],
               proj: RichProjection): Aggregator Or InvalidFunctionSpec = {
    for { timestampCol <- timestampColumn.map(Good(_)).getOrElse(Bad(NoTimestampColumn))
          timeColIndex <- columnIndex(proj.nonPartitionColumns, timestampCol)
          timeColType  <- validatedColumnType(proj.nonPartitionColumns, timeColIndex, Set(LongColumn, TimestampColumn))
          arg          <- validateArgs(args) }
    yield { makeAggregator(timeColIndex, dataColIndex, dataColType, scanMethod, arg) }
  }
}

object TimeBucketingAggFunction {
  val DefaultNumBuckets = 50
}

/**
 * Time bucketing aggregates takes 1 optional argument:
 *   <numBuckets> - number of buckets, defaults to ??
 */
trait TimeBucketingAggFunction extends TimeAggFunction[Int] {
  import Column.ColumnType._
  import filodb.core.SingleKeyTypes._

  def validateArgs(args: Seq[String]): Int Or InvalidFunctionSpec =
    if (args.isEmpty) { Good(TimeBucketingAggFunction.DefaultNumBuckets) }
    else {
      parseParam(IntKeyType, args(0))
    }

  def makeAggregator(timeColIndex: Int, valueColIndex: Int, colType: Column.ColumnType,
                     scanMethod: ChunkScanMethod, arg: Int): Aggregator = scanMethod match {
    case rk: RowKeyChunkScan => makeAggregator(timeColIndex, valueColIndex, colType,
                                               rk.startkey.getLong(0), rk.endkey.getLong(0), arg)
    // For now, for another method, just allow aggregating over all samples.
    // TODO: figure out how to limit "last value" scans to really just the last value.
    case _                   => makeAggregator(timeColIndex, valueColIndex, colType,
                                               Long.MinValue, Long.MaxValue, arg)
  }

  def makeAggregator(timeColIndex: Int, valueColIndex: Int,
                     colType: Column.ColumnType,
                     startTs: Long, endTs: Long, buckets: Int): Aggregator
}

/**
 * Defines all of the valid aggregation functions.  The function name is the case object in snake_case
 */
object AggregationFunction extends Enum[AggregationFunction] {
  import Column.ColumnType._
  val values = findValues

  // partition_keys returns all the partition keys (or time series) in a query
  case object PartitionKeys extends SingleColumnAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Column.ColumnType.values.toSet
    def makeAggregator(colIndex: Int, colType: Column.ColumnType, scanMethod: ChunkScanMethod): Aggregator =
      new PartitionKeysAggregator
  }

  case object NumBytes extends SingleColumnAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Column.ColumnType.values.toSet
    def makeAggregator(colIndex: Int, colType: Column.ColumnType, scanMethod: ChunkScanMethod): Aggregator =
      new NumBytesAggregator(colIndex)
  }

  case object Sum extends SingleColumnAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregator(colIndex: Int, colType: Column.ColumnType, scanMethod: ChunkScanMethod): Aggregator =
    colType match {
      case DoubleColumn => new SumDoublesAggregator(colIndex)
      case o: Any       => ???
    }
  }

  case object Count extends SingleColumnAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Column.ColumnType.values.toSet
    def makeAggregator(colIndex: Int, colType: Column.ColumnType, scanMethod: ChunkScanMethod): Aggregator =
      new CountingAggregator(colIndex)
  }

  case object Last extends TimeAggFunction[Unit] {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def validateArgs(args: Seq[String]): Unit Or InvalidFunctionSpec = Good(())
    def makeAggregator(timeColIndex: Int, valueColIndex: Int, colType: Column.ColumnType,
                       scanMethod: ChunkScanMethod, arg: Unit): Aggregator = colType match {
      case DoubleColumn => new LastDoubleValueAggregator(timeColIndex, valueColIndex)
      case o: Any       => ???
    }
  }

  case object TimeGroupMin extends TimeBucketingAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregator(timeColIndex: Int, valueColIndex: Int,
                       colType: Column.ColumnType,
                       startTs: Long, endTs: Long, buckets: Int): Aggregator = colType match {
      case DoubleColumn => new TimeGroupingMinDoubleAgg(timeColIndex, valueColIndex, startTs, endTs, buckets)
      case o: Any       => ???
    }
  }

  case object TimeGroupMax extends TimeBucketingAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregator(timeColIndex: Int, valueColIndex: Int,
                       colType: Column.ColumnType,
                       startTs: Long, endTs: Long, buckets: Int): Aggregator = colType match {
      case DoubleColumn => new TimeGroupingMaxDoubleAgg(timeColIndex, valueColIndex, startTs, endTs, buckets)
      case o: Any       => ???
    }
  }

  case object TimeGroupAvg extends TimeBucketingAggFunction {
    val allowedTypes: Set[Column.ColumnType] = Set(DoubleColumn)
    def makeAggregator(timeColIndex: Int, valueColIndex: Int,
                       colType: Column.ColumnType,
                       startTs: Long, endTs: Long, buckets: Int): Aggregator = colType match {
      case DoubleColumn => new TimeGroupingAvgDoubleAgg(timeColIndex, valueColIndex, startTs, endTs, buckets)
      case o: Any       => ???
    }
  }
}
