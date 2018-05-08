package filodb.query.exec

import scala.collection.mutable

import monix.reactive.Observable

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.query.AggregationOperator
import filodb.query.AggregationOperator._

/**
  * Aggregation has three phases:
  * 1. Map: Map raw data points to AggregateResult RowReaders.
  * 2. Reduce: Reduce aggregate result RowReaders into fewer aggregate results. This may happen multiple times.
  * 3. Present: Convert the aggregation result into the final presentable result.
  *
  * This object is the facade for the above operations.
  *
  */
object RangeVectorAggregator {

  /**
    * This method is the facade for map and reduce steps of the aggregation.
    * In the reduction-only (non-leaf) phases, skipMapPhase should be true.
    */
  def mapReduce(aggrOp: AggregationOperator,
                params: Seq[Any],
                skipMapPhase: Boolean,
                source: Observable[RangeVector],
                grouping: RangeVectorKey => RangeVectorKey): Observable[RangeVector] = {
    val rowAgg = RowAggregator(aggrOp, params) // row aggregator
    val rvAgg = new RangeVectorAggregator(rowAgg, skipMapPhase, grouping) // range vector aggregator
    // zero result for range vector reduction
    val initialRes = mutable.Map[RangeVectorKey, Iterator[rvAgg.rowAgg.AggHolderType]]()
    // reduce the range vectors using the foldLeft construct. This results in one aggregate per group.
    source.foldLeftF(initialRes)(rvAgg.reduce).flatMap { groups =>
      // now create one range vector per group
      Observable.fromIterable(groups.map { case (rvk, aggHolder) =>
        val rowIterator = aggHolder.map(_.toRowReader)
        new IteratorBackedRangeVector(rvk, rowIterator)
      })
    }

    /* Implementation Note: We are wrapping iterators here for lazy evaluation.
       If there are n range vectors to aggregate, there may be O(n) elements in the stack.
       Tuning of stack space per thread will need to be done.
    */
  }

  /**
    * This method is the facade for the present step of the aggregation
    */
  def present(aggrOp: AggregationOperator,
              params: Seq[Any],
              source: Observable[RangeVector]): Observable[RangeVector] = {
    val aggregator = RowAggregator(aggrOp, params)
    source.map { rv =>
      val toPresent = rv.rows.map { r => aggregator.present(r) }
      new IteratorBackedRangeVector(rv.key, toPresent)
    }
  }
}

/**
  * This class has the plumbing for reducing RangeVector iterators.
  */
class RangeVectorAggregator(val rowAgg: RowAggregator,
                            skipMapPhase: Boolean,
                            grouping: RangeVectorKey => RangeVectorKey) {

  /**
    * Create the zero'th iterator with zero AggHolderType values
    */
  private def zero: Iterator[rowAgg.AggHolderType] = new Iterator[rowAgg.AggHolderType] {
    val sample = rowAgg.zero
    override def hasNext: Boolean = true
    override def next(): rowAgg.AggHolderType = {
      sample.resetToZero // reuse same object for each sample
      sample
    }
  }

  /**
    * Reducer function wraps iterator of range vector to produce another iterator.
    * This is continuously applied to reduce a bunch of range vectors to one.
    *
    * The Map is to be able to hold aggregate groups. Each entry in the map is a new group
    */
  def reduce(acc: mutable.Map[RangeVectorKey, Iterator[rowAgg.AggHolderType]],
           rv: RangeVector): mutable.Map[RangeVectorKey, Iterator[rowAgg.AggHolderType]] = {
    val rows = rv.rows
    val group = grouping(rv.key)
    val accIter = acc.getOrElse(group, zero) // create new group with zero'th iterator if one doesnt exist

    val aggregated = new Iterator[rowAgg.AggHolderType] {
      val mapInto = rowAgg.mapInto
      override def hasNext: Boolean = accIter.hasNext && rows.hasNext
      override def next(): rowAgg.AggHolderType = {
        val curResult = accIter.next()
        val mapped = if (skipMapPhase) rows.next() else rowAgg.map(rows.next(), mapInto)
        rowAgg.reduce(curResult, mapped)
      }
    }
    acc(group) = aggregated
    acc
  }
}

trait AggregateHolder {
  /**
    * Resets the given agg-result to zero value
    */
  def resetToZero(): Unit

  /**
    * Allows for the aggregation result to be stored in a RangeVector
    * so higher level aggregation can be done.
    *
    * This method can be space efficient by returning a mutable row
    */
  def toRowReader: TransientRow
}

/**
  * Implementations are responsible for aggregation at row level
  */
trait RowAggregator {
  /**
    * Type holding aggregation result or data structure
    */
  type AggHolderType <: AggregateHolder

  /**
    * Zero Aggregation Result for the aggregator, aka identity.
    * Combined with any row, should yield the row itself.
    * Note that one object is used per aggregation. The returned object
    * is reused to aggregate each row-key of each RangeVector by resetting
    * before aggregation of next row-key.
    */
  def zero: AggHolderType

  /**
    * For space efficiency purposes, return a reusable row to hold mapped rows
    */
  def mapInto: TransientRow

  /**
    * Maps a single raw data row into a RowReader representing aggregate for single row.
    */
  def map(item: RowReader, mapInto: TransientRow): RowReader

  /**
    * Accumulates AggHolderType as a RowReader into the aggregation result
    */
  def reduce(acc: AggHolderType, aggRes: RowReader): AggHolderType

  /**
    * Present the aggregate result as a RowReader from the AggHolderType RowReader
    */
  def present(aggRes: RowReader): RowReader

  /**
    * Schema of the RowReader returned by toRowReader
    */
  def reductionSchema(source: ResultSchema): ResultSchema

  /**
    * Schema of the final aggregate result
    */
  def presentationSchema(source: ResultSchema): ResultSchema
}

object RowAggregator {
  /**
    * Factory for RowAggregator
    */
  def apply(aggrOp: AggregationOperator, params: Seq[Any] = Nil): RowAggregator = {
    aggrOp match {
      case Min   => MinRowAggregator
      case Max   => MaxRowAggregator
      case Sum   => SumRowAggregator
      case Count => CountRowAggregator
      case Avg   => AvgRowAggregator
      case _     => ???
    }
  }
}

object SumRowAggregator extends RowAggregator {
  class SumHolder(var timestamp: Long = 0L, var sum: Double = 0) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, sum))
    def toRowReader: TransientRow = { row.set(timestamp, sum); row }
    def resetToZero(): Unit = sum = 0
  }
  type AggHolderType = SumHolder
  def zero: SumHolder = new SumHolder
  def mapInto: TransientRow = new TransientRow(Array(0L, 0d))
  def map(item: RowReader, mapInto: TransientRow): RowReader = item
  def reduce(acc: SumHolder, item: RowReader): SumHolder = {
    acc.timestamp = item.getLong(0)
    acc.sum += item.getDouble(1)
    acc
  }
  def present(aggRes: RowReader): RowReader = aggRes
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

object MinRowAggregator extends RowAggregator {
  class MinHolder(var timestamp: Long = 0L, var min: Double = Double.MaxValue) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, min))
    def toRowReader: TransientRow = { row.set(timestamp, min); row }
    def resetToZero(): Unit = min = Double.MaxValue
  }
  type AggHolderType = MinHolder
  def zero: MinHolder = new MinHolder()
  def mapInto: TransientRow = new TransientRow(Array(0L, 0d))
  def map(item: RowReader, mapInto: TransientRow): RowReader = item
  def reduce(acc: MinHolder, aggRes: RowReader): MinHolder = {
    acc.timestamp = aggRes.getLong(0)
    acc.min = Math.min(acc.min, aggRes.getDouble(1))
    acc
  }
  def present(aggRes: RowReader): RowReader = aggRes
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

object MaxRowAggregator extends RowAggregator {
  class MaxHolder(var timestamp: Long = 0L, var max: Double = Double.MinValue) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, max))
    def toRowReader: TransientRow = { row.set(timestamp, max); row }
    def resetToZero(): Unit = max = Double.MinValue
  }
  type AggHolderType = MaxHolder
  def zero: MaxHolder = new MaxHolder()
  def mapInto: TransientRow = new TransientRow(Array(0L, 0d))
  def map(item: RowReader, mapInto: TransientRow): RowReader = item
  def reduce(acc: MaxHolder, aggRes: RowReader): MaxHolder = {
    acc.timestamp = aggRes.getLong(0)
    acc.max = Math.max(acc.max, aggRes.getDouble(1))
    acc
  }
  def present(aggRes: RowReader): RowReader = aggRes
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

object CountRowAggregator extends RowAggregator {
  class CountHolder(var timestamp: Long = 0L, var count: Long = 0) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, count.toDouble))
    def toRowReader: TransientRow = { row.set(timestamp, count.toDouble); row }
    def resetToZero(): Unit = count = 0
  }
  type AggHolderType = CountHolder
  def zero: CountHolder = new CountHolder()
  def mapInto: TransientRow = new TransientRow(Array(0L, 0d))
  def map(item: RowReader, mapInto: TransientRow): RowReader = { mapInto.set(item.getLong(0), 1d); mapInto }
  def reduce(acc: CountHolder, aggRes: RowReader): CountHolder = {
    acc.timestamp = aggRes.getLong(0)
    acc.count += aggRes.getDouble(1).toLong
    acc
  }
  def present(aggRes: RowReader): RowReader = aggRes
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

object AvgRowAggregator extends RowAggregator {
  class AvgHolder(var timestamp: Long = 0L, var mean: Double = 0, var count: Long = 0) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, mean, count))
    def toRowReader: TransientRow = { row.set(timestamp, mean, count); row }
    def resetToZero(): Unit = { count = 0; mean = 0 }
  }
  type AggHolderType = AvgHolder
  def zero: AvgHolder = new AvgHolder()
  def mapInto: TransientRow = new TransientRow(Array(0L, 0d, 0L))
  def map(item: RowReader, mapInto: TransientRow): RowReader =
                    { mapInto.set(item.getLong(0), item.getDouble(1), 1L); mapInto }
  def reduce(acc: AvgHolder, aggRes: RowReader): AvgHolder = {
    val newMean = (acc.mean * acc.count + aggRes.getDouble(1) * aggRes.getLong(2))/ (acc.count + aggRes.getLong(2))
    acc.timestamp = aggRes.getLong(0)
    acc.mean = newMean
    acc.count += aggRes.getLong(2)
    acc
  }
  def present(aggRes: RowReader): RowReader = aggRes // ignore last count column. we rely on schema change
  def reductionSchema(source: ResultSchema): ResultSchema = {
    source.copy(columns = source.columns :+ ColumnInfo("count", ColumnType.LongColumn))
  }
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    // drop last column with count
    reductionSchema.copy(columns = reductionSchema.columns.take(reductionSchema.columns.size-1))
  }
}