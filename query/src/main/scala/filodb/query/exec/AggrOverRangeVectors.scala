package filodb.query.exec

import scala.collection.mutable

import monix.reactive.Observable

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.MemFactory
import filodb.memory.format.{ vectors => bv, BinaryAppendableVector, BinaryVector, RowReader, ZeroCopyUTF8String}
import filodb.query.AggregationOperator
import filodb.query.AggregationOperator._

/**
  * Aggregation has three phases:
  * 1. Map: Map raw data points to AggregateResult RowReaders.
  * 2. Reduce: Reduce aggregate result RowReaders into fewer aggregate results. This may happen multiple times.
  * 3. Present: Convert the aggregation result into the final presentable result.
  *
  * This singleton is the facade for the above operations.
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
                grouping: RangeVector => RangeVectorKey): Observable[RangeVector] = {
    val rowAgg = RowAggregator(aggrOp, params) // row aggregator
    // reduce the range vectors using the foldLeft construct. This results in one aggregate per group.
    val task = source.toListL.map { rvs =>
      // now reduce each group and create one result range vector per group
      val groupedResult = mapReduceInternal(rvs, rowAgg, skipMapPhase, grouping)
      groupedResult.map { case (rvk, aggHolder) =>
        val rowIterator = aggHolder.map(_.toRowReader)
        new IteratorBackedRangeVector(rvk, rowIterator)
      }
    }
    Observable.fromTask(task).flatMap(rvs => Observable.fromIterable(rvs))
  }

  /**
    * This method is the facade for the present step of the aggregation
    */
  def present(aggrOp: AggregationOperator,
              params: Seq[Any],
              source: Observable[RangeVector],
              limit: Int): Observable[RangeVector] = {
    val aggregator = RowAggregator(aggrOp, params)
    source.flatMap(rv => Observable.fromIterable(aggregator.present(rv, limit)))
  }

  private def mapReduceInternal(rvs: List[RangeVector],
                     rowAgg: RowAggregator,
                     skipMapPhase: Boolean,
                     grouping: RangeVector => RangeVectorKey): Map[RangeVectorKey, Iterator[rowAgg.AggHolderType]] = {
    rvs.groupBy(grouping).mapValues { rvs =>
      new Iterator[rowAgg.AggHolderType] {
        val rowIterators = rvs.map(_.rows)
        val rvks = rvs.map(_.key)
        val mapInto = rowAgg.newRowToMapInto
        def hasNext: Boolean = rowIterators.forall(_.hasNext)
        def next(): rowAgg.AggHolderType = {
          var acc = rowAgg.zero
          rowIterators.zip(rvks).foreach { case (rowIter, rvk) =>
            val rvkAsString = CustomRangeVectorKey.toZcUtf8(rvk)
            val mapped = if (skipMapPhase) rowIter.next() else rowAgg.map(rvkAsString, rowIter.next(), mapInto)
            acc = rowAgg.reduce(acc, mapped)
          }
          acc
        }
      }
    }
  }
}

trait AggregateHolder {
  /**
    * Resets the given agg-result to zero value
    */
  def resetToZero(): Unit

  /**
    * Allows for the aggregation result to be stored in a RowReader
    * so it can be placed in a RangeVector and sent over the wire to other nodes
    * where higher level aggregation can be done.
    *
    * This method can be made space efficient by returning a reusable/mutable row
    */
  def toRowReader: TransientRow
}

/**
  * Implementations are responsible for aggregation at row level
  */
trait RowAggregator {
  /**
    * Type holding aggregation result or accumulation data structure
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
    * For space efficiency purposes, create and return a reusable row to hold mapped rows.
    */
  def newRowToMapInto: TransientRow

  /**
    * Maps a single raw data row into a RowReader representing aggregate for single row
    * @param rvk The Range Vector Key of the sample that needs to be mapped
    * @param item the sample to be mapped
    * @param mapInto the RowReader that the method should mutate for mapping the sample
    * @return the mapped row, typically the mapInto param itself
    */
  def map(rvk: ZeroCopyUTF8String, item: RowReader, mapInto: TransientRow): RowReader

  /**
    * Accumulates AggHolderType as a RowReader into the aggregation result
    * @param acc the accumulator to mutate
    * @param aggRes the aggregate result to include in accumulator
    * @return the result accumulator, typically the acc param itself
    */
  def reduce(acc: AggHolderType, aggRes: RowReader): AggHolderType

  /**
    * Present the aggregate result as one ore more final result RangeVectors.
    *
    * Try to keep the Iterator in the RangeVector lazy.
    * If it really HAS to be materialized, then materialize for the
    * indicated limit.
    *
    * @param aggRangeVector The aggregate range vector for a group in the result
    * @param limit number of row-keys to include in the result RangeVector.
    *              Apply limit only on iterators that are NOT lazy and need to be
    *              materialized.
    */
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector]

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
  def apply(aggrOp: AggregationOperator, params: Seq[Any]): RowAggregator = {
    aggrOp match {
      case Min      => MinRowAggregator
      case Max      => MaxRowAggregator
      case Sum      => SumRowAggregator
      case Count    => CountRowAggregator
      case Avg      => AvgRowAggregator
      case TopK     => new TopBottomKRowAggregator(params(0).asInstanceOf[Double].toInt, false)
      case BottomK  => new TopBottomKRowAggregator(params(0).asInstanceOf[Double].toInt, true)
      case _     => ???
    }
  }
}

/**
  * Map: Every sample is mapped to itself
  * Reduce: Accumulator maintains the sum. Reduction happens by adding the value to sum.
  * Present: The sum is directly presented
  */
object SumRowAggregator extends RowAggregator {
  class SumHolder(var timestamp: Long = 0L, var sum: Double = 0) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, sum))
    def toRowReader: TransientRow = { row.set(timestamp, sum); row }
    def resetToZero(): Unit = sum = 0
  }
  type AggHolderType = SumHolder
  def zero: SumHolder = new SumHolder
  def newRowToMapInto: TransientRow = new TransientRow(Array(0L, 0d))
  def map(rvk: ZeroCopyUTF8String, item: RowReader, mapInto: TransientRow): RowReader = item
  def reduce(acc: SumHolder, item: RowReader): SumHolder = {
    acc.timestamp = item.getLong(0)
    acc.sum += item.getDouble(1)
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

/**
  * Map: Every sample is mapped to itself
  * Reduce: Accumulator maintains the min. Reduction happens by choosing one of currentMin, or the value.
  * Present: The min is directly presented
  */
object MinRowAggregator extends RowAggregator {
  class MinHolder(var timestamp: Long = 0L, var min: Double = Double.MaxValue) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, min))
    def toRowReader: TransientRow = { row.set(timestamp, min); row }
    def resetToZero(): Unit = min = Double.MaxValue
  }
  type AggHolderType = MinHolder
  def zero: MinHolder = new MinHolder()
  def newRowToMapInto: TransientRow = new TransientRow(Array(0L, 0d))
  def map(rvk: ZeroCopyUTF8String, item: RowReader, mapInto: TransientRow): RowReader = item
  def reduce(acc: MinHolder, aggRes: RowReader): MinHolder = {
    acc.timestamp = aggRes.getLong(0)
    acc.min = Math.min(acc.min, aggRes.getDouble(1))
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

/**
  * Map: Every sample is mapped to itself
  * Reduce: Accumulator maintains the max. Reduction happens by choosing one of currentMax, or the value.
  * Present: The max is directly presented
  */
object MaxRowAggregator extends RowAggregator {
  class MaxHolder(var timestamp: Long = 0L, var max: Double = Double.MinValue) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, max))
    def toRowReader: TransientRow = { row.set(timestamp, max); row }
    def resetToZero(): Unit = max = Double.MinValue
  }
  type AggHolderType = MaxHolder
  def zero: MaxHolder = new MaxHolder()
  def newRowToMapInto: TransientRow = new TransientRow(Array(0L, 0d))
  def map(rvk: ZeroCopyUTF8String, item: RowReader, mapInto: TransientRow): RowReader = item
  def reduce(acc: MaxHolder, aggRes: RowReader): MaxHolder = {
    acc.timestamp = aggRes.getLong(0)
    acc.max = Math.max(acc.max, aggRes.getDouble(1))
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

/**
  * Map: Every sample is mapped to the count value "1"
  * Reduce: Accumulator maintains the sum of counts. Reduction happens by adding the count to the sum of counts.
  * Present: The count is directly presented
  */
object CountRowAggregator extends RowAggregator {
  class CountHolder(var timestamp: Long = 0L, var count: Long = 0) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, count.toDouble))
    def toRowReader: TransientRow = { row.set(timestamp, count.toDouble); row }
    def resetToZero(): Unit = count = 0
  }
  type AggHolderType = CountHolder
  def zero: CountHolder = new CountHolder()
  def newRowToMapInto: TransientRow = new TransientRow(Array(0L, 0d))
  def map(rvk: ZeroCopyUTF8String, item: RowReader, mapInto: TransientRow): RowReader = {
    mapInto.set(item.getLong(0), 1d)
    mapInto
  }
  def reduce(acc: CountHolder, aggRes: RowReader): CountHolder = {
    acc.timestamp = aggRes.getLong(0)
    acc.count += aggRes.getDouble(1).toLong
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

/**
  * Map: Every sample is mapped to two values: (a) The value itself (b) and its count value "1"
  * Reduce: Accumulator maintains the (a) current mean and (b) sum of counts.
  *         Reduction happens by recalculating mean as (mean1*count1 + mean2*count1) / (count1+count2)
  *         and count as (count1 + count2)
  * Present: The current mean is presented. Count value is dropped from presentation
  */
object AvgRowAggregator extends RowAggregator {
  class AvgHolder(var timestamp: Long = 0L, var mean: Double = 0, var count: Long = 0) extends AggregateHolder {
    val row = new TransientRow(Array(timestamp, mean, count))
    def toRowReader: TransientRow = { row.set(timestamp, mean, count); row }
    def resetToZero(): Unit = { count = 0; mean = 0 }
  }
  type AggHolderType = AvgHolder
  def zero: AvgHolder = new AvgHolder()
  def newRowToMapInto: TransientRow = new TransientRow(Array(0L, 0d, 0L))
  def map(rvk: ZeroCopyUTF8String, item: RowReader, mapInto: TransientRow): RowReader =
                    { mapInto.set(item.getLong(0), item.getDouble(1), 1L); mapInto }
  def reduce(acc: AvgHolder, aggRes: RowReader): AvgHolder = {
    val newMean = (acc.mean * acc.count + aggRes.getDouble(1) * aggRes.getLong(2))/ (acc.count + aggRes.getLong(2))
    acc.timestamp = aggRes.getLong(0)
    acc.mean = newMean
    acc.count += aggRes.getLong(2)
    acc
  }
  // ignore last count column. we rely on schema change
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = {
    source.copy(columns = source.columns :+ ColumnInfo("count", ColumnType.LongColumn))
  }
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    // drop last column with count
    reductionSchema.copy(columns = reductionSchema.columns.take(reductionSchema.columns.size-1))
  }
}

/**
  * Map: Every sample is mapped to top/bottom-k aggregate by choosing itself: (a) The value  (b) and range vector key
  * Reduce: Accumulator maintains the top/bottom-k range vector keys and their corresponding values in a min/max heap.
  *         Reduction happens by adding items heap and retaining only k items at any time.
  * Present: The top/bottom-k samples for each timestamp are placed into distinct RangeVectors for each RangeVectorKey
  *         Materialization is needed here, because it cannot be done lazily.
  */

class TopBottomKRowAggregator(k: Int, bottomK: Boolean) extends RowAggregator {

  private val numRowReaderColumns = 1 + k*2 // one for timestamp, two columns for each top-k

  case class RVKeyAndValue(rvk: ZeroCopyUTF8String, value: Double)
  class TopKHolder(var timestamp: Long = 0L) extends AggregateHolder {
    val valueOrdering = Ordering.by[RVKeyAndValue, Double](kr => kr.value)
    implicit val ordering = if (bottomK) valueOrdering else valueOrdering.reverse
    val heap = mutable.PriorityQueue[RVKeyAndValue]()
    val row = new TransientRow(new Array[Any](numRowReaderColumns))
    def toRowReader: TransientRow = {
      row.set(0, timestamp)
      var i = 1
      while(heap.nonEmpty) {
        val el = heap.dequeue()
        row.set(i, el.rvk)
        row.set(i + 1, el.value)
        i += 2
      }
      row
    }
    def resetToZero(): Unit = { heap.clear() }
  }

  type AggHolderType = TopKHolder
  def zero: TopKHolder = new TopKHolder()
  def newRowToMapInto: TransientRow = new TransientRow(new Array[Any](numRowReaderColumns))
  def map(rvk: ZeroCopyUTF8String, item: RowReader, mapInto: TransientRow): RowReader = {
    mapInto.set(item.getLong(0), rvk, item.getDouble(1))
    var i = 3
    while(i<numRowReaderColumns) {
      mapInto.set(i, "")
      mapInto.set(i + 1, if (bottomK) Double.MaxValue else Double.MinValue)
      i += 2
    }
    mapInto
  }

  def reduce(acc: TopKHolder, aggRes: RowReader): TopKHolder = {
    acc.timestamp = aggRes.getLong(0)
    var i = 1
    while(aggRes.notNull(i)) {
      acc.heap.enqueue(RVKeyAndValue(aggRes.filoUTF8String(i), aggRes.getDouble(i + 1)))
      if (acc.heap.size > k) acc.heap.dequeue()
      i += 2
    }
    acc
  }

  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = {
    val resRvs = mutable.Map[RangeVectorKey, Array[BinaryAppendableVector[_]]]()
    val memFactory = MemFactory.onHeapFactory
    val maxElements = 1000 // FIXME for some reason this isn't working if small
    // We limit the results wherever it is materialized first. So it is done here.
    aggRangeVector.rows.take(limit).foreach { row =>
      var i = 1
      while(row.notNull(i)) {
        val rvk = CustomRangeVectorKey.fromZcUtf8(row.filoUTF8String(i))
        val vectors = resRvs.getOrElseUpdate(rvk,
                       Array(bv.LongBinaryVector.appendingVector(memFactory, maxElements),
                             bv.DoubleVector.appendingVector(memFactory, maxElements)))
        vectors(0).addFromReader(row, 0) // timestamp
        vectors(1).addFromReader(row, i + 1) // value
        i += 2
      }
    }
    resRvs.map { case (key, vectors) =>
      new SerializableRangeVector(key, vectors.map(_.asInstanceOf[BinaryVector[_]]), vectors(0).length)
    }.toSeq
  }

  def reductionSchema(source: ResultSchema): ResultSchema = {
    val cols = new Array[ColumnInfo](numRowReaderColumns)
    cols(0) = source.columns(0)
    var i = 1
    while(i<numRowReaderColumns) {
      cols(i) = ColumnInfo(s"top${(i + 1)/2}-Key", ColumnType.StringColumn)
      cols(i + 1) = ColumnInfo(s"top${(i + 1)/2}-Val", ColumnType.DoubleColumn)
      i += 2
    }
    ResultSchema(cols, 1)
  }

  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    ResultSchema(Array(reductionSchema.columns(0), ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  }
}