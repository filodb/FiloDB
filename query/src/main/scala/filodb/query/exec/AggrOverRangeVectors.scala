package filodb.query.exec

import java.nio.ByteBuffer

import scala.collection.mutable

import com.tdunning.math.stats.{ArrayDigest, TDigest}
import monix.reactive.Observable

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.memory.data.OffheapLFSortedIDMap
import filodb.memory.format.{RowReader, UnsafeUtils, ZeroCopyUTF8String}
import filodb.query._
import filodb.query.AggregationOperator._

/**
  * Reduce combined aggregates from children. Can be applied in a
  * hierarchical manner multiple times to arrive at result.
  */
final case class ReduceAggregateExec(id: String,
                                     dispatcher: PlanDispatcher,
                                     childAggregates: Seq[ExecPlan],
                                     aggrOp: AggregationOperator,
                                     aggrParams: Seq[Any]) extends NonLeafExecPlan {
  def children: Seq[ExecPlan] = childAggregates

  protected def schemaOfCompose(dataset: Dataset): ResultSchema = childAggregates.head.schema(dataset)

  protected def args: String = s"aggrOp=$aggrOp, aggrParams=$aggrParams"

  protected def compose(childResponses: Observable[QueryResponse],
                        queryConfig: QueryConfig): Observable[RangeVector] = {
    val results = childResponses.flatMap {
        case QueryResult(_, _, result) => Observable.fromIterable(result)
        case QueryError(_, ex)         => throw ex
    }
    RangeVectorAggregator.mapReduce(aggrOp, aggrParams, skipMapPhase = true, results, rv => rv.key)
  }
}

/**
  * Performs aggregation operation across RangeVectors within a shard
  */
final case class AggregateMapReduce(aggrOp: AggregationOperator,
                                    aggrParams: Seq[Any],
                                    without: Seq[String],
                                    by: Seq[String]) extends RangeVectorTransformer {
  require(without == Nil || by == Nil, "Cannot specify both without and by clause")
  val withoutLabels = without.map(ZeroCopyUTF8String(_)).toSet
  val byLabels = by.map(ZeroCopyUTF8String(_)).toSet

  protected[exec] def args: String =
    s"aggrOp=$aggrOp, aggrParams=$aggrParams, without=$without, by=$by"
  val aggregator = RowAggregator(aggrOp, aggrParams)

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    def grouping(rv: RangeVector): RangeVectorKey = {
      val groupBy: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] =
        if (by.nonEmpty) rv.key.labelValues.filter(lv => byLabels.contains(lv._1))
        else if (without.nonEmpty) rv.key.labelValues.filterNot(lv =>withoutLabels.contains(lv._1))
        else Map.empty
      CustomRangeVectorKey(groupBy)
    }
    RangeVectorAggregator.mapReduce(aggrOp, aggrParams, skipMapPhase = false, source, grouping)
  }

  override def schema(dataset: Dataset, source: ResultSchema): ResultSchema = {
    // TODO we assume that second column needs to be aggregated. Other dataset types need to be accommodated.
    aggregator.reductionSchema(source)
  }
}

final case class AggregatePresenter(aggrOp: AggregationOperator,
                                    aggrParams: Seq[Any]) extends RangeVectorTransformer {

  protected[exec] def args: String = s"aggrOp=$aggrOp, aggrParams=$aggrParams"
  val aggregator = RowAggregator(aggrOp, aggrParams)

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    RangeVectorAggregator.present(aggrOp, aggrParams, source, limit)
  }

  override def schema(dataset: Dataset, source: ResultSchema): ResultSchema = {
    aggregator.presentationSchema(source)
  }
}

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
    var acc = rowAgg.zero
    val mapInto = rowAgg.newRowToMapInto
    rvs.groupBy(grouping).mapValues { rvs =>
      new Iterator[rowAgg.AggHolderType] {
        val rowIterators = rvs.map(_.rows)
        val rvKeys = rvs.map(_.key)
        def hasNext: Boolean = rowIterators.forall(_.hasNext)
        def next(): rowAgg.AggHolderType = {
          acc.resetToZero()
          rowIterators.zip(rvKeys).foreach { case (rowIter, rvk) =>
            val mapped = if (skipMapPhase) rowIter.next() else rowAgg.map(rvk, rowIter.next(), mapInto)
            acc = if (skipMapPhase) rowAgg.reduceAggregate(acc, mapped) else rowAgg.reduceMappedRow(acc, mapped)
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
  def toRowReader: MutableRowReader
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
  def newRowToMapInto: MutableRowReader

  /**
    * Maps a single raw data row into a RowReader representing aggregate for single row.
    *
    * The mapInto RowReader where the mapped value needs to be stored can represent an
    * AggHolderType as a RowReader, or a "value" that is aggregatable.
    *
    * @param rvk The Range Vector Key of the sample that needs to be mapped
    * @param item the sample to be mapped
    * @param mapInto the RowReader that the method should mutate for mapping the sample
    * @return the mapped row, typically the mapInto param itself
    */
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader

  /**
    * Accumulates Mapped Row as a RowReader into the aggregation result.
    *
    * Default implementation assumes that every row is mapped into an aggregate
    * and hence invokes the `reduceAggregate` method.
    *
    * Override if reducing each sample to AggHolderType may be expensive and it
    * can be optimized by using a different mapped value.
    *
    * @param acc the aggregate holder accumulator to reduce into
    * @param mappedRow the mapped row to reduce
    * @return the result accumulator, typically the acc param itself to reduce GC
    */
  def reduceMappedRow(acc: AggHolderType, mappedRow: RowReader): AggHolderType =
    reduceAggregate(acc, mappedRow)

  /**
    * Accumulates AggHolderType as a RowReader into the aggregation result.
    *
    * @param acc the aggregate holder accumulator to reduce into
    * @param aggRes the aggregate result to reduce
    * @return the result accumulator, typically the acc param itself to reduce GC
    */
  def reduceAggregate(acc: AggHolderType, aggRes: RowReader): AggHolderType

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
      case Quantile => new QuantileRowAggregator(params(0).asInstanceOf[Double])
      case _     => ???
    }
  }
}

/**
  * Map: Every sample is mapped to itself
  * ReduceMappedRow: Same as ReduceAggregate since every row is mapped into an aggregate
  * ReduceAggregate: Accumulator maintains the sum. Reduction happens by adding the value to sum.
  * Present: The sum is directly presented
  */
object SumRowAggregator extends RowAggregator {
  class SumHolder(var timestamp: Long = 0L, var sum: Double = Double.NaN) extends AggregateHolder {
    val row = new TransientRow()
    def toRowReader: MutableRowReader = { row.setValues(timestamp, sum); row }
    def resetToZero(): Unit = sum = Double.NaN
  }
  type AggHolderType = SumHolder
  def zero: SumHolder = new SumHolder
  def newRowToMapInto: MutableRowReader = new TransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item
  def reduceAggregate(acc: SumHolder, aggRes: RowReader): SumHolder = {
    acc.timestamp = aggRes.getLong(0)
    if (!aggRes.getDouble(1).isNaN) {
      if (acc.sum.isNaN) acc.sum = 0
      acc.sum += aggRes.getDouble(1)
    }
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

/**
  * Map: Every sample is mapped to itself
  * ReduceMappedRow: Same as ReduceAggregate since every row is mapped into an aggregate
  * ReduceAggregate: Accumulator maintains the min. Reduction happens by choosing one of currentMin, or the value.
  * Present: The min is directly presented
  */
object MinRowAggregator extends RowAggregator {
  class MinHolder(var timestamp: Long = 0L, var min: Double = Double.NaN) extends AggregateHolder {
    val row = new TransientRow()
    def toRowReader: MutableRowReader = { row.setValues(timestamp, min); row }
    def resetToZero(): Unit = min = Double.NaN
  }
  type AggHolderType = MinHolder
  def zero: MinHolder = new MinHolder()
  def newRowToMapInto: MutableRowReader = new TransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item
  def reduceAggregate(acc: MinHolder, aggRes: RowReader): MinHolder = {
    acc.timestamp = aggRes.getLong(0)
    if (!aggRes.getDouble(1).isNaN) {
      if (acc.min.isNaN)
        acc.min = Double.MaxValue
      acc.min = Math.min(acc.min, aggRes.getDouble(1))
    }
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

/**
  * Map: Every sample is mapped to itself
  * ReduceMappedRow: Same as ReduceAggregate since every row is mapped into an aggregate
  * ReduceAggregate: Accumulator maintains the max. Reduction happens by choosing one of currentMax, or the value.
  * Present: The max is directly presented
  */
object MaxRowAggregator extends RowAggregator {
  class MaxHolder(var timestamp: Long = 0L, var max: Double = Double.NaN) extends AggregateHolder {
    val row = new TransientRow()
    def toRowReader: MutableRowReader = { row.setValues(timestamp, max); row }
    def resetToZero(): Unit = max = Double.NaN
  }
  type AggHolderType = MaxHolder
  def zero: MaxHolder = new MaxHolder()
  def newRowToMapInto: MutableRowReader = new TransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item
  def reduceAggregate(acc: MaxHolder, aggRes: RowReader): MaxHolder = {
    acc.timestamp = aggRes.getLong(0)
    if (!aggRes.getDouble(1).isNaN) {
      if (acc.max.isNaN) acc.max = Double.MinValue
      acc.max = Math.max(acc.max, aggRes.getDouble(1))
    }
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

/**
  * Map: Every sample is mapped to the count value "1"
  * ReduceMappedRow: Same as ReduceAggregate since every row is mapped into an aggregate
  * ReduceAggregate: Accumulator maintains the sum of counts.
  *                  Reduction happens by adding the count to the sum of counts.
  * Present: The count is directly presented
  */
object CountRowAggregator extends RowAggregator {
  class CountHolder(var timestamp: Long = 0L, var count: Double = Double.NaN) extends AggregateHolder {
    val row = new TransientRow()
    def toRowReader: MutableRowReader = { row.setValues(timestamp, count); row }
    def resetToZero(): Unit = count = Double.NaN
  }
  type AggHolderType = CountHolder
  def zero: CountHolder = new CountHolder()
  def newRowToMapInto: MutableRowReader = new TransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = {
    if (!item.getDouble(1).isNaN) {
      mapInto.setLong(0, item.getLong(0))
      mapInto.setDouble(1, 1d)
    }
    else {
      mapInto.setLong(0, item.getLong(0))
      mapInto.setDouble(1, 0d)
    }
    mapInto
  }
  def reduceAggregate(acc: CountHolder, aggRes: RowReader): CountHolder = {
    if (acc.count.isNaN && aggRes.getDouble(1) > 0) acc.count = 0d;
    acc.timestamp = aggRes.getLong(0)
    acc.count += aggRes.getDouble(1)
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

/**
  * Map: Every sample is mapped to two values: (a) The value itself (b) and its count value "1"
  * ReduceAggregate: Accumulator maintains the (a) current mean and (b) sum of counts.
  *                  Reduction happens by recalculating mean as (mean1*count1 + mean2*count1) / (count1+count2)
  *                  and count as (count1 + count2)
  * ReduceMappedRow: Same as ReduceAggregate
  * Present: The current mean is presented. Count value is dropped from presentation
  */
object AvgRowAggregator extends RowAggregator {
  class AvgHolder(var timestamp: Long = 0L,
                  var mean: Double = Double.NaN,
                  var count: Long = 0) extends AggregateHolder {
    val row = new AvgAggTransientRow()
    def toRowReader: MutableRowReader = {
      row.setLong(0, timestamp)
      row.setDouble(1, mean)
      row.setLong(2, count)
      row
    }
    def resetToZero(): Unit = { count = 0; mean = Double.NaN }
  }
  type AggHolderType = AvgHolder
  def zero: AvgHolder = new AvgHolder()
  def newRowToMapInto: MutableRowReader = new AvgAggTransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = {
    mapInto.setLong(0, item.getLong(0))
    mapInto.setDouble(1, item.getDouble(1))
    mapInto.setLong(2, if (item.getDouble(1).isNaN) 0L else 1L)
    mapInto
  }
  def reduceAggregate(acc: AvgHolder, aggRes: RowReader): AvgHolder = {
    acc.timestamp = aggRes.getLong(0)
    if (!aggRes.getDouble(1).isNaN) {
      if (acc.mean.isNaN) acc.mean = 0d
      val newMean = (acc.mean * acc.count + aggRes.getDouble(1) * aggRes.getLong(2)) / (acc.count + aggRes.getLong(2))
      acc.mean = newMean
      acc.count += aggRes.getLong(2)
    }
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
  * ReduceMappedRow: Same as ReduceAggregate
  * ReduceAggregate: Accumulator maintains the top/bottom-k range vector keys and their corresponding values in a
  *                  min/max heap. Reduction happens by adding items heap and retaining only k items at any time.
  * Present: The top/bottom-k samples for each timestamp are placed into distinct RangeVectors for each RangeVectorKey
  *         Materialization is needed here, because it cannot be done lazily.
  */
class TopBottomKRowAggregator(k: Int, bottomK: Boolean) extends RowAggregator {

  private val numRowReaderColumns = 1 + k*2 // one for timestamp, two columns for each top-k
  private val rvkStringCache = mutable.HashMap[RangeVectorKey, ZeroCopyUTF8String]()

  case class RVKeyAndValue(rvk: ZeroCopyUTF8String, value: Double)
  class TopKHolder(var timestamp: Long = 0L) extends AggregateHolder {
    val valueOrdering = Ordering.by[RVKeyAndValue, Double](kr => kr.value)
    implicit val ordering = if (bottomK) valueOrdering else valueOrdering.reverse
    // TODO for later: see if we can use more memory/hava-heap-efficient data structures for this.
    val heap = mutable.PriorityQueue[RVKeyAndValue]()
    val row = new TopBottomKAggTransientRow(k)
    def toRowReader: MutableRowReader = {
      row.setLong(0, timestamp)
      var i = 1
      while(heap.nonEmpty) {
        val el = heap.dequeue()
        row.setString(i, el.rvk)
        row.setDouble(i + 1, el.value)
        i += 2
      }
      row
    }
    def resetToZero(): Unit = { heap.clear() }
  }

  type AggHolderType = TopKHolder
  def zero: TopKHolder = new TopKHolder()
  def newRowToMapInto: MutableRowReader = new TopBottomKAggTransientRow(k)
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = {
    val rvkString = rvkStringCache.getOrElseUpdate(rvk, CustomRangeVectorKey.toZcUtf8(rvk))
    mapInto.setLong(0, item.getLong(0))
    // TODO: Use setBlob instead of setString once RowReeder has the support for blob
    mapInto.setString(1, rvkString)
    mapInto.setDouble(2, item.getDouble(1))
    var i = 3
    while(i<numRowReaderColumns) {
      mapInto.setString(i, CustomRangeVectorKey.emptyAsZcUtf8)
      mapInto.setDouble(i + 1, if (bottomK) Double.MaxValue else Double.MinValue)
      i += 2
    }
    mapInto
  }

  def reduceAggregate(acc: TopKHolder, aggRes: RowReader): TopKHolder = {
    acc.timestamp = aggRes.getLong(0)
    var i = 1
    while(aggRes.notNull(i)) {
      if (!aggRes.getDouble(i + 1).isNaN) {
        acc.heap.enqueue(RVKeyAndValue(aggRes.filoUTF8String(i), aggRes.getDouble(i + 1)))
        if (acc.heap.size > k) acc.heap.dequeue()
      }
      i += 2
    }
    acc
  }

  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = {
    val colSchema = Seq(ColumnInfo("timestamp", ColumnType.LongColumn), ColumnInfo("value", ColumnType.DoubleColumn))
    val recSchema = SerializableRangeVector.toSchema(colSchema)
    val resRvs = mutable.Map[RangeVectorKey, RecordBuilder]()
    // Important TODO / TechDebt: We need to replace Iterators with cursors to better control
    // the chunk iteration, lock acquisition and release. This is much needed for safe memory access.
    try {
      OffheapLFSortedIDMap.validateNoSharedLocks()
      // We limit the results wherever it is materialized first. So it is done here.
      aggRangeVector.rows.take(limit).foreach { row =>
        var i = 1
        while(row.notNull(i)) {
          val rvk = CustomRangeVectorKey.fromZcUtf8(row.filoUTF8String(i))
          val builder = resRvs.getOrElseUpdate(rvk, SerializableRangeVector.toBuilder(recSchema))
          builder.startNewRecord()
          builder.addLong(row.getLong(0))
          builder.addDouble(row.getDouble(i + 1))
          builder.endRecord()
          i += 2
        }
      }
    } finally {
      OffheapLFSortedIDMap.releaseAllSharedLocks()
    }
    resRvs.map { case (key, builder) =>
      val numRows = builder.allContainers.map(_.countRecords).sum
      new SerializableRangeVector(key, numRows, builder.allContainers, recSchema, 0)
    }.toSeq
  }

  def reductionSchema(source: ResultSchema): ResultSchema = {
    val cols = new Array[ColumnInfo](numRowReaderColumns)
    cols(0) = source.columns(0)
    var i = 1
    while(i < numRowReaderColumns) {
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

/**
  * We use the t-Digest data structure to map/reduce quantile calculation.
  * See https://github.com/tdunning/t-digest for more details.
  *
  * Map: We map each row to itself
  * ReduceMappedRow: The mapped row is added to the t-digest
  * ReduceAggregate: We merge t-tigest by adding the t-digests to reduce into a single t-digest
  * Present: The quantile is calculated from the t-digest.
  */
class QuantileRowAggregator(q: Double) extends RowAggregator {

  var buf = ByteBuffer.allocate(500) // initial size of 500 bytes... may need some tuning

  class QuantileHolder(var timestamp: Long = 0L) extends AggregateHolder {
    var tdig = TDigest.createArrayDigest(100)
    val row = new QuantileAggTransientRow()
    def toRowReader: MutableRowReader = {
      row.setLong(0, timestamp)
      val size = tdig.byteSize()
      if (buf.capacity() < size) buf = ByteBuffer.allocate(size) else buf.clear()
      tdig.asSmallBytes(buf)
      buf.flip()
      val len = buf.limit() - buf.position()
      row.setBlob(1, buf.array, buf.arrayOffset + buf.position() + UnsafeUtils.arayOffset, len)
      row
    }

    def resetToZero(): Unit = {
      tdig = TDigest.createArrayDigest(100) // unfortunately, no way to clear and reuse the same object
    }
  }

  type AggHolderType = QuantileHolder

  def zero: QuantileHolder = new QuantileHolder()

  def newRowToMapInto: MutableRowReader = new TransientRow()

  // map the sample RowReader to itself. Remember, mapping to a 1-sample t-digest creates too much GC churn
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item

  override def reduceMappedRow(acc: QuantileHolder, mappedRow: RowReader): QuantileHolder = {
    acc.timestamp = mappedRow.getLong(0)
    val sample = mappedRow.getDouble(1)
    if (!sample.isNaN) acc.tdig.add(sample) // add sample to the t-digest
    acc
  }

  def reduceAggregate(acc: QuantileHolder, aggRes: RowReader): QuantileHolder = {
    acc.timestamp = aggRes.getLong(0)
    acc.tdig.add(ArrayDigest.fromBytes(aggRes.getBuffer(1))) // merge the t-digests
    acc
  }

  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = {
    val mutRow = new TransientRow()
    val result = aggRangeVector.rows.map { r =>
      val qVal = ArrayDigest.fromBytes(r.getBuffer(1)).quantile(q)
      mutRow.setValues(r.getLong(0), qVal)
      mutRow
    }
    Seq(IteratorBackedRangeVector(aggRangeVector.key, result))
  }

  def reductionSchema(source: ResultSchema): ResultSchema = {
    val cols = new Array[ColumnInfo](2)
    cols(0) = source.columns(0)
    // TODO need a first class blob column
    cols(1) = ColumnInfo("tdig", ColumnType.StringColumn)
    ResultSchema(cols, 1)
  }

  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    ResultSchema(Array(reductionSchema.columns(0), ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  }
}
