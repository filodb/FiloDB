package filodb.query.exec.aggregator

import filodb.core.metadata.Column.ColumnType
import filodb.core.query.{MutableRowReader, RangeVector, RangeVectorKey, ResultSchema}
import filodb.memory.format.RowReader
import filodb.query.AggregationOperator
import filodb.query.AggregationOperator._
import filodb.query.exec._

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
    * Should return a new AggHolder.
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

//scalastyle:off cyclomatic.complexity
object RowAggregator {
  def isHistMax(valColType: ColumnType, schema: ResultSchema): Boolean =
    valColType == ColumnType.HistogramColumn && schema.isHistDouble && schema.columns(2).name == "max"

  /**
    * Factory for RowAggregator
    */
  def apply(aggrOp: AggregationOperator, params: Seq[Any], schema: ResultSchema): RowAggregator = {
    val valColType = RangeVectorTransformer.valueColumnType(schema)
    aggrOp match {
      case Min      => MinRowAggregator
      case Max      => MaxRowAggregator
      case Sum if valColType == ColumnType.DoubleColumn => SumRowAggregator
      case Sum if isHistMax(valColType, schema)            => HistMaxSumAggregator
      case Sum if valColType == ColumnType.HistogramColumn => HistSumRowAggregator
      case Count if valColType == ColumnType.DoubleColumn    => CountRowAggregator.double
      case Count if valColType == ColumnType.HistogramColumn => CountRowAggregator.hist
      case Avg      => AvgRowAggregator
      case TopK     => new TopBottomKRowAggregator(params(0).asInstanceOf[Double].toInt, false)
      case BottomK  => new TopBottomKRowAggregator(params(0).asInstanceOf[Double].toInt, true)
      case Quantile => new QuantileRowAggregator(params(0).asInstanceOf[Double])
      case Stdvar   => StdvarRowAggregator
      case Stddev   => StddevRowAggregator
      case CountValues => new CountValuesRowAggregator(params(0).asInstanceOf[String].replaceAll("^\"|\"$", ""))
      case _     => ???
    }
  }
}
