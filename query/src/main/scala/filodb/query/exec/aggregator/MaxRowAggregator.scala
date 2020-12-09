package filodb.query.exec.aggregator

import filodb.core.query._
import filodb.memory.format.RowReader

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
  def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}
