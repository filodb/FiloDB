package filodb.query.exec.aggregator

import filodb.core.query._
import filodb.memory.format.RowReader

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
  def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}
