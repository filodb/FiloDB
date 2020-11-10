package filodb.query.exec.aggregator

import filodb.core.query.{MutableRowReader, RangeParams, RangeVector, RangeVectorKey, ResultSchema, TransientRow}
import filodb.memory.format.RowReader
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
  def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}