package filodb.query.exec.aggregator

import filodb.core.query._
import filodb.memory.format.RowReader

/**
  * Map: Every sample is mapped to itself
  * ReduceMappedRow: Same as ReduceAggregate since every row is mapped into an aggregate
  * ReduceAggregate: Accumulator maintains the min. Reduction happens by choosing one of currentMin, or the value.
  * Present: The min is directly presented
  */
object AbsentRowAggregator extends RowAggregator {
  class AbsentHolder(var timestamp: Long = 0L, var value: Double = 1.0) extends AggregateHolder {
    val row = new TransientRow()
    def toRowReader: MutableRowReader = { row.setValues(timestamp, value); row }
    def resetToZero(): Unit = value = 1.0
  }
  type AggHolderType = AbsentHolder
  def zero: AbsentHolder = new AbsentHolder()
  def newRowToMapInto: MutableRowReader = new TransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item
  def reduceAggregate(acc: AbsentHolder, aggRes: RowReader): AbsentHolder = {
    acc.timestamp = aggRes.getLong(0)
    // NaN means the time series present. 1.0 means absent.
    if (aggRes.getDouble(1).isNaN) {
      acc.value = Double.NaN
    }
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int,
              rangeParams: RangeParams, queryStats: QueryStats): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}
