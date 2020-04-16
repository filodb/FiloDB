package filodb.query.exec.aggregator

import filodb.core.query._
import filodb.memory.format.RowReader

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
    mapInto.setLong(0, item.getLong(0))
    mapInto.setDouble(1, if (item.getDouble(1).isNaN) 0d else 1d)
    mapInto
  }
  def reduceAggregate(acc: CountHolder, aggRes: RowReader): CountHolder = {
    if (acc.count.isNaN && aggRes.getDouble(1) > 0) acc.count = 0d;
    acc.timestamp = aggRes.getLong(0)
    if (!aggRes.getDouble(1).isNaN)
      acc.count += aggRes.getDouble(1)
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}
