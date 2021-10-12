package filodb.query.exec.aggregator

import filodb.core.query._
import filodb.memory.format.RowReader

/**
  * Map: Every sample is mapped to itself.
  * ReduceAggregate: Stores the row's timestamp as the aggregate timestamp.
  * ReduceMappedRow: Same as ReduceAggregate; every row is mapped into an aggregate.
  * Present: '1' is always directly presented.
  */
object GroupRowAggregator extends RowAggregator {
  class GroupHolder(var timestamp: Long = 0L) extends AggregateHolder {
    val row = new TransientRow()
    var groupVal = Double.NaN  // set to 1d if any row has value != Double.NaN
    def toRowReader: MutableRowReader = { row.setValues(timestamp, groupVal); row }
    def resetToZero(): Unit = {groupVal = Double.NaN}
  }
  type AggHolderType = GroupHolder
  def zero: GroupHolder = new GroupHolder
  def newRowToMapInto: MutableRowReader = new TransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item
  def reduceAggregate(acc: GroupHolder, aggRes: RowReader): GroupHolder = {
    acc.timestamp = aggRes.getLong(0)
    if (!aggRes.getDouble(1).isNaN) {
      acc.groupVal = 1d
    }
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}
