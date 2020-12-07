package filodb.query.exec.aggregator

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.RowReader

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
  def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = {
    source.copy(columns = source.columns :+ ColumnInfo("count", ColumnType.LongColumn))
  }
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    // drop last column with count
    reductionSchema.copy(reductionSchema.columns.filterNot(_.name.equals("count")))
  }
}