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
object HistAvgRowAggregator extends RowAggregator {
  class HistAvgHolder(var timestamp: Long = 0L,
                      var mean: Double = Double.NaN,
                      var count: Double = 0d) extends AggregateHolder {
    val row = new HistAvgAggTransientRow()
    def toRowReader: MutableRowReader = {
      row.setLong(0, timestamp)
      row.setDouble(1, mean)
      row.setDouble(2, count)
      row
    }
    def resetToZero(): Unit = { count = 0d; mean = Double.NaN }
  }
  type AggHolderType = HistAvgHolder
  def zero: HistAvgHolder = new HistAvgHolder()
  def newRowToMapInto: MutableRowReader = new HistAvgAggTransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = {
    val count = item.getDouble(2)
    val sum = item.getDouble(1)

    mapInto.setLong(0, item.getLong(0))
    mapInto.setDouble(1, if (count.isNaN || count == 0d) 0d else sum/count)
    mapInto.setDouble(2, count)
    mapInto
  }
  def reduceAggregate(acc: HistAvgHolder, aggRes: RowReader): HistAvgHolder = {
    acc.timestamp = aggRes.getLong(0)
    if (!aggRes.getDouble(1).isNaN) {
      if (acc.mean.isNaN) acc.mean = 0d
      val newMean = (acc.mean * acc.count + aggRes.getDouble(1) * aggRes.getDouble(2)) /
        (acc.count + aggRes.getDouble(2))
      acc.mean = newMean
      acc.count += aggRes.getDouble(2)
    }
    acc
  }
  // ignore last count column. we rely on schema change
  def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams,
              queryStats: QueryStats): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = {
    source.copy(columns = source.columns :+ ColumnInfo("count", ColumnType.LongColumn))
  }
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    // drop last column with count
    reductionSchema.copy(reductionSchema.columns.filterNot(_.name.equals("count")))
  }
}