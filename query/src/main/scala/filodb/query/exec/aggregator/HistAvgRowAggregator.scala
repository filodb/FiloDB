package filodb.query.exec.aggregator

import filodb.core.query._
import filodb.memory.format.RowReader

/**
 * RowAggregator for Histogram Average in one step, rather than the traditional method of
 * calculating the sum of sums and dividing it by the sum of counts.
 *
 * Map: Every histogram sample is mapped to two values, excluding the timestamp:
 * (a) The mean of all observations of the histogram row  (b) and the row's count column.
 *
 * ReduceAggregate: Accumulator maintains the (a) current mean and (b) sum of counts.
 * Reduction happens by recalculating mean as (mean1*count1 + mean2*count1) / (count1+count2)
 * and count as (count1 + count2)
 *
 * ReduceMappedRow: Same as ReduceAggregate
 *
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
    // A histogram row is composed of 3+ columns: 1: Timestamp, 2: Sum, 3: Count
    val sum = item.getDouble(1)
    val count = item.getDouble(2)

    mapInto.setLong(0, item.getLong(0))
    // We calculate the mean by dividing the sum by the count.
    mapInto.setDouble(1, if (count.isNaN || count == 0d) 0d else sum/count)
    mapInto.setDouble(2, count)
    mapInto
  }
  def reduceAggregate(acc: HistAvgHolder, aggRes: RowReader): HistAvgHolder = {
    acc.timestamp = aggRes.getLong(0)
    if (!aggRes.getDouble(1).isNaN && !aggRes.getDouble(2).isNaN) {
      if (acc.mean.isNaN) acc.mean = 0d
      if (acc.count.isNaN) acc.count = 0d
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
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    // drop last column with count
    reductionSchema.copy(reductionSchema.columns.filterNot(_.name.equals("count")))
  }
}