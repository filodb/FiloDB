package filodb.query.exec.aggregator

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.RowReader

/**
  * Map: Every sample is mapped to three values: (a) the stdvar value "0" and (b) the value itself
  * (c) and its count value "1"
  * ReduceAggregate: Accumulator maintains the (a) current stdvar and (b) current mean and (c) sum of counts.
  *                  Reduction happens by:
  *                  (a)recalculating mean as:
  *                     (mean1*count1 + mean2*count1) / (count1+count2)
  *                  (b)recalculating stdvar as:
  *                     From:
  *                     (1)stdvar1 = sumSquare1/count1 - mean1^2
  *                     (2)stdvar2 = sumSquare2/count2 - mean2^2
  *                     stdvar can be derived as:
  *                     stdvar = sumSquare/count - mean^2 = (sumSquare1+sumSquare2)/(count1+count2)-mean^2,
  *                     where,
  *                     sumSquare1 = (stdvar1+mean1^2)*count1
  *                     sumSquare2 = (stdvar2+mean2^2)*count2
  *                     mean = (mean1*count1 + mean2*count2) / (count1+count2)
  * ReduceMappedRow: Same as ReduceAggregate
  * Present: The current stdvar is presented. Mean and Count value is dropped from presentation
  */
object StdvarRowAggregator extends RowAggregator {
  class StdvarHolder(var timestamp: Long = 0L,
                     var stdVar: Double = Double.NaN,
                     var mean: Double = Double.NaN,
                     var count: Long = 0) extends AggregateHolder {
    val row = new StdValAggTransientRow()
    def toRowReader: MutableRowReader = {
      row.setLong(0, timestamp)
      row.setDouble(1, stdVar)
      row.setDouble(2, mean)
      row.setLong(3, count)
      row
    }
    def resetToZero(): Unit = { count = 0; mean = Double.NaN; stdVar = Double.NaN }
  }
  type AggHolderType = StdvarHolder
  def zero: StdvarHolder = new StdvarHolder()
  def newRowToMapInto: MutableRowReader = new StdValAggTransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = {
    mapInto.setLong(0, item.getLong(0))
    mapInto.setDouble(1, 0L)
    mapInto.setDouble(2, item.getDouble(1))
    mapInto.setLong(3, if (item.getDouble(1).isNaN) 0L else 1L)
    mapInto
  }
  def reduceAggregate(acc: StdvarHolder, aggRes: RowReader): StdvarHolder = {
    acc.timestamp = aggRes.getLong(0)

    if (!aggRes.getDouble(1).isNaN && !aggRes.getDouble(2).isNaN) {
      if (acc.mean.isNaN) acc.mean = 0d
      if (acc.stdVar.isNaN) acc.stdVar = 0d

      val aggStdvar = aggRes.getDouble(1)
      val aggMean = aggRes.getDouble(2)
      val aggCount = aggRes.getLong(3)

      val newMean = (acc.mean * acc.count + aggMean * aggCount) / (acc.count + aggCount)
      val accSquareSum = (acc.stdVar + math.pow(acc.mean, 2)) * acc.count
      val aggSquareSum = (aggStdvar + math.pow(aggMean, 2)) * aggCount
      val newStdVar = (accSquareSum + aggSquareSum) / (acc.count + aggCount) - math.pow(newMean, 2)
      acc.stdVar = newStdVar
      acc.mean = newMean
      acc.count += aggCount
    }
    acc
  }
  // ignore last two column. we rely on schema change
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = {
    source.copy(source.columns :+ ColumnInfo("mean", ColumnType.DoubleColumn)
      :+ ColumnInfo("count", ColumnType.LongColumn))
  }
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    // drop last two column with mean and count
    reductionSchema.copy(reductionSchema.columns.dropRight(2))
  }
}
