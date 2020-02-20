package filodb.query.exec.aggregator

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.RowReader

/**
  * Map: Every sample is mapped to three values: (a) the stddev value "0" and (b) the value itself
  * (c) and its count value "1"
  * ReduceAggregate: Similar as reduceAggregate for stdvar, since stddev = sqrt(stdvar)
  * ReduceMappedRow: Same as ReduceAggregate
  * Present: The current stddev is presented. Mean and Count value is dropped from presentation
  */
object StddevRowAggregator extends RowAggregator {
  class StddevHolder(var timestamp: Long = 0L,
                     var stddev: Double = Double.NaN,
                     var mean: Double = Double.NaN,
                     var count: Long = 0) extends AggregateHolder {
    val row = new StdValAggTransientRow()
    def toRowReader: MutableRowReader = {
      row.setLong(0, timestamp)
      row.setDouble(1, stddev)
      row.setDouble(2, mean)
      row.setLong(3, count)
      row
    }
    def resetToZero(): Unit = { count = 0; mean = Double.NaN; stddev = Double.NaN }
  }
  type AggHolderType = StddevHolder
  def zero: StddevHolder = new StddevHolder()
  def newRowToMapInto: MutableRowReader = new StdValAggTransientRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = {
    mapInto.setLong(0, item.getLong(0))
    mapInto.setDouble(1, 0L)
    mapInto.setDouble(2, item.getDouble(1))
    mapInto.setLong(3, if (item.getDouble(1).isNaN) 0L else 1L)
    mapInto
  }
  def reduceAggregate(acc: StddevHolder, aggRes: RowReader): StddevHolder = {
    acc.timestamp = aggRes.getLong(0)
    if (!aggRes.getDouble(1).isNaN && !aggRes.getDouble(2).isNaN) {
      if (acc.mean.isNaN) acc.mean = 0d
      if (acc.stddev.isNaN) acc.stddev = 0d

      val aggStddev = aggRes.getDouble(1)
      val aggMean = aggRes.getDouble(2)
      val aggCount = aggRes.getLong(3)

      val newMean = (acc.mean * acc.count + aggMean * aggCount) / (acc.count + aggCount)
      val accSquareSum = (Math.pow(acc.stddev, 2) + math.pow(acc.mean, 2)) * acc.count
      val aggSquareSum = (Math.pow(aggStddev, 2) + math.pow(aggMean, 2)) * aggCount
      val newStddev = Math.pow((accSquareSum + aggSquareSum) / (acc.count + aggCount) - math.pow(newMean, 2), 0.5)
      acc.stddev = newStddev
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