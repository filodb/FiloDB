package filodb.query.exec.aggregator

import filodb.core.query.{MutableRowReader, QueryStats, RangeParams,
                          RangeVector, RangeVectorKey, ResultSchema, TransientHistMaxMinRow}
import filodb.memory.format.RowReader

object HistMaxMinSumAggregator extends RowAggregator {
  import filodb.memory.format.{vectors => bv}

  class HistSumMaxMinHolder(var timestamp: Long = 0L,
                            var h: bv.MutableHistogram = bv.Histogram.empty,
                            var max: Double = Double.NaN,
                            var min: Double = Double.NaN) extends AggregateHolder {
    val row = new TransientHistMaxMinRow()
    def toRowReader: MutableRowReader = { row.setValues(timestamp, h); row.max = max; row.min = min; row }
    def resetToZero(): Unit = { h = bv.Histogram.empty; max = 0.0; min = 0.0 }
  }
  type AggHolderType = HistSumMaxMinHolder
  def zero: HistSumMaxMinHolder = new HistSumMaxMinHolder
  def newRowToMapInto: MutableRowReader = new TransientHistMaxMinRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item
  def reduceAggregate(acc: HistSumMaxMinHolder, aggRes: RowReader): HistSumMaxMinHolder = {
    acc.timestamp = aggRes.getLong(0)
    val newHist = aggRes.getHistogram(1)
    acc.h match {
      // sum is mutable histogram, copy to be sure it's our own copy
      case hist if hist.numBuckets == 0 => acc.h = bv.MutableHistogram(newHist)
      case h if newHist.numBuckets > 0  => acc.h.add(newHist.asInstanceOf[bv.HistogramWithBuckets])
      case h                            =>
    }
    acc.max = if (acc.max.isNaN) aggRes.getDouble(2) else Math.max(acc.max, aggRes.getDouble(2))
    acc.min = if (acc.min.isNaN) aggRes.getDouble(3) else Math.min(acc.min, aggRes.getDouble(3))
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int,
              rangeParams: RangeParams, queryStats: QueryStats): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

