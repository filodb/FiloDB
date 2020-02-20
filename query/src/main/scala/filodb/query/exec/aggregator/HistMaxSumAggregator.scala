package filodb.query.exec.aggregator

import filodb.core.query.{MutableRowReader, RangeVector, RangeVectorKey, ResultSchema, TransientHistMaxRow}
import filodb.memory.format.RowReader

object HistMaxSumAggregator extends RowAggregator {
  import filodb.memory.format.{vectors => bv}

  class HistSumMaxHolder(var timestamp: Long = 0L,
                         var h: bv.MutableHistogram = bv.Histogram.empty,
                         var m: Double = Double.NaN) extends AggregateHolder {
    val row = new TransientHistMaxRow()
    def toRowReader: MutableRowReader = { row.setValues(timestamp, h); row.max = m; row }
    def resetToZero(): Unit = { h = bv.Histogram.empty; m = 0.0 }
  }
  type AggHolderType = HistSumMaxHolder
  def zero: HistSumMaxHolder = new HistSumMaxHolder
  def newRowToMapInto: MutableRowReader = new TransientHistMaxRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item
  def reduceAggregate(acc: HistSumMaxHolder, aggRes: RowReader): HistSumMaxHolder = {
    acc.timestamp = aggRes.getLong(0)
    val newHist = aggRes.getHistogram(1)
    acc.h match {
      // sum is mutable histogram, copy to be sure it's our own copy
      case hist if hist.numBuckets == 0 => acc.h = bv.MutableHistogram(newHist)
      case h if newHist.numBuckets > 0  => acc.h.add(newHist.asInstanceOf[bv.HistogramWithBuckets])
      case h                            =>
    }
    acc.m = if (acc.m.isNaN) aggRes.getDouble(2) else Math.max(acc.m, aggRes.getDouble(2))
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}

