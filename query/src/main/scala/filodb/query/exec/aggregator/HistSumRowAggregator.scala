package filodb.query.exec.aggregator

import filodb.core.query.{MutableRowReader, RangeParams, RangeVector, RangeVectorKey, ResultSchema, TransientHistRow}
import filodb.memory.format.RowReader

object HistSumRowAggregator extends RowAggregator {
  import filodb.memory.format.{vectors => bv}

  class HistSumHolder(var timestamp: Long = 0L,
                      var h: bv.MutableHistogram = bv.Histogram.empty) extends AggregateHolder {
    val row = new TransientHistRow()
    def toRowReader: MutableRowReader = { row.setValues(timestamp, h); row }
    def resetToZero(): Unit = h = bv.Histogram.empty
  }
  type AggHolderType = HistSumHolder
  def zero: HistSumHolder = new HistSumHolder
  def newRowToMapInto: MutableRowReader = new TransientHistRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item
  def reduceAggregate(acc: HistSumHolder, aggRes: RowReader): HistSumHolder = {
    acc.timestamp = aggRes.getLong(0)
    val newHist = aggRes.getHistogram(1)
    acc.h match {
      // sum is mutable histogram, copy to be sure it's our own copy
      case hist if hist.numBuckets == 0 => acc.h = bv.MutableHistogram(newHist)
      case h if newHist.numBuckets > 0  => acc.h.add(newHist.asInstanceOf[bv.HistogramWithBuckets])
      case h                            =>
    }
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}