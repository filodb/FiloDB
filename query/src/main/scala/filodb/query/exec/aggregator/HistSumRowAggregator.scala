package filodb.query.exec.aggregator

import filodb.core.query.{MutableRowReader, QueryStats, RangeParams,
                          RangeVector, RangeVectorKey, ResultSchema, TransientHistRow}
import filodb.memory.format.RowReader

object HistSumRowAggregator extends RowAggregator {
  import filodb.memory.format.{vectors => bv}

  class HistSumHolder(var timestamp: Long = 0L,
                      var h: bv.MutableHistogram = bv.Histogram.empty) extends AggregateHolder {
    val row = new TransientHistRow()
    def toRowReader: MutableRowReader = { row.setValues(timestamp, h); row }
    def resetToZero(): Unit = h = bv.Histogram.empty
    // Storage owned by this holder and reused across steps, so starting a new sum doesn't allocate in steady state.
    // h is either the shared Histogram.empty or ownedHist. The result is overwritten when the holder is reused
    // for the next step, so consumers must use or copy it before advancing, as with the reused row.
    private var ownedHist: bv.MutableHistogram = _
    def startSum(first: bv.HistogramWithBuckets): Unit = {
      if (ownedHist == null) ownedHist = bv.MutableHistogram.reusable()
      ownedHist.setFrom(first)
      h = ownedHist
    }
  }
  type AggHolderType = HistSumHolder
  def zero: HistSumHolder = new HistSumHolder
  def newRowToMapInto: MutableRowReader = new TransientHistRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item
  def reduceAggregate(acc: HistSumHolder, aggRes: RowReader): HistSumHolder = {
    acc.timestamp = aggRes.getLong(0)
    val newHist = aggRes.getHistogram(1)
    acc.h match {
      // copy into the holder's own storage, since newHist may be reused by its producer
      case hist if hist.numBuckets == 0 => acc.startSum(newHist.asInstanceOf[bv.HistogramWithBuckets])
      case h if newHist.numBuckets > 0  => acc.h.add(newHist.asInstanceOf[bv.HistogramWithBuckets])
      case h                            =>
    }
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int,
              rangeParams: RangeParams, queryStats: QueryStats): Seq[RangeVector] = Seq(aggRangeVector)
  def reductionSchema(source: ResultSchema): ResultSchema = source
  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = reductionSchema
}