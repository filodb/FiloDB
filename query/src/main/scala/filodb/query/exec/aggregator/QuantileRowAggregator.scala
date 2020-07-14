package filodb.query.exec.aggregator

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{RowReader}

//scalastyle:off
/**
  * We use the DDSketch to map/reduce quantile calculation.
  * See https://github.com/DataDog/sketches-java for more details.
  *
  * Map: We map each row to itself
  * ReduceMappedRow: The mapped row is added to the ddsketch
  * ReduceAggregate: We merge ddsketch by adding the ddsketch to reduce into a single ddsketch
  * Present: The quantile is calculated from the ddsketch.
  */
class QuantileRowAggregator(q: Double) extends RowAggregator {
  import filodb.memory.format.{vectors => bv}
  val relativeAccuracy = 0.5

  class QuantileHolder(var timestamp: Long = 0L,
                          var h: bv.DDSHistogram = bv.DDSHistogram.empty(relativeAccuracy)) extends AggregateHolder {
    val row = new TransientHistRow()
    def toRowReader: MutableRowReader = {
      row.setValues(timestamp, h)
      row
    }
    def resetToZero(): Unit = h = bv.DDSHistogram.empty(relativeAccuracy)
  }
  type AggHolderType = QuantileHolder
  def zero: QuantileHolder = new QuantileHolder
  def newRowToMapInto: MutableRowReader = new TransientHistRow()
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = {
    item
  }
  override def reduceMappedRow(acc: QuantileHolder, mappedRow: RowReader): QuantileHolder = {
    acc.timestamp = mappedRow.getLong(0)
    val sample = mappedRow.getDouble(1)
    if (!sample.isNaN) acc.h.accept(sample) // add sample to the dds
    acc
  }
  def reduceAggregate(acc: QuantileHolder, aggRes: RowReader): QuantileHolder = {
    acc.timestamp = aggRes.getLong(0)
    val newHist = aggRes.getHistogram(1)
    acc.h.mergeWith(newHist.asInstanceOf[bv.DDSHistogram]) // merge the ddsketches
    acc
  }
  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = {
    val mutRow = new TransientRow()
    val result = aggRangeVector.rows.mapRow { r =>
      val qVal = r.getHistogram(1).quantile(q)
      mutRow.setValues(r.getLong(0), qVal)
      mutRow
    }
    Seq(IteratorBackedRangeVector(aggRangeVector.key, result))
  }
  def reductionSchema(source: ResultSchema): ResultSchema = {
    val cols = new Array[ColumnInfo](2)
    cols(0) = source.columns(0)
    // TODO need a first class blob column
    cols(1) = ColumnInfo("dds", ColumnType.StringColumn)
    ResultSchema(cols, 1)
  }

  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    ResultSchema(Array(reductionSchema.columns(0), ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  }
}
