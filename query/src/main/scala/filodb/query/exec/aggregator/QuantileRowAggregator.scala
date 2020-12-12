package filodb.query.exec.aggregator

import java.nio.ByteBuffer

import com.tdunning.math.stats.{ArrayDigest, TDigest}

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{RowReader, UnsafeUtils}

//scalastyle:off
/**
  * We use the t-Digest data structure to map/reduce quantile calculation.
  * See https://github.com/tdunning/t-digest for more details.
  *
  * Map: We map each row to itself
  * ReduceMappedRow: The mapped row is added to the t-digest
  * ReduceAggregate: We merge t-tigest by adding the t-digests to reduce into a single t-digest
  * Present: The quantile is calculated from the t-digest.
  */
class QuantileRowAggregator(q: Double) extends RowAggregator {

  var buf = ByteBuffer.allocate(500) // initial size of 500 bytes... may need some tuning

  class QuantileHolder(var timestamp: Long = 0L) extends AggregateHolder {
    var tdig = TDigest.createArrayDigest(100)
    val row = new QuantileAggTransientRow()
    def toRowReader: MutableRowReader = {
      row.setLong(0, timestamp)
      val size = tdig.byteSize()
      if (buf.capacity() < size) buf = ByteBuffer.allocate(size) else buf.clear()
      tdig.asSmallBytes(buf)
      buf.flip()
      val len = buf.limit() - buf.position()
      row.setBlob(1, buf.array, buf.arrayOffset + buf.position() + UnsafeUtils.arayOffset, len)
      row
    }

    def resetToZero(): Unit = {
      tdig = TDigest.createArrayDigest(100) // unfortunately, no way to clear and reuse the same object
    }
  }

  type AggHolderType = QuantileHolder

  def zero: QuantileHolder = new QuantileHolder()

  def newRowToMapInto: MutableRowReader = new TransientRow()

  // map the sample RowReader to itself. Remember, mapping to a 1-sample t-digest creates too much GC churn
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = item

  override def reduceMappedRow(acc: QuantileHolder, mappedRow: RowReader): QuantileHolder = {
    acc.timestamp = mappedRow.getLong(0)
    val sample = mappedRow.getDouble(1)
    if (!sample.isNaN) acc.tdig.add(sample) // add sample to the t-digest
    acc
  }

  def reduceAggregate(acc: QuantileHolder, aggRes: RowReader): QuantileHolder = {
    acc.timestamp = aggRes.getLong(0)
    acc.tdig.add(ArrayDigest.fromBytes(aggRes.getBuffer(1))) // merge the t-digests
    acc
  }

  def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams): Seq[RangeVector] = {
    val mutRow = new TransientRow()
    val result = aggRangeVector.rows.mapRow { r =>
      val qVal = ArrayDigest.fromBytes(r.getBuffer(1)).quantile(q)
      mutRow.setValues(r.getLong(0), qVal)
      mutRow
    }
    Seq(IteratorBackedRangeVector(aggRangeVector.key, result))
  }

  def reductionSchema(source: ResultSchema): ResultSchema = {
    val cols = new Array[ColumnInfo](2)
    cols(0) = source.columns(0)
    // TODO need a first class blob column
    cols(1) = ColumnInfo("tdig", ColumnType.StringColumn)
    ResultSchema(cols, 1)
  }

  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    ResultSchema(Array(reductionSchema.columns(0), ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  }
}
