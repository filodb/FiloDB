package filodb.query.exec.aggregator

import scala.collection.mutable

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.FiloSchedulers
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.data.ChunkMap
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}

/**
  * Map: Every sample is mapped to top/bottom-k aggregate by choosing itself: (a) The value  (b) and range vector key
  * ReduceMappedRow: Same as ReduceAggregate
  * ReduceAggregate: Accumulator maintains the top/bottom-k range vector keys and their corresponding values in a
  *                  min/max heap. Reduction happens by adding items heap and retaining only k items at any time.
  * Present: The top/bottom-k samples for each timestamp are placed into distinct RangeVectors for each RangeVectorKey
  *         Materialization is needed here, because it cannot be done lazily.
  */
class TopBottomKRowAggregator(k: Int, bottomK: Boolean) extends RowAggregator {

  private val numRowReaderColumns = 1 + k*2 // one for timestamp, two columns for each top-k
  private val rvkStringCache = mutable.HashMap[RangeVectorKey, ZeroCopyUTF8String]()

  case class RVKeyAndValue(rvk: ZeroCopyUTF8String, value: Double)
  class TopKHolder(var timestamp: Long = 0L) extends AggregateHolder {
    val valueOrdering = Ordering.by[RVKeyAndValue, Double](kr => kr.value)
    implicit val ordering = if (bottomK) valueOrdering else valueOrdering.reverse
    // TODO for later: see if we can use more memory/hava-heap-efficient data structures for this.
    val heap = mutable.PriorityQueue[RVKeyAndValue]()
    val row = new TopBottomKAggTransientRow(k)
    def toRowReader: MutableRowReader = {
      row.setLong(0, timestamp)
      var i = 1
      while(heap.nonEmpty) {
        val el = heap.dequeue()
        row.setString(i, el.rvk)
        row.setDouble(i + 1, el.value)
        i += 2
      }
      // Reset remaining values of row to overwrite previous row value
      while (i < numRowReaderColumns) {
        row.setString(i, CustomRangeVectorKey.emptyAsZcUtf8)
        row.setDouble(i + 1, if (bottomK) Double.MaxValue else Double.MinValue)
        i += 2
      }
      row
    }
    def resetToZero(): Unit = { heap.clear() }
  }

  type AggHolderType = TopKHolder
  def zero: TopKHolder = new TopKHolder()
  def newRowToMapInto: MutableRowReader = new TopBottomKAggTransientRow(k)
  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = {
    val rvkString = rvkStringCache.getOrElseUpdate(rvk, CustomRangeVectorKey.toZcUtf8(rvk))
    mapInto.setLong(0, item.getLong(0))
    // TODO: Use setBlob instead of setString once RowReeder has the support for blob
    mapInto.setString(1, rvkString)
    mapInto.setDouble(2, item.getDouble(1))
    var i = 3
    while(i<numRowReaderColumns) {
      mapInto.setString(i, CustomRangeVectorKey.emptyAsZcUtf8)
      mapInto.setDouble(i + 1, if (bottomK) Double.MaxValue else Double.MinValue)
      i += 2
    }
    mapInto
  }

  def reduceAggregate(acc: TopKHolder, aggRes: RowReader): TopKHolder = {
    acc.timestamp = aggRes.getLong(0)
    var i = 1
    while(aggRes.notNull(i)) {
      if (!aggRes.getDouble(i + 1).isNaN) {
        acc.heap.enqueue(RVKeyAndValue(aggRes.filoUTF8String(i), aggRes.getDouble(i + 1)))
        if (acc.heap.size > k) acc.heap.dequeue()
      }
      i += 2
    }
    acc
  }

  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = {
    val colSchema = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn))
    val recSchema = SerializedRangeVector.toSchema(colSchema)
    val resRvs = mutable.Map[RangeVectorKey, RecordBuilder]()
    // Important TODO / TechDebt: We need to replace Iterators with cursors to better control
    // the chunk iteration, lock acquisition and release. This is much needed for safe memory access.
    try {
      FiloSchedulers.assertThreadName(QuerySchedName)
      ChunkMap.validateNoSharedLocks(s"TopkQuery-$k-$bottomK")
      // We limit the results wherever it is materialized first. So it is done here.
      aggRangeVector.rows.take(limit).foreach { row =>
        var i = 1
        while (row.notNull(i)) {
          if (row.filoUTF8String(i) != CustomRangeVectorKey.emptyAsZcUtf8) {
            val rvk = CustomRangeVectorKey.fromZcUtf8(row.filoUTF8String(i))
            val builder = resRvs.getOrElseUpdate(rvk, SerializedRangeVector.newBuilder())
            builder.startNewRecord(recSchema)
            builder.addLong(row.getLong(0))
            builder.addDouble(row.getDouble(i + 1))
            builder.endRecord()
          }
          i += 2
        }
      }
    } finally {
      ChunkMap.releaseAllSharedLocks()
    }
    resRvs.map { case (key, builder) =>
      val numRows = builder.allContainers.map(_.countRecords).sum
      new SerializedRangeVector(key, numRows, builder.allContainers, recSchema, 0)
    }.toSeq
  }

  def reductionSchema(source: ResultSchema): ResultSchema = {
    val cols = new Array[ColumnInfo](numRowReaderColumns)
    cols(0) = source.columns(0)
    var i = 1
    while(i < numRowReaderColumns) {
      cols(i) = ColumnInfo(s"top${(i + 1)/2}-Key", ColumnType.StringColumn)
      cols(i + 1) = ColumnInfo(s"top${(i + 1)/2}-Val", ColumnType.DoubleColumn)
      i += 2
    }
    ResultSchema(cols, 1)
  }

  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    ResultSchema(Array(reductionSchema.columns(0), ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  }
}
