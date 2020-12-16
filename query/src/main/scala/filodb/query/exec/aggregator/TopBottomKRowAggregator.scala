package filodb.query.exec.aggregator

import java.util.concurrent.TimeUnit

import scala.collection.{mutable, Iterator}
import scala.collection.mutable.ListBuffer

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
  val colSchema = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  val recSchema = SerializedRangeVector.toSchema(colSchema)

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
    // TODO: Use setBlob instead of setString once RowReader has the support for blob
    mapInto.setString(1, rvkString)
    mapInto.setDouble(2, item.getDouble(1))
    var i = 3
    while(i < numRowReaderColumns) {
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

  private def addRecordToBuilder(builder: RecordBuilder, timeStampMs: Long, value: Double): Unit = {
    builder.startNewRecord(recSchema)
    builder.addLong(timeStampMs)
    builder.addDouble(value)
    builder.endRecord()
  }

  /**
   Create new builder and add NaN till current time
   */
  private def createBuilder(rangeParams: RangeParams, currentTime: Long): RecordBuilder= {
    val builder = SerializedRangeVector.newBuilder();
    val it = Iterator.from(0, rangeParams.stepSecs.toInt)
      .takeWhile(_ <= (currentTime - rangeParams.stepSecs) - rangeParams.startSecs).map { i =>
      val t = i + rangeParams.startSecs
      addRecordToBuilder(builder, t * 1000, Double.NaN)
    }
    // when step == 0 we don't need to add to builder
    if (rangeParams.startSecs != rangeParams.endSecs) it.toList
    builder
  }

  def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams): Seq[RangeVector] = {
    val resRvs = mutable.Map[RangeVectorKey, RecordBuilder]()
    try {
      FiloSchedulers.assertThreadName(QuerySchedName)
      ChunkMap.validateNoSharedLocks(s"TopkQuery-$k-$bottomK")
      // We limit the results wherever it is materialized first. So it is done here.
      val rows = aggRangeVector.rows.take(limit)
      val it = Iterator.from(0, rangeParams.stepSecs.toInt)
        .takeWhile(_ <= (rangeParams.endSecs - rangeParams.startSecs)).map { t =>
        val timestamp = t + rangeParams.startSecs
        val rvkSeen = new ListBuffer[RangeVectorKey]
        val row = rows.next()
        var i = 1
        while (row.notNull(i)) {
          if (row.filoUTF8String(i) != CustomRangeVectorKey.emptyAsZcUtf8) {
            val rvk = CustomRangeVectorKey.fromZcUtf8(row.filoUTF8String(i))
            rvkSeen += rvk
            val builder = resRvs.getOrElseUpdate(rvk, createBuilder(rangeParams, timestamp))
            addRecordToBuilder(builder, TimeUnit.SECONDS.toMillis(timestamp), row.getDouble(i + 1))
          }
          i += 2
        }
        resRvs.keySet.foreach { rvs =>
          if (!rvkSeen.contains(rvs)) addRecordToBuilder(resRvs.get(rvs).get, timestamp * 1000, Double.NaN)
        }
      }
      // address step == 0 case
      if (rangeParams.startSecs == rangeParams.endSecs) it.take(1).toList else it.toList
    } finally {
      aggRangeVector.rows().close()
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
