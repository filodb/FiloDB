package filodb.query.exec.aggregator

import scala.collection.mutable

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.FiloSchedulers
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.data.ChunkMap
import filodb.memory.format.{RowReader, UnsafeUtils}
import filodb.memory.format.ZeroCopyUTF8String._

/**
  * Map: Every sample is mapped to map with value as key and 1 as value
  * ReduceMappedRow: Same as ReduceAggregate
  * ReduceAggregate: Accumulator maintains a map for values and their frequencies. Reduction happens
  *                  by deserializing items blob into map and aggregating with frequencies of map of accumulator
  * Present: Each key from map is placed into distinct RangeVectors. RangeVectorKey is formed by appending the key/value
  *          to existing labelValues.
  *
  */
class CountValuesRowAggregator(label: String, limit: Int = 1000) extends RowAggregator {

  var serializedMap = new Array[Byte](500)
  val rowMap = debox.Map[Double, Int]()
  class CountValuesHolder(var timestamp: Long = 0L) extends AggregateHolder {
    val frequencyMap = debox.Map[Double, Int]()
    val row = new CountValuesTransientRow
    def toRowReader: MutableRowReader = {
      val size = frequencyMap.size * 12
      if (serializedMap.length < size) serializedMap = new Array[Byte](size) // Create bigger array
      CountValuesTransientRow.serialize(frequencyMap, serializedMap)
      row.setLong(0, timestamp)
      row.setBlob(1, serializedMap, UnsafeUtils.arayOffset, size)
      row
    }

    def resetToZero(): Unit = {
      frequencyMap.clear()
    }

    def addValue(value: Double, count: Int): Unit = {
      if (!value.isNaN) {
        if (frequencyMap.contains(value)) frequencyMap(value) = frequencyMap.get(value).get + count
        else frequencyMap(value) = count
        if (frequencyMap.size > limit)
          throw new UnsupportedOperationException("CountValues has no of unique values greater than 1000." +
            "Reduce number of unique values by using topk, comparison operator etc")
      }
    }
  }

  type AggHolderType = CountValuesHolder
  def zero: CountValuesHolder = new CountValuesHolder()

  def newRowToMapInto: MutableRowReader = new CountValuesTransientRow

  def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = {
    mapInto.setLong(0, item.getLong(0))
    rowMap.clear()
    rowMap(item.getDouble(1)) = 1
    CountValuesTransientRow.serialize(rowMap, serializedMap)
    mapInto.setBlob(1, serializedMap, UnsafeUtils.arayOffset, rowMap.size * 12)
    mapInto
  }

  def reduceAggregate(acc: CountValuesHolder, aggRes: RowReader): CountValuesHolder = {
    acc.timestamp = aggRes.getLong(0)

    // No need to create map when there is only one value
    if (CountValuesTransientRow.hasOneSample(aggRes.getBlobNumBytes(1))) {
      acc.addValue(CountValuesTransientRow.getValueForOneSample(aggRes.getBlobBase(1),
        aggRes.getBlobOffset(1)), CountValuesTransientRow.getCountForOneSample(
        aggRes.getBlobBase(1), aggRes.getBlobOffset(1)) )
    } else {
      val aggMap = CountValuesTransientRow.deserialize(aggRes.getBlobBase(1),
        aggRes.getBlobNumBytes(1), aggRes.getBlobOffset(1))
      aggMap.keysSet.foreach(x => acc.addValue(x, aggMap.get(x).get))
    }
    acc
  }

  def present(aggRangeVector: RangeVector, limit: Int): Seq[RangeVector] = {
    val colSchema = Seq(ColumnInfo("timestamp", ColumnType.LongColumn), ColumnInfo("value", ColumnType.DoubleColumn))
    val recSchema = SerializedRangeVector.toSchema(colSchema)
    val resRvs = mutable.Map[RangeVectorKey, RecordBuilder]()
    // Important TODO / TechDebt: We need to replace Iterators with cursors to better control
    // the chunk iteration, lock acquisition and release. This is much needed for safe memory access.
    try {
      FiloSchedulers.assertThreadName(QuerySchedName)
      // aggRangeVector.rows.take below triggers the ChunkInfoIterator which requires lock/release
      ChunkMap.validateNoSharedLocks(s"CountValues-$label")
      aggRangeVector.rows.take(limit).foreach { row =>
        val rowMap = CountValuesTransientRow.deserialize(row.getBlobBase(1),
          row.getBlobNumBytes(1), row.getBlobOffset(1))
        rowMap.foreach { (k, v) =>
          val rvk = CustomRangeVectorKey(aggRangeVector.key.labelValues +
            (label.utf8 -> k.toString.utf8))
          val builder = resRvs.getOrElseUpdate(rvk, SerializedRangeVector.newBuilder())
          builder.startNewRecord(recSchema)
          builder.addLong(row.getLong(0))
          builder.addDouble(v)
          builder.endRecord()
        }
      }
    }
    finally {
      ChunkMap.releaseAllSharedLocks()
    }
    resRvs.map { case (key, builder) =>
      val numRows = builder.allContainers.map(_.countRecords).sum
      new SerializedRangeVector(key, numRows, builder.allContainers, recSchema, 0)
    }.toSeq
  }

  def reductionSchema(source: ResultSchema): ResultSchema = {
    val cols = new Array[ColumnInfo](2)
    cols(0) = source.columns(0)
    cols(1) = ColumnInfo("map", ColumnType.StringColumn)
    ResultSchema(cols, 1)
  }

  def presentationSchema(reductionSchema: ResultSchema): ResultSchema = {
    ResultSchema(Array(reductionSchema.columns(0), ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  }
}

