package filodb.core.query

import java.time.{LocalDateTime, YearMonth, ZoneOffset}
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.StrictLogging
import debox.Buffer
import kamon.Kamon
import kamon.metric.MeasurementUnit
import org.joda.time.DateTime

import filodb.core.binaryrecord2.{MapItemConsumer, RecordBuilder, RecordContainer, RecordSchema}
import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType._
import filodb.core.store._
import filodb.memory.{BinaryRegionLarge, MemFactory, UTF8StringMedium, UTF8StringShort}
import filodb.memory.data.ChunkMap
import filodb.memory.format.{RowReader, ZeroCopyUTF8String => UTF8Str}
import filodb.memory.format.vectors.Histogram

/**
  * Identifier for a single RangeVector.
  * Sub-classes must be a case class or override equals/hashcode since this class is used in a
  * hash table.
  */
trait RangeVectorKey extends java.io.Serializable {
  def labelValues: Map[UTF8Str, UTF8Str]
  def sourceShards: Seq[Int]
  def partIds: Seq[Int]
  def schemaNames: Seq[String]
  def keySize: Int
  override def toString: String = s"/shard:${sourceShards.mkString(",")}/$labelValues"
}

class SeqMapConsumer extends MapItemConsumer {
  val pairs = new collection.mutable.ArrayBuffer[(UTF8Str, UTF8Str)]
  def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
    val keyUtf8 = new UTF8Str(keyBase, keyOffset + 1, UTF8StringShort.numBytes(keyBase, keyOffset))
    val valUtf8 = new UTF8Str(valueBase, valueOffset + 2, UTF8StringMedium.numBytes(valueBase, valueOffset))
    pairs += (keyUtf8 -> valUtf8)
  }
}

/**
  * Range Vector Key backed by a BinaryRecord v2 partition key, which is basically a pointer to memory on or offheap.
  */
final case class PartitionRangeVectorKey(partKeyData: Either[ReadablePartition, (Array[Byte], Long)],
                                         partSchema: RecordSchema,
                                         partKeyCols: Seq[ColumnInfo],
                                         sourceShard: Int,
                                         groupNum: Int,
                                         partId: Int,
                                         schemaName: String) extends RangeVectorKey {
  def partBase: Array[Byte] = partKeyData match {
    case Left(part) => part.partKeyBase
    case Right((p, _)) => p
  }
  def partOffset: Long = partKeyData match {
    case Left(part) => part.partKeyOffset
    case Right((_, off)) => off
  }

  override def sourceShards: Seq[Int] = Seq(sourceShard)
  override def partIds: Seq[Int] = Seq(partId)
  override def schemaNames: Seq[String] = Seq(schemaName)

  def keySize: Int = {
    partKeyData match {
      case Left(part) =>BinaryRegionLarge.numBytes(part.partKeyBase, part.partKeyOffset)
      case Right((base, off)) =>  BinaryRegionLarge.numBytes(base, off)
    }
  }

  def labelValues: Map[UTF8Str, UTF8Str] = {
    partKeyCols.zipWithIndex.flatMap { case (c, pos) =>
      c.colType match {
        case StringColumn => Seq(UTF8Str(c.name) -> partSchema.asZCUTF8Str(partBase, partOffset, pos))
        case IntColumn    => Seq(UTF8Str(c.name) -> UTF8Str(partSchema.getInt(partBase, partOffset, pos).toString))
        case LongColumn   => Seq(UTF8Str(c.name) -> UTF8Str(partSchema.getLong(partBase, partOffset, pos).toString))
        case MapColumn    => val consumer = new SeqMapConsumer
          partSchema.consumeMapItems(partBase, partOffset, pos, consumer)
          consumer.pairs
        case _            => throw new UnsupportedOperationException("Not supported yet")
      }
    }.toMap
  }
  override def toString: String = s"/shard:$sourceShard/${partSchema.stringify(partBase, partOffset)} [grp$groupNum]"
}

final case class CustomRangeVectorKey(labelValues: Map[UTF8Str, UTF8Str],
                                      sourceShards: Seq[Int] = Nil,
                                      partIds: Seq[Int] = Nil,
                                      schemaNames: Seq[String] = Nil)
  extends RangeVectorKey {
  def keySize: Int = {
    labelValues.foldLeft(0) { case (s, e) =>
      s + e._1.numBytes + e._2.numBytes
    }
  }
}

object CustomRangeVectorKey {
  def fromZcUtf8(str: UTF8Str): CustomRangeVectorKey = {
    CustomRangeVectorKey(str.asNewString.split("\u03BC").map(_.split("\u03C0")).filter(_.length == 2).map { lv =>
      UTF8Str(lv(0)) -> UTF8Str(lv(1))
    }.toMap)
  }

  def toZcUtf8(rvk: RangeVectorKey): UTF8Str = {
    // TODO can we optimize this further? Can we use a binary field in the row-reader ?
    val str = rvk.labelValues.toSeq.map(lv=>s"${lv._1.asNewString}\u03C0${lv._2.asNewString}").sorted.mkString("\u03BC")
    UTF8Str(str)
  }

  val empty = CustomRangeVectorKey(Map.empty)
  val emptyAsZcUtf8 = toZcUtf8(empty)
}

case class RvRange(startMs: Long, stepMs: Long, endMs: Long)

/**
  * Represents a single result of any FiloDB Query.
  */
trait RangeVector {
  def key: RangeVectorKey

  def rows(): RangeVectorCursor

  /**
   * If Some, then it describes start/step/end of output data.
   * Present only for time series data that is periodic. If raw data is requested, then None.
   */
  def outputRange: Option[RvRange]

  // FIXME remove default in numRows since many impls simply default to None. Shouldn't scalars implement this
  def numRows: Option[Int] = None

  def prettyPrint(formatTime: Boolean = true): String = "RV String Not supported"
}

/**
  *  A marker trait to identify range vector that can be serialized for write into wire. If Range Vector does not
  *  implement this marker trait, then query engine will convert it to one that does.
  */
sealed trait SerializableRangeVector extends RangeVector {
  /**
   * Used to calculate number of samples sent over the wire for limiting resources used by query
   */
  def numRowsSerialized: Int

  /**
   * Estimates the total size (in bytes) of all rows after serialization.
   */
  def estimateSerializedRowBytes: Long

  def estimatedSerializedBytes: Long = estimateSerializedRowBytes + key.keySize
}

object SerializableRangeVector {
  val SizeOfByte = 1
  val SizeOfBoolean = 1
  val SizeOfChar = 1
  val SizeOfShort = 2
  val SizeOfFloat = 4
  val SizeOfInt = 4
  val SizeOfDouble = 8
  val SizeOfLong = 8
}

/**
  * Range Vector that represents a scalar result. Scalar results result in only one range vector.
  */
sealed trait ScalarRangeVector extends SerializableRangeVector {
  def key: RangeVectorKey = CustomRangeVectorKey(Map.empty)
  def getValue(time: Long): Double
}

/**
  * ScalarRangeVector which has time specific value
  */
final case class ScalarVaryingDouble(val timeValueMap: Map[Long, Double],
                                     override val outputRange: Option[RvRange]) extends ScalarRangeVector {
  import NoCloseCursor._
  override def rows: RangeVectorCursor = timeValueMap.toList.sortWith(_._1 < _._1).
                                            map { x => new TransientRow(x._1, x._2) }.iterator
  def getValue(time: Long): Double = timeValueMap(time)

  override def numRowsSerialized: Int = timeValueMap.size

  override def estimateSerializedRowBytes: Long = {
    // Include the size of each timestamp / value in the map.
    timeValueMap.size * (SerializableRangeVector.SizeOfLong + SerializableRangeVector.SizeOfDouble)
  }
}

final case class RangeParams(startSecs: Long, stepSecs: Long, endSecs: Long)

/**
 * Populate the vector from a single row.
 * Let say startMs=10, stepMs=10, endMs=50, RowReader has a double value 5 with a timestamp 100.
 * The result vector would be [10 -> 5, 20 -> 5, 30 -> 5, 40 -> 5, 50->5].
 * @param rangeVectorKey the range vector key.
 * @param startMs the start timestamp in ms.
 * @param stepMs the step in ms.
 * @param endMs the end timestamp in ms
 * @param rowReader the row read that provide the value.
 * @param schema the schema.
 */
final class RepeatValueVector(rangeVectorKey: RangeVectorKey,
                              startMs: Long, stepMs: Long, endMs: Long,
                              rowReader: Option[RowReader],
                              schema: RecordSchema) extends SerializableRangeVector {
  override def outputRange: Option[RvRange] = Some(RvRange(startMs, stepMs, endMs))
  override val numRows: Option[Int] = Some((endMs - startMs) / math.max(1, stepMs) + 1).map(_.toInt)

  lazy val containers: Seq[RecordContainer] = {
    val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
    rowReader.map(builder.addFromReader(_, schema, 0))
    builder.allContainers.toList
  }

  val recordSchema: RecordSchema = schema

  // There is potential for optimization.
  // The parent transformer does not need to iterate all rows.
  // It can transform one data because data at all steps are identical. It just need to return a RepeatValueVector.
  override def rows(): RangeVectorCursor = {
    import NoCloseCursor._
    // If rowReader is empty, iterate nothing.
    val it = Iterator.from(0, rowReader.map(_ => stepMs.toInt).getOrElse(1))
      .takeWhile(_ <= endMs - startMs).map { i =>
      val rr = rowReader.get
      val t = i + startMs
      new RowReader {
        override def notNull(columnNo: Int): Boolean = rr.notNull(columnNo)
        override def getBoolean(columnNo: Int): Boolean = rr.getBoolean(columnNo)
        override def getInt(columnNo: Int): Int = rr.getInt(columnNo)
        override def getLong(columnNo: Int): Long = if (columnNo == 0) t else rr.getLong(columnNo)
        override def getDouble(columnNo: Int): Double = rr.getDouble(columnNo)
        override def getFloat(columnNo: Int): Float = rr.getFloat(columnNo)
        override def getString(columnNo: Int): String = rr.getString(columnNo)
        override def getAny(columnNo: Int): Any = rr.getAny(columnNo)
        override def getBlobBase(columnNo: Int): Any = rr.getBlobBase(columnNo)
        override def getBlobOffset(columnNo: Int): Long = rr.getBlobOffset(columnNo)
        override def getBlobNumBytes(columnNo: Int): Int = rr.getBlobNumBytes(columnNo)
        override def getHistogram(columnNo: Int): Histogram = rr.getHistogram(columnNo)
      }
    }
    // address step == 0 case
    if (startMs == endMs) it.take(1)
    else it
  }
  override def key: RangeVectorKey = rangeVectorKey

  /**
   * Used to calculate number of samples sent over the wire for limiting resources used by query
   */
  override def numRowsSerialized: Int = 1

  /**
   * Estimates the total size (in bytes) of all rows after serialization.
   */
  override def estimateSerializedRowBytes: Long = containers.size
}

object RepeatValueVector extends StrictLogging {
  import filodb.core._
  def apply(rv: RangeVector,
            startMs: Long, stepMs: Long, endMs: Long,
            schema: RecordSchema,
            execPlan: String,
            queryStats: QueryStats): RepeatValueVector = {
    val startNs = Utils.currentThreadCpuTimeNanos
    try {
      var nextRow: Option[RowReader] = None
      try {
        ChunkMap.validateNoSharedLocks(execPlan)
        val rows = rv.rows()
        if (rows.hasNext) {
           nextRow = Some(rows.next())
        }
      } finally {
        rv.rows().close()
        // clear exec plan
        // When the query is done, clean up lingering shared locks caused by iterator limit.
        ChunkMap.releaseAllSharedLocks()
      }
      new RepeatValueVector(rv.key, startMs, stepMs, endMs, nextRow, schema)
    } finally {
      queryStats.getCpuNanosCounter(Nil).addAndGet(Utils.currentThreadCpuTimeNanos - startNs)
    }
  }
}

sealed trait ScalarSingleValue extends ScalarRangeVector {
  def rangeParams: RangeParams
  override def outputRange: Option[RvRange] = Some(RvRange(rangeParams.startSecs * 1000,
                                             rangeParams.stepSecs * 1000, rangeParams.endSecs * 1000))
  val numRowsSerialized : Int = 1

  override def rows(): RangeVectorCursor = {
    import NoCloseCursor._
    val it = Iterator.from(0, rangeParams.stepSecs.toInt)
                     .takeWhile(_ <= rangeParams.endSecs - rangeParams.startSecs).map { i =>
      val t = i + rangeParams.startSecs
      new TransientRow(t * 1000, getValue(t * 1000))
    }
    // address step == 0 case
    if (rangeParams.startSecs == rangeParams.endSecs) it.take(1)
    else it
  }

  // Negligible bytes sent over-the-wire. Don't bother calculating accurately.
  override def estimateSerializedRowBytes: Long = SerializableRangeVector.SizeOfDouble
}

/**
  * ScalarRangeVector which has one value for all time's
  */
final case class ScalarFixedDouble(rangeParams: RangeParams, value: Double) extends ScalarSingleValue {
  def getValue(time: Long): Double = value
}

/**
  * ScalarRangeVector for which value is the time of the instant sample in seconds.
  */
final case class TimeScalar(rangeParams: RangeParams) extends ScalarSingleValue {
  override def getValue(time: Long): Double = time.toDouble / 1000
}

/**
  * ScalarRangeVector for which value is UTC Hour
  */
final case class HourScalar(rangeParams: RangeParams) extends ScalarSingleValue {
  override def getValue(time: Long): Double = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC)
                                              .getHour
}

/**
  * ScalarRangeVector for which value is current UTC Minute
  */
final case class MinuteScalar(rangeParams: RangeParams) extends ScalarSingleValue {
  override def getValue(time: Long): Double = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC)
                                              .getMinute
}

/**
  * ScalarRangeVector for which value is current UTC Minute
  */
final case class MonthScalar(rangeParams: RangeParams) extends ScalarSingleValue {
  override def getValue(time: Long): Double = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC)
                                              .getMonthValue
}

/**
  * ScalarRangeVector for which value is current UTC Year
  */
final case class YearScalar(rangeParams: RangeParams) extends ScalarSingleValue {
  override def getValue(time: Long): Double = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC)
                                              .getYear
}

/**
  * ScalarRangeVector for which value is current UTC day of month
  */
final case class DayOfMonthScalar(rangeParams: RangeParams) extends ScalarSingleValue {
  override def getValue(time: Long): Double = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC)
                                              .getDayOfMonth
}

/**
  * ScalarRangeVector for which value is current UTC day of week
  */
final case class DayOfWeekScalar(rangeParams: RangeParams) extends ScalarSingleValue {
  override def getValue(time: Long): Double = {
    val dayOfWeek = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC).getDayOfWeek.getValue
    if (dayOfWeek == 7) 0 else dayOfWeek
  }
}

/**
  * ScalarRangeVector for which value is current UTC days in month
  */
final case class DaysInMonthScalar(rangeParams: RangeParams) extends ScalarSingleValue {
  override def getValue(time: Long): Double = {
      val ldt = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC)
      YearMonth.from(ldt).lengthOfMonth()
  }
}

// First column of columnIDs should be the timestamp column
final case class RawDataRangeVector(key: RangeVectorKey,
                                    partition: ReadablePartition,
                                    chunkMethod: ChunkScanMethod,
                                    columnIDs: Array[Int],
                                    dataBytesScannedCtr: AtomicLong,
                                    maxBytesScanned: Long,
                                    queryId: String) extends RangeVector {
  // Iterators are stateful, for correct reuse make this a def
  // UPDATE: 10/31/2022: Using CountingChunkInfoIterator instead of default `ReadablePartition.infos` iterator.
  // This is done to count and track the dataBytesScanned info for raw queries
  def rows(): RangeVectorCursor = partition.timeRangeRows(
    chunkMethod,
    columnIDs,
    new CountingChunkInfoIterator(
      partition.infos(chunkMethod), columnIDs, dataBytesScannedCtr, maxBytesScanned, queryId)
  )

  // Obtain ChunkSetInfos from specific window of time from partition
  def chunkInfos(windowStart: Long, windowEnd: Long): ChunkInfoIterator = {
    new CountingChunkInfoIterator(
      partition.infos(windowStart, windowEnd), columnIDs, dataBytesScannedCtr, maxBytesScanned, queryId
    )
  }

  // the query engine is based around one main data column to query, so it will always be the second column passed in
  def valueColID: Int = columnIDs(1)

  def publishInterval: Option[Long] = partition.publishInterval

  override def outputRange: Option[RvRange] = None

  def minResolutionMs: Int = partition.minResolutionMs
}

/**
  * A RangeVector designed to return one row per ChunkSetInfo, with the following schema:
  * ID (Long), NumRows (Int), startTime (Long), endTime (Long), numBytes(I) of chunk, readerclass of chunk
  * @param column the Column to return detailed chunk info about, must be a DataColumn
  */
final case class ChunkInfoRangeVector(key: RangeVectorKey,
                                      partition: ReadablePartition,
                                      chunkMethod: ChunkScanMethod,
                                      column: Column) extends RangeVector {
  val reader = new ChunkInfoRowReader(column)
  import NoCloseCursor._
  // Iterators are stateful, for correct reuse make this a def
  def rows(): RangeVectorCursor = partition.infos(chunkMethod).map { info =>
    reader.setInfo(info)
    reader
  }
  override def outputRange: Option[RvRange] = None
}

/**
  * SerializedRangeVector represents a RangeVector that can be serialized over the wire.
  * RecordContainers may be shared amongst all the SRV's from a single Result to minimize space and heap usage --
  *   this is the reason for the startRecordNo, the row # of the first container.
  * PLEASE PLEASE use Kryo to serialize this as it will make sure the single shared RecordContainer is
  * only serialized once as a single instance.
  */
final class SerializedRangeVector(val key: RangeVectorKey,
                                  val numRowsSerialized: Int,
                                  containers: Seq[RecordContainer],
                                  val schema: RecordSchema,
                                  val startRecordNo: Int,
                                  override val outputRange: Option[RvRange]) extends RangeVector with
                                          SerializableRangeVector with java.io.Serializable {

  override val numRows = {
    if (SerializedRangeVector.canRemoveEmptyRows(outputRange, schema)) {
      Some(((outputRange.get.endMs - outputRange.get.startMs) / outputRange.get.stepMs).toInt + 1)
    } else {
      Some(numRowsSerialized)
    }
  }
  import NoCloseCursor._
  // Possible for records to spill across containers, so we read from all containers
  override def rows: RangeVectorCursor = {
    val it = containers.toIterator.flatMap(_.iterate(schema)).slice(startRecordNo, startRecordNo + numRowsSerialized)
    if (SerializedRangeVector.canRemoveEmptyRows(outputRange, schema)) {
      new Iterator[RowReader] {
        var curTime = outputRange.get.startMs
        val bufIt = it.buffered
        val emptyDouble = new TransientRow(0L, Double.NaN)
        val emptyHist = new TransientHistRow(0L, Histogram.empty)
        override def hasNext: Boolean = curTime <= outputRange.get.endMs
        override def next(): RowReader = {
          if (bufIt.hasNext && bufIt.head.getLong(0) == curTime) {
            curTime += outputRange.get.stepMs
            bufIt.next()
          }
          else {
            if (schema.columns(1).colType == DoubleColumn) {
              emptyDouble.timestamp = curTime
              curTime += outputRange.get.stepMs
              emptyDouble
            } else {
              emptyHist.timestamp = curTime
              curTime += outputRange.get.stepMs
              emptyHist
            }
          }
        }
      }
    } else it
  }

  override def estimateSerializedRowBytes: Long =
    containers.toIterator.flatMap(_.iterate(schema))
      .slice(startRecordNo, startRecordNo + numRowsSerialized)
      .foldLeft(0)(_ + _.recordLength)

  def containersIterator : Iterator[RecordContainer] = containers.toIterator

  /**
    * Pretty prints all the elements into strings using record schema
    */
  override def prettyPrint(formatTime: Boolean = true): String = {
    val curTime = System.currentTimeMillis
    key.toString + "\n\t" +
      rows.map { reader =>
        val firstCol = if (formatTime && schema.isTimeSeries) {
          val timeStamp = reader.getLong(0)
          s"${new DateTime(timeStamp).toString()} (${(curTime - timeStamp)/1000}s ago) $timeStamp"
        } else {
          schema.columnTypes.head match {
            case BinaryRecordColumn => schema.brSchema(0).stringify(reader.getBlobBase(0), reader.getBlobOffset(0))
            case _ => reader.getAny(0).toString
          }
        }
        (firstCol +: (1 until schema.numColumns).map(reader.getAny(_).toString)).mkString("\t")
      }.mkString("\n\t") + "\n"
  }
}

object SerializedRangeVector extends StrictLogging {
  import filodb.core._

  val queryResultBytes = Kamon.histogram("query-engine-result-bytes").withoutTags
  val queryCpuTime = Kamon.counter("query-engine-cpu-time", MeasurementUnit.time.nanoseconds).withoutTags

  def canRemoveEmptyRows(outputRange: Option[RvRange], sch: RecordSchema) : Boolean = {
    outputRange.isDefined && // metadata queries
      sch.isTimeSeries && // metadata queries
      outputRange.get.startMs != outputRange.get.endMs && // instant queries can have raw data
      sch.columns.size == 2 &&
      (sch.columns(1).colType == DoubleColumn || sch.columns(1).colType == HistogramColumn)
  }

  /**
    * Creates a SerializedRangeVector out of another RangeVector by sharing a previously used RecordBuilder.
    * The most efficient option when you need to create multiple SRVs as the containers are automatically
    * shared correctly.
    * The containers are sent whole as most likely more than one would be sent, so they should mostly be packed.
    */
  // scalastyle:off null
  def apply(rv: RangeVector,
            builder: RecordBuilder,
            schema: RecordSchema,
            execPlan: String,
            queryStats: QueryStats): SerializedRangeVector = {
    val startNs = Utils.currentThreadCpuTimeNanos
    var numRows = 0
    try {
      val oldContainerOpt = builder.currentContainer
      val startRecordNo = oldContainerOpt.map(_.numRecords).getOrElse(0)
      try {
        ChunkMap.validateNoSharedLocks(execPlan)
        val rows = rv.rows
        while (rows.hasNext) {
          val nextRow = rows.next()
          // Don't encode empty / NaN data over the wire
          if (!canRemoveEmptyRows(rv.outputRange, schema) ||
            schema.columns(1).colType == DoubleColumn && !java.lang.Double.isNaN(nextRow.getDouble(1)) ||
            schema.columns(1).colType == HistogramColumn && !nextRow.getHistogram(1).isEmpty) {
            numRows += 1
            builder.addFromReader(nextRow, schema, 0)
          }
        }
      } finally {
        rv.rows().close()
        // clear exec plan
        // When the query is done, clean up lingering shared locks caused by iterator limit.
        ChunkMap.releaseAllSharedLocks()
      }
      // If there weren't containers before, then take all of them.  If there were, discard earlier ones, just
      // start with the most recent one we started adding to
      val containers = oldContainerOpt match {
        // The toList ensures we have an immutable representation of containers those are required just for this
        // SerializedRangeVector. By not having a toList we end up pointing to a mutable ArrayBuffer which gets multiple
        // record containers, including all others where this builder was used. While Kryo is smart enough to not
        // serialize the same RecordContainer twice and just send the reference, we end up double counting the result
        // bytes and in serialization with gRPC where we dont maintain the graph of pointers to serialize the record
        // just once, we end up serializing almost twice the amount of data and also exceed our 4MB recommended limit
        // or a gRPC message
        case None => builder.allContainers.toList
        case Some(firstContainer) => builder.allContainers.dropWhile(_ != firstContainer)
      }
      new SerializedRangeVector(rv.key, numRows, containers, schema, startRecordNo, rv.outputRange)
    } finally {
      queryStats.getCpuNanosCounter(Nil).addAndGet(Utils.currentThreadCpuTimeNanos - startNs)
    }
  }
  // scalastyle:on null

  /**
    * Creates a SerializedRangeVector out of another RV and ColumnInfo schema.  Convenient but no sharing.
    * Since it wastes space when working with multiple RVs, should be used mostly for testing.
    */
  def apply(rv: RangeVector, cols: Seq[ColumnInfo], queryStats: QueryStats): SerializedRangeVector = {
    val schema = toSchema(cols)
    apply(rv, newBuilder(), schema, "Test-Only-Plan", queryStats)
  }

  // TODO: make this configurable....
  val MaxContainerSize = 4096    // 4KB allows for roughly 200 time/value samples

  // Reuse RecordSchemas as there aren't too many schemas
  val SchemaCacheSize = 100
  val schemaCache = concurrentCache[Seq[ColumnInfo], RecordSchema](SchemaCacheSize)

  def toSchema(colSchema: Seq[ColumnInfo], brSchemas: Map[Int, RecordSchema] = Map.empty): RecordSchema =
    schemaCache.getOrElseUpdate(colSchema, { cols => new RecordSchema(cols, brSchema = brSchemas) })

  def newBuilder(): RecordBuilder = newBuilder(MaxContainerSize)

  def newBuilder(maxContainerSize: Int): RecordBuilder =
    new RecordBuilder(MemFactory.onHeapFactory, maxContainerSize)
}

final case class IteratorBackedRangeVector(key: RangeVectorKey,
                                           rows: RangeVectorCursor,
                                           override val outputRange: Option[RvRange]) extends RangeVector


final case class BufferRangeVector(key: RangeVectorKey,
                                   timestamps: Buffer[Long],
                                   values: Buffer[Double],
                                   override val outputRange: Option[RvRange]) extends RangeVector {
  require(timestamps.length == values.length, s"${timestamps.length} ts != ${values.length} values")

  def rows(): RangeVectorCursor = new RangeVectorCursor {
    val row = new TransientRow()
    var n = 0
    def hasNext: Boolean = n < timestamps.length
    def next: RowReader = {
      row.setValues(timestamps(n), values(n))
      n += 1
      row
    }
    def close(): Unit = {}
  }
}
