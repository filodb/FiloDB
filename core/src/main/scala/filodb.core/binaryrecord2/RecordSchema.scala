package filodb.core.binaryrecord2

import scala.collection.mutable.ArrayBuffer

import org.agrona.DirectBuffer
import org.agrona.concurrent.UnsafeBuffer
import spire.syntax.cfor._

import filodb.core.metadata.{Column, Schemas}
import filodb.core.metadata.Column.ColumnType.{LongColumn, MapColumn, TimestampColumn}
import filodb.core.query.ColumnInfo
import filodb.memory.{BinaryRegion, BinaryRegionLarge, UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{RowReader, UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.{vectors => bv}

// scalastyle:off number.of.methods

/**
 * A RecordSchema is the schema for a BinaryRecord - what type of each field a BR holds.
 * Since it knows the schema it can also read values out efficiently.  It does not mutate memory or BinaryRecords.
 *
 * One instance of this class is meant to serve all of the BinaryRecords of this schema.
 * Note that BinaryRecords are not regular Java objects, but rather just a memory location or pointer.
 * The methods of this class must be used for access.
 *
 * RecordSchema v2 has a feature called partition key fields.  The idea is that all fields starting at an optional
 * partitionFieldStart belong to the "partition key".  Special features for partition key fields:
 * - A hashcode is calculated for all partition key fields and stored in the record itself for fast comparisons
 *   and raw data recovery
 * - The partition key fields between BinaryRecords can be compared very fast for equality, so long as all the
 *   partition key fields share the same schema (they can start at differend field #'s).  This takes advantage of
 *   the fact that all variable length fields after the partitionFieldStart are contiguous and can be binary compared
 *
 * @param columns In order, the column of each field in this schema
 * @param hash the 16-bit Schema Hash (see DataSchema class) for verification purposes
 * @param brSchema schema of any binary record type column
 * @param partitionFieldStart Some(n) from n to the last field are considered the partition key.  A field number.
 * @param predefinedKeys A list of predefined keys to save space for the tags/MapColumn field(s)
 */
final class RecordSchema(val columns: Seq[ColumnInfo],
                         // val hash: Int,
                         val partitionFieldStart: Option[Int] = None,
                         val predefinedKeys: Seq[String] = Nil,
                         val brSchema: Map[Int, RecordSchema] = Map.empty) {
  import RecordSchema._
  import BinaryRegion.NativePointer

  override def toString: String = s"RecordSchema<$columns, $partitionFieldStart>"

  val colNames = columns.map(_.name)
  val columnTypes = columns.map(_.colType)
  require(columnTypes.nonEmpty, "columnTypes cannot be empty")
  require(predefinedKeys.length <= 64, "Too many predefined keys")
  require(partitionFieldStart.isEmpty ||
          partitionFieldStart.get < columnTypes.length, s"partitionFieldStart $partitionFieldStart is too high")

  // Offset to fixed area for each field.  Extra element at end is end of fixed size area / hash.
  // Note: these offsets start at 4, after the length header
  val fixedStart = partitionFieldStart.map(x => 6).getOrElse(4)
  private val offsets = columnTypes.map(colTypeToFieldSize).scan(fixedStart)(_ + _).toArray

  // Offset from BR start to beginning of variable area.  Also the minimum length of a BR.
  val variableAreaStart = partitionFieldStart.map(x => 4).getOrElse(0) + offsets.last

  val (predefKeyOffsets, predefKeyBytes, predefKeyNumMap) = makePredefinedStructures(predefinedKeys)

  val numFields = columnTypes.length

  // Typed, efficient functions, one for each field/column, to add to a RecordBuilder efficiently from a RowReader
  // with no boxing or extra allocations involved
  val builderAdders = columnTypes.zipWithIndex.map {
    case (Column.ColumnType.LongColumn, colNo) =>
      (row: RowReader, builder: RecordBuilder) => builder.addLong(row.getLong(colNo))
    case (Column.ColumnType.TimestampColumn, colNo) =>
      (row: RowReader, builder: RecordBuilder) => builder.addLong(row.getLong(colNo))
    case (Column.ColumnType.DoubleColumn, colNo) =>
      (row: RowReader, builder: RecordBuilder) => builder.addDouble(row.getDouble(colNo))
    case (Column.ColumnType.HistogramColumn, colNo) =>
      (row: RowReader, builder: RecordBuilder) => builder.addBlob(row.getHistogram(colNo).serialize())
    case (Column.ColumnType.IntColumn, colNo) =>
      (row: RowReader, builder: RecordBuilder) => builder.addInt(row.getInt(colNo))
    case (Column.ColumnType.StringColumn, colNo) =>
      // TODO: we REALLY need a better API than ZeroCopyUTF8String as it creates so much garbage
      (row: RowReader, builder: RecordBuilder) => builder.addBlob(row.filoUTF8String(colNo))
    case (Column.ColumnType.BinaryRecordColumn, colNo) =>
      (row: RowReader, builder: RecordBuilder) =>
        builder.addBlob(row.getBlobBase(colNo), row.getBlobOffset(colNo), row.getBlobNumBytes(colNo))
    case (t: Column.ColumnType, colNo) =>
      // TODO: add more efficient methods
      (row: RowReader, builder: RecordBuilder) => builder.addSlowly(row.getAny(colNo))
  }.toArray

  def numColumns: Int = columns.length

  def isTimeSeries: Boolean = columnTypes.length >= 1 &&
    (columnTypes.head == LongColumn || columnTypes.head == TimestampColumn)

  /**
    * Offset to the fixed field primitive or pointer which are at the beginning of the BR
    * @param index column number (and not column id)
    */
  def fieldOffset(index: Int): Int = offsets(index)

  def numBytes(base: Any, offset: Long): Int = BinaryRegionLarge.numBytes(base, offset)

  /**
   * Retrieves the partition hash field from a BinaryRecord.  If partitionFieldStart is None, the results
   * of this will be undefined.
   */
  def partitionHash(base: Any, offset: Long): Int = UnsafeUtils.getInt(base, offset + offsets.last)
  def partitionHash(address: NativePointer): Int = UnsafeUtils.getInt(address + offsets.last)

  /**
   * Retrieves an Int from field # index.  No schema matching is done for speed - you must use this only when
   * the columnType at that field is really an int.
   */
  def getInt(address: NativePointer, index: Int): Int = UnsafeUtils.getInt(address + offsets(index))
  def getInt(base: Any, offset: Long, index: Int): Int = UnsafeUtils.getInt(base, offset + offsets(index))

  /**
   * Retrieves a Long from field # index.  No schema matching is done for speed - you must use this only when
   * the columnType at that field is really a Long.
   */
  def getLong(address: NativePointer, index: Int): Long = UnsafeUtils.getLong(address + offsets(index))
  def getLong(base: Any, offset: Long, index: Int): Long = UnsafeUtils.getLong(base, offset + offsets(index))

  /**
   * Retrieves a Double from field # index.  No schema matching is done for speed - you must use this only when
   * the columnType at that field is really a Double.
   */
  def getDouble(address: NativePointer, index: Int): Double = UnsafeUtils.getDouble(address + offsets(index))
  def getDouble(base: Any, offset: Long, index: Int): Double = UnsafeUtils.getDouble(base, offset + offsets(index))

  /**
   * Retrieves the value class for a native BinaryRecord UTF8 string field.  This should not result in any
   * allocations so long as the severe restrictions for value classes are followed.  Don't use in a collection!
   */
  def utf8StringPointer(address: NativePointer, index: Int): UTF8StringMedium = {
    val utf8Addr = address + UnsafeUtils.getInt(address + offsets(index))
    new UTF8StringMedium(utf8Addr)
  }

  /**
   * Extracts out the base, offset, length of a string/blob field.  Much preferable to using
   * asJavaString/asZCUTF8Str methods due to not needing allocations.
   */
  def blobBase(base: Any, offset: Long, index: Int): Any = base
  def blobOffset(base: Any, offset: Long, index: Int): Long =
    offset + UnsafeUtils.getInt(base, offset + offsets(index)) + 2
  def blobNumBytes(base: Any, offset: Long, index: Int): Int =
    UTF8StringMedium.numBytes(base, offset + UnsafeUtils.getInt(base, offset + offsets(index)))

  /**
   * Sets an existing DirectBuffer to wrap around the given blob/UTF8/Histogram bytes, including the
   * 2-byte length prefix.  Since the DirectBuffer is already allocated, this results in no new allocations.
   * Could be used to efficiently retrieve blobs or histograms again and again.
   */
  def blobAsBuffer(base: Any, offset: Long, index: Int, buf: DirectBuffer): Unit = {
    // Number of bytes to give out should not be beyond range of record
    val blobLen = Math.min(numBytes(base, offset), blobNumBytes(base, offset, index) + 2)
    base match {
      case a: Array[Byte] =>
        buf.wrap(a, utf8StringOffset(base, offset, index).toInt - UnsafeUtils.arayOffset, blobLen)
      case UnsafeUtils.ZeroPointer =>
        buf.wrap(utf8StringOffset(base, offset, index), blobLen)
    }
  }

  // Same as above but allocates a new UnsafeBuffer wrapping the blob as a reference
  def blobAsBuffer(base: Any, offset: Long, index: Int): DirectBuffer = {
    val newBuf = new UnsafeBuffer(Array.empty[Byte])
    blobAsBuffer(base, offset, index, newBuf)
    newBuf
  }

  /**
    * Used for extracting the offset for a UTF8StringMedium.
    * Note that blobOffset method is for the offset to the actual blob bytes, not including length header.
    */
  def utf8StringOffset(base: Any, offset: Long, index: Int): Long =
    offset + UnsafeUtils.getInt(base, offset + offsets(index))

  /**
   * COPIES the BinaryRecord field # index out as a new Java String on the heap.  Allocation + copying cost.
   */
  def asJavaString(base: Any, offset: Long, index: Int): String =
    UTF8StringMedium.toString(base, offset + UnsafeUtils.getInt(base, offset + offsets(index)))

  // TEMPorary: to be deprecated
  def asZCUTF8Str(base: Any, offset: Long, index: Int): ZeroCopyUTF8String = {
    val realOffset = offset + UnsafeUtils.getInt(base, offset + offsets(index))
    new ZeroCopyUTF8String(base, realOffset + 2, UTF8StringMedium.numBytes(base, realOffset))
  }
  def asZCUTF8Str(address: NativePointer, index: Int): ZeroCopyUTF8String =
    asZCUTF8Str(UnsafeUtils.ZeroPointer, address, index)

  /**
   * EXPENSIVE to do at server side. Creates a easy-to-read string
   * representation of the contents of this BinaryRecord.
   */
  def stringify(base: Any, offset: Long): String = {
    import Column.ColumnType._
    val result = new ArrayBuffer[String]()
    columnTypes.zipWithIndex.map {
      case (IntColumn, i)    => result += s"${colNames(i)}=${getInt(base, offset, i)}"
      case (LongColumn, i)   => result += s"${colNames(i)}=${getLong(base, offset, i)}"
      case (DoubleColumn, i) => result += s"${colNames(i)}=${getDouble(base, offset, i)}"
      case (StringColumn, i) => result += s"${colNames(i)}=${asJavaString(base, offset, i)}"
      case (TimestampColumn, i) => result += s"${colNames(i)}=${getLong(base, offset, i)}"
      case (MapColumn, i)    => val consumer = new StringifyMapItemConsumer
                                consumeMapItems(base, offset, i, consumer)
                                result += s"${colNames(i)}=${consumer.prettyPrint}"
      case (BinaryRecordColumn, i)  => result += s"${colNames(i)}=${brSchema(i).stringify(base, offset)}"
      case (HistogramColumn, i) =>
        result += s"${colNames(i)}= ${bv.BinaryHistogram.BinHistogram(blobAsBuffer(base, offset, i))}"
    }
    val schemaName = Schemas.global.schemaName(RecordSchema.schemaID(base, offset))
    val schemaStr = partitionFieldStart.map(x => s"schema=$schemaName ").getOrElse("")
    s"b2[$schemaStr ${result.mkString(",")}]"
  }

  def stringify(address: NativePointer): String = stringify(UnsafeUtils.ZeroPointer, address)
  def stringify(bytes: Array[Byte]): String = stringify(bytes, UnsafeUtils.arayOffset)

  /**
   * Extremely detailed breakdown of this BinaryRecord, including offset numbers and bytes possibly.
   */
  def debugString(base: Any, offset: Long): String = {
    def varOffset(i: Int): Int = UnsafeUtils.getInt(base, offset + offsets(i))

    import Column.ColumnType._
    val schemaStr = if (partitionFieldStart.isEmpty) "" else {
                      val id = RecordSchema.schemaID(base, offset)
                      s"  schemaID: $id (${Schemas.global.schemaName(id)})\n"
                    }
    val partHashStr = if (partitionFieldStart.isEmpty) "" else s"  partitionHash: ${partitionHash(base, offset)}\n"
    val colDetails = columnTypes.zipWithIndex.map {
      case (IntColumn, i)    => f"  +${offsets(i)}%05d ${colNames(i)}%14s I ${getInt(base, offset, i)}"
      case (LongColumn, i)   => f"  +${offsets(i)}%05d ${colNames(i)}%14s L ${getLong(base, offset, i)}"
      case (DoubleColumn, i) => f"  +${offsets(i)}%05d ${colNames(i)}%14s D ${getDouble(base, offset, i)}"
      case (StringColumn, i) =>
        f"  +${offsets(i)}%05d ${colNames(i)}%14s S -> +${varOffset(i)}%05d\n" +
        s"\t${blobNumBytes(base, offset, i)} bytes: [${asJavaString(base, offset, i)}]"
      case (TimestampColumn, i) => f"  +${offsets(i)}%05d ${colNames(i)}%14s L ${getLong(base, offset, i)}"
      case (MapColumn, i)    => val consumer = new DebugStringMapItemConsumer(offset)
                                consumeMapItems(base, offset, i, consumer)
                                f"  +${offsets(i)}%05d ${colNames(i)}%14s M -> +${varOffset(i)}%05d\n" +
                                  consumer.lines.mkString("\n")
      case (BinaryRecordColumn, i)  => s"${colNames(i)}=${brSchema(i).stringify(base, offset)}"
      case (HistogramColumn, i) =>
        f"  +${offsets(i)}%05d ${colNames(i)}%14s H -> +${varOffset(i)}%05d\n" +
        s"\t${bv.BinaryHistogram.BinHistogram(blobAsBuffer(base, offset, i))}"
    }
    s"BRDEBUG: ${numBytes(base, offset)} bytes, $numFields fields\n$schemaStr$partHashStr" + colDetails.mkString("\n")
  }

  /**
    * EXPENSIVE to do at server side. Creates a stringified map with contents of this BinaryRecord.
    */
  def toStringPairs(base: Any, offset: Long): Seq[(String, String)] = {
    import Column.ColumnType._
    val result = new collection.mutable.ArrayBuffer[(String, String)]()
    columnTypes.zipWithIndex.map {
      case (IntColumn, i)    => result += ((colNames(i), getInt(base, offset, i).toString))
      case (LongColumn, i)   => result += ((colNames(i), getLong(base, offset, i).toString))
      case (DoubleColumn, i) => result += ((colNames(i), getDouble(base, offset, i).toString))
      case (StringColumn, i) => result += ((colNames(i), asJavaString(base, offset, i).toString))
      case (TimestampColumn, i) => result += ((colNames(i), getLong(base, offset, i).toString))
      case (MapColumn, i)    => val consumer = new StringifyMapItemConsumer
                                consumeMapItems(base, offset, i, consumer)
                                result ++= consumer.stringPairs
      case (BinaryRecordColumn, i) => result ++= brSchema(i).toStringPairs(blobBase(base, offset, i),
                                                                           blobOffset(base, offset, i))
                                result += ("_type_" ->
                                              Schemas.global.schemaName(
                                                RecordSchema.schemaID(blobBase(base, offset, i),
                                                                      blobOffset(base, offset, i))))
      case (HistogramColumn, i) =>
        result += ((colNames(i), bv.BinaryHistogram.BinHistogram(blobAsBuffer(base, offset, i)).toString))
    }
    result
  }

  /**
   * Iterates through each key/value pair of a MapColumn field without any object allocations.
   * How is this done?  By calling the consumer for each pair and directly passing the base and offset.
   * The consumer would use the UTF8StringMedium object to work with the UTF8String blobs.
   *
   * TODO: have a version of consumer that is passed the value class if both key and value are offheap.
   * This can only be done however if we move the predefined keys offheap.
   */
  def consumeMapItems(base: Any, offset: Long, index: Int, consumer: MapItemConsumer): Unit = {
    val mapOffset = offset + UnsafeUtils.getInt(base, offset + offsets(index))
    val mapNumBytes = UnsafeUtils.getShort(base, mapOffset).toInt
    var curOffset = mapOffset + 2
    val endOffset = curOffset + mapNumBytes
    var itemIndex = 0
    while (curOffset < endOffset) {
      // Read key length.  Is it a predefined key?
      val keyLen = UnsafeUtils.getShort(base, curOffset) & 0x00FF
      val keyIndex = keyLen ^ 0x0C0
      if (keyIndex < 64) {   // predefined key; no key bytes
        consumer.consume(predefKeyBytes, predefKeyOffsets(keyIndex), base, curOffset + 1, itemIndex)
        curOffset += 3 + (UnsafeUtils.getShort(base, curOffset + 1) & 0x0FFFF)
      } else {
        consumer.consume(base, curOffset, base, curOffset + 1 + keyLen, itemIndex)
        curOffset += 3 + keyLen + (UnsafeUtils.getShort(base, curOffset + 1 + keyLen) & 0x0FFFF)
      }
      itemIndex += 1
    }
  }

  /**
    * Returns the offset from start of the BinaryRecord to the
    * UTF8StringMedium (2 byte length header + UTF8 string bytes)
    */
  def getStringOffset(base: Any, offset: Long, index: Int): Int = {
    UnsafeUtils.getInt(base, offset + offsets(index))
  }

  def consumeMapItems(address: NativePointer, index: Int, consumer: MapItemConsumer): Unit =
    consumeMapItems(UnsafeUtils.ZeroPointer, address, index, consumer)

  /**
   * Returns true if the two BinaryRecords are equal
   */
  def equals(base1: Any, offset1: Long, base2: Any, offset2: Long): Boolean =
    BinaryRegionLarge.equals(base1, offset1, base2, offset2)
  def equals(record1: NativePointer, record2: NativePointer): Boolean =
    BinaryRegionLarge.equals(UnsafeUtils.ZeroPointer, record1, UnsafeUtils.ZeroPointer, record2)

  /**
   * Returns the BinaryRecordv2 as its own byte array, copying if needed
   */
  def asByteArray(base: Any, offset: Long): Array[Byte] = base match {
    case a: Array[Byte] if offset == UnsafeUtils.arayOffset => a
    case other: Any              => BinaryRegionLarge.asNewByteArray(base, offset)
    case UnsafeUtils.ZeroPointer => BinaryRegionLarge.asNewByteArray(base, offset)
  }
  def asByteArray(address: NativePointer): Array[Byte] = asByteArray(UnsafeUtils.ZeroPointer, address)


  def toHexString(base: Any, offset: Long): String =
    s"0x${asByteArray(base, offset).map("%02X" format _).mkString}"

  /**
   * Allows us to compare two RecordSchemas against each other
   */
  override def equals(other: Any): Boolean = other match {
    case UnsafeUtils.ZeroPointer => false
    case r: RecordSchema => columnTypes == r.columnTypes &&
                            partitionFieldStart == r.partitionFieldStart &&
                            predefinedKeys == r.predefinedKeys
    case other: Any      => false
  }

  override def hashCode: Int = ((columnTypes.hashCode * 31) + partitionFieldStart.hashCode) * 31 +
                               predefinedKeys.hashCode

  import debox.{Map => DMap}   // An unboxed, fast Map

  private def makePredefinedStructures(predefinedKeys: Seq[String]): (Array[Long], Array[Byte], DMap[Long, Int]) = {
    // Convert predefined keys to UTF8StringShorts.  First estimate size they would all take.
    val totalNumBytes = predefinedKeys.map(_.length + 1).sum
    val stringBytes = new Array[Byte](totalNumBytes)
    val keyToNum = DMap.empty[Long, Int]
    var index = 0
    val offsets = predefinedKeys.scanLeft(UnsafeUtils.arayOffset.toLong) { case (offset, str) =>
                    val bytes = str.getBytes
                    require(bytes.size < 192, s"Predefined key $str too long")
                    UTF8StringShort.copyByteArrayTo(bytes, stringBytes, offset)
                    keyToNum(makeKeyKey(bytes)) = index
                    index += 1
                    offset + bytes.size + 1
                  }.toArray
    (offsets, stringBytes, keyToNum)
  }

  // For serialization purposes
  private[filodb] def toSerializableTuple: (Seq[ColumnInfo], Option[Int], Seq[String], Map[Int, RecordSchema]) =
    (columns, partitionFieldStart, predefinedKeys, brSchema)
}

trait MapItemConsumer {
  /**
   * Invoked for each key and value pair.
   * @param (keyBase, keyOffset) pointer to the key UTF8StringShort
   * @param (valueBase, valueOffset) pointer to the value UTF8StringMedium
   * @param index an increasing index of the pair within the map, starting at 0
   */
  def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit
}

/**
 * A MapItemConsumer which turns the key and value pairs into strings
 */
class StringifyMapItemConsumer extends MapItemConsumer {
  val stringPairs = new collection.mutable.ArrayBuffer[(String, String)]
  def prettyPrint: String = "{" + stringPairs.map { case (k, v) => s"$k: $v" }.mkString(", ") + "}"
  def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
    stringPairs += (UTF8StringShort.toString(keyBase, keyOffset) ->
                    UTF8StringMedium.toString(valueBase, valueOffset))
  }
}

// Used for debugPrint only
class DebugStringMapItemConsumer(baseOffset: Long) extends MapItemConsumer {
  val lines = new collection.mutable.ArrayBuffer[String]
  def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
    // TODO: emit actual bytes
    val keyPrefix = if (keyBase == valueBase) f"+${keyOffset - baseOffset}%05d"
                    else f"Predef: 0x${UnsafeUtils.getByte(valueBase, valueOffset - 1)}%02x"
    lines += f"\tKey: $keyPrefix [${UTF8StringShort.toString(keyBase, keyOffset)}] " +
             f"Value: +${valueOffset - baseOffset}%05d [${UTF8StringMedium.toString(valueBase, valueOffset)}]"
  }
}

object RecordSchema {
  import Column.ColumnType._

  val colTypeToFieldSize = Map[Column.ColumnType, Int](IntColumn -> 4,
                                                       LongColumn -> 8,
                                                       DoubleColumn -> 8,
                                                       TimestampColumn -> 8,  // Just a long ms timestamp
                                                       StringColumn -> 4,
                                                       BinaryRecordColumn -> 4,
                                                       MapColumn -> 4,
                                                       HistogramColumn -> 4)

  // Creates a Long from a byte array
  private def eightBytesToLong(bytes: Array[Byte], index: Int, len: Int): Long = {
    var num = 0L
    cforRange { 0 until len } { i =>
      num = (num << 8) ^ (bytes(index + i) & 0x00ff)
    }
    num
  }

  /**
   * Creates a "unique" Long key for each incoming predefined key for quick lookup.  This will not be perfect
   * but probably good enough for the beginning.
   * If the key is 8 or less bytes, we just directly convert the bytes to a long for exact match.
   * If the key is > 8 bytes, we use XXHash to compute a 64-bit hash.
   */
  private[binaryrecord2] def makeKeyKey(strBytes: Array[Byte]): Long =
    makeKeyKey(strBytes, 0, strBytes.size, 7)

  private[binaryrecord2] def makeKeyKey(strBytes: Array[Byte], index: Int, len: Int, keyHash: Int): Long = {
    if (keyHash != 7) keyHash
    else if (len <= 8) { eightBytesToLong(strBytes, index, len) }
    else { BinaryRegion.hasher64.hash(strBytes, index, len, BinaryRegion.Seed) }
  }

  /**
   * Extracts the schemaID out of a BinaryRecord which contains a partition key.  Do not use this on a BR which
   * is not ingestion or partition key.
   */
  final def schemaID(base: Any, offset: Long): Int = {
    require(UnsafeUtils.getInt(base, offset) >= 2, "Empty BinaryRecord/not large enough")
    UnsafeUtils.getShort(base, offset + 4) & 0x0ffff
  }

  final def schemaID(addr: BinaryRegion.NativePointer): Int = schemaID(UnsafeUtils.ZeroPointer, addr)

  final def schemaID(bytes: Array[Byte]): Int =
    if (bytes.size >= 6) schemaID(bytes, UnsafeUtils.arayOffset) else -1

  def fromSerializableTuple(tuple: (Seq[ColumnInfo],
                                    Option[Int], Seq[String], Map[Int, RecordSchema])): RecordSchema =
    new RecordSchema(tuple._1, tuple._2, tuple._3, tuple._4)
}

// Used with PartitionTimeRangeReader, when a user queries for a partition column
final class PartKeyUTF8Iterator(schema: RecordSchema, base: Any, offset: Long, fieldNo: Int) extends bv.UTF8Iterator {
  val blob = schema.asZCUTF8Str(base, offset, fieldNo)
  final def next: ZeroCopyUTF8String = blob
}

final class PartKeyLongIterator(schema: RecordSchema, base: Any, offset: Long, fieldNo: Int) extends bv.LongIterator {
  val num = schema.getLong(base, offset, fieldNo)
  final def next: Long = num
}

/**
 * This is a trait meant to provide a RowReader API for the new BinaryRecord v2.
 * NOTE: Strings cause an allocation of a ZeroCopyUTF8String instance.  TODO: provide a better API that does
 * not result in allocations.
 * It is meant to be reused again and again and is MUTABLE.
 */
trait BinaryRecordRowReaderBase extends RowReader {
  def schema: RecordSchema
  def recordBase: Any
  def recordOffset: Long

  // BinaryRecordV2 fields always have a value
  def notNull(columnNo: Int): Boolean = columnNo >= 0 && columnNo < schema.numFields
  def getBoolean(columnNo: Int): Boolean = schema.getInt(recordBase, recordOffset, columnNo) != 0
  def getInt(columnNo: Int): Int = schema.getInt(recordBase, recordOffset, columnNo)
  def getLong(columnNo: Int): Long = schema.getLong(recordBase, recordOffset, columnNo)
  def getDouble(columnNo: Int): Double = schema.getDouble(recordBase, recordOffset, columnNo)
  def getFloat(columnNo: Int): Float = ???
  def getString(columnNo: Int): String = filoUTF8String(columnNo).toString
  override def getHistogram(columnNo: Int): bv.Histogram =
    bv.BinaryHistogram.BinHistogram(blobAsBuffer(columnNo)).toHistogram

  // getAny is not used for anything fast, and we can make sure Map items are returned correctly
  def getAny(columnNo: Int): Any = schema.columnTypes(columnNo) match {
    case MapColumn => val consumer = new StringifyMapItemConsumer
                      schema.consumeMapItems(recordBase, recordOffset, columnNo, consumer)
                      consumer.stringPairs.toMap
    case _: Any    => schema.columnTypes(columnNo).keyType.extractor.getField(this, columnNo)
  }
  override def filoUTF8String(i: Int): ZeroCopyUTF8String = schema.asZCUTF8Str(recordBase, recordOffset, i)

  def getBlobBase(columnNo: Int): Any = schema.blobBase(recordBase, recordOffset, columnNo)
  def getBlobOffset(columnNo: Int): Long = schema.blobOffset(recordBase, recordOffset, columnNo)
  def getBlobNumBytes(columnNo: Int): Int = schema.blobNumBytes(recordBase, recordOffset, columnNo)

  val buf = new UnsafeBuffer(Array.empty[Byte])
  // NOTE: this method reuses the same buffer to avoid allocations.
  override def blobAsBuffer(columnNo: Int): DirectBuffer = {
    UnsafeUtils.wrapDirectBuf(recordBase, schema.utf8StringOffset(recordBase, recordOffset, columnNo),
                              getBlobNumBytes(columnNo) + 2, buf)
    buf
  }
}

final class BinaryRecordRowReader(val schema: RecordSchema,
                                  var recordBase: Any = UnsafeUtils.ZeroPointer,
                                  var recordOffset: Long = 0L) extends BinaryRecordRowReaderBase

final class MultiSchemaBRRowReader(var recordBase: Any = UnsafeUtils.ZeroPointer,
                                   var recordOffset: Long = 0L) extends BinaryRecordRowReaderBase {
  var schema: RecordSchema = _
}
