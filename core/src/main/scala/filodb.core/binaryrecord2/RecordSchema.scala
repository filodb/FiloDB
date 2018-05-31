package filodb.core.binaryrecord2

import filodb.core.metadata.{Column, Dataset}
import filodb.memory.{BinaryRegion, BinaryRegionLarge, UTF8StringMedium}
import filodb.memory.format.{RowReader, UnsafeUtils, ZeroCopyUTF8String}

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
 * @param columnTypes In order, the field or column type of each field in this schema
 * @param partitionFieldStart Some(n) from n to the last field are considered the partition key.  A field number.
 * @param predefinedKeys A list of predefined keys to save space for the tags/MapColumn field(s)
 */
final class RecordSchema(val columnTypes: Seq[Column.ColumnType],
                         val partitionFieldStart: Option[Int] = None,
                         val predefinedKeys: Seq[String] = Nil) {
  import RecordSchema._
  import BinaryRegion.NativePointer

  require(columnTypes.nonEmpty, "columnTypes cannot be empty")
  require(predefinedKeys.length < 4096, "Too many predefined keys")
  require(partitionFieldStart.isEmpty ||
          partitionFieldStart.get < columnTypes.length, s"partitionFieldStart $partitionFieldStart is too high")

  // Offset to fixed area for each field.  Extra elemnt at end is end of fixed size area / hash.
  // Note: these offsets start at 4, after the length header
  private val offsets = columnTypes.map(colTypeToFieldSize).scan(4)(_ + _).toArray

  // Offset from BR start to beginning of variable area.  Also the minimum length of a BR.
  val variableAreaStart = partitionFieldStart.map(x => 4).getOrElse(0) + offsets.last

  val (predefKeyOffsets, predefKeyBytes, predefKeyNumMap) = makePredefinedStructures(predefinedKeys)

  val numFields = columnTypes.length

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
   * EXPENSIVE. Creates a easy-to-read Java String representation of the contents of this BinaryRecord.
   */
  def stringify(base: Any, offset: Long): String = {
    import Column.ColumnType._
    val parts: Seq[Any] = columnTypes.zipWithIndex.map {
      case (IntColumn, i)    => getInt(base, offset, i)
      case (LongColumn, i)   => getLong(base, offset, i)
      case (DoubleColumn, i) => getDouble(base, offset, i)
      case (StringColumn, i) => asJavaString(base, offset, i)
      case (TimestampColumn, i) => getLong(base, offset, i)
      case (BitmapColumn, i) => getInt(base, offset, i) != 0
      case (MapColumn, i)    =>
        val consumer = new StringifyMapItemConsumer
        consumeMapItems(base, offset, i, consumer)
        consumer.prettyPrint
    }
    s"b2[${parts.mkString(",")}]"
  }

  def stringify(address: NativePointer): String = stringify(UnsafeUtils.ZeroPointer, address)

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
    val mapNumBytes = UnsafeUtils.getInt(base, mapOffset)
    var curOffset = mapOffset + 4
    val endOffset = curOffset + mapNumBytes
    var itemIndex = 0
    while (curOffset < endOffset) {
      // Read key length.  Is it a predefined key?
      val keyLen = UnsafeUtils.getShort(base, curOffset) & 0x0FFFF
      val keyIndex = keyLen ^ 0x0F000
      if (keyIndex < 0x1000) {   // predefined key; no key bytes
        consumer.consume(predefKeyBytes, predefKeyOffsets(keyIndex), base, curOffset + 2, itemIndex)
        curOffset += 4 + (UnsafeUtils.getShort(base, curOffset + 2) & 0x0FFFF)
      } else {
        consumer.consume(base, curOffset, base, curOffset + 2 + keyLen, itemIndex)
        curOffset += 4 + keyLen + (UnsafeUtils.getShort(base, curOffset + 2 + keyLen) & 0x0FFFF)
      }
      itemIndex += 1
    }
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

  import debox.{Map => DMap}   // An unboxed, fast Map

  private def makePredefinedStructures(predefinedKeys: Seq[String]): (Array[Long], Array[Byte], DMap[Long, Int]) = {
    // Convert predefined keys to UTF8StringMediums.  First estimate size they would all take.
    val totalNumBytes = predefinedKeys.map(_.length + 2).sum
    val stringBytes = new Array[Byte](totalNumBytes)
    val keyToNum = DMap.empty[Long, Int]
    var index = 0
    val offsets = predefinedKeys.scanLeft(UnsafeUtils.arayOffset.toLong) { case (offset, str) =>
                    val bytes = str.getBytes
                    UTF8StringMedium.copyByteArrayTo(bytes, stringBytes, offset)
                    keyToNum(makeKeyKey(bytes)) = index
                    index += 1
                    offset + bytes.size + 2
                  }.toArray
    (offsets, stringBytes, keyToNum)
  }

  // For serialization purposes
  private[filodb] def toSerializableTuple: (Seq[Column.ColumnType], Option[Int], Seq[String]) =
    (columnTypes, partitionFieldStart, predefinedKeys)
}

trait MapItemConsumer {
  /**
   * Invoked for each key and value pair.  The (base, offset) points to a UTF8StringMedium, use that objects
   * methods to work with each UTF8 string.
   * @param (keyBase,keyOffset) pointer to the key UTF8String
   * @param (valueBase, valueOffset) pointer to the value UTF8String
   * @param index an increasing index of the pair within the map, starting at 0
   */
  def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit
}

/**
 * A MapItemConsumer which turns the key and value pairs into strings
 */
class StringifyMapItemConsumer extends MapItemConsumer {
  val stringPairs = new collection.mutable.ArrayBuffer[(String, String)]
  def prettyPrint: String = "[" + stringPairs.map { case (k, v) => s"$k: $v" }.mkString(", ") + "]"
  def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
    stringPairs += (UTF8StringMedium.toString(keyBase, keyOffset) ->
                    UTF8StringMedium.toString(valueBase, valueOffset))
  }
}

object RecordSchema {
  import Column.ColumnType._

  val colTypeToFieldSize = Map[Column.ColumnType, Int](IntColumn -> 4,
                                                       LongColumn -> 8,
                                                       DoubleColumn -> 8,
                                                       TimestampColumn -> 8,  // Just a long ms timestamp
                                                       StringColumn -> 4,
                                                       MapColumn -> 4)

  /**
   * Creates a "unique" Long key for each incoming predefined key for quick lookup.  This will not be perfect
   * but probably good enough for the beginning.
   * TODO: improve on this.  One reason for difficulty is that we need custom hashCode and equals functions and
   * we don't want to box.
   * In the output, the lower 32 bits is the hashcode of the bytes.
   */
  private[binaryrecord2] def makeKeyKey(strBytes: Array[Byte]): Long = {
    val hash = BinaryRegion.hasher32.hash(strBytes, 0, strBytes.size, BinaryRegion.Seed)
    (UnsafeUtils.getInt(strBytes, UnsafeUtils.arayOffset).toLong << 32) | hash
  }

  /**
   * Create an "ingestion" RecordSchema with the data columns followed by the partition columns.
   */
  def ingestion(dataset: Dataset, predefinedKeys: Seq[String] = Nil): RecordSchema = {
    val colTypes = dataset.dataColumns.map(_.columnType) ++ dataset.partitionColumns.map(_.columnType)
    new RecordSchema(colTypes, Some(dataset.dataColumns.length), predefinedKeys)
  }

  def fromSerializableTuple(tuple: (Seq[Column.ColumnType], Option[Int], Seq[String])): RecordSchema =
    new RecordSchema(tuple._1, tuple._2, tuple._3)
}

/**
 * This is a class meant to provide a RowReader API for the new BinaryRecord v2.
 * NOTE: Strings cause an allocation of a ZeroCopyUTF8String instance.  TODO: provide a better API that does
 * not result in allocations.
 * It is meant to be reused again and again and is MUTABLE.
 */
final class BinaryRecordRowReader(schema: RecordSchema,
                                  var recordBase: Any = UnsafeUtils.ZeroPointer,
                                  var recordOffset: Long = 0L) extends RowReader {
  // BinaryRecordV2 fields always have a value
  def notNull(columnNo: Int): Boolean = columnNo >= 0 && columnNo < schema.numFields
  def getBoolean(columnNo: Int): Boolean = schema.getInt(recordBase, recordOffset, columnNo) != 0
  def getInt(columnNo: Int): Int = schema.getInt(recordBase, recordOffset, columnNo)
  def getLong(columnNo: Int): Long = schema.getLong(recordBase, recordOffset, columnNo)
  def getDouble(columnNo: Int): Double = schema.getDouble(recordBase, recordOffset, columnNo)
  def getFloat(columnNo: Int): Float = ???
  def getString(columnNo: Int): String = ???
  def getAny(columnNo: Int): Any = ???
  override def filoUTF8String(i: Int): ZeroCopyUTF8String = schema.asZCUTF8Str(recordBase, recordOffset, i)
}