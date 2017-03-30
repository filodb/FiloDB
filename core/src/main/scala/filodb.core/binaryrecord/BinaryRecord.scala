package filodb.core.binaryrecord

import java.nio.ByteBuffer
import org.boon.primitive.ByteBuf
import org.velvia.filo.{RowReader, SeqRowReader, UnsafeUtils, ZeroCopyBinary, ZeroCopyUTF8String}
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.language.postfixOps
import scalaxy.loops._

import filodb.core.metadata.RichProjection

/**
 * BinaryRecord is a record type that supports flexible schemas and reads/writes/usage
 * with no serialization at all for extreme performance and low latency with minimal GC pressure.
 * It will be used within FiloDB for very quick sorting of partition/segment/rowkeys and routing
 * between nodes.
 *
 * It also implements RowReader, so values can be extracted without another instantiation.
 */
class BinaryRecord private[binaryrecord](val schema: RecordSchema,
                                         val base: Any,
                                         val offset: Long,
                                         val numBytes: Int)
extends ZeroCopyBinary with RowReader {
  import BinaryRecord._
  import ZeroCopyBinary._

  // private final compiles to a JVM bytecode field, cheaper to access (as opposed to a method)
  private final val fields = schema.fields

  final def notNull(fieldNo: Int): Boolean = {
    val word = fieldNo / 32
    ((UnsafeUtils.getInt(base, offset + word * 4) >> (fieldNo % 32)) & 1) == 0
  }

  final def noneNull: Boolean = {
    for { field <- 0 until fields.size optimized } {
      if (!notNull(field)) return false
    }
    true
  }

  final def getBoolean(columnNo: Int): Boolean = fields(columnNo).get[Boolean](this)
  final def getInt(columnNo: Int): Int = fields(columnNo).get[Int](this)
  final def getLong(columnNo: Int): Long = fields(columnNo).get[Long](this)
  final def getDouble(columnNo: Int): Double = fields(columnNo).get[Double](this)
  final def getFloat(columnNo: Int): Float = ???
  final def getString(columnNo: Int): String = filoUTF8String(columnNo).asNewString

  final def getAny(columnNo: Int): Any = fields(columnNo).getAny(this)

  override final def filoUTF8String(columnNo: Int): ZeroCopyUTF8String =
    fields(columnNo).get[ZeroCopyUTF8String](this)

  override def toString: String =
    s"b[${(0 until fields.size).map(getAny).mkString(", ")}]"

  /**
   * Does a field-by-field (semantic) comparison of this BinaryRecord against another BinaryRecord.
   * It is assumed that the other BinaryRecord has the exact same schema, at least for all of the fields
   * present in this BinaryRecord (other.schema.numFields >= this.schema.numFields)
   * It is pretty fast as the field by field comparison involves no deserialization and uses intrinsics
   * in many places.
   * NOTE: if all fields in this BinaryRecord compare the same as the same fields in the other, then the
   * comparison returns equal (0).  This semantic is needed for row key range scans to work where only the
   * first few fields may be compared.
   */
  override final def compare(other: ZeroCopyBinary): Int = other match {
    case rec2: BinaryRecord =>
      for { field <- 0 until fields.size optimized } {
        val cmp = fields(field).cmpRecords(this, rec2)
        if (cmp != 0) return cmp
      }
      0
    case zcb: ZeroCopyBinary =>
      super.compare(zcb)
  }

  override final def equals(other: Any): Boolean = other match {
    case rec2: BinaryRecord => this.compare(rec2) == 0
    case zcb: ZeroCopyBinary => zcb.equals(this)
    case other: Any         => false
  }

  /**
   * Returns an array of bytes which is sortable byte-wise for its contents (which is not the goal of
   * BinaryRecord).  Null fields will have default values read out.
   * The produced bytes cannot be deserialized from or extracted, it is strictly for comparison.
   */
  def toSortableBytes(numFields: Int = 2): Array[Byte] = {
    val fieldsToWrite = Math.min(fields.size, numFields)
    val buf = ByteBuf.create(SortableByteBufSize)
    for { fieldNo <- 0 until fieldsToWrite optimized } {
      fields(fieldNo).writeSortable(this, buf)
    }
    buf.toBytes
  }
}

class ArrayBinaryRecord(schema: RecordSchema, override val bytes: Array[Byte]) extends
BinaryRecord(schema, bytes, UnsafeUtils.arayOffset, bytes.size)

object BinaryRecord {
  val DefaultMaxRecordSize = 8192
  val MaxSmallOffset = 0x7fff
  val MaxSmallLen    = 0xffff
  val SortableByteBufSize = 100

  def apply(schema: RecordSchema, bytes: Array[Byte]): BinaryRecord =
    new ArrayBinaryRecord(schema, bytes)

  def apply(projection: RichProjection, bytes: Array[Byte]): BinaryRecord =
    apply(projection.rowKeyBinSchema, bytes)

  def apply(schema: RecordSchema, buffer: ByteBuffer): BinaryRecord =
    if (buffer.hasArray) { apply(schema, buffer.array) }
    else if (buffer.isDirect) {
      val addr: Long = buffer.asInstanceOf[sun.nio.ch.DirectBuffer].address
      //scalastyle:off
      new BinaryRecord(schema, null, addr, buffer.capacity)
      //scalastyle:on
    } else { throw new IllegalArgumentException("Buffer is neither array or direct") }

  def apply(schema: RecordSchema, reader: RowReader, maxBytes: Int = DefaultMaxRecordSize): BinaryRecord = {
    val builder = BinaryRecordBuilder(schema, maxBytes)
    for { i <- 0 until schema.fields.size optimized } {
      val field = schema.fields(i)
      if (reader.notNull(i)) {
        field.fieldType.addFromReader(builder, field, reader)
      } else {
        field.fieldType.addNull(builder, field)
      }
    }
    // TODO: Someday, instead of allocating a new buffer every time, just use a giant chunk of offheap memory
    // and keep allocating from that
    builder.build(copy = true)
  }

  // This is designed for creating partition keys. No nulls, custom extractors for computed columns
  def apply(schema: RecordSchema, reader: RowReader, extractors: Array[TypedFieldExtractor[_]]):
    BinaryRecord = {
    val builder = BinaryRecordBuilder(schema, DefaultMaxRecordSize)
    for { i <- 0 until schema.fields.size optimized } {
      val field = schema.fields(i)
      field.fieldType.addWithExtractor(builder, field, reader, extractors(i))
    }
    builder.build(copy = true)
  }

  def apply(projection: RichProjection, items: Seq[Any]): BinaryRecord =
    if (items.length < projection.rowKeyColumns.length) {
      apply(RecordSchema(projection.rowKeyColumns.take(items.length)), SeqRowReader(items))
    } else {
      apply(projection.rowKeyBinSchema, SeqRowReader(items))
    }

  implicit val ordering = new Ordering[BinaryRecord] {
    def compare(a: BinaryRecord, b: BinaryRecord): Int = a.compare(b)
  }

  // Create the fixed-field int for variable length data blobs.  If the result is negative (bit 31 set),
  // then the offset and length are both packed in; otherwise, the fixed int is just an offset to a
  // 4-byte int containing length, followed by the actual blob
  final def blobFixedInt(offset: Int, blobLength: Int): Int =
    if (offset <= MaxSmallOffset && blobLength <= MaxSmallLen) {
      0x80000000 | (offset << 16) | blobLength
    } else {
      offset
    }

  final def getBlobOffsetLen(binRecord: BinaryRecord, fixedData: Int): (Long, Int) = {
    if (fixedData < 0) {
      (binRecord.offset + ((fixedData & 0x7fff0000) >> 16), fixedData & 0xffff)
    } else {
      (binRecord.offset + fixedData + 4,
       UnsafeUtils.getInt(binRecord.base, binRecord.offset + fixedData))
    }
  }

  final def getBlobOffsetLen(binRec: BinaryRecord, field: Field): (Long, Int) = {
    val fixedData = UnsafeUtils.getInt(binRec.base, binRec.offset + field.fixedDataOffset)
    getBlobOffsetLen(binRec, fixedData)
  }
}

/**
 * Instead of trying to somehow make BinaryRecord itself Java-Serialization friendly, and supporting
 * horrible mutable fields in a class that already uses Unsafe, we keep BinaryRecord itself with an
 * immutable API, and delegate Java Serialization support to this wrapper class.  NOTE: for high-volume
 * BinaryRecord transfers, transfer the schema separately and just transmit the bytes from the BinaryRecord.
 * This class is meant for low-volume use cases and always transfers the schema with every record.
 */
@SerialVersionUID(1009L)
case class BinaryRecordWrapper(var binRec: BinaryRecord) extends java.io.Externalizable {
  //scalastyle:off
  def this() = this(null)
  //scalastyle:on
  def writeExternal(out: java.io.ObjectOutput): Unit = {
    out.writeUTF(binRec.schema.toString)
    out.writeInt(binRec.length)
    out.write(binRec.bytes)
  }
  def readExternal(in: java.io.ObjectInput): Unit = {
    val schema = RecordSchema(in.readUTF())
    val recordBytes = new Array[Byte](in.readInt())
    in.readFully(recordBytes, 0, recordBytes.size)
    binRec = BinaryRecord(schema, recordBytes)
  }
}

case class OutOfBytesException(needed: Int, max: Int) extends
Exception(s"BinaryRecordBuilder: needed $needed bytes, but only had $max.")

class BinaryRecordBuilder(schema: RecordSchema, val base: Any, val offset: Long, maxBytes: Int) {
  var numBytes = schema.variableDataStartOffset

  // Initialize null words - assume every field is null until it is set
  for { nullWordNo <- 0 until schema.nullBitWords } {
    UnsafeUtils.setInt(base, offset + nullWordNo * 4, -1)
  }

  // We don't appear to need to initialize null fields or the fixedData area to 0's, because both
  // ByteBuffer allocate and allocateDirect seems to initialize memory for us to 0's already

  def setNull(fieldNo: Int): Unit = {
    val wordOffset = offset + (fieldNo / 32) * 4
    UnsafeUtils.setInt(base, wordOffset,
                            UnsafeUtils.getInt(base, wordOffset) | (1 << (fieldNo % 32)))
  }

  def setNotNull(fieldNo: Int): Unit = {
    val wordOffset = offset + (fieldNo / 32) * 4
    val mask = ~(1 << (fieldNo % 32))
    UnsafeUtils.setInt(base, wordOffset,
                            UnsafeUtils.getInt(base, wordOffset) & mask)
  }

  /**
   * Reserves space from the variable length area at the end.  Space will always be word-aligned.
   * If it succeeds, the numBytes will be moved up at the end of the call.
   * @param bytesToReserve the number of bytes to reserve.  Will be rounded up to a word (4-byte) boundary.
   * @return Some(origOffset) the original offset of the variable length space to write to, or None if there
   *         isn't room to write
   */
  def reserveVarBytes(bytesToReserve: Int): Option[Long] = {
    val roundedLen = (bytesToReserve + 3) & -4
    if (numBytes + roundedLen <= maxBytes) {
      val offsetToWrite = offset + numBytes
      numBytes += roundedLen
      Some(offsetToWrite)
    } else {
      None
    }
  }

  /**
   * Appends a variable length blob to the end, returning the 32-bit fixed length data field that either
   * contains both offset and length or just the offset, in which case first 4 bytes in var section contains
   * the length.  Bytes will be copied from original blob.
   * @throws OutOfBytesException if bytes for the blob cannot be allocated
   */
  def appendBlob(blob: ZeroCopyBinary): Int = {
    // First, get the fixed int which encodes offset and len and see if we need another 4 bytes for offset
    val fixedData = BinaryRecord.blobFixedInt(numBytes, blob.length)
    val neededBytes = blob.length + (if (fixedData < 0) 0 else 4)

    reserveVarBytes(neededBytes).map { destOffset =>
      if (fixedData < 0) {
        blob.copyTo(base, destOffset)
      } else {
        UnsafeUtils.setInt(base, destOffset, blob.length)
        blob.copyTo(base, destOffset + 4)
      }
    }.getOrElse(throw OutOfBytesException(neededBytes, maxBytes))
    fixedData
  }

  /**
   * Builds a final BinaryRecord.
   * @param copy if true, copies to a new Array[Byte].  Use if initially allocated giant buffer, otherwise
   *             the free space in that buffer cannot be reclaimed.
   */
  def build(copy: Boolean = false): BinaryRecord = if (copy) {
    val newAray = new Array[Byte](numBytes)
    UnsafeUtils.unsafe.copyMemory(base, offset, newAray, UnsafeUtils.arayOffset, numBytes)
    new ArrayBinaryRecord(schema, newAray)
  } else {
    new BinaryRecord(schema, base, offset, numBytes)
  }
}

object BinaryRecordBuilder {
  def apply(schema: RecordSchema, maxBytes: Int): BinaryRecordBuilder = {
    val aray = new Array[Byte](maxBytes)
    new BinaryRecordBuilder(schema, aray, UnsafeUtils.arayOffset, aray.size)
  }
}