package filodb.core.binaryrecord

import org.velvia.filo.{RowReader, SeqRowReader, UnsafeUtils, ZeroCopyBinary, ZeroCopyUTF8String}
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

  // Also, for why using lazy val is not that bad: https://dzone.com/articles/cost-laziness
  // The cost of computing the hash (and say using it in a bloom filter) is much higher.
  lazy val cachedHash64: Long = {
    base match {
      case a: Array[Byte] => hasher64.hash(a, offset.toInt - UnsafeUtils.arayOffset, numBytes, Seed)
      case o: Any         => hasher64.hash(asNewByteArray, 0, numBytes, Seed)
    }
  }

  /**
   * Returns an array of bytes for this binary record.  If this BinaryRecord is already a byte array
   * with exactly numBytes bytes, then just return that, to avoid another copy.  Otherwise, call
   * asNewByteArray to return a copy.
   */
  def bytes: Array[Byte] = {
    //scalastyle:off
    if (base != null && base.isInstanceOf[Array[Byte]] && offset == UnsafeUtils.arayOffset) {
      //scalastyle:on
      base.asInstanceOf[Array[Byte]]
    } else {
      asNewByteArray
    }
  }
}

class ArrayBinaryRecord(schema: RecordSchema, override val bytes: Array[Byte]) extends
BinaryRecord(schema, bytes, UnsafeUtils.arayOffset, bytes.size)

object BinaryRecord {
  val DefaultMaxRecordSize = 8192
  val MaxSmallOffset = 0x7fff
  val MaxSmallLen    = 0xffff

  val hasher64 = ZeroCopyBinary.xxhashFactory.hash64

  def apply(schema: RecordSchema, bytes: Array[Byte]): BinaryRecord =
    new ArrayBinaryRecord(schema, bytes)

  def apply(projection: RichProjection, bytes: Array[Byte]): BinaryRecord =
    apply(projection.rowKeyBinSchema, bytes)

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

  def apply(projection: RichProjection, items: Seq[Any]): BinaryRecord =
    apply(projection.rowKeyBinSchema, SeqRowReader(items))

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
}

case object OutOfBytesException extends Exception("BinaryRecordBuilder: no more space to add blob")

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

    reserveVarBytes(blob.length + (if (fixedData < 0) 0 else 4)).map { destOffset =>
      if (fixedData < 0) {
        blob.copyTo(base, destOffset)
      } else {
        UnsafeUtils.setInt(base, destOffset, blob.length)
        blob.copyTo(base, destOffset + 4)
      }
    }.getOrElse(throw OutOfBytesException)
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