package filodb.core.binaryrecord

import java.sql.Timestamp
import org.boon.primitive.ByteBuf
import org.velvia.filo.{RowReader, UnsafeUtils, ZeroCopyUTF8String}
import org.velvia.filo.RowReader._

import filodb.core.SingleKeyTypes.{Int32HighBit, Long64HighBit}
import filodb.core.metadata.Column.ColumnType

trait FieldType[@specialized T] {
  def numFixedBytes: Int = numFixedWords * 4
  def numFixedWords: Int

  def extract(data: BinaryRecord, field: Field): T

  // NOTE: the add method modifies the state of the BinaryRecordBuilder.  Don't call more than once if
  // you don't want to waste memory.
  def add(builder: BinaryRecordBuilder, field: Field, data: T): Unit

  def addNull(builder: BinaryRecordBuilder, field: Field): Unit = {}

  def addFromReader(builder: BinaryRecordBuilder,
                    field: Field,
                    reader: RowReader): Unit

  def addWithExtractor(builder: BinaryRecordBuilder,
                       field: Field,
                       reader: RowReader,
                       extractor: TypedFieldExtractor[_]): Unit

  def writeSortable(data: BinaryRecord, field: Field, buf: ByteBuf): Unit

  def compare(rec1: BinaryRecord, rec2: BinaryRecord, field: Field): Int
}

abstract class SimpleFieldType[@specialized T: TypedFieldExtractor] extends FieldType[T] {
  private final val extractor = implicitly[TypedFieldExtractor[T]]
  final def addFromReader(builder: BinaryRecordBuilder,
                          field: Field,
                          reader: RowReader): Unit =
    add(builder, field, extractor.getField(reader, field.num))

  final def addWithExtractor(builder: BinaryRecordBuilder,
                             field: Field,
                             reader: RowReader,
                             customExtractor: TypedFieldExtractor[_]): Unit =
    add(builder, field, customExtractor.asInstanceOf[TypedFieldExtractor[T]].getField(reader, field.num))
}

object FieldType {
  import ColumnType._

  val columnToField = Map[ColumnType, FieldType[_]](
    IntColumn     -> IntFieldType,
    LongColumn    -> LongFieldType,
    StringColumn  -> UTF8StringFieldType,
    BitmapColumn  -> BooleanFieldType,
    DoubleColumn  -> DoubleFieldType,
    TimestampColumn -> LongFieldType  // default to long handling due to BinaryRecord
  )
}

object IntFieldType extends SimpleFieldType[Int] {
  val numFixedWords: Int = 1
  final def extract(data: BinaryRecord, field: Field): Int =
    UnsafeUtils.getInt(data.base, data.offset + field.fixedDataOffset)

  final def add(builder: BinaryRecordBuilder, field: Field, data: Int): Unit = {
    UnsafeUtils.setInt(builder.base, builder.offset + field.fixedDataOffset, data)
    builder.setNotNull(field.num)
  }

  final def writeSortable(data: BinaryRecord, field: Field, buf: ByteBuf): Unit =
    buf.writeInt(extract(data, field) ^ Int32HighBit)

  final def compare(rec1: BinaryRecord, rec2: BinaryRecord, field: Field): Int =
    UnsafeUtils.getInt(rec1.base, rec1.offset + field.fixedDataOffset) -
    UnsafeUtils.getInt(rec2.base, rec2.offset + field.fixedDataOffset)
}

object LongFieldType extends SimpleFieldType[Long] {
  val numFixedWords: Int = 2
  final def extract(data: BinaryRecord, field: Field): Long =
    UnsafeUtils.getLong(data.base, data.offset + field.fixedDataOffset)

  final def add(builder: BinaryRecordBuilder, field: Field, data: Long): Unit = {
    UnsafeUtils.setLong(builder.base, builder.offset + field.fixedDataOffset, data)
    builder.setNotNull(field.num)
  }

  final def writeSortable(data: BinaryRecord, field: Field, buf: ByteBuf): Unit =
    buf.writeLong(extract(data, field) ^ Long64HighBit)

  final def compare(rec1: BinaryRecord, rec2: BinaryRecord, field: Field): Int =
    java.lang.Long.compare(UnsafeUtils.getLong(rec1.base, rec1.offset + field.fixedDataOffset),
                           UnsafeUtils.getLong(rec2.base, rec2.offset + field.fixedDataOffset))
}

object BooleanFieldType extends SimpleFieldType[Boolean] {
  val numFixedWords: Int = 1
  final def extract(data: BinaryRecord, field: Field): Boolean =
    UnsafeUtils.getInt(data.base, data.offset + field.fixedDataOffset) != 0

  final def add(builder: BinaryRecordBuilder, field: Field, data: Boolean): Unit =
    IntFieldType.add(builder, field, if (data) 1 else 0)

  final def writeSortable(data: BinaryRecord, field: Field, buf: ByteBuf): Unit =
    buf.writeByte(if (extract(data, field)) 1 else 0)

  final def compare(rec1: BinaryRecord, rec2: BinaryRecord, field: Field): Int =
    java.lang.Boolean.compare(extract(rec1, field), extract(rec2, field))
}

object DoubleFieldType extends SimpleFieldType[Double] {
  val numFixedWords: Int = 2
  final def extract(data: BinaryRecord, field: Field): Double =
    UnsafeUtils.getDouble(data.base, data.offset + field.fixedDataOffset)

  final def add(builder: BinaryRecordBuilder, field: Field, data: Double): Unit = {
    UnsafeUtils.setDouble(builder.base, builder.offset + field.fixedDataOffset, data)
    builder.setNotNull(field.num)
  }

  final def writeSortable(data: BinaryRecord, field: Field, buf: ByteBuf): Unit =
    buf.writeDouble(extract(data, field))

  final def compare(rec1: BinaryRecord, rec2: BinaryRecord, field: Field): Int =
    java.lang.Double.compare(extract(rec1, field), extract(rec2, field))
}

object TimestampFieldType extends SimpleFieldType[Timestamp] {
  val numFixedWords: Int = 2
  final def extract(data: BinaryRecord, field: Field): Timestamp =
    new Timestamp(LongFieldType.extract(data, field))

  final def add(builder: BinaryRecordBuilder, field: Field, data: Timestamp): Unit =
    LongFieldType.add(builder, field, data.getTime)

  final def writeSortable(data: BinaryRecord, field: Field, buf: ByteBuf): Unit =
    LongFieldType.writeSortable(data, field, buf)

  final def compare(rec1: BinaryRecord, rec2: BinaryRecord, field: Field): Int =
    java.lang.Long.compare(UnsafeUtils.getLong(rec1.base, rec1.offset + field.fixedDataOffset),
                           UnsafeUtils.getLong(rec2.base, rec2.offset + field.fixedDataOffset))
}

object UTF8StringFieldType extends SimpleFieldType[ZeroCopyUTF8String] {
  val numFixedWords: Int = 1
  final def extract(data: BinaryRecord, field: Field): ZeroCopyUTF8String = {
    val (offset, len) = BinaryRecord.getBlobOffsetLen(data, field)
    new ZeroCopyUTF8String(data.base, offset, len)
  }

  final def add(builder: BinaryRecordBuilder, field: Field, data: ZeroCopyUTF8String): Unit = {
    UnsafeUtils.setInt(builder.base, builder.offset + field.fixedDataOffset,
                       builder.appendBlob(data))
    builder.setNotNull(field.num)
  }

  override final def addNull(builder: BinaryRecordBuilder, field: Field): Unit = {
    // A string with offset 0 and length 0 -> ""
    UnsafeUtils.setInt(builder.base, builder.offset + field.fixedDataOffset, 0x80000000)
  }

  // Only write first 8 bytes of string, padded to 8 with 0's if needed
  final def writeSortable(data: BinaryRecord, field: Field, buf: ByteBuf): Unit = {
    val first8bytes = new Array[Byte](8)
    val utf8str = extract(data, field)
    if (utf8str.length < 8) {
      utf8str.copyTo(first8bytes, UnsafeUtils.arayOffset)
      UnsafeUtils.unsafe.setMemory(first8bytes, UnsafeUtils.arayOffset + utf8str.length,
                                   8 - utf8str.length, 0)
    } else {
      utf8str.copyTo(first8bytes, UnsafeUtils.arayOffset, n=8)
    }
    buf.add(first8bytes)
  }

  // Very efficient compare method does not even need to allocate new ZeroCopyUTF8String instances
  final def compare(rec1: BinaryRecord, rec2: BinaryRecord, field: Field): Int = {
    val (off1, len1) = BinaryRecord.getBlobOffsetLen(rec1, field)
    val (off2, len2) = BinaryRecord.getBlobOffsetLen(rec2, field)
    val wordCmp = UnsafeUtils.wordCompare(rec1.base, off1, rec2.base, off2, Math.min(len1, len2))
    if (wordCmp != 0) wordCmp else len1 - len2
  }
}