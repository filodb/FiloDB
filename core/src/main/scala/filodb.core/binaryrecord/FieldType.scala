package filodb.core.binaryrecord

import java.sql.Timestamp
import org.velvia.filo.{RowReader, UnsafeUtils, ZeroCopyUTF8String}
import org.velvia.filo.RowReader._
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
}

abstract class SimpleFieldType[@specialized T: TypedFieldExtractor] extends FieldType[T] {
  val extractor = implicitly[TypedFieldExtractor[T]]
  final def addFromReader(builder: BinaryRecordBuilder,
                          field: Field,
                          reader: RowReader): Unit =
    add(builder, field, extractor.getField(reader, field.num))
}

object FieldType {
  import ColumnType._

  val columnToField = Map[ColumnType, FieldType[_]](
    IntColumn     -> IntFieldType,
    LongColumn    -> LongFieldType,
    StringColumn  -> UTF8StringFieldType,
    BitmapColumn  -> BooleanFieldType,
    DoubleColumn  -> DoubleFieldType,
    TimestampColumn -> TimestampFieldType
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
}

object LongFieldType extends SimpleFieldType[Long] {
  val numFixedWords: Int = 2
  final def extract(data: BinaryRecord, field: Field): Long =
    UnsafeUtils.getLong(data.base, data.offset + field.fixedDataOffset)

  final def add(builder: BinaryRecordBuilder, field: Field, data: Long): Unit = {
    UnsafeUtils.setLong(builder.base, builder.offset + field.fixedDataOffset, data)
    builder.setNotNull(field.num)
  }
}

object BooleanFieldType extends SimpleFieldType[Boolean] {
  val numFixedWords: Int = 1
  final def extract(data: BinaryRecord, field: Field): Boolean =
    UnsafeUtils.getInt(data.base, data.offset + field.fixedDataOffset) != 0

  final def add(builder: BinaryRecordBuilder, field: Field, data: Boolean): Unit =
    IntFieldType.add(builder, field, if (data) 1 else 0)
}

object DoubleFieldType extends SimpleFieldType[Double] {
  val numFixedWords: Int = 2
  final def extract(data: BinaryRecord, field: Field): Double =
    UnsafeUtils.getDouble(data.base, data.offset + field.fixedDataOffset)

  final def add(builder: BinaryRecordBuilder, field: Field, data: Double): Unit = {
    UnsafeUtils.setDouble(builder.base, builder.offset + field.fixedDataOffset, data)
    builder.setNotNull(field.num)
  }
}

object TimestampFieldType extends SimpleFieldType[Timestamp] {
  val numFixedWords: Int = 2
  final def extract(data: BinaryRecord, field: Field): Timestamp =
    new Timestamp(LongFieldType.extract(data, field))

  final def add(builder: BinaryRecordBuilder, field: Field, data: Timestamp): Unit =
    LongFieldType.add(builder, field, data.getTime)
}

object UTF8StringFieldType extends SimpleFieldType[ZeroCopyUTF8String] {
  val numFixedWords: Int = 1
  final def extract(data: BinaryRecord, field: Field): ZeroCopyUTF8String = {
    val fixedData = UnsafeUtils.getInt(data.base, data.offset + field.fixedDataOffset)
    val (offset, len) = BinaryRecord.getBlobOffsetLen(data, fixedData)
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
}