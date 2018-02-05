package filodb.core.binaryrecord

import scala.language.existentials

import org.boon.primitive.ByteBuf

import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType

// scalastyle:off equals.hash.code
// Case classes already have equals, but we need to customize hash code
final case class Field(num: Int, colType: ColumnType, fixedDataOffset: Int, fieldType: FieldType[_]) {
  final def get[T](record: BinaryRecord): T = fieldType.asInstanceOf[FieldType[T]].extract(record, this)
  final def getAny(record: BinaryRecord): Any = fieldType.extract(record, this)

  // We need a hashCode that works across JVMs, so cannot hashCode something like fieldType
  override def hashCode: Int = num + 100 * colType.hashCode + 10000 * fixedDataOffset

  final def writeSortable(record: BinaryRecord, buf: ByteBuf): Unit =
    fieldType.writeSortable(record, this, buf)
  final def cmpRecords(rec1: BinaryRecord, rec2: BinaryRecord): Int = fieldType.compare(rec1, rec2, this)
}

/**
 * Stores offsets and other information for a BinaryRecord for a given schema (seq of column types)
 */
final class RecordSchema(val columnTypes: Seq[ColumnType]) {
  // Computes offsets for every field, where they would go etc
  val numFields = columnTypes.length

  // Number of 32-bit words at beginning for null check
  val nullBitWords = (numFields + 31) / 32
  val fixedDataStartOffset = nullBitWords * 4

  // val fields - fixed data field section
  var curOffset = fixedDataStartOffset
  val fields = columnTypes.zipWithIndex.map { case (colType, no) =>
    if (colType == ColumnType.MapColumn) require(no == columnTypes.length - 1)
    val field = Field(no, colType, curOffset, FieldType.columnToField(colType))
    curOffset += field.fieldType.numFixedBytes
    field
  }.toArray

  val extractors = columnTypes.map(_.keyType.extractor).toArray

  val variableDataStartOffset = curOffset

  override lazy val hashCode = fields.foldLeft(1)(_ * _.hashCode)

  override def toString: String = columnTypes.map(_.toString).mkString(":")
}

object RecordSchema {
  val empty = new RecordSchema(Nil)

  def apply(columns: Seq[Column]): RecordSchema = new RecordSchema(columns.map(_.columnType))

  def apply(colType: ColumnType): RecordSchema = new RecordSchema(Seq(colType))

  def apply(schemaStr: String): RecordSchema = {
    val types = schemaStr.split(':').toSeq.map(s => ColumnType.withName(s))
    new RecordSchema(types)
  }
}