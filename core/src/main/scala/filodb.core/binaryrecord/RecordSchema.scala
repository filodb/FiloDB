package filodb.core.binaryrecord

import org.boon.primitive.ByteBuf
import scala.language.existentials

import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType

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
final class RecordSchema(columnTypes: Seq[ColumnType]) {
  // Computes offsets for every field, where they would go etc
  val numFields = columnTypes.length

  // Number of 32-bit words at beginning for null check
  val nullBitWords = (numFields + 31) / 32
  val fixedDataStartOffset = nullBitWords * 4

  // val fields - fixed data field section
  var curOffset = fixedDataStartOffset
  val fields = columnTypes.zipWithIndex.map { case (colType, no) =>
    val field = Field(no, colType, curOffset, FieldType.columnToField(colType))
    curOffset += field.fieldType.numFixedBytes
    field
  }.toArray

  val variableDataStartOffset = curOffset

  override lazy val hashCode = fields.foldLeft(1)(_ * _.hashCode)

  override def toString: String = columnTypes.map(_.toString).mkString(":")
}

object RecordSchema {
  def apply(columns: Seq[Column]): RecordSchema = new RecordSchema(columns.map(_.columnType))

  def apply(schemaStr: String): RecordSchema = {
    val types = schemaStr.split(':').toSeq.map(s => ColumnType.withName(s))
    new RecordSchema(types)
  }
}