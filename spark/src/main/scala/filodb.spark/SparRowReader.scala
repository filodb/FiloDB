package filodb.spark

import java.nio.ByteBuffer

import org.apache.spark.sql.Row
import org.velvia.filo.{RowReader, FastFiloRowReader}

class SparkRowReader(chunks: Array[ByteBuffer],
                     classes: Array[Class[_]])
  extends FastFiloRowReader(chunks, classes) with Row {
  def apply(i: Int): Any = getAny(i)

  def copy(): org.apache.spark.sql.Row = ???

  def getByte(i: Int): Byte = ???

  def getShort(i: Int): Short = ???

  def isNullAt(i: Int): Boolean = !notNull(i)

  def length: Int = parsers.length

  def toSeq: Seq[Any] = ???
}

case class RddRowReader(row: Row) extends RowReader {
  def notNull(columnNo: Int): Boolean = !row.isNullAt(columnNo)
  def getBoolean(columnNo: Int): Boolean = row.getBoolean(columnNo)
  def getInt(columnNo: Int): Int = row.getInt(columnNo)
  def getLong(columnNo: Int): Long = row.getLong(columnNo)
  def getDouble(columnNo: Int): Double = row.getDouble(columnNo)
  def getFloat(columnNo: Int): Float = row.getFloat(columnNo)
  def getString(columnNo: Int): String = row.getString(columnNo)
}
