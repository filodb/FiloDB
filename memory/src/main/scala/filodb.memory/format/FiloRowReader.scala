package filodb.memory.format

import java.nio.ByteBuffer

/**
 * A RowReader designed for iteration over rows of multiple Filo vectors, ideally all
 * with the same length.
 * An Iterator[RowReader] sets the rowNo and returns this RowReader, and
 * the application is responsible for calling the right method to extract each value.
 * For example, a Spark Row can inherit from RowReader.
 */
trait FiloRowReader extends RowReader {
  def parsers: Array[FiloVector[_]]
  def rowNo: Int
  def setRowNo(newRowNo: Int): Unit
}

/**
 * Just a concrete implementation.
 * Designed to minimize allocation by having iterator repeatedly set/update rowNo.
 * Thus, this is not appropriate for Seq[RowReader] or conversion to Seq.
 */
class FastFiloRowReader(val parsers: Array[FiloVector[_]]) extends FiloRowReader {
  var rowNo: Int = -1
  def setRowNo(newRowNo: Int): Unit = { rowNo = newRowNo }

  def this(chunks: Array[ByteBuffer], classes: Array[Class[_]], emptyLen: Int = 0) =
    this(FiloVector.makeVectors(chunks, classes, emptyLen))

  final def notNull(columnNo: Int): Boolean = columnNo < parsers.length && parsers(columnNo).isAvailable(rowNo)
  final def getBoolean(columnNo: Int): Boolean = parsers(columnNo).asInstanceOf[FiloVector[Boolean]](rowNo)
  final def getInt(columnNo: Int): Int = parsers(columnNo).asInstanceOf[FiloVector[Int]](rowNo)
  final def getLong(columnNo: Int): Long = parsers(columnNo).asInstanceOf[FiloVector[Long]](rowNo)
  final def getDouble(columnNo: Int): Double = parsers(columnNo).asInstanceOf[FiloVector[Double]](rowNo)
  final def getFloat(columnNo: Int): Float = parsers(columnNo).asInstanceOf[FiloVector[Float]](rowNo)
  final def getString(columnNo: Int): String = parsers(columnNo).asInstanceOf[FiloVector[String]](rowNo)
  override final def filoUTF8String(columnNo: Int): ZeroCopyUTF8String =
    parsers(columnNo).asInstanceOf[FiloVector[ZeroCopyUTF8String]](rowNo)
  final def getAny(columnNo: Int): Any = parsers(columnNo).boxed(rowNo)
}

// A RowReader that can safely be used in Seqs.  IE the rowNo is final and won't change.
case class SafeFiloRowReader(reader: FiloRowReader, rowNo: Int) extends FiloRowReader {
  val parsers = reader.parsers
  require(rowNo < parsers(0).length)
  def setRowNo(newRowNo: Int): Unit = {}

  final def notNull(columnNo: Int): Boolean = parsers(columnNo).isAvailable(rowNo)
  final def getBoolean(columnNo: Int): Boolean = parsers(columnNo).asInstanceOf[FiloVector[Boolean]](rowNo)
  final def getInt(columnNo: Int): Int = parsers(columnNo).asInstanceOf[FiloVector[Int]](rowNo)
  final def getLong(columnNo: Int): Long = parsers(columnNo).asInstanceOf[FiloVector[Long]](rowNo)
  final def getDouble(columnNo: Int): Double = parsers(columnNo).asInstanceOf[FiloVector[Double]](rowNo)
  final def getFloat(columnNo: Int): Float = parsers(columnNo).asInstanceOf[FiloVector[Float]](rowNo)
  final def getString(columnNo: Int): String = parsers(columnNo).asInstanceOf[FiloVector[String]](rowNo)
  override final def filoUTF8String(columnNo: Int): ZeroCopyUTF8String =
    parsers(columnNo).asInstanceOf[FiloVector[ZeroCopyUTF8String]](rowNo)
  final def getAny(columnNo: Int): Any = parsers(columnNo).boxed(rowNo)
}

