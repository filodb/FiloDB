package filodb.memory.format

/**
 * A RowReader designed for iteration over rows of multiple Filo vectors.  The user has to
 * set the row number to read from.
 * Not the fastest as it does not use the faster iteration based methods from each DataReader, but may work
 * in certain use cases.
 * Designed to minimize allocation by having iterator repeatedly set/update rowNo.
 * Thus, this is not appropriate for Seq[RowReader] or conversion to Seq.
 */
abstract class MutableFiloRowReader extends RowReader {
  import BinaryVector.BinaryVectorPtr

  var rowNo: Int = -1
  def setRowNo(newRowNo: Int): Unit = { rowNo = newRowNo }

  // These methods must be implemented
  def reader(columnNo: Int): VectorDataReader
  def vectBase(columnNo: Int): Any
  def vectAddr(columnNo: Int): BinaryVectorPtr

  final def notNull(columnNo: Int): Boolean = true   // TODO: fix this, but for now don't need null check
  final def getBoolean(columnNo: Int): Boolean = ???
  final def getInt(columnNo: Int): Int =
    reader(columnNo).asIntReader.apply(vectBase(columnNo), vectAddr(columnNo), rowNo)
  final def getLong(columnNo: Int): Long =
    reader(columnNo).asLongReader.apply(vectBase(columnNo), vectAddr(columnNo), rowNo)
  final def getDouble(columnNo: Int): Double =
    reader(columnNo).asDoubleReader(vectBase(columnNo), vectAddr(columnNo), rowNo)
  final def getFloat(columnNo: Int): Float = ???
  final def getString(columnNo: Int): String = ???
  override final def filoUTF8String(columnNo: Int): ZeroCopyUTF8String =
    reader(columnNo).asUTF8Reader.apply(vectBase(columnNo), vectAddr(columnNo), rowNo)
  final def getAny(columnNo: Int): Any = ???
  def getBlobBase(columnNo: Int): Any = ???
  def getBlobOffset(columnNo: Int): Long = ???
  def getBlobNumBytes(columnNo: Int): Int = ???
}

