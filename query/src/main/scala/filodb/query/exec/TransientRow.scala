package filodb.query.exec

import filodb.memory.format.{RowReader, ZeroCopyUTF8String}


trait MutableRowReader extends RowReader {
  def setLong(columnNo: Int, value: Long): Unit
  def setDouble(columnNo: Int, value: Double): Unit
  def setString(columnNo: Int, value: ZeroCopyUTF8String): Unit
}

/**
  * Represents intermediate sample which will be part of a transformed RangeVector.
  * IMPORTANT: It is mutable for memory efficiency purposes. Consumers from
  * iterators should be aware of the semantics of ability to save the next() value.
  */
final class TransientRow(var timestamp: Long, var value: Double) extends MutableRowReader {
  def this() = this(0L, 0d)

  def setValues(ts: Long, valu: Double): Unit = {
    timestamp = ts
    value = valu
  }

  def setLong(columnNo: Int, valu: Long): Unit =
    if (columnNo == 0) timestamp = valu
    else throw new IllegalArgumentException()

  def setDouble(columnNo: Int, valu: Double): Unit =
    if (columnNo == 1) value = valu
    else throw new IllegalArgumentException()

  def setString(columnNo: Int, value: ZeroCopyUTF8String): Unit = throw new IllegalArgumentException()

  def copyFrom(r: RowReader): Unit = {
    timestamp = r.getLong(0)
    value = r.getDouble(1)
  }

  def notNull(columnNo: Int): Boolean = columnNo < 2
  def getBoolean(columnNo: Int): Boolean = throw new IllegalArgumentException()
  def getInt(columnNo: Int): Int = throw new IllegalArgumentException()
  def getLong(columnNo: Int): Long = if (columnNo == 0) timestamp else throw new IllegalArgumentException()
  def getDouble(columnNo: Int): Double = if (columnNo == 1) value else throw new IllegalArgumentException()
  def getFloat(columnNo: Int): Float = throw new IllegalArgumentException()
  def getString(columnNo: Int): String = throw new IllegalArgumentException()
  def getAny(columnNo: Int): Any = throw new IllegalArgumentException()

}

final class AvgAggTransientRow extends MutableRowReader {
  var timestamp: Long = _
  var avg: Double = _
  var count: Long = _

  def setLong(columnNo: Int, valu: Long): Unit =
    if (columnNo == 0) timestamp = valu
    else if (columnNo == 2) count = valu
    else throw new IllegalArgumentException()

  def setDouble(columnNo: Int, valu: Double): Unit =
    if (columnNo == 1) avg = valu
    else throw new IllegalArgumentException()

  def setString(columnNo: Int, value: ZeroCopyUTF8String): Unit = throw new IllegalArgumentException()

  def notNull(columnNo: Int): Boolean = columnNo < 2
  def getBoolean(columnNo: Int): Boolean = throw new IllegalArgumentException()
  def getInt(columnNo: Int): Int = throw new IllegalArgumentException()
  def getLong(columnNo: Int): Long = if (columnNo == 0) timestamp
                                     else if (columnNo == 2) count
                                     else throw new IllegalArgumentException()
  def getDouble(columnNo: Int): Double = if (columnNo == 1) avg
                                         else throw new IllegalArgumentException()
  def getFloat(columnNo: Int): Float = throw new IllegalArgumentException()
  def getString(columnNo: Int): String = throw new IllegalArgumentException()
  def getAny(columnNo: Int): Any = throw new IllegalArgumentException()
}

final class TopBottomKAggTransientRow(val k: Int) extends MutableRowReader {
  var timestamp: Long = _
  val partKeys: Array[ZeroCopyUTF8String] = new Array[ZeroCopyUTF8String](k)
  val values: Array[Double] = new Array[Double](k)

  def setLong(columnNo: Int, valu: Long): Unit =
    if (columnNo == 0) timestamp = valu
    else throw new IllegalArgumentException()

  def setDouble(columnNo: Int, valu: Double): Unit =
    values((columnNo-1)/2) = valu

  def setString(columnNo: Int, valu: ZeroCopyUTF8String): Unit =
    partKeys((columnNo-1)/2) = valu

  def notNull(columnNo: Int): Boolean = columnNo < 2*k + 1
  def getBoolean(columnNo: Int): Boolean = throw new IllegalArgumentException()
  def getInt(columnNo: Int): Int = throw new IllegalArgumentException()
  def getLong(columnNo: Int): Long = if (columnNo == 0) timestamp else throw new IllegalArgumentException()
  def getDouble(columnNo: Int): Double = values((columnNo-1)/2)
  def getFloat(columnNo: Int): Float = throw new IllegalArgumentException()
  def getString(columnNo: Int): String = partKeys((columnNo-1)/2).toString
  def getAny(columnNo: Int): Any = {
    if (columnNo == 0) timestamp
    else if (columnNo % 2 == 1) partKeys((columnNo-1)/2)
    else values((columnNo-1)/2)
  }
}
