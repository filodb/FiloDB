package filodb.core.query



import filodb.memory.format.{vectors => bv, RowReader, ZeroCopyUTF8String}

trait MutableRowReader extends RowReader {
  def setLong(columnNo: Int, value: Long): Unit
  def setDouble(columnNo: Int, value: Double): Unit
  def setString(columnNo: Int, value: ZeroCopyUTF8String): Unit
  def setBlob(columnNo: Int, base: Array[Byte], offset: Int, length: Int): Unit
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

  def setBlob(columnNo: Int, base: Array[Byte], offset: Int, length: Int): Unit = throw new IllegalArgumentException()

  def copyFrom(r: RowReader): Unit = {
    timestamp = r.getLong(0)
    value = r.getDouble(1)
  }

  def notNull(columnNo: Int): Boolean = columnNo < 2
  def getBoolean(columnNo: Int): Boolean = throw new IllegalArgumentException()
  def getInt(columnNo: Int): Int = throw new IllegalArgumentException()
  def getLong(columnNo: Int): Long = if (columnNo == 0) timestamp else throw new IllegalArgumentException()
  def getDouble(columnNo: Int): Double = if (columnNo == 1) value
  else throw new IllegalArgumentException(s"Invalid col $columnNo")
  def getFloat(columnNo: Int): Float = throw new IllegalArgumentException()
  def getString(columnNo: Int): String = throw new IllegalArgumentException()
  def getAny(columnNo: Int): Any = throw new IllegalArgumentException()

  def getBlobBase(columnNo: Int): Any = throw new IllegalArgumentException()
  def getBlobOffset(columnNo: Int): Long = throw new IllegalArgumentException()
  def getBlobNumBytes(columnNo: Int): Int = throw new IllegalArgumentException()

  override def toString: String = s"TransientRow(t=$timestamp, v=$value)"
}

class TransientHistRow(var timestamp: Long = 0L,
                       var value: bv.HistogramWithBuckets = bv.Histogram.empty) extends MutableRowReader {
  def setValues(ts: Long, hist: bv.HistogramWithBuckets): Unit = {
    timestamp = ts
    value = hist
  }

  def setLong(columnNo: Int, valu: Long): Unit =
    if (columnNo == 0) timestamp = valu
    else throw new IllegalArgumentException()

  def setDouble(columnNo: Int, valu: Double): Unit = throw new IllegalArgumentException()
  def setString(columnNo: Int, value: ZeroCopyUTF8String): Unit = throw new IllegalArgumentException()
  def setBlob(columnNo: Int, base: Array[Byte], offset: Int, length: Int): Unit = throw new IllegalArgumentException()

  def notNull(columnNo: Int): Boolean = columnNo < 2
  def getBoolean(columnNo: Int): Boolean = throw new IllegalArgumentException()
  def getInt(columnNo: Int): Int = throw new IllegalArgumentException()
  def getLong(columnNo: Int): Long = if (columnNo == 0) timestamp else throw new IllegalArgumentException()
  def getDouble(columnNo: Int): Double = throw new IllegalArgumentException()
  def getFloat(columnNo: Int): Float = throw new IllegalArgumentException()
  def getString(columnNo: Int): String = throw new IllegalArgumentException()
  override def getHistogram(col: Int): bv.Histogram = if (col == 1) value else throw new IllegalArgumentException()
  def getAny(columnNo: Int): Any = throw new IllegalArgumentException()

  def getBlobBase(columnNo: Int): Any = throw new IllegalArgumentException()
  def getBlobOffset(columnNo: Int): Long = throw new IllegalArgumentException()
  def getBlobNumBytes(columnNo: Int): Int = throw new IllegalArgumentException()

  override def toString: String = s"TransientHistRow(t=$timestamp, v=$value)"
}

// 0: Timestamp, 1: Histogram, 2: Max/Double
final class TransientHistMaxRow(var max: Double = 0.0) extends TransientHistRow() {
  override def setDouble(columnNo: Int, valu: Double): Unit =
    if (columnNo == 2) max = valu else throw new IllegalArgumentException()
  override def getDouble(columnNo: Int): Double =
    if (columnNo == 2) max else throw new IllegalArgumentException()

  override def toString: String = s"TransientHistMaxRow(t=$timestamp, h=$value, max=$max)"
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
  def setBlob(columnNo: Int, base: Array[Byte], offset: Int, length: Int): Unit = throw new IllegalArgumentException()

  def notNull(columnNo: Int): Boolean = columnNo < 3
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
  def getBlobBase(columnNo: Int): Any = throw new IllegalArgumentException()
  def getBlobOffset(columnNo: Int): Long = throw new IllegalArgumentException()
  def getBlobNumBytes(columnNo: Int): Int = throw new IllegalArgumentException()
}

final class QuantileAggTransientRow() extends MutableRowReader {
  var timestamp: Long = _
  var blobBase: Array[Byte] = _
  var blobOffset: Int = _
  var blobLength: Int = _

  def setLong(columnNo: Int, valu: Long): Unit =
    if (columnNo == 0) timestamp = valu
    else throw new IllegalArgumentException()

  def setBlob(columnNo: Int, base: Array[Byte], offset: Int, length: Int): Unit =
    if (columnNo == 1) {
      blobBase = base
      blobOffset = offset
      blobLength = length
    }
    else throw new IllegalArgumentException()

  def setDouble(columnNo: Int, value: Double): Unit = ???
  def setString(columnNo: Int, value: ZeroCopyUTF8String): Unit = ???
  def notNull(columnNo: Int): Boolean = ???
  def getBoolean(columnNo: Int): Boolean = ???
  def getInt(columnNo: Int): Int = ???
  def getLong(columnNo: Int): Long = if (columnNo == 0) timestamp else throw new IllegalArgumentException()
  def getDouble(columnNo: Int): Double = ???
  def getFloat(columnNo: Int): Float = ???
  def getString(columnNo: Int): String = ???
  def getAny(columnNo: Int): Any = ???
  def getBlobBase(columnNo: Int): Any = if (columnNo == 1) blobBase
  else throw new IllegalArgumentException()
  def getBlobOffset(columnNo: Int): Long = if (columnNo == 1) blobOffset
  else throw new IllegalArgumentException()
  def getBlobNumBytes(columnNo: Int): Int = if (columnNo == 1) blobLength
  else throw new IllegalArgumentException()

  override def filoUTF8String(columnNo: Int): ZeroCopyUTF8String = {
    // Needed since blobs are serialized as strings (for now) underneath the covers.
    if (columnNo == 1) new ZeroCopyUTF8String(blobBase, blobOffset, blobLength)
    else throw new IllegalArgumentException()
  }
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

  def setBlob(columnNo: Int, base: Array[Byte], offset: Int, length: Int): Unit = throw new IllegalArgumentException()

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
  def getBlobBase(columnNo: Int): Any = throw new IllegalArgumentException()
  def getBlobOffset(columnNo: Int): Long = throw new IllegalArgumentException()
  def getBlobNumBytes(columnNo: Int): Int = throw new IllegalArgumentException()
}

