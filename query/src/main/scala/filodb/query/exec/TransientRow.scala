package filodb.query.exec

import filodb.memory.format.RowReader

/**
  * Represents intermediate sample which will be part of a transformed RangeVector.
  * IMPORTANT: It is mutable for memory efficiency purposes. Consumers from
  * iterators should be aware of the semantics of ability to save the next() value.
  */
final class TransientRow(data: Array[Any] = Array(0L, 0.0d)) extends RowReader {

  /**
    * Convenience getter for fetching timestamp based row-key
    */
  def timestamp: Long = getLong(0)

  /**
    * Convenience getter for fetching double value column
    */
  def value: Double = getDouble(1)

  def set(i: Int, value: Any): Unit = data(i) = value
  def set(values: Any*): Unit = {
    for {i <- values.indices} data(i) = values(i)
  }
  def copyFrom(r: RowReader): TransientRow = {
    for {i <- data.indices} data(i) = r.getAny(i)
    this
  }
  def notNull(columnNo: Int): Boolean = columnNo < data.length
  def getBoolean(columnNo: Int): Boolean = data(columnNo).asInstanceOf[Boolean]
  def getInt(columnNo: Int): Int = data(columnNo).asInstanceOf[Int]
  def getLong(columnNo: Int): Long = data(columnNo).asInstanceOf[Long]
  def getDouble(columnNo: Int): Double = data(columnNo).asInstanceOf[Double]
  def getFloat(columnNo: Int): Float = data(columnNo).asInstanceOf[Float]
  def getString(columnNo: Int): String = data(columnNo).asInstanceOf[String]
  def getAny(columnNo: Int): Any = data(columnNo)

}
