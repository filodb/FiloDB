package filodb.memory.format

import filodb.memory.format.vectors.ConstAppendingVector

/**
 * A trait for making RowReader appends to BinaryAppendableVector efficient and unboxed.  Type specific
 * code for adding to specific types of vectors are in each type specific implementation.  This avoids
 * boxing overhead.
 */
trait RowReaderAppender {
  def appender: BinaryAppendableVector[_]
  def col: Int

  // Appends data to rowReader.  Type specific code.  Assumes data is available from reader.
  def appendData(row: RowReader): Unit

  /**
   * Appends a row to appender, checking for availability.
   */
  final def append(row: RowReader): Unit = {
    if (row.notNull(col)) { appendData(row) }
    else                  { appender.addNA() }
  }
}

// NOTE: we have to define type specific stuff to ensure the right specialized method of add is used.
// No way to make this generic with specialization unfortunately, because the append signature does not
// take in anything with type A.
// Even if you try returning something of type A, it does not work.
class IntReaderAppender(val appender: BinaryAppendableVector[Int], val col: Int) extends RowReaderAppender {
  final def appendData(row: RowReader): Unit = appender.addData(row.getInt(col))
}

class LongReaderAppender(val appender: BinaryAppendableVector[Long], val col: Int) extends RowReaderAppender {
  final def appendData(row: RowReader): Unit = appender.addData(row.getLong(col))
}

class DoubleReaderAppender(val appender: BinaryAppendableVector[Double], val col: Int) extends RowReaderAppender {
  final def appendData(row: RowReader): Unit = appender.addData(row.getDouble(col))
}

class StringReaderAppender(val appender: BinaryAppendableVector[ZeroCopyUTF8String], val col: Int)
extends RowReaderAppender {
  final def appendData(row: RowReader): Unit = appender.addData(row.filoUTF8String(col))
}

/**
 * An appender that creates correctly-sized ConstVectors for static partition columns, and skips
 * reading from the RowReaader for efficiency
 */
final case class ConstAppender[A](appender: ConstAppendingVector[A], col: Int) extends RowReaderAppender {
  final def appendData(row: RowReader): Unit = appender.addNA()
}