package filodb.core.query

import scala.collection.Iterator

import filodb.memory.format.RowReader

/**
  * Please note this is not the ideal contract of cursor.
  * Instead, it is a stop-gap implementation that gets us ability to
  * release resources from a query. Earlier implementation purely on
  * Iterators didnt help us nicely with that. The expectation is that
  * moving to this trait will help us get compile time checks
  * that force developer to care for "closing" the cursor before
  * completing the query
  */
trait RangeVectorCursor extends Iterator[RowReader] { self =>
  /**
    * This method mut release all resources (example locks) acquired
    * for the purpose of executing this query
    */
  def close(): Unit
  def map2(f: RowReader => RowReader): RangeVectorCursor = new RangeVectorCursor {
    def hasNext = self.hasNext
    def next() = f(self.next())
    def close(): Unit = self.close()
  }
}

class CustomCloseCursor(iter: Iterator[RowReader])(cl: => Unit) extends RangeVectorCursor {
  override def close(): Unit = cl // invoke function
  override def hasNext: Boolean = iter.hasNext
  override def next(): RowReader = iter.next()
}

object NoCloseCursor {
  implicit class NoCloseIterator(iter: Iterator[RowReader]) extends RangeVectorCursor {
    override def close(): Unit = {}
    override def hasNext: Boolean = iter.hasNext
    override def next(): RowReader = iter.next()
  }
}

