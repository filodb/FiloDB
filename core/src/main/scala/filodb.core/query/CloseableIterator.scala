package filodb.core.query

import scala.collection.Iterator

import filodb.memory.format.RowReader

trait CloseableIterator[R <: RowReader] extends Iterator[R] { self =>
  /**
    * This method mut release all resources (example locks) acquired
    * for the purpose of executing this query
    */
  def close(): Unit
  def map2(f: R => R): CloseableIterator[R] = new CloseableIterator[R] {
    def hasNext = self.hasNext
    def next() = f(self.next())
    def close(): Unit = self.close()
  }
}

class CustomCloseIterator(iter: Iterator[RowReader])(cl: => Unit) extends CloseableIterator[RowReader] {
  override def close(): Unit = cl // invoke function
  override def hasNext: Boolean = iter.hasNext
  override def next(): RowReader = iter.next()
}

object NoCloseIterator {
  implicit class NoCloseIterator(iter: Iterator[RowReader]) extends CloseableIterator[RowReader] {
    override def close(): Unit = {}
    override def hasNext: Boolean = iter.hasNext
    override def next(): RowReader = iter.next()
  }
}

