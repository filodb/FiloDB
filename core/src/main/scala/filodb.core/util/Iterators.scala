package filodb.core.util

import org.velvia.filo.{FastFiloRowReader, RowReader}

// From http://stackoverflow.com/questions/10642337/is-there-are-iterative-version-of-groupby-in-scala
object Iterators {

  implicit class RichIterator[T](origIt: Iterator[T]) {
    def sortedGroupBy[B](func: T => B): Iterator[(B, Iterator[T])] = new Iterator[(B, Iterator[T])] {
      var iter = origIt

      def hasNext: Boolean = iter.hasNext

      def next: (B, Iterator[T]) = {
        val first = iter.next()
        val firstValue = func(first)
        val (i1, i2) = iter.span(el => func(el) == firstValue)
        iter = i2
        (firstValue, Iterator.single(first) ++ i1)
      }
    }
  }

}
