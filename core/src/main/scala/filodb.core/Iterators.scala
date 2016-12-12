package filodb.core

import java.io.Closeable
import java.util.concurrent.ArrayBlockingQueue
import monix.execution.Scheduler
import monix.reactive.{Notification, Observable}

// From http://stackoverflow.com/questions/10642337/is-there-are-iterative-version-of-groupby-in-scala
object Iterators {
  implicit class RichIterator[T](origIt: Iterator[T]) {
    def sortedGroupBy[B](func: T => B): Iterator[(B, Iterator[T])] = new Iterator[(B, Iterator[T])] {
      var iter = origIt
      def hasNext: Boolean = iter.hasNext
      def next: (B, Iterator[T]) = {
        val first = iter.next()
        val firstValue = func(first)
        val (i1,i2) = iter.span(el => func(el) == firstValue)
        iter = i2
        (firstValue, Iterator.single(first) ++ i1)
      }
    }
  }

  implicit class RichObservable[T](origObs: Observable[T]) {
    def toIterator(scheduler: Scheduler = Scheduler.Implicits.global): Iterator[T] =
      new ObservableIterator(origObs)(scheduler)
  }

  /**
   * A class implementing an Iterator pulling elements from a queue fed by an Observable subscription
   * thus bridging the push (Rx/Observable) and pull (Iterator) chasm  :)
   */
  class ObservableIterator[T](observable: Observable[T],
                              queueSize: Int = 8)
                             (implicit scheduler: Scheduler)
  extends Iterator[T] with Closeable {
    import Notification._

    private val queue = new ArrayBlockingQueue[Notification[T]](queueSize)
    private var cached: Notification[T] = OnComplete
    private var completed = false
    private val subscription = observable.materialize
                                         .foreach(queue.put)

    def hasNext: Boolean = {
      cacheNext()
      !completed
    }

    def next: T = cached match {
      case OnNext(elem) => elem
      case x: Any       => throw new RuntimeException("should not call next() after error or completed")
    }

    private def cacheNext(): Unit = if (!completed) {
      queue.take() match {
        case OnComplete  =>  completed = true
        case OnError(ex) =>  completed = true; throw ex
        case o: OnNext[T] => cached = o
      }
    }

    override def close(): Unit = {
      subscription.cancel
    }
  }
}