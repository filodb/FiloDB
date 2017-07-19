package filodb.core

import java.io.Closeable
import java.util.concurrent.ArrayBlockingQueue

import monix.execution.{Ack, Scheduler}
import monix.reactive.observables.ObservableLike.Operator
import monix.reactive.observers.Subscriber
import monix.reactive.{Notification, Observable, OverflowStrategy}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.control.NonFatal

// From http://stackoverflow.com/questions/10642337/is-there-are-iterative-version-of-groupby-in-scala
object Iterators {
  // Use the I/O Pool by default instead of the global fork-join pool -- for the ObservableIterator.
  // The I/O pool allows unlimited number of threads, otherwise the FJ pool might have 1 thread only
  // and cause a deadlock with the BlockingQueue
  val ioPool = Scheduler.io("filodb-io")

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

    def sortedGroupBy[B](groupingFunc: T => B): Observable[(B, Seq[T])] =
      origObs.liftByOperator(new SortedGroupByOperator(groupingFunc))
  }

  /**
   * A class implementing an Iterator pulling elements from a queue fed by an Observable subscription
   * thus bridging the push (Rx/Observable) and pull (Iterator) chasm  :)
   */
  final class ObservableIterator[T](observable: Observable[T],
                              queueSize: Int = 8)
                             (implicit scheduler: Scheduler)
  extends Iterator[T] with Closeable {
    import Notification._

    private val queue = new ArrayBlockingQueue[Notification[T]](queueSize)
    private var cached: Notification[T] = OnComplete
    private var completed = false
    private val subscription = observable
      // Can also use (but might be slightly slower)  .asyncBoundary(OverflowStrategy.Default)
      .subscribeOn(Iterators.ioPool)
      .materialize
      .foreach(queue.put)

    final def hasNext: Boolean = {
      cacheNext()
      !completed
    }

    final def next: T = cached match {
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

  import Ack._

  /**
   * An Observable operator that implements a grouping function assuming the stream has some order to it,
   * basically grouping together consecutive elements and emitting the group at the end of when the grouping
   * func output stays the same
   *
   * NOTE: Most of the code here mimics the built in Operators code in Monix.
   * TODO: provide a trait that abstracts common logic out.
   */
  final class SortedGroupByOperator[A, B](groupingFunc: A => B) extends Operator[A, (B, Seq[A])] {
    def apply(out: Subscriber[(B, Seq[A])]): Subscriber[A] =
      new Subscriber[A] {
        implicit val scheduler = out.scheduler
        var buf = new ArrayBuffer[A]()
        var lastGroupVal: Option[B] = None
        var ack: Future[Ack] = Continue

        final def onNext(elem: A): Future[Ack] = {
          var streamError = true
          try {
            val thisGroup = groupingFunc(elem)
            lastGroupVal.foreach { groupVal =>
              if (thisGroup != groupVal) {
                streamError = false
                ack = out.onNext((groupVal, buf))
                buf = new ArrayBuffer[A]()
                lastGroupVal = Some(thisGroup)
              } else {
                ack = Continue
              }
            }
            buf += elem
            if (lastGroupVal.isEmpty) {
              lastGroupVal = Some(thisGroup)
              ack = Continue
            }
            ack
          } catch {
            case NonFatal(ex) if streamError =>
              out.onError(ex)
              Stop
          }
        }

        def onError(ex: Throwable): Unit = {
          out.onError(ex)
        }

        def onComplete(): Unit = {
          ack.syncOnContinue {
            lastGroupVal.foreach { groupVal => out.onNext((groupVal, buf)) }
            buf = new ArrayBuffer[A]()
            lastGroupVal = None
            out.onComplete()
          }
        }
      }
  }
}