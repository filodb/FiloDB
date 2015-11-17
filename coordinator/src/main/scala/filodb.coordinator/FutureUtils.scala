package filodb.coordinator

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}

import filodb.core.TooManyRequests

import scala.concurrent.{ExecutionContext, Future}

trait FutureUtils {
  val limiter: ConcurrentLimiter

  /**
   * Only executes f if the limiter value is below the limit, otherwise
   * fails fast and returns TooManyRequests
   */
  def rateLimit(f: => Future[Response])(implicit ec: ExecutionContext): Future[Response] = {
    if (limiter.obtain()) {
      val future = f
      future.onComplete {
        case x: Any => limiter.release()   // Must release when future is finished
      }
      future
    } else {
      Future.successful(TooManyRequests)
    }
  }
}

object FutureUtils {
  /**
   * Obtains an ExecutionContext with a bounded number of tasks.  Additional attempts to add more Runnables
   * to the context when the number of tasks has reached maxTasks will result in blockage.
   * See http://quantifind.com/blog/2015/06/throttling-instantiations-of-scala-futures-1/
   * @param maxTasks the maximum number of tasks that can be added before blocking
   * @param poolName the string prefix to append to worker threads of this threadpool
   * @param numWorkers the number of threads in the thread pool
   */
  def getBoundedExecContext(maxTasks: Int,
                            poolName: String,
                            numWorkers: Int = sys.runtime.availableProcessors): ExecutionContext = {
    ExecutionContext.fromExecutorService(
      new ThreadPoolExecutor(
        numWorkers, numWorkers,
        0L, TimeUnit.SECONDS,
        new ArrayBlockingQueue[Runnable](maxTasks) {
          override def offer(e: Runnable): Boolean = {
            put(e); // may block if waiting for empty room
            true
          }
        },
        new ThreadFactory {
          val counter = new AtomicInteger
          override def newThread(r: Runnable): Thread =
            new Thread(r, s"$poolName-${counter.incrementAndGet}")
        }
      )
     )
  }
}

/**
 * A simple concurrent resource limiter using j.u.c.AtomicInteger
 * @param limit the maximum number of outstanding futures at one time
 */
private[core] class ConcurrentLimiter(limit: Int) {
  val counter = new AtomicInteger

  def obtain(): Boolean = {
    val newValue = counter.incrementAndGet()
    if (newValue > limit) {
      counter.decrementAndGet()
      false
    } else {
      true
    }
  }

  def release(): Unit = { counter.decrementAndGet() }

  def get: Int = counter.get()
}
