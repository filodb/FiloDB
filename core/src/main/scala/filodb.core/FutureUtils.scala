package filodb.core

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit, ThreadFactory, ThreadPoolExecutor}
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
   * to the context when the number of tasks has reached maxTasks will result in tasks being run in their
   * own thread - which slows things down, but doesn't block and create deadlocks.
   * @param maxTasks the maximum number of tasks that can be queued
   * @param poolName the string prefix to append to worker threads of this threadpool
   * @param corePoolSize the minimum number of threads in the thread pool
   * @param maxPoolSize the maximum number of threads the pool expands to when the queue is full
   */
  def getBoundedExecContext(maxTasks: Int,
                            poolName: String,
                            corePoolSize: Int = sys.runtime.availableProcessors,
                            maxPoolSize: Int = sys.runtime.availableProcessors * 4): ExecutionContext = {
    ExecutionContext.fromExecutorService(
      new ThreadPoolExecutor(
        corePoolSize, maxPoolSize,
        10L, TimeUnit.SECONDS,
        new ArrayBlockingQueue[Runnable](maxTasks),
        new ThreadFactory {
          val counter = new AtomicInteger
          override def newThread(r: Runnable): Thread =
            new Thread(r, s"$poolName-${counter.incrementAndGet}")
        },
        new ThreadPoolExecutor.CallerRunsPolicy
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