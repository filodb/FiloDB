package filodb.core

import java.util.concurrent.atomic.AtomicInteger
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