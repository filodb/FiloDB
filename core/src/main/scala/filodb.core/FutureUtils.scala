package filodb.core

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

import filodb.core.messages.{Response, TooManyRequests}

trait FutureUtils {
  val limiter: ConcurrentLimiter

  /**
   * Only executes f if the limiter value is below the limit, otherwise
   * fails fast and returns TooManyRequests
   */
  def rateLimit(f: => Future[Response]): Future[Response] = {
    if (limiter.obtain()) {
      f
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