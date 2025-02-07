package filodb.core

import scala.concurrent.duration.Duration

/**
 * Rate-limiter utility.
 * @param period a "successful" attempt will be indicated only after a full
 *               period has elapsed since the previous success.
 */
class RateLimiter(period: Duration) {
  private var lastSuccessNanos = 0L;

  /**
   * Returns true to indicate an attempt was "successful", else it was "failed".
   * Successes are returned only after a full period has elapsed since the previous success.
   *
   * NOTE: this operation is not thread-safe, but if at least one concurrent invocation is
   *   successful, then one of the successful timestamps will be recorded internally as the
   *   most-recent success. In practice, this means async calls may succeed in bursts (which
   *   may be acceptable in some use-cases).
   */
  def attempt(): Boolean = {
    val nowNanos = System.nanoTime()
    if (nowNanos - lastSuccessNanos > period.toNanos) {
      lastSuccessNanos = nowNanos
      return true
    }
    false
  }
}
