package filodb.memory

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging

import filodb.memory.data.Shutdown

object EvictionLock {
  val maxTimeoutMillis = 2048
  val direCircumstanceTimeoutMillis = 32768
}
/**
 * This lock protects memory regions accessed by queries and prevents them from
 * being evicted.
 */
class EvictionLock(debugInfo: String = "none") extends StrictLogging {

  import EvictionLock._
  // Acquired when reclaiming on demand. Acquire shared lock to prevent block reclamation.
  private final val reclaimLock = new Latch
  @volatile private final var numFailures = 0

  /**
   * Try to acquire lock with multiple retries.
   * If 5 consecutive attempts to tryExclusiveReclaimLock fail, node is restarted.
   *
   * Run only on ingestion thread.
   *
   * @param finalTimeoutMillis timeout starts with 1 and doubles each retry to get lock until finalTimeoutMillis
   * @return if the lock was acquired
   */
  def tryExclusiveReclaimLock(finalTimeoutMillis: Int): Boolean = {
    // Attempting to acquire the exclusive lock must wait for concurrent queries to finish, but
    // waiting will also stall new queries from starting. To protect against this, attempt with
    // a timeout to let any stalled queries through. To prevent starvation of the exclusive
    // lock attempt, increase the timeout each time, but eventually give up. The reason why
    // waiting for an exclusive lock causes this problem is that the thread must enqueue itself
    // into the lock as a waiter, and all new shared requests must wait their turn. The risk
    // with timing out is that if there's a continuous stream of long running queries (more than
    // one second), then the exclusive lock will never be acquired, and then ensureFreeBlocks
    // won't be able to do its job. The timeout settings might need to be adjusted in that case.
    // Perhaps the timeout should increase automatically if ensureFreeBlocks failed the last time?
    // This isn't safe to do until we gain higher confidence that the shared lock is always
    // released by queries.

    var timeout = 1;
    while (true) {
      if (reclaimLock.tryAcquireExclusiveNanos(TimeUnit.MILLISECONDS.toNanos(timeout))) {
        numFailures = 0
        return true
      } else { // if we did not get lock, count failures and judge if the node is in bad state
        if (finalTimeoutMillis >= maxTimeoutMillis / 2) {
          // Start warning when the current headroom has dipped below the halfway point.
          // The lock state is logged in case it's stuck due to a runaway query somewhere.
          logger.warn(s"Lock for ensureHeadroom timed out: ${this}")
        }
      }
      if (timeout >= finalTimeoutMillis) {
        numFailures += 1
        if (numFailures >= 5) {
          Shutdown.haltAndCatchFire(new RuntimeException(s"Headroom task was unable to acquire exclusive lock " +
            s"for $numFailures consecutive attempts. Shutting down process. $debugInfo"))
        }
        return false
      }
      Thread.`yield`()
      timeout = Math.min(finalTimeoutMillis, timeout << 1)
    }
    false // never reached, but scala compiler complains otherwise
  }

  def releaseExclusive(): Unit = reclaimLock.releaseExclusive()

  def acquireSharedLock(): Unit = reclaimLock.lock()

  def releaseSharedLock(): Unit = reclaimLock.unlock()

  override def toString: String = reclaimLock.toString
}
