package filodb.memory

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.typesafe.scalalogging.StrictLogging

object EvictionLock {
  val maxTimeoutMillis = 2048
  /**
   * This timeout must be greater than query timeout.
   * At the same time, we don't want it too high to avoid blocking ingestion
   * for too long - otherwise, alerting queries can return incomplete data.
   */
  val direCircumstanceMaxTimeoutMillis = 90000
}

/**
 * This lock protects memory regions accessed by queries and prevents them from
 * being evicted.
 */
class EvictionLock(trackQueriesHoldingEvictionLock: Boolean = false,
                   debugInfo: String = "none") extends StrictLogging {
  // Acquired when reclaiming on demand. Acquire shared lock to prevent block reclamation.
  private final val reclaimLock = new Latch
  private final val runningQueries = new ConcurrentHashMap[String, String]()

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
        return true
      }
      if (timeout >= finalTimeoutMillis) {
        if (finalTimeoutMillis > 10000) {
          logger.error(s"Could not acquire exclusive access to eviction lock $debugInfo " +
            s"with finalTimeoutMillis=$finalTimeoutMillis $this")
        }
        return false
      }
      Thread.`yield`()
      timeout = Math.min(finalTimeoutMillis, timeout << 1)
    }
    false // never reached, but scala compiler complains otherwise
  }

  def releaseExclusive(): Unit = reclaimLock.releaseExclusive()

  def acquireSharedLock(timeoutMs: Long, holderId: String, promQL: String): Boolean = {
    if (trackQueriesHoldingEvictionLock) runningQueries.put(holderId, promQL)
    reclaimLock.tryAcquireSharedNanos(timeoutMs * 1000000)
  }

  def releaseSharedLock(holderId: String): Unit = {
    if (trackQueriesHoldingEvictionLock) runningQueries.remove(holderId)
    reclaimLock.unlock()
  }

  override def toString: String = s"debugInfo=$debugInfo lockState: ${reclaimLock} " +
    s"RunningQueries: ${if (trackQueriesHoldingEvictionLock) runningQueries else "NotTracked"}"
}
