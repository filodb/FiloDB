package filodb.memory

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.typesafe.scalalogging.StrictLogging

object EvictionLock {
  val maxTimeoutMillis = 2048
  val direCircumstanceMaxTimeoutMillis = 65536
}

/**
 * This lock protects memory regions accessed by queries and prevents them from
 * being evicted.
 */
class EvictionLock(debugInfo: String = "none") extends StrictLogging {
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
      // if we did not get lock, count failures and judge if the node is in bad state
      if (timeout >= finalTimeoutMillis) {
        logger.error(s"Could not acquire exclusive access to eviction lock $debugInfo " +
          s"with finalTimeoutMillis=$finalTimeoutMillis LockState: $this runningQueries: $runningQueries")
        return false
      }
      Thread.`yield`()
      timeout = Math.min(finalTimeoutMillis, timeout << 1)
    }
    false // never reached, but scala compiler complains otherwise
  }

  def releaseExclusive(): Unit = reclaimLock.releaseExclusive()

  def acquireSharedLock(holderId: String, promQL: String): Unit = {
    runningQueries.put(holderId, promQL)
    reclaimLock.lock()
  }

  def releaseSharedLock(holderId: String): Unit = {
    runningQueries.remove(holderId)
    reclaimLock.unlock()
  }

  override def toString: String = reclaimLock.toString
}
