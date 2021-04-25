package filodb.memory

import java.util.concurrent.TimeUnit

/**
 * This lock protects memory regions accessed by queries and prevents them from being evicted.
 */
class EvictionLock {

  // Acquired when reclaiming on demand. Acquire shared lock to prevent block reclamation.
  private final val reclaimLock = new Latch

  def tryExclusiveReclaimLock(finalTimeoutMillis: Int): Boolean = {
    // Attempting to acquire the exclusive lock must wait for concurrent queries to finish, but
    // waiting will also stall new queries from starting. To protect against this, attempt with
    // a timeout to let any stalled queries through. To prevent starvation of the exclusive
    // lock attempt, increase the timeout each time, but eventually give up. The reason why
    // waiting for an exclusive lock causes this problem is that the thread must enqueue itself
    // into the lock as a waiter, and all new shared requests must wait their turn. The risk
    // with timing out is that if there's a continuous stream of long running queries (more than
    // one second), then the exclusive lock will never be acqiured, and then ensureFreeBlocks
    // won't be able to do its job. The timeout settings might need to be adjusted in that case.
    // Perhaps the timeout should increase automatically if ensureFreeBlocks failed the last time?
    // This isn't safe to do until we gain higher confidence that the shared lock is always
    // released by queries.

    var timeout = 1;
    while (true) {
      val acquired = reclaimLock.tryAcquireExclusiveNanos(TimeUnit.MILLISECONDS.toNanos(timeout))
      if (acquired) {
        return true
      }
      if (timeout >= finalTimeoutMillis) {
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
