package filodb.core.memstore

import java.util.concurrent.{Executors, ScheduledExecutorService}

/**
 * Shared ScheduledExecutorService used by every CardinalitySnapshotDriver
 * on this JVM. Sized to a small fixed pool — tasks are I/O bound (Redis
 * calls) and short-lived, so a few threads are enough for thousands of
 * shards' worth of scheduled work.
 */
object CardinalitySnapshotScheduler {
  private lazy val scheduler: ScheduledExecutorService =
    Executors.newScheduledThreadPool(4, (r: Runnable) => {
      val t = new Thread(r, "cardinality-snapshot")
      t.setDaemon(true)
      t
    })
  def get: ScheduledExecutorService = scheduler
}
