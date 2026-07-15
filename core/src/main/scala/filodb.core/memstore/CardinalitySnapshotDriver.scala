package filodb.core.memstore

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging

import filodb.core.memstore.ratelimit.CardinalityTracker

/**
 * Drives periodic snapshots of a shard's CardinalityTracker into a
 * CardinalitySnapshotSink. Owned by TimeSeriesShard.
 *
 * The tracker is scanned at depth 2 (ns) once per interval; the resulting
 * records are handed to the sink. The driver tracks which (ws, ns) pairs were
 * written last cycle so that disappearing namespaces can be evicted from the
 * sink's downstream store.
 *
 * `snapshotOnce()` is the unit of work — visible so tests can call it
 * directly. `start(scheduler, intervalSeconds)` schedules it on a shared
 * executor with a per-shard jittered first-delay.
 */
class CardinalitySnapshotDriver(partition: String,
                                shardNum: Int,
                                cardTracker: CardinalityTracker,
                                sink: CardinalitySnapshotSink)
    extends StrictLogging {

  private val running = new AtomicBoolean(false)
  @volatile private var scheduled: Option[ScheduledFuture[_]] = None
  @volatile private var lastCycleTouched: Set[(String, String)] = Set.empty

  def snapshotOnce(): Unit = {
    if (!running.compareAndSet(false, true)) {
      logger.warn(s"Snapshot for partition=$partition shard=$shardNum still " +
        s"running; skipping this tick")
      return
    }
    try {
      val liveNs = cardTracker.scan(Seq.empty, depth = 2)
        .filter(r => r.prefix.length == 2 && r.value.tsCount > 0)
      val touched = mutable.Set.empty[(String, String)]

      liveNs.foreach { nsRec =>
        val key = (nsRec.prefix(0), nsRec.prefix(1))
        touched += key
      }

      try sink.publish(partition, shardNum, liveNs, Map.empty)
      catch { case NonFatal(t) =>
        logger.warn(s"sink.publish threw for partition=$partition shard=$shardNum", t)
        return
      }

      val touchedSet = touched.toSet
      val stale = lastCycleTouched -- touchedSet
      if (stale.nonEmpty) {
        try sink.evict(partition, shardNum, stale)
        catch { case NonFatal(t) =>
          logger.warn(s"sink.evict threw for partition=$partition shard=$shardNum", t)
        }
      }
      lastCycleTouched = touchedSet
    } finally running.set(false)
  }

  def start(scheduler: ScheduledExecutorService, intervalSeconds: Int,
            totalShardsHint: Int): Unit = {
    require(scheduled.isEmpty, "already started")
    val jitteredFirstDelayMs =
      ((shardNum.toLong * (intervalSeconds * 1000L)) /
        math.max(totalShardsHint, 1)) % (intervalSeconds * 1000L)
    scheduled = Some(scheduler.scheduleAtFixedRate(
      () => snapshotOnce(),
      jitteredFirstDelayMs, intervalSeconds * 1000L, TimeUnit.MILLISECONDS))
    logger.info(s"CardinalitySnapshotDriver started partition=$partition " +
      s"shard=$shardNum interval=${intervalSeconds}s " +
      s"firstDelayMs=$jitteredFirstDelayMs")
  }

  def close(): Unit = {
    scheduled.foreach(_.cancel(false))
    scheduled = None
    // Best-effort wait for an in-flight snapshotOnce() to finish so that
    // downstream sink.close() doesn't race with a mid-flight publish/evict.
    // Bounded to ~1s — snapshots are I/O-bound (a few Redis round-trips)
    // and should complete well within that window in practice.
    val deadlineMs = System.currentTimeMillis() + 1000L
    while (running.get() && System.currentTimeMillis() < deadlineMs) {
      Thread.sleep(20L)
    }
  }
}
