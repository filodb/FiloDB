package filodb.core.memstore

import scala.collection.mutable
import scala.util.control.NonFatal

import com.apple.its.redis.core.client.pipeline.RedisClientPipeline
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
// Jedis Response arrives transitively via com.apple.its:redis-cluster; a future RCL upgrade
// that relocates/shades Jedis would require revisiting this import.
import redis.clients.jedis.Response

import filodb.core.memstore.ratelimit.CardinalityRecord
import filodb.core.store.AciRedisConfig

object RedisCardinalitySnapshotSink {
  private[memstore] val commandsCounter =
    Kamon.counter("filodb-cardinality-snapshot-redis-commands")
  private[memstore] val commandFailures =
    Kamon.counter("filodb-cardinality-snapshot-redis-command-failures")
  private[memstore] val batchLatency =
    Kamon.histogram("filodb-cardinality-snapshot-redis-batch-latency",
      MeasurementUnit.time.milliseconds)
}

/**
 * CardinalitySnapshotSink backed by the managed Valkey (aci-redis) cluster via the
 * RCL client. Namespace-only: writes the ns_total:{partition}:{ws}:{ns} HASH
 * (field shard-N) with `hset`, removes stale shard fields with `hdel`. Every command
 * is single-key, so the cluster client routes each by its key; a per-call pipeline
 * scatter-gathers them across shards and results surface on sync().
 */
class RedisCardinalitySnapshotSink(cfg: AciRedisConfig)
    extends CardinalitySnapshotSink with StrictLogging {

  private val client = RedisSnapshotClient.acquire(cfg)

  logger.info(s"RedisCardinalitySnapshotSink connected via aci-redis cluster=${cfg.clusterName} " +
    s"workspace=${cfg.workspaceName} mtls=${cfg.mtlsEnabled}")

  private def requireNoColon(s: String, field: String): Unit =
    require(!s.contains(':'), s"$field must not contain colon: '$s'")

  override def publish(partition: String, shardNum: Int,
                       ns: Seq[CardinalityRecord],
                       perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit = {
    requireNoColon(partition, "partition")
    if (ns.isEmpty) return
    // Validate all keys up front so we never throw mid-pipeline (which would leak the borrowed conn).
    ns.foreach { r => requireNoColon(r.prefix(0), "ws"); requireNoColon(r.prefix(1), "ns") }

    val shardField = s"shard-$shardNum"
    val startNanos = System.nanoTime()
    var hsetCount = 0L
    var failureCount = 0L
    val pipeline: RedisClientPipeline = client.pipelined()
    val pending = mutable.ArrayBuffer.empty[Response[java.lang.Long]]
    try {
      ns.foreach { nsRec =>
        val hkey = s"ns_total:$partition:${nsRec.prefix(0)}:${nsRec.prefix(1)}"
        pending += pipeline.hset(hkey, shardField, nsRec.value.activeTsCount.toString)
        hsetCount += 1
      }
      pipeline.sync()
      pending.foreach { r =>
        try { r.get(); () }
        catch { case NonFatal(t) =>
          failureCount += 1
          logger.warn(s"redis hset failed partition=$partition shard=$shardNum: ${t.getMessage}")
        }
      }
    } catch {
      case NonFatal(t) =>
        failureCount = hsetCount
        logger.warn(s"redis publish pipeline failed partition=$partition shard=$shardNum: ${t.getMessage}")
        // sync() is the pipeline's only connection-release path (no close()); if enqueue
        // threw before we synced, force it best-effort so borrowed connections return to the pool.
        try pipeline.sync() catch { case NonFatal(_) => () }
    } finally {
      val tags = TagSet.from(Map("partition" -> partition, "op" -> "publish"))
      // commandsCounter measures commands attempted, not succeeded; a total failure reads as
      // N attempted here + N failed in commandFailures below.
      RedisCardinalitySnapshotSink.commandsCounter.withTags(tags.withTag("cmd", "hset")).increment(hsetCount)
      if (failureCount > 0L)
        RedisCardinalitySnapshotSink.commandFailures.withTags(tags).increment(failureCount)
      RedisCardinalitySnapshotSink.batchLatency.withTags(tags)
        .record(math.max(0L, (System.nanoTime() - startNanos) / 1000000L))
    }
  }

  override def evict(partition: String, shardNum: Int,
                     stale: Set[(String, String)]): Unit = {
    if (stale.isEmpty) return
    requireNoColon(partition, "partition")
    stale.foreach { case (ws, nsName) => requireNoColon(ws, "ws"); requireNoColon(nsName, "ns") }

    val shardField = s"shard-$shardNum"
    val startNanos = System.nanoTime()
    var hdelCount = 0L
    var failureCount = 0L
    val pipeline: RedisClientPipeline = client.pipelined()
    val pending = mutable.ArrayBuffer.empty[Response[java.lang.Long]]
    try {
      stale.foreach { case (ws, nsName) =>
        pending += pipeline.hdel(s"ns_total:$partition:$ws:$nsName", shardField)
        hdelCount += 1
      }
      pipeline.sync()
      pending.foreach { r =>
        try { r.get(); () }
        catch { case NonFatal(t) =>
          failureCount += 1
          logger.warn(s"redis hdel failed partition=$partition shard=$shardNum: ${t.getMessage}")
        }
      }
    } catch {
      case NonFatal(t) =>
        failureCount = hdelCount
        logger.warn(s"redis evict pipeline failed partition=$partition shard=$shardNum: ${t.getMessage}")
        // sync() is the pipeline's only connection-release path (no close()); if enqueue
        // threw before we synced, force it best-effort so borrowed connections return to the pool.
        try pipeline.sync() catch { case NonFatal(_) => () }
    } finally {
      val tags = TagSet.from(Map("partition" -> partition, "op" -> "evict"))
      // commandsCounter measures commands attempted, not succeeded; a total failure reads as
      // N attempted here + N failed in commandFailures below.
      RedisCardinalitySnapshotSink.commandsCounter.withTags(tags.withTag("cmd", "hdel")).increment(hdelCount)
      if (failureCount > 0L)
        RedisCardinalitySnapshotSink.commandFailures.withTags(tags).increment(failureCount)
      RedisCardinalitySnapshotSink.batchLatency.withTags(tags)
        .record(math.max(0L, (System.nanoTime() - startNanos) / 1000000L))
    }
  }

  // The shared RedisClusterClient is owned by RedisSnapshotClient and closed by its
  // JVM shutdown hook; per-shard shutdown must not tear it down.
  override def close(): Unit = ()
}
