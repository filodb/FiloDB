package filodb.core.memstore

import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import io.lettuce.core.{RedisFuture, ScoredValue}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.StringCodec

import filodb.core.memstore.ratelimit.CardinalityRecord

/**
 * CardinalitySnapshotSink backed by Redis via Lettuce. All commands within a
 * single publish() or evict() call are pipelined (auto-flush disabled around
 * the batch) - at ~2 000 commands per shard cycle, un-pipelined ~2 seconds vs.
 * pipelined ~milliseconds.
 *
 * Thread-safe: the underlying Lettuce connection is multiplexed and safe
 * for concurrent commands.
 */
class RedisCardinalitySnapshotSink(host: String, port: Int,
                                    commandTimeoutMs: Long)
    extends CardinalitySnapshotSink with StrictLogging {

  private val client = RedisSnapshotClient.acquire(host, port, commandTimeoutMs)
  private val connection: StatefulRedisConnection[String, String] =
    client.connect(StringCodec.UTF8)
  private val async = connection.async()

  logger.info(s"RedisCardinalitySnapshotSink connected host=$host port=$port " +
    s"commandTimeoutMs=$commandTimeoutMs")

  private def requireNoColon(s: String, field: String): Unit =
    require(!s.contains(':'), s"$field must not contain colon: '$s'")

  override def publish(partition: String, shardNum: Int,
                       ns: Seq[CardinalityRecord],
                       perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit = {
    requireNoColon(partition, "partition")
    val shardField = s"shard-$shardNum"

    async.setAutoFlushCommands(false)
    try {
      val pending = scala.collection.mutable.ArrayBuffer.empty[RedisFuture[_]]

      ns.foreach { nsRec =>
        val ws = nsRec.prefix(0)
        val nsName = nsRec.prefix(1)
        requireNoColon(ws, "ws")
        requireNoColon(nsName, "ns")
        val hkey = s"ns_total:$partition:$ws:$nsName"
        pending += async.hset(hkey, shardField, nsRec.value.activeTsCount.toString)
      }

      perMetric.foreach { case (nsPrefix, metrics) =>
        val ws = nsPrefix(0)
        val nsName = nsPrefix(1)
        val zkey = s"zset:$partition:shard-$shardNum:$ws:$nsName"
        pending += async.del(zkey)
        if (metrics.nonEmpty) {
          val members: Array[ScoredValue[String]] = metrics.map { r =>
            ScoredValue.just(r.value.activeTsCount.toDouble, r.prefix(2))
          }.toArray
          pending += async.zadd(zkey, members: _*)
        }
      }

      async.flushCommands()
      pending.foreach { f =>
        try f.get()
        catch { case NonFatal(t) =>
          logger.warn(s"redis command failed partition=$partition shard=$shardNum: " +
            t.getMessage)
        }
      }
    } finally async.setAutoFlushCommands(true)
  }

  override def evict(partition: String, shardNum: Int,
                     stale: Set[(String, String)]): Unit = {
    if (stale.isEmpty) return
    requireNoColon(partition, "partition")
    val shardField = s"shard-$shardNum"

    async.setAutoFlushCommands(false)
    try {
      val pending = scala.collection.mutable.ArrayBuffer.empty[RedisFuture[_]]
      stale.foreach { case (ws, nsName) =>
        requireNoColon(ws, "ws")
        requireNoColon(nsName, "ns")
        pending += async.hdel(s"ns_total:$partition:$ws:$nsName", shardField)
        pending += async.del(s"zset:$partition:shard-$shardNum:$ws:$nsName")
      }
      async.flushCommands()
      pending.foreach { f =>
        try f.get()
        catch { case NonFatal(t) =>
          logger.warn(s"redis evict failed partition=$partition shard=$shardNum: " +
            t.getMessage)
        }
      }
    } finally async.setAutoFlushCommands(true)
  }

  override def close(): Unit = {
    try connection.close() catch { case _: Throwable => }
    logger.info(s"RedisCardinalitySnapshotSink closed host=$host port=$port")
  }
}
