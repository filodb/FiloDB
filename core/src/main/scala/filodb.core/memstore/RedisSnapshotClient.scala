package filodb.core.memstore

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import io.lettuce.core.{RedisClient, RedisURI}

/**
 * JVM-scoped Lettuce client cache. One `RedisClient` per (host, port).
 * Shared across all shards on this JVM to bound the connection count.
 *
 * The client is not closed until JVM shutdown. This is intentional — shard
 * shutdown is not a signal to tear down the shared Redis client.
 */
object RedisSnapshotClient {
  private val clients = new ConcurrentHashMap[(String, Int), RedisClient]()

  def acquire(host: String, port: Int, commandTimeoutMs: Long): RedisClient =
    clients.computeIfAbsent((host, port), _ => {
      val uri = RedisURI.Builder.redis(host, port)
        .withTimeout(Duration.ofMillis(commandTimeoutMs))
        .build()
      RedisClient.create(uri)
    })

  /** For tests only. Never call in production code. */
  private[memstore] def releaseAllForTest(): Unit = {
    clients.values().forEach { c => try c.shutdown() catch { case _: Throwable => } }
    clients.clear()
  }
}
