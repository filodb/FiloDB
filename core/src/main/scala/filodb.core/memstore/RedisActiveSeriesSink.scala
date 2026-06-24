package filodb.core.memstore

import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.{LinkedBlockingQueue, ScheduledExecutorService, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.typesafe.scalalogging.StrictLogging
import io.lettuce.core.{RedisClient, RedisURI}
import io.lettuce.core.api.StatefulRedisConnection

import filodb.memory.BinaryRegion

/**
 * Lettuce-backed sink that mirrors active-series transitions into Redis.
 *
 * Key shape:   active:{ws}:{ns}      (Redis SET)
 * Member:      8 raw bytes = xxhash64(partKeyBytes)
 *
 * Writes are async and batched. The ingest path enqueues a small Op record;
 * a single scheduled flusher drains the queue and pipelines SADD/SREM via
 * Lettuce's async API. The ingest thread never waits on Redis.
 *
 * Failure handling: Redis errors are logged at warn and silently dropped.
 * The shard remains the source of truth; this sink is a best-effort mirror.
 */
class RedisActiveSeriesSink(host: String,
                            port: Int,
                            batchSize: Int,
                            batchIntervalMillis: Long)
    extends ActiveSeriesSink with StrictLogging {

  private val maxQueueSize = 100000
  private val keyPrefix = "active:"

  private val queue = new LinkedBlockingQueue[RedisActiveSeriesSink.Op](maxQueueSize)
  private val droppedCount = new java.util.concurrent.atomic.AtomicLong(0L)

  private val client: RedisClient =
    RedisClient.create(RedisURI.Builder.redis(host, port).withTimeout(Duration.ofSeconds(2)).build())
  private val connection: StatefulRedisConnection[Array[Byte], Array[Byte]] =
    client.connect(io.lettuce.core.codec.ByteArrayCodec.INSTANCE)
  private val async = connection.async()

  private val closed = new AtomicBoolean(false)
  private val scheduler: ScheduledExecutorService = {
    val exec = new ScheduledThreadPoolExecutor(1, (r: Runnable) => {
      val t = new Thread(r, "active-series-redis-flusher")
      t.setDaemon(true)
      t
    })
    exec.setRemoveOnCancelPolicy(true)
    exec
  }
  scheduler.scheduleWithFixedDelay(() => flushSafely(),
                                   batchIntervalMillis, batchIntervalMillis, TimeUnit.MILLISECONDS)

  logger.info(s"RedisActiveSeriesSink started host=$host port=$port " +
              s"batchSize=$batchSize batchIntervalMs=$batchIntervalMillis")

  override def onActivate(shardKeyValues: Seq[String], partKeyBytes: Array[Byte]): Unit =
    enqueue(RedisActiveSeriesSink.Add, shardKeyValues, partKeyBytes)

  override def onDeactivate(shardKeyValues: Seq[String], partKeyBytes: Array[Byte]): Unit =
    enqueue(RedisActiveSeriesSink.Rem, shardKeyValues, partKeyBytes)

  private def enqueue(op: RedisActiveSeriesSink.OpKind,
                      shardKeyValues: Seq[String],
                      partKeyBytes: Array[Byte]): Unit = {
    if (closed.get() || shardKeyValues.size < 2) return
    val ws = shardKeyValues(0)
    val ns = shardKeyValues(1)
    val key = (keyPrefix + ws + ":" + ns).getBytes("UTF-8")
    val member = fingerprint(partKeyBytes)
    val record = RedisActiveSeriesSink.Op(op, key, member)
    if (!queue.offer(record)) {
      val dropped = droppedCount.incrementAndGet()
      if (dropped == 1L || dropped % 10000 == 0)
        logger.warn(s"RedisActiveSeriesSink queue full, dropped op total=$dropped")
    }
    if (queue.size >= batchSize) flushSafely()
  }

  private def fingerprint(partKeyBytes: Array[Byte]): Array[Byte] = {
    val h = BinaryRegion.hasher64.hash(partKeyBytes, 0, partKeyBytes.length, BinaryRegion.Seed)
    val buf = ByteBuffer.allocate(8)
    buf.putLong(h)
    buf.array()
  }

  private def flushSafely(): Unit = {
    try flush() catch {
      case t: Throwable =>
        logger.warn(s"RedisActiveSeriesSink flush failed: ${t.getMessage}")
    }
  }

  private def flush(): Unit = {
    if (queue.isEmpty) return
    val drained = new java.util.ArrayList[RedisActiveSeriesSink.Op](batchSize)
    queue.drainTo(drained, batchSize * 4)
    if (drained.isEmpty) return

    // group ops by (key, kind) so we can issue one SADD/SREM per group with multiple members.
    val groups = mutable.Map.empty[(Seq[Byte], RedisActiveSeriesSink.OpKind), mutable.ArrayBuffer[Array[Byte]]]
    drained.asScala.foreach { op =>
      val k = (op.key.toSeq, op.kind)
      groups.getOrElseUpdate(k, mutable.ArrayBuffer.empty).append(op.member)
    }

    groups.foreach { case ((keySeq, kind), members) =>
      val key = keySeq.toArray
      val membersArr = members.toArray
      kind match {
        case RedisActiveSeriesSink.Add => async.sadd(key, membersArr: _*)
        case RedisActiveSeriesSink.Rem => async.srem(key, membersArr: _*)
      }
    }
    // Flush the pipeline so commands actually go on the wire.
    async.flushCommands()
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try flushSafely() catch { case _: Throwable => }
      scheduler.shutdownNow()
      try connection.close() catch { case _: Throwable => }
      try client.shutdown() catch { case _: Throwable => }
      logger.info("RedisActiveSeriesSink closed")
    }
  }
}

object RedisActiveSeriesSink {
  sealed trait OpKind
  case object Add extends OpKind
  case object Rem extends OpKind

  final case class Op(kind: OpKind, key: Array[Byte], member: Array[Byte])
}
