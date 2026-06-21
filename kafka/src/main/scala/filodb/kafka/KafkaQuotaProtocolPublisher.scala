package filodb.kafka

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import filodb.core.DatasetRef
import filodb.core.memstore.ratelimit.{CloseableQuotaProtocol, QuotaBreachEvent}
import filodb.core.metrics.FilodbMetrics

/**
 * Abstraction over the Kafka producer so the protocol logic can be unit-tested without an
 * embedded broker. The implementation is fire-and-forget; the protocol does not block on send.
 */
trait QuotaBreachSender extends AutoCloseable {
  def send(topic: String, key: String, payload: String): Unit
}

class KafkaProducerQuotaBreachSender(
    producer: org.apache.kafka.clients.producer.Producer[String, String])
  extends QuotaBreachSender with StrictLogging {

  private val errors = FilodbMetrics.counter("filodb_quota_protocol_publish_errors_total")
  private val sent = FilodbMetrics.counter("filodb_quota_protocol_publish_sent_total")

  override def send(topic: String, key: String, payload: String): Unit = {
    val rec = new ProducerRecord[String, String](topic, key, payload)
    producer.send(rec, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          errors.increment()
          logger.warn(s"Failed to publish quota breach event to $topic, key=$key", exception)
        } else {
          sent.increment()
        }
      }
    })
  }

  override def close(): Unit = {
    try producer.close()
    catch { case t: Throwable => logger.warn("Failed to close quota-breach Kafka producer", t) }
  }
}

/**
 * Reflection-loaded `QuotaExceededProtocol` implementation that publishes a JSON
 * `QuotaBreachEvent` to a Kafka control topic on every breach surfaced by `CardinalityTracker`.
 *
 * The publisher dedupes per `(dataset, workspace, namespace)` over a configurable window so a
 * sustained breach (which fires `quotaExceeded` once per rejected sample) doesn't flood the
 * topic. The producer is created lazily so reflection-loading doesn't fail when Kafka is
 * unreachable — the first breach pays the connect cost.
 *
 * Wire-format and schema versioning live on `QuotaBreachEvent`. The Kafka record key is
 * `dataset|workspace|namespace`, providing partition affinity per tenant for downstream
 * consumers.
 *
 * Public ctor takes the `filodb.quota-protocol` sub-config and is invoked via reflection by
 * `QuotaProtocolFactory`; the package-private ctor is for tests with a stub sender.
 */
class KafkaQuotaProtocolPublisher private[kafka] (
    topic: String,
    dedupWindowMillis: Long,
    clusterType: String,
    partition: String,
    senderProvider: () => QuotaBreachSender,
    nowMillis: () => Long
) extends CloseableQuotaProtocol with StrictLogging {

  // Per-(dataset, ws, ns) last-emit timestamp. Bounded growth in practice — one entry per active
  // tenant — so an unbounded ConcurrentHashMap is fine without TTL eviction.
  private val lastEmittedAt =
    new ConcurrentHashMap[(String, String, String), java.lang.Long]()

  // Lazy-init so ctor doesn't block on Kafka. `volatile`-equivalent via @volatile-style read.
  @volatile private var senderRef: QuotaBreachSender = _
  private val senderLock = new Object

  // Kamon-style counters for visibility into dedup behavior; useful when validating the publisher
  // in dev before mosaic-gateway is consuming the topic.
  private val emittedCount = FilodbMetrics.counter("filodb_quota_protocol_emitted_total")
  private val dedupedCount = FilodbMetrics.counter("filodb_quota_protocol_deduped_total")

  // Public reflection ctor.
  def this(config: Config) = this(
    topic = config.getString("topic"),
    dedupWindowMillis = config.getDuration("dedup-window").toMillis,
    clusterType =
      if (config.hasPath("cluster-type")) config.getString("cluster-type") else "",
    partition =
      if (config.hasPath("partition")) config.getString("partition") else "",
    senderProvider = () => KafkaQuotaProtocolPublisher.buildKafkaSender(config),
    nowMillis = () => System.currentTimeMillis())

  override def quotaExceeded(
      ref: DatasetRef, shardNum: Int, shardKeyPrefix: Seq[String], quota: Long): Unit = {
    val event = QuotaBreachEvent(
      ref = ref,
      shardKeyPrefix = shardKeyPrefix,
      shardNum = shardNum,
      quota = quota,
      clusterType = clusterType,
      partition = partition,
      breachedAtMillis = nowMillis())
    if (!shouldEmit(event.dedupKey, event.breachedAtMillis)) {
      dedupedCount.increment()
      return
    }
    val recordKey = s"${event.dataset}|${event.workspace}|${event.namespace}"
    try {
      sender().send(topic, recordKey, event.toJson)
      emittedCount.increment()
    } catch {
      case t: Throwable =>
        // Synchronous send failure (e.g. producer unable to acquire metadata). Async errors are
        // counted via the Kafka producer callback, so don't double-count here.
        logger.warn(s"Synchronous failure emitting quota breach for $recordKey", t)
    }
  }

  /** CAS-style dedup: only one thread per key per window wins. */
  private def shouldEmit(key: (String, String, String), now: Long): Boolean = {
    val prev = lastEmittedAt.get(key)
    if (prev == null) {
      lastEmittedAt.putIfAbsent(key, java.lang.Long.valueOf(now)) == null
    } else if ((now - prev.longValue()) >= dedupWindowMillis) {
      lastEmittedAt.replace(key, prev, java.lang.Long.valueOf(now))
    } else {
      false
    }
  }

  private def sender(): QuotaBreachSender = {
    val s = senderRef
    if (s != null) s
    else senderLock.synchronized {
      if (senderRef == null) senderRef = senderProvider()
      senderRef
    }
  }

  override def close(): Unit = {
    val s = senderRef
    if (s != null) s.close()
  }
}

object KafkaQuotaProtocolPublisher {

  /** Default sender provider: real Kafka producer driven by `kafka-client-config` sub-config. */
  private[kafka] def buildKafkaSender(config: Config): QuotaBreachSender = {
    val clientConfig = config.getConfig("kafka-client-config")
    val props = new java.util.Properties()
    clientConfig.entrySet().asScala.foreach { e =>
      props.put(e.getKey, e.getValue.unwrapped().toString)
    }
    // Always use String serializers; tenants are short text and payloads are JSON. Override only
    // if explicitly set in kafka-client-config so ops keep escape hatches.
    props.putIfAbsent("key.serializer", classOf[StringSerializer].getName)
    props.putIfAbsent("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    new KafkaProducerQuotaBreachSender(producer)
  }
}
