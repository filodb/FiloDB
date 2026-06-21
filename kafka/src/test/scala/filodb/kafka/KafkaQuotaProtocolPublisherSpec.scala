package filodb.kafka

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.DatasetRef

class KafkaQuotaProtocolPublisherSpec extends AnyFunSpec with Matchers {

  private val ref = DatasetRef("prometheus")

  private class RecordingSender extends QuotaBreachSender {
    case class Sent(topic: String, key: String, payload: String)
    val sent = new ConcurrentLinkedQueue[Sent]()
    var closed = false
    override def send(topic: String, key: String, payload: String): Unit = {
      sent.add(Sent(topic, key, payload)); ()
    }
    override def close(): Unit = closed = true
  }

  private def newPublisher(
      sender: QuotaBreachSender,
      dedupWindowMillis: Long = 60000L,
      now: AtomicLong = new AtomicLong(1000L)): KafkaQuotaProtocolPublisher =
    new KafkaQuotaProtocolPublisher(
      topic = "test-topic",
      dedupWindowMillis = dedupWindowMillis,
      clusterType = "raw",
      partition = "us-east-1",
      senderProvider = () => sender,
      nowMillis = () => now.get())

  describe("KafkaQuotaProtocolPublisher.quotaExceeded") {

    it("emits a JSON event to the configured topic on first breach") {
      val sender = new RecordingSender
      val pub = newPublisher(sender)
      pub.quotaExceeded(ref, shardNum = 3, shardKeyPrefix = Seq("ws", "ns"), quota = 100L)
      val all = sender.sent.asScala.toList
      all.size shouldEqual 1
      all.head.topic shouldEqual "test-topic"
      all.head.key shouldEqual "prometheus|ws|ns"
      all.head.payload should include(""""dataset":"prometheus"""")
      all.head.payload should include(""""quota":100""")
      all.head.payload should include(""""shardNum":3""")
      all.head.payload should include(""""partition":"us-east-1"""")
    }

    it("dedupes repeated breaches for the same (dataset, ws, ns) within the window") {
      val sender = new RecordingSender
      val now = new AtomicLong(1000L)
      val pub = newPublisher(sender, dedupWindowMillis = 60000L, now = now)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns"), 100L)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns"), 100L)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns"), 100L)
      sender.sent.size() shouldEqual 1
    }

    it("re-emits once the dedup window elapses") {
      val sender = new RecordingSender
      val now = new AtomicLong(1000L)
      val pub = newPublisher(sender, dedupWindowMillis = 5000L, now = now)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns"), 100L)
      now.set(1000L + 4000L)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns"), 100L) // still inside window
      now.set(1000L + 5000L)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns"), 100L) // exactly at window edge
      sender.sent.size() shouldEqual 2
    }

    it("does not collapse breaches across distinct (ws, ns) tenants") {
      val sender = new RecordingSender
      val pub = newPublisher(sender)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns1"), 100L)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns2"), 100L)
      pub.quotaExceeded(DatasetRef("other"), 0, Seq("ws", "ns1"), 100L)
      sender.sent.size() shouldEqual 3
    }

    it("dedupes metric-level breaches (length-3 prefix) at the (ws, ns) granularity") {
      val sender = new RecordingSender
      val pub = newPublisher(sender)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns", "metricA"), 10L)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns", "metricB"), 10L)
      // Both share the (prometheus, ws, ns) dedup key, so only the first should be emitted.
      sender.sent.size() shouldEqual 1
      sender.sent.peek().payload should include(""""metricA"""")
    }

    it("close() releases the underlying sender") {
      val sender = new RecordingSender
      val pub = newPublisher(sender)
      pub.quotaExceeded(ref, 0, Seq("ws", "ns"), 100L)
      pub.close()
      sender.closed shouldEqual true
    }
  }
}
