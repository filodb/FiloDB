package filodb.core.memstore

import scala.collection.mutable

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityValue}

class CardinalitySnapshotSinkSpec extends AnyFunSpec with Matchers {

  private def rec(prefix: Seq[String], active: Long): CardinalityRecord =
    CardinalityRecord(shard = 0, prefix = prefix,
      value = CardinalityValue(tsCount = active, activeTsCount = active,
        billableTsCount = active, childrenCount = 0L, childrenQuota = 1000000L))

  describe("NoOpCardinalitySnapshotSink") {
    it("ignores publish/evict/close without throwing") {
      noException should be thrownBy {
        NoOpCardinalitySnapshotSink.publish(
          partition = "tsdb0", shardNum = 3,
          ns = Seq(rec(Seq("ws1", "ns1"), 100)),
          perMetric = Map(Seq("ws1", "ns1") -> Seq(rec(Seq("ws1", "ns1", "cpu"), 100))))
        NoOpCardinalitySnapshotSink.evict("tsdb0", 3, Set(("ws1", "ns1")))
        NoOpCardinalitySnapshotSink.close()
      }
    }
  }

  describe("RecordingCardinalitySnapshotSink (test fixture)") {
    it("captures publish and evict calls in order") {
      val sink = new RecordingCardinalitySnapshotSink

      sink.publish("tsdb0", 3,
        ns = Seq(rec(Seq("wsA", "nsA"), 100)),
        perMetric = Map(Seq("wsA", "nsA") ->
          Seq(rec(Seq("wsA", "nsA", "cpu"), 60), rec(Seq("wsA", "nsA", "mem"), 40))))
      sink.evict("tsdb0", 3, Set(("wsA", "nsOld")))

      sink.publishCalls should have size 1
      sink.publishCalls.head.partition shouldEqual "tsdb0"
      sink.publishCalls.head.shardNum shouldEqual 3
      sink.publishCalls.head.ns.map(_.prefix) shouldEqual Seq(Seq("wsA", "nsA"))
      sink.publishCalls.head.perMetric(Seq("wsA", "nsA")).map(_.prefix.last) should
        contain theSameElementsAs Seq("cpu", "mem")

      sink.evictCalls should have size 1
      sink.evictCalls.head.stale shouldEqual Set(("wsA", "nsOld"))
    }
  }
}

/** Thread-safe in-memory sink used in unit tests. */
class RecordingCardinalitySnapshotSink extends CardinalitySnapshotSink {
  import filodb.core.memstore.RecordingCardinalitySnapshotSink._
  private val lock = new Object
  private val pubBuf = mutable.Buffer.empty[PublishCall]
  private val evictBuf = mutable.Buffer.empty[EvictCall]

  override def publish(partition: String, shardNum: Int,
                       ns: Seq[CardinalityRecord],
                       perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit =
    lock.synchronized { pubBuf += PublishCall(partition, shardNum, ns, perMetric) }

  override def evict(partition: String, shardNum: Int,
                     stale: Set[(String, String)]): Unit =
    lock.synchronized { evictBuf += EvictCall(partition, shardNum, stale) }

  override def close(): Unit = ()

  def publishCalls: Seq[PublishCall] = lock.synchronized(pubBuf.toSeq)
  def evictCalls: Seq[EvictCall] = lock.synchronized(evictBuf.toSeq)
  def reset(): Unit = lock.synchronized { pubBuf.clear(); evictBuf.clear() }
}

object RecordingCardinalitySnapshotSink {
  final case class PublishCall(partition: String, shardNum: Int,
                                ns: Seq[CardinalityRecord],
                                perMetric: Map[Seq[String], Seq[CardinalityRecord]])
  final case class EvictCall(partition: String, shardNum: Int,
                              stale: Set[(String, String)])
}
