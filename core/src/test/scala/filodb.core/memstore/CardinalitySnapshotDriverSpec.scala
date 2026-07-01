package filodb.core.memstore

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.DatasetRef
import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityStore,
  CardinalityTracker, CardinalityValue}

class CardinalitySnapshotDriverSpec extends AnyFunSpec with Matchers {

  private def value(active: Long, children: Int = 0): CardinalityValue =
    CardinalityValue(tsCount = active, activeTsCount = active,
      billableTsCount = active, childrenCount = children, childrenQuota = 1000000L)

  /** In-memory CardinalityStore backing test tracker(s). */
  private def newTracker(shardNum: Int): CardinalityTracker = {
    val store = new InMemoryCardinalityStore
    new CardinalityTracker(
      ref = DatasetRef("test"),
      shard = shardNum,
      shardKeyLen = 3,
      defaultChildrenQuota = Seq(1000000L, 1000000L, 1000000L, 1000000L),
      store = store)
  }

  describe("snapshotOnce") {
    it("publishes ns records and perMetric groups for every (ws, ns) on the tracker") {
      val tracker = newTracker(shardNum = 3)
      tracker.modifyCount(Seq("wsA", "ns1", "cpu"), 1, 1, 1)
      tracker.modifyCount(Seq("wsA", "ns1", "mem"), 1, 1, 1)
      tracker.modifyCount(Seq("wsA", "ns2", "cpu"), 1, 1, 1)

      val sink = new RecordingCardinalitySnapshotSink
      val driver = new CardinalitySnapshotDriver(
        partition = "tsdb0", shardNum = 3, cardTracker = tracker, sink = sink)

      driver.snapshotOnce()

      sink.publishCalls should have size 1
      val call = sink.publishCalls.head
      call.partition shouldEqual "tsdb0"
      call.shardNum shouldEqual 3
      call.ns.map(_.prefix).toSet shouldEqual Set(Seq("wsA", "ns1"), Seq("wsA", "ns2"))
      call.perMetric(Seq("wsA", "ns1")).map(_.prefix.last).toSet shouldEqual Set("cpu", "mem")
      call.perMetric(Seq("wsA", "ns2")).map(_.prefix.last).toSet shouldEqual Set("cpu")
    }

    it("evicts (ws, ns) that was in last cycle but not this cycle") {
      val tracker = newTracker(shardNum = 0)
      tracker.modifyCount(Seq("wsA", "ns1", "cpu"), 1, 1, 1)
      tracker.modifyCount(Seq("wsA", "ns2", "cpu"), 1, 1, 1)

      val sink = new RecordingCardinalitySnapshotSink
      val driver = new CardinalitySnapshotDriver(
        partition = "tsdb0", shardNum = 0, cardTracker = tracker, sink = sink)

      driver.snapshotOnce()
      tracker.decrementCount(Seq("wsA", "ns2", "cpu"))
      sink.reset()
      driver.snapshotOnce()

      sink.publishCalls should have size 1
      sink.publishCalls.head.ns.map(_.prefix) shouldEqual Seq(Seq("wsA", "ns1"))
      sink.evictCalls should have size 1
      sink.evictCalls.head.stale shouldEqual Set(("wsA", "ns2"))
    }

    it("issues no evict on the first cycle") {
      val tracker = newTracker(shardNum = 0)
      tracker.modifyCount(Seq("wsA", "ns1", "cpu"), 1, 1, 1)

      val sink = new RecordingCardinalitySnapshotSink
      val driver = new CardinalitySnapshotDriver(
        partition = "tsdb0", shardNum = 0, cardTracker = tracker, sink = sink)

      driver.snapshotOnce()

      sink.evictCalls shouldBe empty
    }

    it("filters out zombie records (tsCount = 0) from both scans") {
      val store = new InMemoryCardinalityStore
      // Seed a zombie: tsCount = 0 but activeTsCount = 5. This is exactly the
      // state CardinalityTracker.decrementCount leaves behind after removing the
      // last series in a namespace, because it only decrements tsCount/childrenCount.
      val zombieNs = CardinalityRecord(shard = 0, prefix = Seq("wsA", "zombieNs"),
        value = CardinalityValue(tsCount = 0, activeTsCount = 5,
          billableTsCount = 5, childrenCount = 0, childrenQuota = 1000000L))
      val zombieMetric = CardinalityRecord(shard = 0, prefix = Seq("wsA", "zombieNs", "cpu"),
        value = CardinalityValue(tsCount = 0, activeTsCount = 5,
          billableTsCount = 5, childrenCount = 0, childrenQuota = 1000000L))
      store.store(zombieNs)
      store.store(zombieMetric)

      val tracker = new CardinalityTracker(
        ref = DatasetRef("test"),
        shard = 0,
        shardKeyLen = 3,
        defaultChildrenQuota = Seq(1000000L, 1000000L, 1000000L, 1000000L),
        store = store)

      val sink = new RecordingCardinalitySnapshotSink
      val driver = new CardinalitySnapshotDriver(
        partition = "tsdb0", shardNum = 0, cardTracker = tracker, sink = sink)

      driver.snapshotOnce()

      sink.publishCalls should have size 1
      sink.publishCalls.head.ns shouldBe empty
      sink.publishCalls.head.perMetric shouldBe empty
    }

    it("swallows sink exceptions and does not update lastCycleTouched") {
      val tracker = newTracker(shardNum = 0)
      tracker.modifyCount(Seq("wsA", "ns1", "cpu"), 1, 1, 1)

      val throwingSink = new CardinalitySnapshotSink {
        override def publish(p: String, s: Int, ns: Seq[CardinalityRecord],
                             perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit =
          throw new RuntimeException("simulated")
        override def evict(p: String, s: Int, stale: Set[(String, String)]): Unit = ()
        override def close(): Unit = ()
      }
      val driver = new CardinalitySnapshotDriver(
        partition = "tsdb0", shardNum = 0, cardTracker = tracker, sink = throwingSink)

      noException should be thrownBy driver.snapshotOnce()
    }
  }
}

class InMemoryCardinalityStore extends CardinalityStore {
  import filodb.core.memstore.ratelimit.CardinalityRecord
  private val m = scala.collection.mutable.Map.empty[Seq[String], CardinalityRecord]
  override def store(rec: CardinalityRecord): Unit = m.put(rec.prefix, rec)
  override def getOrZero(prefix: Seq[String], zero: CardinalityRecord): CardinalityRecord =
    m.getOrElse(prefix, zero)
  override def remove(prefix: Seq[String]): Unit = { m.remove(prefix); () }
  override def scanChildren(prefix: Seq[String], depth: Int): Seq[CardinalityRecord] =
    m.values.filter(r => r.prefix.length == depth && r.prefix.startsWith(prefix)).toSeq
  override def close(): Unit = ()
}
