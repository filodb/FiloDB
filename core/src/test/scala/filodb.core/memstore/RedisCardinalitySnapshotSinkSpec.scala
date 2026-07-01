package filodb.core.memstore

import io.lettuce.core.RedisClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityValue}

class RedisCardinalitySnapshotSinkSpec
    extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private lazy val host = Option(System.getProperty("filodb.test.redis.host"))
  private lazy val port = Option(System.getProperty("filodb.test.redis.port"))
                            .map(_.toInt)
  private lazy val enabled = host.isDefined && port.isDefined

  private lazy val sink: RedisCardinalitySnapshotSink =
    new RedisCardinalitySnapshotSink(host.get, port.get, commandTimeoutMs = 500L)

  override def afterAll(): Unit = {
    if (enabled) sink.close()
    RedisSnapshotClient.releaseAllForTest()
  }

  private def value(active: Long): CardinalityValue =
    CardinalityValue(active, active, active, 0, 1000000L)

  private def rec(prefix: Seq[String], active: Long): CardinalityRecord =
    CardinalityRecord(shard = 0, prefix = prefix, value = value(active))

  private def flushDb(): Unit = {
    val c = RedisClient.create(s"redis://${host.get}:${port.get}")
    try {
      val conn = c.connect(); try conn.sync().flushdb() finally conn.close()
    } finally c.shutdown()
  }

  describe("publish") {
    it("writes ns_total HASH field and per-shard ZSET for each (ws, ns)") {
      assume(enabled, "Redis not configured; set -Dfilodb.test.redis.host and .port")
      flushDb()

      sink.publish(partition = "tsdb0", shardNum = 3,
        ns = Seq(rec(Seq("wsA", "ns1"), 100), rec(Seq("wsA", "ns2"), 200)),
        perMetric = Map(
          Seq("wsA", "ns1") -> Seq(rec(Seq("wsA", "ns1", "cpu"), 60),
                                    rec(Seq("wsA", "ns1", "mem"), 40)),
          Seq("wsA", "ns2") -> Seq(rec(Seq("wsA", "ns2", "cpu"), 200))))

      val c = RedisClient.create(s"redis://${host.get}:${port.get}")
      try {
        val conn = c.connect()
        try {
          val sync = conn.sync()
          sync.hget("ns_total:tsdb0:wsA:ns1", "shard-3") shouldEqual "100"
          sync.hget("ns_total:tsdb0:wsA:ns2", "shard-3") shouldEqual "200"
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "cpu") shouldEqual 60.0
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "mem") shouldEqual 40.0
          sync.zscore("zset:tsdb0:shard-3:wsA:ns2", "cpu") shouldEqual 200.0
        } finally conn.close()
      } finally c.shutdown()
    }

    it("overwrites the previous ZSET rather than merging") {
      assume(enabled)
      flushDb()
      sink.publish("tsdb0", 3,
        ns = Seq(rec(Seq("wsA", "ns1"), 100)),
        perMetric = Map(Seq("wsA", "ns1") ->
          Seq(rec(Seq("wsA", "ns1", "cpu"), 60), rec(Seq("wsA", "ns1", "mem"), 40))))
      sink.publish("tsdb0", 3,
        ns = Seq(rec(Seq("wsA", "ns1"), 100)),
        perMetric = Map(Seq("wsA", "ns1") ->
          Seq(rec(Seq("wsA", "ns1", "cpu"), 30), rec(Seq("wsA", "ns1", "disk"), 5))))

      val c = RedisClient.create(s"redis://${host.get}:${port.get}")
      try {
        val conn = c.connect()
        try {
          val sync = conn.sync()
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "cpu") shouldEqual 30.0
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "mem") shouldBe null
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "disk") shouldEqual 5.0
        } finally conn.close()
      } finally c.shutdown()
    }
  }

  describe("evict") {
    it("removes ns_total field and deletes ZSET for stale (ws, ns)") {
      assume(enabled)
      flushDb()
      sink.publish("tsdb0", 3,
        ns = Seq(rec(Seq("wsA", "ns1"), 100)),
        perMetric = Map(Seq("wsA", "ns1") -> Seq(rec(Seq("wsA", "ns1", "cpu"), 100))))
      sink.evict("tsdb0", 3, Set(("wsA", "ns1")))

      val c = RedisClient.create(s"redis://${host.get}:${port.get}")
      try {
        val conn = c.connect()
        try {
          val sync = conn.sync()
          sync.hget("ns_total:tsdb0:wsA:ns1", "shard-3") shouldBe null
          sync.exists("zset:tsdb0:shard-3:wsA:ns1") shouldEqual 0L
        } finally conn.close()
      } finally c.shutdown()
    }
  }

  describe("colon safety") {
    it("throws IllegalArgumentException when ws or ns contains a colon") {
      assume(enabled)
      an[IllegalArgumentException] should be thrownBy
        sink.publish("tsdb0", 3,
          ns = Seq(rec(Seq("ws:bad", "ns1"), 100)),
          perMetric = Map(Seq("ws:bad", "ns1") ->
            Seq(rec(Seq("ws:bad", "ns1", "cpu"), 100))))
    }
  }
}
