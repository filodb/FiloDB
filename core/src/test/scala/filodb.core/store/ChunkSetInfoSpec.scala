package filodb.core.store

import org.scalatest.{FunSpec, Matchers}

import filodb.core._

class ChunkSetInfoSpec extends FunSpec with Matchers {
  import NamesTestData._

  val info1 = ChunkSetMeta(13, 5000, firstKey, lastKey)

  it("should serialize and deserialize ChunkSetInfo and no skips") {
    val infoRead1 = ChunkSetInfo.fromBytes(ChunkSetInfo.toBytes(info1))
    infoRead1 shouldEqual info1
  }

  it("should find intersection range when one of timestamps match") {
    val ts = System.currentTimeMillis
    val info1 = ChunkSetMeta(1, 1, ts - 10000, ts + 30000)

    // left edge touches
    info1.intersection(ts - 10001, ts - 10000) should equal (Some((ts - 10000, ts - 10000)))

    // right edge touches inside
    info1.intersection(ts + 29995, ts + 30000) should equal (Some((ts + 29995, ts + 30000)))
    info1.intersection(ts + 30000, ts + 30001) should equal (Some((ts + 30000, ts + 30000)))
  }

  it("should not find intersection if key1 is greater than key2") {
    val info1 = ChunkSetMeta(1, 1, 1000, 2000)
    info1.intersection(1999, 1888) should equal (None)
  }

  it("should find intersection range of nonmatching timestamps") {
    val ts = System.currentTimeMillis
    val info1 = ChunkSetMeta(1, 1, ts - 10000, ts + 30000)

    // wholly inside
    info1.intersection(ts, ts + 15000) should equal (Some((ts, ts + 15000)))

    // partially inside to left
    info1.intersection(ts - 15000, ts - 9999) should equal (Some((ts - 10000, ts - 9999)))

    // partially inside to right
    info1.intersection(ts, ts + 30001) should equal (Some((ts, ts + 30000)))

    // wholly outside
    info1.intersection(ts + 30001, ts + 40000) should equal (None)
  }
}