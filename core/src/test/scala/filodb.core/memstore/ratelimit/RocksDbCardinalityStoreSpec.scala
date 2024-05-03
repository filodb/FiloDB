package filodb.core.memstore.ratelimit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.core.memstore.ratelimit.CardinalityStore._
import filodb.core.MetricsTestData

class RocksDbCardinalityStoreSpec extends AnyFunSpec with Matchers {
  it ("should correctly return overflow record") {
    val dset = MetricsTestData.timeseriesDatasetMultipleShardKeys
    val shard = 0
    val numOverflow = 123

    val db = new RocksDbCardinalityStore(dset.ref, shard)

    (0 until MAX_RESULT_SIZE + numOverflow).foreach{ i =>
      val prefix = Seq("ws", "ns", s"metric-$i")
      db.store(CardinalityRecord(shard, prefix, CardinalityValue(1, 1, 1, 1)))
    }

    Seq(Nil, Seq("ws"), Seq("ws", "ns")).foreach{ prefix =>
      val res = db.scanChildren(prefix, 3)
      res.size shouldEqual MAX_RESULT_SIZE + 1  // one extra for the overflow CardinalityRecord
      res.contains(CardinalityRecord(shard, OVERFLOW_PREFIX,
        CardinalityValue(numOverflow, numOverflow, numOverflow, numOverflow))) shouldEqual true
    }
  }

  it("test rocksdb store can correctly store and retrieve a long value") {
    val dset = MetricsTestData.timeseriesDatasetMultipleShardKeys
    val shard = 0
    val db = new RocksDbCardinalityStore(dset.ref, shard)

    (0 until 1000).foreach{ i =>
      val prefix = Seq("ws", "ns", s"metric-$i")
      // storing 3 billion ( this value is greater than ~2.15 billion max limit of int32)
      val cardnalityVal = CardinalityValue(3000000000L+i, 3000000000L+i, 3000000000L+i, 3000000000L+i)
      db.store(CardinalityRecord(shard, prefix, cardnalityVal))
    }

    Seq(Nil, Seq("ws"), Seq("ws", "ns")).foreach{ prefix =>
      val res = db.scanChildren(prefix, 3)
      res.size shouldEqual 1000
      res.foreach{ record =>
        record.value.tsCount should be >= (3000000000L)
        record.value.activeTsCount should be >= (3000000000L)
        record.value.childrenCount should be >= (3000000000L)
        record.value.childrenQuota should be >= (3000000000L)
      }
    }
  }
}
