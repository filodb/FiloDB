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
      db.store(CardinalityRecord(shard, prefix, 1, 1, 1, 1))
    }

    Seq(Nil, Seq("ws"), Seq("ws", "ns")).foreach{ prefix =>
      val res = db.scanChildren(prefix, 3)
      res.size shouldEqual MAX_RESULT_SIZE + 1  // one extra for the overflow CardinalityRecord
      res.contains(CardinalityRecord(shard, OVERFLOW_PREFIX,
        numOverflow, numOverflow, numOverflow, numOverflow)) shouldEqual true
    }
  }
}
