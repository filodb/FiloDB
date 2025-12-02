package filodb.core.memstore.ratelimit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.core.{DatasetRef, MachineMetricsData}

class CardinalityTrackerSpec extends AnyFunSpec with Matchers {

  val ref = MachineMetricsData.dataset2.ref

  private def newCardStore = {
    new RocksDbCardinalityStore(DatasetRef("test"), 0)
  }

  private def dsCardStore = {
    new RocksDbCardinalityStore(DatasetRef("ds_test"), 1)
  }

  it("should enforce quota when set explicitly for all levels") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(4, 4, 4, 4), newCardStore)
    t.setQuota(Seq("a", "aa", "aaa"), 1) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(0, 0, 0, 1))
    t.setQuota(Seq("a", "aa"), 2) shouldEqual
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(0, 0, 0, 2))
    t.setQuota(Seq("a"), 1) shouldEqual CardinalityRecord(0, Seq("a"), CardinalityValue(0, 0, 0, 1))

    t.modifyCount(Seq("a", "aa", "aaa"), 1, 1) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 1, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 1, 1, 1)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 1, 1, 2)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(1, 1, 1, 1)))

    t.modifyCount(Seq("a", "aa", "aab"), 1, 1) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 2, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 2, 1, 1)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 2, 2, 2)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(1, 1, 1, 4)))

    // aab stopped ingesting
    t.modifyCount(Seq("a", "aa", "aab"), 0, -1) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 1, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 1, 1, 1)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 1, 2, 2)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(1, 0, 1, 4)))

    val ex = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aac"), 1, 0)
    }
    ex.prefix shouldEqual (Seq("a", "aa"))

    // increment should not have been applied for any prefix
    t.scan(Seq("a"), 1)(0) shouldEqual CardinalityRecord(0, Seq("a"), CardinalityValue(2, 1, 1, 1))
    t.scan(Seq("a", "aa"), 2)(0) shouldEqual CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 1, 2, 2))
    t.scan(Seq("a", "aa", "aac"), 3)(0) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aac"), CardinalityValue(0, 0, 0, 4))

    // aab was purged
    t.decrementCount(Seq("a", "aa", "aab")) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 1, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 1, 1, 1)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 1, 2, 2)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(0, 0, 0, 4)))

    t.close()
  }

  it("should invoke quota exceeded protocol when breach occurs") {

    class MyQEP extends QuotaExceededProtocol {
      var breachedPrefix: Seq[String] = Nil
      var breachedQuota = -1L
      def quotaExceeded(ref: DatasetRef, shard: Int, shardKeyPrefix: Seq[String], quota: Long): Unit = {
        breachedPrefix = shardKeyPrefix
        breachedQuota = quota
      }
    }

    val qp = new MyQEP
    val t = new CardinalityTracker(ref, 0, 3, Seq(1, 1, 1, 1), newCardStore, qp)
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    @scala.annotation.unused val ex = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    }
    qp.breachedQuota shouldEqual 1
    qp.breachedPrefix shouldEqual Seq("a", "aa", "aaa")
    t.close()
  }

  it("should enforce quota when not set for any level") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(4, 4, 4, 4), newCardStore)
    t.modifyCount(Seq("a", "ab", "aba"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 0, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 0, 1, 4)),
        CardinalityRecord(0, Seq("a", "ab"), CardinalityValue(1, 0, 1, 4)),
        CardinalityRecord(0, Seq("a", "ab", "aba"), CardinalityValue(1, 0, 1, 4)))
    t.close()
  }

  it("should be able to enforce for top 2 levels always, and enforce for 3rd level only in some cases") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(20, 20, 20, 20), newCardStore)
    t.setQuota(Seq("a"), 10) shouldEqual CardinalityRecord(0, Seq("a"), CardinalityValue(0, 0, 0, 10))
    t.setQuota(Seq("a", "aa"), 10) shouldEqual
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(0, 0, 0, 10))
    // enforce for 3rd level only for aaa
    t.setQuota(Seq("a", "aa", "aaa"), 2) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(0, 0, 0, 2))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(1, 0, 1, 2)))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(2, 0, 2, 2)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(3, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(3, 0, 2, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(1, 0, 1, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(4, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(4, 0, 2, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(2, 0, 2, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(5, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(5, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(5, 0, 2, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(3, 0, 3, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(6, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(6, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(6, 0, 2, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(4, 0, 4, 20)))

    val ex = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    }
     ex.prefix shouldEqual Seq("a", "aa", "aaa")
    t.close()

  }

  it("should be able to increase and decrease quota after it has been set before") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(20, 20, 20, 20), newCardStore)
    t.setQuota(Seq("a"), 10) shouldEqual CardinalityRecord(0, Seq("a"), CardinalityValue(0, 0, 0, 10))
    t.setQuota(Seq("a", "aa"), 10) shouldEqual
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(0, 0, 0, 10))
    // enforce for 3rd level only for aaa
    t.setQuota(Seq("a", "aa", "aaa"), 2) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(0, 0, 0, 2))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(1, 0, 1, 2)))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(2, 0, 2, 2)))

    val ex = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    }
    ex.prefix shouldEqual (Seq("a", "aa", "aaa"))

    // increase quota
    t.setQuota(Seq("a", "aa", "aaa"), 5) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(2, 0, 2, 5))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(3, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(3, 0, 3, 5)))

    // decrease quota
    t.setQuota(Seq("a", "aa", "aaa"), 4) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(3, 0, 3, 4))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(4, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(4, 0, 4, 4)))
    val ex2 = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    }
    ex2.prefix shouldEqual Seq("a", "aa", "aaa")
    t.close()
  }

  it("should be able to decrease quota if count is higher than new quota") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(20, 20, 20, 20), newCardStore)
    t.setQuota(Seq("a"), 10) shouldEqual
      CardinalityRecord(0, Seq("a"), CardinalityValue(0, 0, 0, 10))
    t.setQuota(Seq("a", "aa"), 10) shouldEqual
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(0, 0, 0, 10))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(1, 0, 1, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(2, 0, 2, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(3, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(3, 0, 3, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(4, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(4, 0, 4, 20)))

    t.scan(Seq("a", "aa", "aab"), 3)(0) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(4, 0, 4, 20))

    t.setQuota(Seq("a", "aa", "aab"), 3)
    t.scan(Seq("a", "aa", "aab"), 3)(0) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(4, 0, 4, 3))
    val ex2 = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aab"), 1, 0)
    }
    ex2.prefix shouldEqual Seq("a", "aa", "aab")
    t.close()
  }

  it ("should be able to increment and decrement counters fast and correctly") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(50000, 50000, 50000, 50000), newCardStore)

    (1 to 30000).foreach(_ => t.modifyCount(Seq("a", "b", "c"), 1, 1))
    t.scan(Seq("a", "b", "c"), 3)(0) shouldEqual
      CardinalityRecord(0, Seq("a", "b", "c"), CardinalityValue(30000, 30000, 30000, 50000))
    (1 to 30000).foreach(_ => t.modifyCount(Seq("a", "b", "c"), 0, -1))
    t.scan(Seq("a", "b", "c"), 3)(0) shouldEqual
      CardinalityRecord(0, Seq("a", "b", "c"), CardinalityValue(30000, 0, 30000, 50000))
    (1 to 30000).foreach(_ => t.decrementCount(Seq("a", "b", "c")))
    t.scan(Seq("a", "b", "c"), 3)(0) shouldEqual
      CardinalityRecord(0, Seq("a", "b", "c"), CardinalityValue(0, 0, 0, 50000))
  }

  it ("should be able to do scan") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(100, 100, 100, 100), newCardStore)
    (1 to 10).foreach(_ => t.modifyCount(Seq("a", "ac", "aca"), 1, 0))
    (1 to 20).foreach(_ => t.modifyCount(Seq("a", "ac", "acb"), 1, 0))
    (1 to 11).foreach(_ => t.modifyCount(Seq("a", "ac", "acc"), 1, 0))
    (1 to 6).foreach(_ => t.modifyCount(Seq("a", "ac", "acd"), 1, 0))
    (1 to 1).foreach(_ => t.modifyCount(Seq("a", "ac", "ace"), 1, 0))
    (1 to 9).foreach(_ => t.modifyCount(Seq("a", "ac", "acf"), 1, 0))
    (1 to 15).foreach(_ => t.modifyCount(Seq("a", "ac", "acg"), 1, 0))

    (1 to 15).foreach(_ => t.modifyCount(Seq("b", "bc", "bcg"), 1, 0))
    (1 to 9).foreach(_ => t.modifyCount(Seq("b", "bc", "bch"), 1, 0))
    (1 to 9).foreach(_ => t.modifyCount(Seq("b", "bd", "bdh"), 1, 0))

    (1 to 3).foreach(_ => t.modifyCount(Seq("c", "cc", "ccg"), 1, 0))
    (1 to 2).foreach(_ => t.modifyCount(Seq("c", "cc", "cch"), 1, 0))

    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0)
    t.modifyCount(Seq("a", "aa", "aac"), 1, 0)
    t.modifyCount(Seq("a", "aa", "aad"), 1, 0)
    t.modifyCount(Seq("b", "ba", "baa"), 1, 0)
    t.modifyCount(Seq("b", "bb", "bba"), 1, 0)
    t.modifyCount(Seq("a", "ab", "aba"), 1, 0)
    t.modifyCount(Seq("a", "ab", "abb"), 1, 0)
    t.modifyCount(Seq("a", "ab", "abc"), 1, 0)
    t.modifyCount(Seq("a", "ab", "abd"), 1, 0)
    t.modifyCount(Seq("a", "ab", "abe"), 1, 0)

    t.scan(Seq("a", "ac"), 3) should contain theSameElementsAs Seq(
      CardinalityRecord(0, Seq("a", "ac", "aca"), CardinalityValue(10, 0, 10, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acb"), CardinalityValue(20, 0, 20, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acc"), CardinalityValue(11, 0, 11, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acd"), CardinalityValue(6, 0, 6, 100)),
      CardinalityRecord(0, Seq("a", "ac", "ace"), CardinalityValue(1, 0, 1, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acf"), CardinalityValue(9, 0, 9, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acg"), CardinalityValue(15, 0, 15, 100)),
    )

    t.scan(Seq("a"), 2) should contain theSameElementsAs Seq(
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(4, 0, 4, 100)),
      CardinalityRecord(0, Seq("a", "ab"), CardinalityValue(5, 0, 5, 100)),
      CardinalityRecord(0, Seq("a", "ac"), CardinalityValue(72, 0, 7, 100))
    )

    t.scan(Nil, 1) should contain theSameElementsAs Seq(
      CardinalityRecord(0, Seq("c"), CardinalityValue(5, 0, 1, 100)),
      CardinalityRecord(0, Seq("a"), CardinalityValue(81, 0, 3, 100)),
      CardinalityRecord(0, Seq("b"), CardinalityValue(35, 0, 4, 100))
    )

    t.close()
  }

  it ("modifyCount with aggregation should update count correctly") {
    val t = new CardinalityTracker(ref, 1, 3, Seq(50000, 50000, 50000, 50000), dsCardStore,
      flushCount = Some(5000))
    (1 to 1000).foreach(_ => t.modifyCount(Seq("a", "b", "c"), 1, 1))
    (1 to 1000).foreach(_ => t.modifyCount(Seq("a", "b", "d"), 1, 1))
    (1 to 1000).foreach(_ => t.modifyCount(Seq("a", "c", "d"), 1, 1))
    (1 to 1000).foreach(_ => t.modifyCount(Seq("b", "c", "d"), 1, 1))

    // check no flush until flush is called
    // it should return the default value returned by cardinalityTracker.getCardinality
    t.scan(Seq("a", "b", "c"), 3)(0) shouldEqual
      CardinalityRecord(1, Seq("a", "b", "c"), CardinalityValue(0, 0, 0, 50000))

    t.flushCardinalityCount()

    // validate expected records after flush is called
    t.scan(Seq("a", "b", "c"), 3)(0) shouldEqual
      CardinalityRecord(1, Seq("a", "b", "c"), CardinalityValue(1000, 1000, 0, 50000))
    t.scan(Seq("a", "b"), 2)(0) shouldEqual
      CardinalityRecord(1, Seq("a", "b"), CardinalityValue(2000, 2000, 2000, 50000))
    t.scan(Seq("a", "b", "d"), 3)(0) shouldEqual
      CardinalityRecord(1, Seq("a", "b", "d"), CardinalityValue(1000, 1000, 0, 50000))
    t.scan(Seq("b", "c"), 2)(0) shouldEqual
      CardinalityRecord(1, Seq("b", "c"), CardinalityValue(1000, 1000, 1000, 50000))
    t.scan(Seq("b", "c", "d"), 3)(0) shouldEqual
      CardinalityRecord(1, Seq("b", "c", "d"), CardinalityValue(1000, 1000, 0, 50000))
    t.scan(Seq("a"), 1)(0) shouldEqual
      CardinalityRecord(1, Seq("a"), CardinalityValue(3000, 3000, 3000, 50000))
    t.scan(Seq("b"), 1)(0) shouldEqual
      CardinalityRecord(1, Seq("b"), CardinalityValue(1000, 1000, 1000, 50000))
    t.scan(Seq(), 0)(0) shouldEqual
      CardinalityRecord(1, Seq(), CardinalityValue(4000, 4000, 4000, 50000))

    t.close()
  }

  it ("modifyCount with aggregation should throw QuotaReachedException when childrenCount breached") {
    val t = new CardinalityTracker(ref, 1, 3, Seq(2, 2, 2, 2), dsCardStore, flushCount = Some(5000))
    t.modifyCount(Seq("a", "b", "c"), 1, 1)
    t.modifyCount(Seq("a", "b", "d"), 1, 1)
    val ex = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "b", "e"), 1, 1)
    }
    ex.prefix shouldEqual Seq("a")
    t.close()
  }

  it("modifyCount with aggregation should flush counts to RocksDB if threshold reached") {
    val t = new CardinalityTracker(ref, 1, 3, Seq(5, 5, 5, 5),
      dsCardStore, flushCount = Some(5))
    t.modifyCount(Seq("a", "b", "c"), 1, 1)
    t.modifyCount(Seq("a", "b", "d"), 1, 1)

    // check not flushed until now since threshold not crossed
    t.getCardinalityCountMapDSClone().size shouldEqual 5

    // adding another child which triggers flush based on flushCount parameter
    t.modifyCount(Seq("a", "b", "e"), 1, 1)
    t.flushCardinalityCount()

    // check if map is cleared after flush
    t.getCardinalityCountMapDSClone().size shouldEqual 0

    // check data integrity in RocksDB
    t.scan(Seq(), 0)(0) shouldEqual
      CardinalityRecord(1, Seq(), CardinalityValue(3, 3, 3, 5))
    t.scan(Seq("a"), 1)(0) shouldEqual
      CardinalityRecord(1, Seq("a"), CardinalityValue(3, 3, 3, 5))
    t.scan(Seq("a", "b"), 2)(0) shouldEqual
      CardinalityRecord(1, Seq("a", "b"), CardinalityValue(3, 3, 3, 5))
    t.scan(Seq("a", "b", "c"), 3)(0) shouldEqual
      CardinalityRecord(1, Seq("a", "b", "c"), CardinalityValue(1, 1, 0, 5))

    t.close()
  }

  it("modifyCount with aggregation and without aggregation should give same tsCount and activeCount values") {
    val s = new CardinalityTracker(ref, 0, 3, Seq(5000, 5000, 5000, 5000), newCardStore)
    val t = new CardinalityTracker(ref, 1, 3, Seq(5000, 5000, 5000, 5000), dsCardStore,
      flushCount = Some(5000))

    // update raw card count
    (1 to 1000).foreach(_ => s.modifyCount(Seq("a", "b", "c"), 1, 1))
    (1 to 1000).foreach(_ => s.modifyCount(Seq("a", "b", "d"), 1, 1))
    (1 to 1000).foreach(_ => s.modifyCount(Seq("a", "c", "d"), 1, 1))
    (1 to 1000).foreach(_ => s.modifyCount(Seq("b", "c", "d"), 1, 1))

    // update ds card count
    (1 to 1000).foreach(_ => t.modifyCount(Seq("a", "b", "c"), 1, 1))
    (1 to 1000).foreach(_ => t.modifyCount(Seq("a", "b", "d"), 1, 1))
    (1 to 1000).foreach(_ => t.modifyCount(Seq("a", "c", "d"), 1, 1))
    (1 to 1000).foreach(_ => t.modifyCount(Seq("b", "c", "d"), 1, 1))

    t.flushCardinalityCount()

    t.scan(Seq("a", "b", "c"), 3)(0).value.tsCount shouldEqual
      s.scan(Seq("a", "b", "c"), 3)(0).value.tsCount

    t.scan(Seq("a", "b"), 2)(0).value.tsCount shouldEqual
      s.scan(Seq("a", "b"), 2)(0).value.tsCount

    t.scan(Seq("a", "b", "d"), 3)(0).value.tsCount shouldEqual
      s.scan(Seq("a", "b", "d"), 3)(0).value.tsCount

    t.scan(Seq("b", "c"), 2)(0).value.tsCount shouldEqual
      s.scan(Seq("b", "c"), 2)(0).value.tsCount

    t.scan(Seq("b", "c", "d"), 3)(0).value.tsCount shouldEqual
      s.scan(Seq("b", "c", "d"), 3)(0).value.tsCount

    t.scan(Seq("a"), 1)(0).value.tsCount shouldEqual
      s.scan(Seq("a"), 1)(0).value.tsCount

    t.scan(Seq("b"), 1)(0).value.tsCount shouldEqual
      s.scan(Seq("b"), 1)(0).value.tsCount

    s.close()
    t.close()
  }
}