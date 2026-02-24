package filodb.core.memstore.aggregation

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BucketManagerSpec extends AnyFunSpec with Matchers {

  describe("BucketManager basic operations") {
    it("should create and retrieve buckets") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(3, templates)

      val bucket1 = manager.getOrCreateBucket(30000L)
      val bucket2 = manager.getOrCreateBucket(60000L)

      manager.size shouldEqual 2
      manager.getBucket(30000L) shouldEqual Some(bucket1)
      manager.getBucket(60000L) shouldEqual Some(bucket2)
      manager.getBucket(90000L) shouldEqual None
    }

    it("should return same bucket for repeated gets") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(3, templates)

      val bucket1 = manager.getOrCreateBucket(30000L)
      val bucket2 = manager.getOrCreateBucket(30000L)

      bucket1 shouldBe theSameInstanceAs(bucket2)
      manager.size shouldEqual 1
    }

    it("should check bucket existence") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(3, templates)

      manager.hasBucket(30000L) shouldEqual false

      manager.getOrCreateBucket(30000L)
      manager.hasBucket(30000L) shouldEqual true
    }

    it("should return all buckets sorted by timestamp") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(3, templates)

      manager.getOrCreateBucket(60000L)
      manager.getOrCreateBucket(30000L)
      manager.getOrCreateBucket(90000L)

      val buckets = manager.allBuckets
      buckets.length shouldEqual 3
      buckets.map(_._1) shouldEqual Seq(30000L, 60000L, 90000L)
    }

    it("should clear all buckets") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(3, templates)

      manager.getOrCreateBucket(30000L)
      manager.getOrCreateBucket(60000L)
      manager.size shouldEqual 2

      manager.clear()
      manager.size shouldEqual 0
      manager.isEmpty shouldEqual true
    }
  }

  describe("BucketManager eviction") {
    it("should evict oldest bucket when max exceeded") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(maxBuckets = 3, templates)

      manager.getOrCreateBucket(30000L)
      manager.getOrCreateBucket(60000L)
      manager.getOrCreateBucket(90000L)
      manager.size shouldEqual 3

      // Adding 4th bucket should evict oldest (30000)
      manager.getOrCreateBucket(120000L)
      manager.size shouldEqual 3
      manager.hasBucket(30000L) shouldEqual false
      manager.hasBucket(60000L) shouldEqual true
      manager.hasBucket(90000L) shouldEqual true
      manager.hasBucket(120000L) shouldEqual true
    }

    it("should evict buckets older than threshold") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(5, templates)

      val bucket30 = manager.getOrCreateBucket(30000L)
      val bucket60 = manager.getOrCreateBucket(60000L)
      val bucket90 = manager.getOrCreateBucket(90000L)
      val bucket120 = manager.getOrCreateBucket(120000L)

      bucket30.aggregate(Array(10.0))
      bucket60.aggregate(Array(20.0))

      // Evict buckets older than 90000
      val evicted = manager.evictBucketsOlderThan(90000L)

      evicted.length shouldEqual 2
      evicted.map(_._1) shouldEqual Seq(30000L, 60000L)
      evicted(0)._2.emit()(0) shouldEqual 10.0
      evicted(1)._2.emit()(0) shouldEqual 20.0

      manager.size shouldEqual 2
      manager.hasBucket(30000L) shouldEqual false
      manager.hasBucket(60000L) shouldEqual false
      manager.hasBucket(90000L) shouldEqual true
      manager.hasBucket(120000L) shouldEqual true
    }

    it("should evict buckets up to and including threshold") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(5, templates)

      manager.getOrCreateBucket(30000L)
      manager.getOrCreateBucket(60000L)
      manager.getOrCreateBucket(90000L)
      manager.getOrCreateBucket(120000L)

      val evicted = manager.evictBucketsUpTo(60000L)

      evicted.length shouldEqual 2
      evicted.map(_._1) shouldEqual Seq(30000L, 60000L)

      manager.size shouldEqual 2
      manager.hasBucket(90000L) shouldEqual true
      manager.hasBucket(120000L) shouldEqual true
    }

    it("should return empty sequence when no buckets to evict") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(3, templates)

      manager.getOrCreateBucket(90000L)
      manager.getOrCreateBucket(120000L)

      val evicted = manager.evictBucketsOlderThan(30000L)
      evicted shouldBe empty

      manager.size shouldEqual 2
    }
  }

  describe("BucketManager statistics") {
    it("should track oldest and newest bucket timestamps") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(5, templates)

      manager.oldestBucketTimestamp shouldEqual None
      manager.newestBucketTimestamp shouldEqual None

      manager.getOrCreateBucket(60000L)
      manager.getOrCreateBucket(30000L)
      manager.getOrCreateBucket(90000L)

      manager.oldestBucketTimestamp shouldEqual Some(30000L)
      manager.newestBucketTimestamp shouldEqual Some(90000L)
    }

    it("should provide bucket statistics") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(3, templates)

      val bucket1 = manager.getOrCreateBucket(30000L)
      val bucket2 = manager.getOrCreateBucket(60000L)

      bucket1.aggregate(Array(10.0))
      bucket1.aggregate(Array(20.0))
      bucket2.aggregate(Array(30.0))

      val stats = manager.stats

      stats.activeBucketCount shouldEqual 2
      stats.maxBuckets shouldEqual 3
      stats.oldestBucket shouldEqual Some(30000L)
      stats.newestBucket shouldEqual Some(60000L)
      stats.totalSamples shouldEqual 3
      stats.utilizationPercent shouldEqual (2.0 / 3.0 * 100.0)
    }

    it("should handle empty manager stats") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(3, templates)

      val stats = manager.stats

      stats.activeBucketCount shouldEqual 0
      stats.totalSamples shouldEqual 0
      stats.oldestBucket shouldEqual None
      stats.newestBucket shouldEqual None
    }
  }

  describe("BucketManager with multiple aggregators") {
    it("should handle multiple aggregators per bucket") {
      val templates = Array(
        Aggregator.create(AggregationType.Sum),
        Aggregator.create(AggregationType.Avg),
        Aggregator.create(AggregationType.Count)
      )
      val manager = new BucketManager(3, templates)

      val bucket = manager.getOrCreateBucket(30000L)

      bucket.aggregate(Array(10.0, 10.0, 1.0))
      bucket.aggregate(Array(20.0, 20.0, 1.0))
      bucket.aggregate(Array(30.0, 30.0, 1.0))

      val results = bucket.emit()
      results(0) shouldEqual 60.0   // sum
      results(1) shouldEqual 20.0   // avg
      results(2) shouldEqual 3L     // count
    }
  }

  describe("BucketManager memory bounds") {
    it("should respect max bucket limit") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val maxBuckets = 10
      val manager = new BucketManager(maxBuckets, templates)

      // Add more buckets than max
      (0 until 20).foreach { i =>
        manager.getOrCreateBucket(i * 30000L)
      }

      // Should never exceed max
      manager.size should be <= maxBuckets
    }

    it("should always maintain most recent buckets") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(maxBuckets = 3, templates)

      // Add buckets in sequence
      manager.getOrCreateBucket(30000L)
      manager.getOrCreateBucket(60000L)
      manager.getOrCreateBucket(90000L)
      manager.getOrCreateBucket(120000L)
      manager.getOrCreateBucket(150000L)

      manager.size shouldEqual 3

      // Should have most recent 3 buckets
      manager.hasBucket(90000L) shouldEqual true
      manager.hasBucket(120000L) shouldEqual true
      manager.hasBucket(150000L) shouldEqual true
    }
  }

  describe("BucketManager error handling") {
    it("should validate max buckets > 0") {
      val templates = Array(Aggregator.create(AggregationType.Sum))

      intercept[IllegalArgumentException] {
        new BucketManager(0, templates)
      }

      intercept[IllegalArgumentException] {
        new BucketManager(-1, templates)
      }
    }

    it("should validate non-empty templates") {
      intercept[IllegalArgumentException] {
        new BucketManager(3, null)
      }

      intercept[IllegalArgumentException] {
        new BucketManager(3, Array.empty[Aggregator])
      }
    }

    it("should throw on access to non-existent bucket") {
      val templates = Array(Aggregator.create(AggregationType.Sum))
      val manager = new BucketManager(3, templates)

      intercept[NoSuchElementException] {
        manager(30000L)
      }
    }
  }

  describe("BucketManager factory methods") {
    it("should create manager for single aggregator") {
      val aggregator = Aggregator.create(AggregationType.Sum)
      val manager = BucketManager.forSingleAggregator(3, aggregator)

      manager.size shouldEqual 0
      manager.stats.maxBuckets shouldEqual 3

      val bucket = manager.getOrCreateBucket(30000L)
      bucket.aggregate(Array(42.0))
      bucket.emit()(0) shouldEqual 42.0
    }
  }
}
