package filodb.core.memstore.aggregation

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TimeBucketSpec extends AnyFunSpec with Matchers {

  describe("TimeBucket time bucketing logic") {
    it("should ceil timestamps to bucket boundaries correctly") {
      val interval = 30000L // 30 seconds

      // 12:00:05 -> 12:00:30
      TimeBucket.ceilToBucket(5000L, interval) shouldEqual 30000L

      // 12:00:25 -> 12:00:30
      TimeBucket.ceilToBucket(25000L, interval) shouldEqual 30000L

      // 12:00:30 -> 12:00:30 (already at boundary)
      TimeBucket.ceilToBucket(30000L, interval) shouldEqual 30000L

      // 12:00:31 -> 12:01:00
      TimeBucket.ceilToBucket(31000L, interval) shouldEqual 60000L
    }

    it("should floor timestamps to bucket start correctly") {
      val interval = 30000L

      TimeBucket.floorToBucket(5000L, interval) shouldEqual 0L
      TimeBucket.floorToBucket(25000L, interval) shouldEqual 0L
      TimeBucket.floorToBucket(30000L, interval) shouldEqual 30000L
      TimeBucket.floorToBucket(31000L, interval) shouldEqual 30000L
    }

    it("should handle 1-minute buckets") {
      val interval = 60000L

      TimeBucket.ceilToBucket(0L, interval) shouldEqual 0L
      TimeBucket.ceilToBucket(1L, interval) shouldEqual 60000L
      TimeBucket.ceilToBucket(59999L, interval) shouldEqual 60000L
      TimeBucket.ceilToBucket(60000L, interval) shouldEqual 60000L
      TimeBucket.ceilToBucket(60001L, interval) shouldEqual 120000L
    }

    it("should handle 5-minute buckets") {
      val interval = 300000L // 5 minutes

      TimeBucket.ceilToBucket(0L, interval) shouldEqual 0L
      TimeBucket.ceilToBucket(150000L, interval) shouldEqual 300000L
      TimeBucket.ceilToBucket(300000L, interval) shouldEqual 300000L
      TimeBucket.ceilToBucket(450000L, interval) shouldEqual 600000L
    }

    it("should determine if timestamps are in same bucket") {
      val interval = 30000L

      TimeBucket.sameBucket(5000L, 25000L, interval) shouldEqual true
      TimeBucket.sameBucket(25000L, 30000L, interval) shouldEqual true
      TimeBucket.sameBucket(5000L, 31000L, interval) shouldEqual false
      TimeBucket.sameBucket(30000L, 31000L, interval) shouldEqual false
    }

    it("should calculate bucket start correctly") {
      val interval = 30000L

      TimeBucket.bucketStart(30000L, interval) shouldEqual 0L
      TimeBucket.bucketStart(60000L, interval) shouldEqual 30000L
      TimeBucket.bucketStart(90000L, interval) shouldEqual 60000L
    }
  }

  describe("TimeBucket aggregation") {
    it("should aggregate values correctly") {
      val aggregators = Array(Aggregator.create(AggregationType.Sum))
      val bucket = TimeBucket(30000L, aggregators, 0)

      bucket.aggregate(Array(10.0))
      bucket.aggregate(Array(20.0))
      bucket.aggregate(Array(30.0))

      bucket.sampleCount shouldEqual 3
      val results = bucket.emit()
      results(0) shouldEqual 60.0
    }

    it("should aggregate multiple columns") {
      val aggregators = Array(
        Aggregator.create(AggregationType.Sum),
        Aggregator.create(AggregationType.Min),
        Aggregator.create(AggregationType.Max)
      )
      val bucket = TimeBucket(30000L, aggregators, 0)

      bucket.aggregate(Array(10.0, 100.0, 50.0))
      bucket.aggregate(Array(20.0, 50.0, 75.0))
      bucket.aggregate(Array(30.0, 75.0, 25.0))

      val results = bucket.emit()
      results(0) shouldEqual 60.0  // sum
      results(1) shouldEqual 50.0  // min
      results(2) shouldEqual 75.0  // max
    }

    it("should aggregate with timestamp for Last/First") {
      val aggregators = Array(
        Aggregator.create(AggregationType.Last),
        Aggregator.create(AggregationType.First)
      )
      val bucket = TimeBucket(30000L, aggregators, 0)

      bucket.aggregateWithTimestamp(Array(10.0, 10.0), 1000L)
      bucket.aggregateWithTimestamp(Array(20.0, 20.0), 3000L)
      bucket.aggregateWithTimestamp(Array(15.0, 15.0), 2000L)

      val results = bucket.emit()
      results(0) shouldEqual 20.0  // last (most recent timestamp)
      results(1) shouldEqual 10.0  // first (earliest timestamp)
    }

    it("should require matching column count") {
      val aggregators = Array(Aggregator.create(AggregationType.Sum))
      val bucket = TimeBucket(30000L, aggregators, 0)

      intercept[IllegalArgumentException] {
        bucket.aggregate(Array(10.0, 20.0))  // Too many values
      }
    }

    it("should track sample count") {
      val aggregators = Array(Aggregator.create(AggregationType.Sum))
      val bucket = TimeBucket(30000L, aggregators, 0)

      bucket.isEmpty shouldEqual true
      bucket.sampleCount shouldEqual 0

      bucket.aggregate(Array(10.0))
      bucket.isEmpty shouldEqual false
      bucket.sampleCount shouldEqual 1

      bucket.aggregate(Array(20.0))
      bucket.sampleCount shouldEqual 2
    }

    it("should reset correctly") {
      val aggregators = Array(Aggregator.create(AggregationType.Sum))
      val bucket = TimeBucket(30000L, aggregators, 0)

      bucket.aggregate(Array(10.0))
      bucket.aggregate(Array(20.0))
      bucket.sampleCount shouldEqual 2

      bucket.reset()
      bucket.sampleCount shouldEqual 0
      bucket.isEmpty shouldEqual true
      bucket.emit()(0).asInstanceOf[Double].isNaN shouldEqual true
    }
  }

  describe("TimeBucket acceptance window") {
    it("should accept samples within tolerance") {
      val interval = 30000L      // 30 seconds
      val tolerance = 60000L     // 60 seconds
      val bucketTs = 30000L      // Bucket at 30s

      val aggregators = Array(Aggregator.create(AggregationType.Sum))
      val bucket = TimeBucket(bucketTs, aggregators, 0)

      // Bucket covers [0, 30000]
      // With tolerance: [-60000, 90000]

      bucket.canAccept(0L, interval, tolerance) shouldEqual true      // At bucket start
      bucket.canAccept(15000L, interval, tolerance) shouldEqual true  // Middle of bucket
      bucket.canAccept(30000L, interval, tolerance) shouldEqual true  // At bucket end

      // Within tolerance before bucket
      bucket.canAccept(-30000L, interval, tolerance) shouldEqual true
      bucket.canAccept(-60000L, interval, tolerance) shouldEqual true

      // Within tolerance after bucket
      bucket.canAccept(60000L, interval, tolerance) shouldEqual true
      bucket.canAccept(90000L, interval, tolerance) shouldEqual true

      // Outside tolerance
      bucket.canAccept(-60001L, interval, tolerance) shouldEqual false
      bucket.canAccept(90001L, interval, tolerance) shouldEqual false
    }

    it("should reject samples outside tolerance window") {
      val interval = 60000L      // 1 minute
      val tolerance = 120000L    // 2 minutes
      val bucketTs = 60000L      // Bucket at 1 minute

      val aggregators = Array(Aggregator.create(AggregationType.Sum))
      val bucket = TimeBucket(bucketTs, aggregators, 0)

      // Bucket covers [0, 60000]
      // With tolerance: [-120000, 180000]

      bucket.canAccept(-120001L, interval, tolerance) shouldEqual false
      bucket.canAccept(180001L, interval, tolerance) shouldEqual false
    }

    it("should handle zero tolerance") {
      val interval = 30000L
      val tolerance = 0L
      val bucketTs = 30000L

      val aggregators = Array(Aggregator.create(AggregationType.Sum))
      val bucket = TimeBucket(bucketTs, aggregators, 0)

      // With zero tolerance, only accept within bucket boundaries
      bucket.canAccept(0L, interval, tolerance) shouldEqual true
      bucket.canAccept(15000L, interval, tolerance) shouldEqual true
      bucket.canAccept(30000L, interval, tolerance) shouldEqual true
      bucket.canAccept(-1L, interval, tolerance) shouldEqual false
      bucket.canAccept(30001L, interval, tolerance) shouldEqual false
    }
  }

  describe("TimeBucket creation") {
    it("should create bucket from templates") {
      val templates = Array(
        Aggregator.create(AggregationType.Sum),
        Aggregator.create(AggregationType.Avg)
      )

      val bucket = TimeBucket.create(30000L, templates)

      bucket.bucketTimestamp shouldEqual 30000L
      bucket.sampleCount shouldEqual 0
      bucket.aggregators.length shouldEqual 2
      bucket.aggregators(0) shouldBe a[SumAggregator]
      bucket.aggregators(1) shouldBe a[AvgAggregator]
    }

    it("should create independent buckets from same templates") {
      val templates = Array(Aggregator.create(AggregationType.Sum))

      val bucket1 = TimeBucket.create(30000L, templates)
      val bucket2 = TimeBucket.create(60000L, templates)

      bucket1.aggregate(Array(10.0))
      bucket2.aggregate(Array(20.0))

      bucket1.emit()(0) shouldEqual 10.0
      bucket2.emit()(0) shouldEqual 20.0
    }
  }
}
