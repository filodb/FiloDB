package filodb.core.memstore.aggregation

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}

class BucketAggregationStateSpec extends AnyFunSpec with Matchers {

  // Helper: create a config array with a single Sum column at index 0
  def singleSumConfig(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): Array[Option[AggregationConfig]] =
    Array(Some(AggregationConfig(0, AggregationType.Sum, intervalMs, toleranceMs)))

  // Helper: multi-column config
  def multiColumnConfig(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): Array[Option[AggregationConfig]] =
    Array(
      Some(AggregationConfig(0, AggregationType.Sum, intervalMs, toleranceMs)),
      Some(AggregationConfig(1, AggregationType.Min, intervalMs, toleranceMs)),
      Some(AggregationConfig(2, AggregationType.Max, intervalMs, toleranceMs))
    )

  // Helper: config with one aggregating and one non-aggregating column
  def mixedConfig(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): Array[Option[AggregationConfig]] =
    Array(
      Some(AggregationConfig(0, AggregationType.Sum, intervalMs, toleranceMs)),
      None // non-aggregating column
    )

  // Helper: histogram sum config
  def histogramSumConfig(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): Array[Option[AggregationConfig]] =
    Array(Some(AggregationConfig(0, AggregationType.HistogramSum, intervalMs, toleranceMs)))

  // Helper: histogram last config
  def histogramLastConfig(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): Array[Option[AggregationConfig]] =
    Array(Some(AggregationConfig(0, AggregationType.HistogramLast, intervalMs, toleranceMs)))

  // Helper: create a serialized histogram as DirectBuffer
  // Serialize into a fresh buffer each call to avoid shared BinaryHistogram.histBuf being overwritten
  def createHistogramBuffer(bucketCounts: Seq[(Double, Long)]): org.agrona.DirectBuffer = {
    val boundaries = bucketCounts.map(_._1).toArray :+ Double.PositiveInfinity
    val counts = bucketCounts.map(_._2).toArray :+ 0L
    LongHistogram(CustomBuckets(boundaries), counts)
      .serialize(Some(new org.agrona.concurrent.UnsafeBuffer(new Array[Byte](4096))))
  }

  describe("BucketAggregationState.aggregate - basic scalar") {
    it("should aggregate a sample into the correct bucket") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      // sampleTs=100000 -> ceilToBucket(100000, 60000) = 120000
      val result = state.aggregate(100000L, 100000L, Array(10.0: Any))
      result shouldEqual true

      state.activeBucketTimestamps shouldEqual Set(120000L)
      val values = state.getBucketValues(120000L)
      values shouldBe defined
      values.get(0).asInstanceOf[Double] shouldEqual 10.0
    }

    it("should aggregate multiple samples into the same bucket") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      state.aggregate(100000L, 100000L, Array(10.0: Any))
      state.aggregate(110000L, 110000L, Array(20.0: Any))
      state.aggregate(115000L, 115000L, Array(30.0: Any))

      // All ceil to bucket 120000
      state.activeBucketTimestamps shouldEqual Set(120000L)
      val values = state.getBucketValues(120000L)
      values.get(0).asInstanceOf[Double] shouldEqual 60.0
    }

    it("should put samples in different buckets based on timestamp") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      state.aggregate(100000L, 100000L, Array(10.0: Any))  // bucket 120000
      state.aggregate(160000L, 160000L, Array(20.0: Any))  // bucket 180000

      state.activeBucketTimestamps shouldEqual Set(120000L, 180000L)
      state.getBucketValues(120000L).get(0).asInstanceOf[Double] shouldEqual 10.0
      state.getBucketValues(180000L).get(0).asInstanceOf[Double] shouldEqual 20.0
    }
  }

  describe("BucketAggregationState.aggregate - tolerance checking") {
    it("should reject samples outside the tolerance window") {
      val toleranceMs = 30000L
      val state = new BucketAggregationState(singleSumConfig(toleranceMs = toleranceMs), 1)

      // ingestionTime - sampleTs > toleranceMs => rejected
      // sampleTs=10000, ingestionTime=50000, diff=40000 > 30000 => rejected
      val result = state.aggregate(10000L, 50000L, Array(10.0: Any))
      result shouldEqual false
      state.activeBucketTimestamps shouldBe empty
    }

    it("should accept samples within the tolerance window") {
      val toleranceMs = 30000L
      val state = new BucketAggregationState(singleSumConfig(toleranceMs = toleranceMs), 1)

      // ingestionTime - sampleTs = 20000 <= 30000 => accepted
      val result = state.aggregate(30000L, 50000L, Array(10.0: Any))
      result shouldEqual true
      state.activeBucketTimestamps should not be empty
    }

    it("should accept samples at exactly the tolerance boundary") {
      val toleranceMs = 30000L
      val state = new BucketAggregationState(singleSumConfig(toleranceMs = toleranceMs), 1)

      // ingestionTime - sampleTs = 30000 <= 30000 => accepted
      val result = state.aggregate(20000L, 50000L, Array(10.0: Any))
      result shouldEqual true
    }
  }

  describe("BucketAggregationState.aggregate - finalized buckets") {
    it("should reject samples for finalized buckets") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      // Aggregate and then finalize a bucket
      state.aggregate(100000L, 100000L, Array(10.0: Any))
      state.markFinalized(120000L)

      // Try to aggregate into the same bucket -> should be rejected
      val result = state.aggregate(110000L, 110000L, Array(20.0: Any))
      result shouldEqual false
    }
  }

  describe("BucketAggregationState.aggregate - no aggregation configured") {
    it("should return false when no aggregation configs exist") {
      val state = new BucketAggregationState(Array(None), 1)

      val result = state.aggregate(100000L, 100000L, Array(10.0: Any))
      result shouldEqual false
    }
  }

  describe("BucketAggregationState.aggregate - multi-column") {
    it("should aggregate different types across columns") {
      val state = new BucketAggregationState(multiColumnConfig(), 3)

      state.aggregate(100000L, 100000L, Array(10.0: Any, 50.0: Any, 20.0: Any))
      state.aggregate(110000L, 110000L, Array(20.0: Any, 30.0: Any, 40.0: Any))
      state.aggregate(115000L, 115000L, Array(30.0: Any, 70.0: Any, 10.0: Any))

      val values = state.getBucketValues(120000L).get
      values(0).asInstanceOf[Double] shouldEqual 60.0  // Sum
      values(1).asInstanceOf[Double] shouldEqual 30.0  // Min
      values(2).asInstanceOf[Double] shouldEqual 40.0  // Max
    }
  }

  describe("BucketAggregationState.aggregate - mixed agg and non-agg columns") {
    it("should aggregate configured columns and keep first value for non-configured columns") {
      val state = new BucketAggregationState(mixedConfig(), 2)

      state.aggregate(100000L, 100000L, Array(10.0: Any, "label-1": Any))
      state.aggregate(110000L, 110000L, Array(20.0: Any, "label-2": Any))

      val values = state.getBucketValues(120000L).get
      values(0).asInstanceOf[Double] shouldEqual 30.0    // Sum of 10+20
      values(1).asInstanceOf[String] shouldEqual "label-1" // First non-agg value kept
    }
  }

  describe("BucketAggregationState.getBucketsToFinalize") {
    it("should return buckets older than the threshold in sorted order") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      state.aggregate(50000L, 50000L, Array(1.0: Any))    // bucket 60000
      state.aggregate(110000L, 110000L, Array(2.0: Any))   // bucket 120000
      state.aggregate(170000L, 170000L, Array(3.0: Any))   // bucket 180000

      val toFinalize = state.getBucketsToFinalize(150000L)
      toFinalize shouldEqual Seq(60000L, 120000L)
    }

    it("should return empty when no buckets are older than threshold") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      state.aggregate(100000L, 100000L, Array(1.0: Any))

      val toFinalize = state.getBucketsToFinalize(60000L)
      toFinalize shouldBe empty
    }

    it("should not include buckets at exactly the threshold") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      state.aggregate(100000L, 100000L, Array(1.0: Any)) // bucket 120000

      val toFinalize = state.getBucketsToFinalize(120000L)
      toFinalize shouldBe empty
    }
  }

  describe("BucketAggregationState.markFinalized") {
    it("should remove bucket from active and add to finalized tracking") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      state.aggregate(100000L, 100000L, Array(10.0: Any))
      state.isActive(120000L) shouldEqual true

      state.markFinalized(120000L)
      state.isActive(120000L) shouldEqual false
      state.getBucketValues(120000L) shouldEqual None
      state.activeBucketTimestamps shouldBe empty
    }
  }

  describe("BucketAggregationState.cleanupOldFinalizedTracking") {
    it("should remove very old finalized bucket tracking") {
      val toleranceMs = 30000L
      val state = new BucketAggregationState(singleSumConfig(toleranceMs = toleranceMs), 1)

      // Finalize some buckets
      state.aggregate(50000L, 50000L, Array(1.0: Any))  // bucket 60000
      state.markFinalized(60000L)

      state.aggregate(110000L, 110000L, Array(2.0: Any)) // bucket 120000
      state.markFinalized(120000L)

      // Stats should show 2 finalized
      state.stats.finalizedBucketCount shouldEqual 2

      // Cleanup with threshold that removes the oldest
      // cleanupThreshold = thresholdTs - 2*tolerance = 200000 - 60000 = 140000
      // Buckets < 140000 are removed -> bucket 60000 and 120000 are both removed
      state.cleanupOldFinalizedTracking(200000L)
      state.stats.finalizedBucketCount shouldEqual 0
    }

    it("should retain recent finalized buckets") {
      val toleranceMs = 30000L
      val state = new BucketAggregationState(singleSumConfig(toleranceMs = toleranceMs), 1)

      state.aggregate(100000L, 100000L, Array(1.0: Any)) // bucket 120000
      state.markFinalized(120000L)

      // threshold = 130000, cleanupThreshold = 130000 - 60000 = 70000
      // bucket 120000 >= 70000 -> retained
      state.cleanupOldFinalizedTracking(130000L)
      state.stats.finalizedBucketCount shouldEqual 1
    }
  }

  describe("BucketAggregationState.stats") {
    it("should report correct stats") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      state.stats.activeBucketCount shouldEqual 0
      state.stats.finalizedBucketCount shouldEqual 0
      state.stats.latestSampleTimestamp shouldEqual Long.MinValue

      state.aggregate(100000L, 100000L, Array(1.0: Any))
      state.aggregate(200000L, 200000L, Array(2.0: Any))

      state.stats.activeBucketCount shouldEqual 2
      state.stats.latestSampleTimestamp shouldEqual 200000L

      state.markFinalized(120000L)
      state.stats.activeBucketCount shouldEqual 1
      state.stats.finalizedBucketCount shouldEqual 1
    }
  }

  describe("BucketAggregationState.clear") {
    it("should reset all state") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      state.aggregate(100000L, 100000L, Array(1.0: Any))
      state.markFinalized(120000L)
      state.aggregate(200000L, 200000L, Array(2.0: Any))

      state.clear()

      state.stats.activeBucketCount shouldEqual 0
      state.stats.finalizedBucketCount shouldEqual 0
      state.stats.latestSampleTimestamp shouldEqual Long.MinValue
      state.activeBucketTimestamps shouldBe empty
    }
  }

  describe("BucketAggregationState - histogram aggregation") {
    it("should aggregate histograms using HistogramSum") {
      val state = new BucketAggregationState(histogramSumConfig(), 1)

      val hist1 = createHistogramBuffer(Seq((1.0, 5L), (2.0, 10L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 3L), (2.0, 7L)))

      state.aggregate(100000L, 100000L, Array(hist1: Any))
      state.aggregate(110000L, 110000L, Array(hist2: Any))

      val aggregatedHist = state.getAggregatedHistogram(0, 120000L)
      aggregatedHist shouldBe defined
      aggregatedHist.get.numBuckets shouldEqual 3 // 2 user-defined + infinity

      // Bucket counts should be summed: (1.0, 8), (2.0, 17), (+Inf, 0)
      aggregatedHist.get.bucketValue(0) shouldEqual 8.0
      aggregatedHist.get.bucketValue(1) shouldEqual 17.0
    }

    it("should keep last histogram using HistogramLast") {
      val state = new BucketAggregationState(histogramLastConfig(), 1)

      val hist1 = createHistogramBuffer(Seq((1.0, 5L), (2.0, 10L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 99L), (2.0, 99L)))

      // hist1 at ts=100000, hist2 at ts=120000 (later)
      state.aggregate(100000L, 100000L, Array(hist1: Any))
      state.aggregate(110000L, 110000L, Array(hist2: Any)) // later ts -> should replace

      val aggregatedHist = state.getAggregatedHistogram(0, 120000L)
      aggregatedHist shouldBe defined
      // Should have hist2's values (the later one)
      aggregatedHist.get.bucketValue(0) shouldEqual 99.0
      aggregatedHist.get.bucketValue(1) shouldEqual 99.0
    }

    it("should not replace histogram with earlier timestamp for HistogramLast") {
      val state = new BucketAggregationState(histogramLastConfig(), 1)

      val hist1 = createHistogramBuffer(Seq((1.0, 99L), (2.0, 99L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 1L), (2.0, 1L)))

      // hist1 at ts=110000 (later), hist2 at ts=100000 (earlier)
      state.aggregate(110000L, 110000L, Array(hist1: Any))
      state.aggregate(100000L, 110000L, Array(hist2: Any)) // earlier ts -> should not replace

      val aggregatedHist = state.getAggregatedHistogram(0, 120000L)
      aggregatedHist shouldBe defined
      // Should still have hist1's values
      aggregatedHist.get.bucketValue(0) shouldEqual 99.0
    }

    it("should return None for non-existent histogram bucket") {
      val state = new BucketAggregationState(histogramSumConfig(), 1)
      state.getAggregatedHistogram(0, 999999L) shouldEqual None
    }
  }

  describe("BucketAggregationState - latestSampleTimestamp tracking") {
    it("should track latest sample timestamp correctly with out-of-order samples") {
      val state = new BucketAggregationState(singleSumConfig(), 1)

      state.aggregate(100000L, 100000L, Array(1.0: Any))
      state.stats.latestSampleTimestamp shouldEqual 100000L

      state.aggregate(200000L, 200000L, Array(2.0: Any))
      state.stats.latestSampleTimestamp shouldEqual 200000L

      // Out-of-order sample - should not decrease latest timestamp
      state.aggregate(150000L, 200000L, Array(3.0: Any))
      state.stats.latestSampleTimestamp shouldEqual 200000L
    }
  }
}
