package filodb.core.memstore.aggregation

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.memory.format.SeqRowReader
import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}

class BucketAggregationStateSpec extends AnyFunSpec with Matchers {

  // Helper: create a state with a single Sum column at index 0
  def singleSumState(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): BucketAggregationState =
    new BucketAggregationState(
      Array[Option[AggregationType]](Some(AggregationType.Sum)),
      intervalMs, toleranceMs, 1)

  // Helper: multi-column state (Sum, Min, Max)
  def multiColumnState(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): BucketAggregationState =
    new BucketAggregationState(
      Array[Option[AggregationType]](
        Some(AggregationType.Sum), Some(AggregationType.Min), Some(AggregationType.Max)),
      intervalMs, toleranceMs, 3)

  // Helper: state with one aggregating and one non-aggregating column
  def mixedState(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): BucketAggregationState =
    new BucketAggregationState(
      Array[Option[AggregationType]](Some(AggregationType.Sum), None),
      intervalMs, toleranceMs, 2)

  // Helper: histogram sum state
  def histogramSumState(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): BucketAggregationState =
    new BucketAggregationState(
      Array[Option[AggregationType]](Some(AggregationType.HistogramSum)),
      intervalMs, toleranceMs, 1)

  // Helper: histogram last state
  def histogramLastState(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): BucketAggregationState =
    new BucketAggregationState(
      Array[Option[AggregationType]](Some(AggregationType.HistogramLast)),
      intervalMs, toleranceMs, 1)

  // Helper: create a serialized histogram as DirectBuffer
  // Serialize into a fresh buffer each call to avoid shared BinaryHistogram.histBuf being overwritten
  def createHistogramBuffer(bucketCounts: Seq[(Double, Long)]): org.agrona.DirectBuffer = {
    val boundaries = bucketCounts.map(_._1).toArray :+ Double.PositiveInfinity
    val counts = bucketCounts.map(_._2).toArray :+ 0L
    LongHistogram(CustomBuckets(boundaries), counts)
      .serialize(Some(new org.agrona.concurrent.UnsafeBuffer(new Array[Byte](4096))))
  }

  describe("TimeBucket.ceilToBucket") {
    it("ceils timestamps to the next bucket boundary") {
      val interval = 30000L
      TimeBucket.ceilToBucket(5000L, interval) shouldEqual 30000L
      TimeBucket.ceilToBucket(25000L, interval) shouldEqual 30000L
      TimeBucket.ceilToBucket(30000L, interval) shouldEqual 30000L
      TimeBucket.ceilToBucket(31000L, interval) shouldEqual 60000L
    }

    it("handles boundary values") {
      val interval = 60000L
      TimeBucket.ceilToBucket(0L, interval) shouldEqual 0L
      TimeBucket.ceilToBucket(1L, interval) shouldEqual 60000L
      TimeBucket.ceilToBucket(59999L, interval) shouldEqual 60000L
      TimeBucket.ceilToBucket(60001L, interval) shouldEqual 120000L
    }
  }

  describe("BucketAggregationState.aggregate - basic scalar") {
    it("should aggregate a sample into the correct bucket") {
      val state = singleSumState()

      // sampleTs=100000 -> ceilToBucket(100000, 60000) = 120000
      val result = state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))
      result shouldEqual true

      state.activeBucketTimestamps shouldEqual Set(120000L)
      val values = state.getBucketValues(120000L)
      values shouldBe defined
      values.get(0).asInstanceOf[Double] shouldEqual 10.0
    }

    it("should aggregate multiple samples into the same bucket") {
      val state = singleSumState()

      state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))
      state.aggregate(110000L, SeqRowReader(Seq(20.0: Any)))
      state.aggregate(115000L, SeqRowReader(Seq(30.0: Any)))

      // All ceil to bucket 120000
      state.activeBucketTimestamps shouldEqual Set(120000L)
      val values = state.getBucketValues(120000L)
      values.get(0).asInstanceOf[Double] shouldEqual 60.0
    }

    it("should put samples in different buckets based on timestamp") {
      val state = singleSumState()

      state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))  // bucket 120000
      state.aggregate(160000L, SeqRowReader(Seq(20.0: Any)))  // bucket 180000

      state.activeBucketTimestamps shouldEqual Set(120000L, 180000L)
      state.getBucketValues(120000L).get(0).asInstanceOf[Double] shouldEqual 10.0
      state.getBucketValues(180000L).get(0).asInstanceOf[Double] shouldEqual 20.0
    }
  }

  describe("BucketAggregationState.aggregate - tolerance checking") {
    it("should reject samples outside the event-time tolerance window") {
      val toleranceMs = 30000L
      val state = singleSumState(toleranceMs = toleranceMs)

      // First sample sets watermark to 200000
      state.aggregate(200000L, SeqRowReader(Seq(10.0: Any)))

      // Sample more than tolerance behind watermark: rejected
      // sampleTs=169999, watermark=200000, diff=30001 > 30000 => rejected
      val result = state.aggregate(169999L, SeqRowReader(Seq(10.0: Any)))
      result shouldEqual false
    }

    it("should accept samples within the event-time tolerance window") {
      val toleranceMs = 30000L
      val state = singleSumState(toleranceMs = toleranceMs)

      // First sample sets watermark to 200000
      state.aggregate(200000L, SeqRowReader(Seq(10.0: Any)))

      // Sample within tolerance of watermark: 200000 - 170000 = 30000 <= 30000 => accepted
      val result = state.aggregate(170000L, SeqRowReader(Seq(10.0: Any)))
      result shouldEqual true
    }

    it("should accept samples at exactly the tolerance boundary") {
      val toleranceMs = 30000L
      val state = singleSumState(toleranceMs = toleranceMs)

      // First sample sets watermark to 200000
      state.aggregate(200000L, SeqRowReader(Seq(10.0: Any)))

      // sampleTs = watermark - tolerance = 170000 => accepted
      val result = state.aggregate(170000L, SeqRowReader(Seq(10.0: Any)))
      result shouldEqual true
    }
  }

  describe("BucketAggregationState.aggregate - tolerance boundary edge cases") {
    it("should accept sample at exactly the tolerance boundary (sampleTs == watermark - toleranceMs)") {
      val toleranceMs = 30000L
      val state = singleSumState(toleranceMs = toleranceMs)

      // First sample sets watermark
      state.aggregate(200000L, SeqRowReader(Seq(10.0: Any)))

      // Exactly at boundary: sampleTs=170000, watermark=200000, diff=30000 == toleranceMs => accepted
      val result = state.aggregate(170000L, SeqRowReader(Seq(10.0: Any)))
      result shouldEqual true
      state.activeBucketTimestamps should not be empty
    }

    it("should reject sample one ms past tolerance boundary (sampleTs == watermark - toleranceMs - 1)") {
      val toleranceMs = 30000L
      val state = singleSumState(toleranceMs = toleranceMs)

      // First sample sets watermark
      state.aggregate(200000L, SeqRowReader(Seq(10.0: Any)))

      // One ms past: sampleTs=169999, watermark=200000, diff=30001 > toleranceMs => rejected
      val result = state.aggregate(169999L, SeqRowReader(Seq(10.0: Any)))
      result shouldEqual false
    }

    it("should handle zero tolerance: only accept samples at or after the watermark") {
      val state = singleSumState(toleranceMs = 0L)

      // First sample sets watermark to 100000
      val inOrderResult = state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))
      inOrderResult shouldEqual true

      // Any sample behind watermark by even 1ms should be rejected
      val oooResult = state.aggregate(99999L, SeqRowReader(Seq(20.0: Any)))
      oooResult shouldEqual false

      // Sample at exactly the watermark: accepted (>= watermark - 0)
      val atWatermark = state.aggregate(100000L, SeqRowReader(Seq(30.0: Any)))
      atWatermark shouldEqual true
    }
  }

  describe("BucketAggregationState.aggregate - finalized buckets") {
    it("should reject samples for finalized buckets") {
      val state = singleSumState()

      // Aggregate and then finalize a bucket
      state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))
      state.markFinalized(120000L)

      // Try to aggregate into the same bucket -> should be rejected
      val result = state.aggregate(110000L, SeqRowReader(Seq(20.0: Any)))
      result shouldEqual false
    }
  }

  describe("BucketAggregationState.aggregate - no aggregation configured") {
    it("should return false when no aggregation configs exist") {
      val state = new BucketAggregationState(Array[Option[AggregationType]](None), 60000L, 30000L, 1)

      val result = state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))
      result shouldEqual false
    }
  }

  describe("BucketAggregationState.aggregate - multi-column") {
    it("should aggregate different types across columns") {
      val state = multiColumnState()

      state.aggregate(100000L, SeqRowReader(Seq(10.0: Any, 50.0: Any, 20.0: Any)))
      state.aggregate(110000L, SeqRowReader(Seq(20.0: Any, 30.0: Any, 40.0: Any)))
      state.aggregate(115000L, SeqRowReader(Seq(30.0: Any, 70.0: Any, 10.0: Any)))

      val values = state.getBucketValues(120000L).get
      values(0).asInstanceOf[Double] shouldEqual 60.0  // Sum
      values(1).asInstanceOf[Double] shouldEqual 30.0  // Min
      values(2).asInstanceOf[Double] shouldEqual 40.0  // Max
    }
  }

  describe("BucketAggregationState.aggregate - mixed agg and non-agg columns") {
    it("should aggregate configured columns and keep first value for non-configured columns") {
      val state = mixedState()

      state.aggregate(100000L, SeqRowReader(Seq(10.0: Any, "label-1": Any)))
      state.aggregate(110000L, SeqRowReader(Seq(20.0: Any, "label-2": Any)))

      val values = state.getBucketValues(120000L).get
      values(0).asInstanceOf[Double] shouldEqual 30.0    // Sum of 10+20
      values(1).asInstanceOf[String] shouldEqual "label-1" // First non-agg value kept
    }
  }

  describe("BucketAggregationState.getBucketsToFinalize") {
    it("should return buckets older than the threshold in sorted order") {
      val state = singleSumState()

      state.aggregate(50000L, SeqRowReader(Seq(1.0: Any)))    // bucket 60000
      state.aggregate(110000L, SeqRowReader(Seq(2.0: Any)))   // bucket 120000
      state.aggregate(170000L, SeqRowReader(Seq(3.0: Any)))   // bucket 180000

      val toFinalize = state.getBucketsToFinalize(150000L)
      toFinalize shouldEqual Seq(60000L, 120000L)
    }

    it("should return empty when no buckets are older than threshold") {
      val state = singleSumState()

      state.aggregate(100000L, SeqRowReader(Seq(1.0: Any)))

      val toFinalize = state.getBucketsToFinalize(60000L)
      toFinalize shouldBe empty
    }

    it("should not include buckets at exactly the threshold") {
      val state = singleSumState()

      state.aggregate(100000L, SeqRowReader(Seq(1.0: Any))) // bucket 120000

      val toFinalize = state.getBucketsToFinalize(120000L)
      toFinalize shouldBe empty
    }
  }

  describe("BucketAggregationState.markFinalized") {
    it("should remove bucket from active and add to finalized tracking") {
      val state = singleSumState()

      state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))
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
      val state = singleSumState(toleranceMs = toleranceMs)

      // Finalize some buckets
      state.aggregate(50000L, SeqRowReader(Seq(1.0: Any)))  // bucket 60000
      state.markFinalized(60000L)

      state.aggregate(110000L, SeqRowReader(Seq(2.0: Any))) // bucket 120000
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
      val state = singleSumState(toleranceMs = toleranceMs)

      state.aggregate(100000L, SeqRowReader(Seq(1.0: Any))) // bucket 120000
      state.markFinalized(120000L)

      // threshold = 130000, cleanupThreshold = 130000 - 60000 = 70000
      // bucket 120000 >= 70000 -> retained
      state.cleanupOldFinalizedTracking(130000L)
      state.stats.finalizedBucketCount shouldEqual 1
    }
  }

  describe("BucketAggregationState.stats") {
    it("should report correct stats") {
      val state = singleSumState()

      state.stats.activeBucketCount shouldEqual 0
      state.stats.finalizedBucketCount shouldEqual 0
      state.stats.latestSampleTimestamp shouldEqual Long.MinValue

      state.aggregate(100000L, SeqRowReader(Seq(1.0: Any)))
      state.aggregate(200000L, SeqRowReader(Seq(2.0: Any)))

      state.stats.activeBucketCount shouldEqual 2
      state.stats.latestSampleTimestamp shouldEqual 200000L

      state.markFinalized(120000L)
      state.stats.activeBucketCount shouldEqual 1
      state.stats.finalizedBucketCount shouldEqual 1
    }
  }

  describe("BucketAggregationState.clear") {
    it("should reset all state") {
      val state = singleSumState()

      state.aggregate(100000L, SeqRowReader(Seq(1.0: Any)))
      state.markFinalized(120000L)
      state.aggregate(200000L, SeqRowReader(Seq(2.0: Any)))

      state.clear()

      state.stats.activeBucketCount shouldEqual 0
      state.stats.finalizedBucketCount shouldEqual 0
      state.stats.latestSampleTimestamp shouldEqual Long.MinValue
      state.activeBucketTimestamps shouldBe empty
    }
  }

  describe("BucketAggregationState - histogram aggregation") {
    it("should aggregate histograms using HistogramSum") {
      val state = histogramSumState()

      val hist1 = createHistogramBuffer(Seq((1.0, 5L), (2.0, 10L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 3L), (2.0, 7L)))

      state.aggregate(100000L, SeqRowReader(Seq(hist1: Any)))
      state.aggregate(110000L, SeqRowReader(Seq(hist2: Any)))

      val aggregatedHist = state.getAggregatedHistogram(0, 120000L)
      aggregatedHist shouldBe defined
      aggregatedHist.get.numBuckets shouldEqual 3 // 2 user-defined + infinity

      // Bucket counts should be summed: (1.0, 8), (2.0, 17), (+Inf, 0)
      aggregatedHist.get.bucketValue(0) shouldEqual 8.0
      aggregatedHist.get.bucketValue(1) shouldEqual 17.0
    }

    it("should keep last histogram using HistogramLast") {
      val state = histogramLastState()

      val hist1 = createHistogramBuffer(Seq((1.0, 5L), (2.0, 10L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 99L), (2.0, 99L)))

      // hist1 at ts=100000, hist2 at ts=120000 (later)
      state.aggregate(100000L, SeqRowReader(Seq(hist1: Any)))
      state.aggregate(110000L, SeqRowReader(Seq(hist2: Any))) // later ts -> should replace

      val aggregatedHist = state.getAggregatedHistogram(0, 120000L)
      aggregatedHist shouldBe defined
      // Should have hist2's values (the later one)
      aggregatedHist.get.bucketValue(0) shouldEqual 99.0
      aggregatedHist.get.bucketValue(1) shouldEqual 99.0
    }

    it("should not replace histogram with earlier timestamp for HistogramLast") {
      val state = histogramLastState()

      val hist1 = createHistogramBuffer(Seq((1.0, 99L), (2.0, 99L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 1L), (2.0, 1L)))

      // hist1 at ts=110000 (later), hist2 at ts=100000 (earlier)
      state.aggregate(110000L, SeqRowReader(Seq(hist1: Any)))
      state.aggregate(100000L, SeqRowReader(Seq(hist2: Any))) // earlier ts -> should not replace

      val aggregatedHist = state.getAggregatedHistogram(0, 120000L)
      aggregatedHist shouldBe defined
      // Should still have hist1's values
      aggregatedHist.get.bucketValue(0) shouldEqual 99.0
    }

    it("should return None for non-existent histogram bucket") {
      val state = histogramSumState()
      state.getAggregatedHistogram(0, 999999L) shouldEqual None
    }
  }

  describe("BucketAggregationState - latestSampleTimestamp tracking") {
    it("should track latest sample timestamp correctly with out-of-order samples") {
      val state = singleSumState()

      state.aggregate(100000L, SeqRowReader(Seq(1.0: Any)))
      state.stats.latestSampleTimestamp shouldEqual 100000L

      state.aggregate(200000L, SeqRowReader(Seq(2.0: Any)))
      state.stats.latestSampleTimestamp shouldEqual 200000L

      // Out-of-order sample - should not decrease latest timestamp
      state.aggregate(150000L, SeqRowReader(Seq(3.0: Any)))
      state.stats.latestSampleTimestamp shouldEqual 200000L
    }
  }

}
