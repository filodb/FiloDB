package filodb.core.memstore.aggregation

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.memory.format.SeqRowReader

/**
 * TDD tests for the event-time watermark model.
 * These tests verify that ingest tolerance, bucket finalization, and query visibility
 * depend ONLY on the high-water mark of sample timestamps (latestSampleTimestamp),
 * NOT on wall-clock time or ingestionTime.
 */
class EventTimeWatermarkSpec extends AnyFunSpec with Matchers {

  def singleSumState(intervalMs: Long = 60000L, toleranceMs: Long = 30000L): BucketAggregationState =
    new BucketAggregationState(
      Array[Option[AggregationType]](Some(AggregationType.Sum)),
      intervalMs, toleranceMs, 1)

  // ======================== Ingest tolerance tests ========================

  describe("Event-time ingest tolerance") {

    it("first sample is always accepted regardless of timestamp") {
      val state = singleSumState()

      // Far-past timestamp — first sample, no watermark yet
      val result1 = state.aggregate(1L, SeqRowReader(Seq(10.0: Any)))
      result1 shouldEqual true
      state.hasActiveBuckets shouldEqual true

      // Fresh state, far-future timestamp
      val state2 = singleSumState()
      val result2 = state2.aggregate(Long.MaxValue - 100000L, SeqRowReader(Seq(20.0: Any)))
      result2 shouldEqual true
      state2.hasActiveBuckets shouldEqual true
    }

    it("subsequent sample within event-time tolerance is accepted") {
      val toleranceMs = 30000L
      val state = singleSumState(toleranceMs = toleranceMs)

      // First sample sets watermark to T1
      val t1 = 200000L
      state.aggregate(t1, SeqRowReader(Seq(10.0: Any)))

      // Sample at exactly watermark - tolerance: accepted
      val atBoundary = t1 - toleranceMs
      val result = state.aggregate(atBoundary, SeqRowReader(Seq(20.0: Any)))
      result shouldEqual true
    }

    it("sample outside event-time tolerance is rejected") {
      val toleranceMs = 30000L
      val state = singleSumState(toleranceMs = toleranceMs)

      // First sample sets watermark to T1
      val t1 = 200000L
      state.aggregate(t1, SeqRowReader(Seq(10.0: Any)))

      // Sample more than tolerance behind watermark: rejected
      val tooOld = t1 - toleranceMs - 1
      val result = state.aggregate(tooOld, SeqRowReader(Seq(20.0: Any)))
      result shouldEqual false
    }

    it("future-dated sample advances watermark and expands window") {
      val toleranceMs = 30000L
      val state = singleSumState(toleranceMs = toleranceMs)

      val t1 = 100000L
      state.aggregate(t1, SeqRowReader(Seq(1.0: Any)))
      state.stats.latestSampleTimestamp shouldEqual t1

      // Future sample advances watermark
      val t2 = t1 + 600000L // +10 minutes
      state.aggregate(t2, SeqRowReader(Seq(2.0: Any)))
      state.stats.latestSampleTimestamp shouldEqual t2

      // Sample within tolerance of new watermark: accepted
      val withinNewWindow = t2 - 5000L
      val result1 = state.aggregate(withinNewWindow, SeqRowReader(Seq(3.0: Any)))
      result1 shouldEqual true

      // Original timestamp now outside tolerance of new watermark
      val outsideNewWindow = t1 // t2 - t1 = 600000 > 30000
      val result2 = state.aggregate(outsideNewWindow, SeqRowReader(Seq(4.0: Any)))
      result2 shouldEqual false
    }
  }

  // ======================== Query visibility tests ========================

  describe("Event-time query visibility") {

    it("all active buckets are visible immediately after ingest (no wall-clock gate)") {
      val state = singleSumState(intervalMs = 60000L, toleranceMs = 30000L)

      // Ingest future-dated samples
      val futureTs = System.currentTimeMillis() + 3600000L // 1 hour in the future
      state.aggregate(futureTs, SeqRowReader(Seq(42.0: Any)))

      val bucketTs = TimeBucket.ceilToBucket(futureTs, 60000L)
      state.activeBucketTimestamps should contain(bucketTs)

      // 2-arg iterator returns all active buckets immediately — no wall-clock gate
      val result = state.bucketValuesIteratorInRange(0L, Long.MaxValue).toSeq
      result should have length 1
      result.head._1 shouldEqual bucketTs
      result.head._2(0).asInstanceOf[Double] shouldEqual 42.0
    }

    it("query determinism: same state produces same result regardless of wall-clock") {
      val state = singleSumState(intervalMs = 60000L, toleranceMs = 30000L)

      state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))
      state.aggregate(200000L, SeqRowReader(Seq(20.0: Any)))

      // Multiple queries produce identical results (no System.currentTimeMillis() involved)
      val result1 = state.bucketValuesIteratorInRange(0L, Long.MaxValue).toSeq
      val result2 = state.bucketValuesIteratorInRange(0L, Long.MaxValue).toSeq

      result1.map(_._1) shouldEqual result2.map(_._1)
      result1.length shouldEqual 2
    }
  }

  // ======================== Finalization tests ========================

  describe("Event-time bucket finalization") {

    it("bucket finalizes when watermark advances past bucketTs + tolerance") {
      val intervalMs = 60000L
      val toleranceMs = 30000L
      val state = singleSumState(intervalMs, toleranceMs)

      // Sample at 100000 → bucket 120000
      state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))
      state.activeBucketTimestamps should contain(120000L)

      // Watermark advances. New threshold = ceilToBucket(watermark - tolerance, interval)
      // watermark = 200000, threshold = ceilToBucket(200000-30000, 60000) = ceilToBucket(170000, 60000) = 180000
      // bucket 120000 < 180000 → should finalize
      state.aggregate(200000L, SeqRowReader(Seq(20.0: Any)))

      val thresholdTs = TimeBucket.ceilToBucket(
        state.stats.latestSampleTimestamp - toleranceMs,
        intervalMs
      )
      val bucketsToFinalize = state.getBucketsToFinalize(thresholdTs)
      bucketsToFinalize should contain(120000L)
    }

    it("trailing buckets stay active when no new samples arrive (no wall-clock reaper)") {
      val state = singleSumState()

      // Ingest one sample and stop
      state.aggregate(100000L, SeqRowReader(Seq(10.0: Any)))
      state.hasActiveBuckets shouldEqual true

      // Without new samples, the bucket stays active forever — no wall-clock reaper
      // (The old getStaleBuckets method is being removed; there's no mechanism to
      // finalize a bucket without watermark advancement)
      state.hasActiveBuckets shouldEqual true
      state.activeBucketTimestamps should contain(120000L)
    }
  }
}
