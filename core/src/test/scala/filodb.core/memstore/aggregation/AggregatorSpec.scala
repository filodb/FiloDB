package filodb.core.memstore.aggregation

import org.agrona.{DirectBuffer, ExpandableArrayBuffer, MutableDirectBuffer}
import org.agrona.concurrent.UnsafeBuffer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.memory.format.vectors.{Base2ExpHistogramBuckets, BinaryHistogram, CustomBuckets,
  GeometricBuckets, HistogramBuckets, LongHistogram, MutableHistogram}

class AggregatorSpec extends AnyFunSpec with Matchers {

  // Helper to create a serialized histogram as DirectBuffer
  // Serialize into a fresh buffer each call to avoid shared BinaryHistogram.histBuf being overwritten
  private def createHistogramBuffer(bucketCounts: Seq[(Double, Long)]): DirectBuffer = {
    val boundaries = bucketCounts.map(_._1).toArray :+ Double.PositiveInfinity
    val counts = bucketCounts.map(_._2).toArray :+ 0L
    LongHistogram(CustomBuckets(boundaries), counts)
      .serialize(Some(new org.agrona.concurrent.UnsafeBuffer(new Array[Byte](4096))))
  }

  // --- Helpers for multi-format histogram creation ---

  private def freshBuf(): MutableDirectBuffer = new ExpandableArrayBuffer(4096)

  private def makeDeltaBuf(buckets: HistogramBuckets, values: Array[Long]): DirectBuffer = {
    val buf = freshBuf()
    BinaryHistogram.writeDelta(buckets, values, buf)
    buf
  }

  private def makeXorBuf(buckets: HistogramBuckets, values: Array[Double]): DirectBuffer = {
    val buf = freshBuf()
    BinaryHistogram.writeDoubles(buckets, values, buf)
    buf
  }

  private val geoNoBuckets  = GeometricBuckets(1.0, 2.0, 8, minusOne = false)
  private val geo1Buckets   = GeometricBuckets(1.0, 2.0, 8, minusOne = true)
  private val customBuckets8 = CustomBuckets(Array(1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, Double.PositiveInfinity))
  private val otelBuckets   = Base2ExpHistogramBuckets(1, 1, 7)

  private def monotonicLongValues(sampleIdx: Int, numBuckets: Int): Array[Long] = {
    val base = sampleIdx * 10L
    (0 until numBuckets).map(b => base + b * 5L).toArray
  }

  private def monotonicDoubleValues(sampleIdx: Int, numBuckets: Int): Array[Double] =
    monotonicLongValues(sampleIdx, numBuckets).map(_.toDouble)

  private def referenceAggregate(samples: Seq[DirectBuffer]): MutableDirectBuffer = {
    var acc: MutableHistogram = null
    samples.foreach { buf =>
      val hist = BinaryHistogram.BinHistogram(buf).toHistogram
      if (acc == null) acc = MutableHistogram(hist)
      else acc.add(hist)
    }
    val out = new ExpandableArrayBuffer(4096)
    acc.serialize(Some(out))
    out
  }

  private def bytesEqual(a: DirectBuffer, b: DirectBuffer): Boolean = {
    val lenA = a.getShort(0).toInt + 2
    val lenB = b.getShort(0).toInt + 2
    if (lenA != lenB) return false
    (0 until lenA).forall(i => a.getByte(i) == b.getByte(i))
  }

  describe("SumAggregator") {
    it("should sum numeric values correctly") {
      val agg = new SumAggregator
      agg.add(10.0)
      agg.add(20.0)
      agg.add(30.0)
      agg.result() shouldEqual 60.0
    }

    it("should handle different numeric types") {
      val agg = new SumAggregator
      agg.add(10)      // Int
      agg.add(20L)     // Long
      agg.add(15.5)    // Double
      agg.add(4.5f)    // Float
      agg.result() shouldEqual 50.0
    }

    it("should return NaN for empty aggregator") {
      val agg = new SumAggregator
      agg.result().asInstanceOf[Double].isNaN shouldEqual true
    }

    it("should ignore NaN and infinity values") {
      val agg = new SumAggregator
      agg.add(10.0)
      agg.add(Double.NaN)
      agg.add(20.0)
      agg.add(Double.PositiveInfinity)
      agg.add(30.0)
      agg.result() shouldEqual 60.0
    }

    it("should reset correctly") {
      val agg = new SumAggregator
      agg.add(10.0)
      agg.add(20.0)
      agg.reset()
      agg.result().asInstanceOf[Double].isNaN shouldEqual true
    }
  }

  describe("MinAggregator") {
    it("should find minimum value") {
      val agg = new MinAggregator
      agg.add(30.0)
      agg.add(10.0)
      agg.add(20.0)
      agg.result() shouldEqual 10.0
    }

    it("should handle negative values") {
      val agg = new MinAggregator
      agg.add(10.0)
      agg.add(-5.0)
      agg.add(20.0)
      agg.result() shouldEqual -5.0
    }

    it("should return NaN for empty aggregator") {
      val agg = new MinAggregator
      agg.result().asInstanceOf[Double].isNaN shouldEqual true
    }

    it("should ignore NaN values") {
      val agg = new MinAggregator
      agg.add(30.0)
      agg.add(Double.NaN)
      agg.add(10.0)
      agg.result() shouldEqual 10.0
    }
  }

  describe("MaxAggregator") {
    it("should find maximum value") {
      val agg = new MaxAggregator
      agg.add(10.0)
      agg.add(30.0)
      agg.add(20.0)
      agg.result() shouldEqual 30.0
    }

    it("should handle negative values") {
      val agg = new MaxAggregator
      agg.add(-10.0)
      agg.add(-5.0)
      agg.add(-20.0)
      agg.result() shouldEqual -5.0
    }

    it("should return NaN for empty aggregator") {
      val agg = new MaxAggregator
      agg.result().asInstanceOf[Double].isNaN shouldEqual true
    }
  }

  describe("LastAggregator") {
    it("should keep last value without timestamp") {
      val agg = new LastAggregator
      agg.add(10.0)
      agg.add(20.0)
      agg.add(30.0)
      agg.result() shouldEqual 30.0
    }

    it("should keep most recent value with timestamps") {
      val agg = new LastAggregator
      agg.addWithTimestamp(10.0, 1000L)
      agg.addWithTimestamp(20.0, 3000L)
      agg.addWithTimestamp(15.0, 2000L)  // Older timestamp, should be ignored
      agg.result() shouldEqual 20.0
    }

    it("should handle equal timestamps") {
      val agg = new LastAggregator
      agg.addWithTimestamp(10.0, 1000L)
      agg.addWithTimestamp(20.0, 1000L)  // Same timestamp, should update
      agg.result() shouldEqual 20.0
    }

    it("should return NaN for empty aggregator") {
      val agg = new LastAggregator
      agg.result().asInstanceOf[Double].isNaN shouldEqual true
    }
  }

  describe("FirstAggregator") {
    it("should keep first value without timestamp") {
      val agg = new FirstAggregator
      agg.add(10.0)
      agg.add(20.0)
      agg.add(30.0)
      agg.result() shouldEqual 10.0
    }

    it("should keep earliest value with timestamps") {
      val agg = new FirstAggregator
      agg.addWithTimestamp(10.0, 2000L)
      agg.addWithTimestamp(20.0, 1000L)  // Earlier timestamp
      agg.addWithTimestamp(30.0, 3000L)
      agg.result() shouldEqual 20.0
    }

    it("should not update with later timestamps") {
      val agg = new FirstAggregator
      agg.addWithTimestamp(10.0, 1000L)
      agg.addWithTimestamp(20.0, 2000L)  // Later, should be ignored
      agg.result() shouldEqual 10.0
    }

    it("should return NaN for empty aggregator") {
      val agg = new FirstAggregator
      agg.result().asInstanceOf[Double].isNaN shouldEqual true
    }
  }

  describe("CountAggregator") {
    it("should count all values") {
      val agg = new CountAggregator
      agg.add(10.0)
      agg.add(20.0)
      agg.add(30.0)
      agg.result() shouldEqual 3L
    }

    it("should count different types") {
      val agg = new CountAggregator
      agg.add(10)
      agg.add(20L)
      agg.add(30.0)
      agg.add("string")
      agg.result() shouldEqual 4L
    }

    it("should return 0 for empty aggregator") {
      val agg = new CountAggregator
      agg.result() shouldEqual 0L
    }

    it("should not count null values") {
      val agg = new CountAggregator
      agg.add(10.0)
      agg.add(null)
      agg.add(20.0)
      agg.result() shouldEqual 2L
    }
  }

  describe("Aggregator factory") {
    it("should create correct aggregator types") {
      Aggregator.create(AggregationType.Sum) shouldBe a[SumAggregator]
      Aggregator.create(AggregationType.Min) shouldBe a[MinAggregator]
      Aggregator.create(AggregationType.Max) shouldBe a[MaxAggregator]
      Aggregator.create(AggregationType.Last) shouldBe a[LastAggregator]
      Aggregator.create(AggregationType.First) shouldBe a[FirstAggregator]
      Aggregator.create(AggregationType.Count) shouldBe a[CountAggregator]
      Aggregator.create(AggregationType.HistogramSum) shouldBe a[HistogramAggregator]
      Aggregator.create(AggregationType.HistogramLast) shouldBe a[HistogramLastAggregator]
    }
  }

  describe("AggregationType") {
    it("should parse aggregation types correctly") {
      AggregationType.parse("sum") shouldEqual Some(AggregationType.Sum)
      AggregationType.parse("min") shouldEqual Some(AggregationType.Min)
      AggregationType.parse("max") shouldEqual Some(AggregationType.Max)
      AggregationType.parse("last") shouldEqual Some(AggregationType.Last)
      AggregationType.parse("first") shouldEqual Some(AggregationType.First)
      AggregationType.parse("count") shouldEqual Some(AggregationType.Count)
      AggregationType.parse("histogram") shouldEqual Some(AggregationType.HistogramSum)
      AggregationType.parse("histogram_sum") shouldEqual Some(AggregationType.HistogramSum)
      AggregationType.parse("histogram_last") shouldEqual Some(AggregationType.HistogramLast)
    }

    it("should be case-insensitive") {
      AggregationType.parse("SUM") shouldEqual Some(AggregationType.Sum)
      AggregationType.parse("MAX") shouldEqual Some(AggregationType.Max)
    }

    it("should return None for invalid types") {
      AggregationType.parse("invalid") shouldEqual None
      AggregationType.parse("") shouldEqual None
      AggregationType.parse("median") shouldEqual None
    }
  }

  describe("HistogramAggregator") {
    it("should accumulate histograms from DirectBuffer") {
      val agg = new HistogramAggregator
      val hist1 = createHistogramBuffer(Seq((1.0, 5L), (2.0, 10L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 3L), (2.0, 7L)))

      agg.add(hist1)
      agg.add(hist2)

      val resultAgg = agg.asInstanceOf[HistogramAggregator]
      resultAgg.getAccumulator shouldBe defined

      val hist = resultAgg.getAccumulator.get
      hist.numBuckets shouldEqual 3 // 2 user-defined + infinity
      hist.bucketValue(0) shouldEqual 8.0  // 5 + 3
      hist.bucketValue(1) shouldEqual 17.0 // 10 + 7
    }

    it("should handle single histogram") {
      val agg = new HistogramAggregator
      val hist = createHistogramBuffer(Seq((1.0, 5L), (2.0, 10L)))

      agg.add(hist)

      val resultAgg = agg.asInstanceOf[HistogramAggregator]
      resultAgg.getAccumulator shouldBe defined
      resultAgg.getAccumulator.get.bucketValue(0) shouldEqual 5.0
    }

    it("should return serialized buffer from result()") {
      val agg = new HistogramAggregator
      val hist = createHistogramBuffer(Seq((1.0, 5L)))

      agg.add(hist)

      val result = agg.result()
      result shouldBe a[DirectBuffer]
    }

    it("should return empty histogram for no data") {
      val agg = new HistogramAggregator
      val result = agg.result()
      result shouldBe a[DirectBuffer]
    }

    it("should reset correctly") {
      val agg = new HistogramAggregator
      val hist = createHistogramBuffer(Seq((1.0, 5L)))

      agg.add(hist)
      agg.reset()

      agg.asInstanceOf[HistogramAggregator].getAccumulator shouldEqual None
    }

    it("should ignore non-histogram values") {
      val agg = new HistogramAggregator
      agg.add(42.0)
      agg.add("string")
      agg.add(null)

      agg.asInstanceOf[HistogramAggregator].getAccumulator shouldEqual None
    }
  }

  // ==========================================================================
  // In-place histogram aggregation fast path (Tier 1 optimization)
  // ==========================================================================

  describe("BinaryHistogram.addValuesTo") {

    // --- Correctness parity: delta formats ---

    it("should produce identical output for HistFormat_Geometric_Delta") {
      val N = 10
      val samples = (1 to N).map(i => makeDeltaBuf(geoNoBuckets, monotonicLongValues(i, 8)))
      val ref = referenceAggregate(samples)

      val firstHist = BinaryHistogram.BinHistogram(samples.head).toHistogram
      val acc = MutableHistogram(firstHist)
      samples.tail.foreach { buf =>
        BinaryHistogram.addValuesTo(buf, acc.values)
      }
      acc.makeMonotonic()
      val result = new ExpandableArrayBuffer(4096)
      acc.serialize(Some(result))

      bytesEqual(ref, result) shouldBe true
    }

    it("should produce identical output for HistFormat_Custom_Delta") {
      val N = 10
      val samples = (1 to N).map(i => makeDeltaBuf(customBuckets8, monotonicLongValues(i, 8)))
      val ref = referenceAggregate(samples)

      val firstHist = BinaryHistogram.BinHistogram(samples.head).toHistogram
      val acc = MutableHistogram(firstHist)
      samples.tail.foreach { buf =>
        BinaryHistogram.addValuesTo(buf, acc.values)
      }
      acc.makeMonotonic()
      val result = new ExpandableArrayBuffer(4096)
      acc.serialize(Some(result))

      bytesEqual(ref, result) shouldBe true
    }

    it("should produce identical output for HistFormat_OtelExp_Delta") {
      val N = 10
      val numBuckets = otelBuckets.numBuckets  // 7+1=8
      val samples = (1 to N).map(i => makeDeltaBuf(otelBuckets, monotonicLongValues(i, numBuckets)))
      val ref = referenceAggregate(samples)

      val firstHist = BinaryHistogram.BinHistogram(samples.head).toHistogram
      val acc = MutableHistogram(firstHist)
      samples.tail.foreach { buf =>
        BinaryHistogram.addValuesTo(buf, acc.values)
      }
      acc.makeMonotonic()
      val result = new ExpandableArrayBuffer(4096)
      acc.serialize(Some(result))

      bytesEqual(ref, result) shouldBe true
    }

    it("should produce identical output for HistFormat_Geometric1_Delta") {
      val N = 10
      val samples = (1 to N).map(i => makeDeltaBuf(geo1Buckets, monotonicLongValues(i, 8)))
      val ref = referenceAggregate(samples)

      val firstHist = BinaryHistogram.BinHistogram(samples.head).toHistogram
      val acc = MutableHistogram(firstHist)
      samples.tail.foreach { buf =>
        BinaryHistogram.addValuesTo(buf, acc.values)
      }
      acc.makeMonotonic()
      val result = new ExpandableArrayBuffer(4096)
      acc.serialize(Some(result))

      bytesEqual(ref, result) shouldBe true
    }

    // --- Correctness parity: XOR formats ---

    it("should produce identical output for HistFormat_Geometric_XOR") {
      val N = 10
      val samples = (1 to N).map(i => makeXorBuf(geoNoBuckets, monotonicDoubleValues(i, 8)))
      val ref = referenceAggregate(samples)

      val firstHist = BinaryHistogram.BinHistogram(samples.head).toHistogram
      val acc = MutableHistogram(firstHist)
      samples.tail.foreach { buf =>
        BinaryHistogram.addValuesTo(buf, acc.values)
      }
      acc.makeMonotonic()
      val result = new ExpandableArrayBuffer(4096)
      acc.serialize(Some(result))

      bytesEqual(ref, result) shouldBe true
    }

    it("should produce identical output for HistFormat_Custom_XOR") {
      val N = 10
      val samples = (1 to N).map(i => makeXorBuf(customBuckets8, monotonicDoubleValues(i, 8)))
      val ref = referenceAggregate(samples)

      val firstHist = BinaryHistogram.BinHistogram(samples.head).toHistogram
      val acc = MutableHistogram(firstHist)
      samples.tail.foreach { buf =>
        BinaryHistogram.addValuesTo(buf, acc.values)
      }
      acc.makeMonotonic()
      val result = new ExpandableArrayBuffer(4096)
      acc.serialize(Some(result))

      bytesEqual(ref, result) shouldBe true
    }

    it("should produce identical output for HistFormat_OtelExp_XOR") {
      val N = 10
      val numBuckets = otelBuckets.numBuckets
      val samples = (1 to N).map(i => makeXorBuf(otelBuckets, monotonicDoubleValues(i, numBuckets)))
      val ref = referenceAggregate(samples)

      val firstHist = BinaryHistogram.BinHistogram(samples.head).toHistogram
      val acc = MutableHistogram(firstHist)
      samples.tail.foreach { buf =>
        BinaryHistogram.addValuesTo(buf, acc.values)
      }
      acc.makeMonotonic()
      val result = new ExpandableArrayBuffer(4096)
      acc.serialize(Some(result))

      bytesEqual(ref, result) shouldBe true
    }

    // --- Deferred monotonic correction ---

    it("should produce monotonic result for non-monotonic merged buckets") {
      // Buckets where individual samples are monotonic, but sum may temporarily violate monotonicity
      val buckets = CustomBuckets(Array(1.0, 2.0, 4.0, Double.PositiveInfinity))
      val sample1 = makeDeltaBuf(buckets, Array(100L, 50L, 200L, 300L))  // NOT monotonic: 100, 50, 200, 300
      val sample2 = makeDeltaBuf(buckets, Array(10L, 20L, 30L, 40L))

      val firstHist = BinaryHistogram.BinHistogram(sample1).toHistogram
      val acc = MutableHistogram(firstHist)
      BinaryHistogram.addValuesTo(sample2, acc.values)
      acc.makeMonotonic()

      // After makeMonotonic, values should be non-decreasing
      (0 until acc.numBuckets - 1).foreach { b =>
        acc.bucketValue(b) should be <= acc.bucketValue(b + 1)
      }
    }

    // --- Schema mismatch rejection ---

    it("should throw IllegalArgumentException on bucket count mismatch") {
      val smallBuckets = GeometricBuckets(1.0, 2.0, 4)
      val largeBuckets = GeometricBuckets(1.0, 2.0, 8)

      val smallBuf = makeDeltaBuf(smallBuckets, monotonicLongValues(1, 4))

      val firstHist = BinaryHistogram.BinHistogram(
        makeDeltaBuf(largeBuckets, monotonicLongValues(1, 8))
      ).toHistogram
      val acc = MutableHistogram(firstHist)

      intercept[IllegalArgumentException] {
        BinaryHistogram.addValuesTo(smallBuf, acc.values)
      }
    }

    // --- First-sample cold path ---

    it("should initialize accumulator with correct bucket scheme on first sample") {
      val agg = new HistogramAggregator
      val buf = makeDeltaBuf(geoNoBuckets, monotonicLongValues(1, 8))
      agg.add(buf)

      val accOpt = agg.getAccumulator
      accOpt shouldBe defined
      accOpt.get.numBuckets shouldEqual 8
      accOpt.get.bucketValue(0) shouldEqual 10.0  // sampleIdx=1 → base=10, first bucket=10+0*5=10
    }
  }

  describe("HistogramAggregator fast path integration") {

    it("should aggregate multiple delta-format DirectBuffers correctly") {
      val agg = new HistogramAggregator
      val N = 10
      val samples = (1 to N).map(i => makeDeltaBuf(geoNoBuckets, monotonicLongValues(i, 8)))
      samples.foreach(agg.add)

      val result = agg.result().asInstanceOf[DirectBuffer]
      val resultHist = BinaryHistogram.BinHistogram(result).toHistogram

      // Verify summed values: for bucket b, sum over i=1..10 of (i*10 + b*5)
      // = 10*(1+2+...+10) + b*5*10 = 550 + 50b
      (0 until 8).foreach { b =>
        resultHist.bucketValue(b) shouldEqual (550.0 + 50.0 * b)
      }
    }

    it("should aggregate multiple XOR-format DirectBuffers correctly") {
      val agg = new HistogramAggregator
      val N = 10
      val samples = (1 to N).map(i => makeXorBuf(geoNoBuckets, monotonicDoubleValues(i, 8)))
      samples.foreach(agg.add)

      val result = agg.result().asInstanceOf[DirectBuffer]
      val resultHist = BinaryHistogram.BinHistogram(result).toHistogram

      (0 until 8).foreach { b =>
        resultHist.bucketValue(b) shouldEqual (550.0 + 50.0 * b)
      }
    }

    it("should defer makeMonotonic until result() and still produce monotonic output") {
      val agg = new HistogramAggregator
      val buckets = CustomBuckets(Array(1.0, 2.0, 4.0, Double.PositiveInfinity))
      // Non-monotonic values: bucket 1 < bucket 0
      agg.add(makeDeltaBuf(buckets, Array(100L, 50L, 200L, 300L)))
      agg.add(makeDeltaBuf(buckets, Array(10L, 20L, 30L, 40L)))

      val result = agg.result().asInstanceOf[DirectBuffer]
      val resultHist = BinaryHistogram.BinHistogram(result).toHistogram

      (0 until resultHist.numBuckets - 1).foreach { b =>
        resultHist.bucketValue(b) should be <= resultHist.bucketValue(b + 1)
      }
    }

    it("should handle mixed delta and XOR format samples in same aggregator") {
      val agg = new HistogramAggregator
      val deltaSample = makeDeltaBuf(geoNoBuckets, monotonicLongValues(1, 8))
      val xorSample = makeXorBuf(geoNoBuckets, monotonicDoubleValues(2, 8))

      agg.add(deltaSample)
      agg.add(xorSample)

      val result = agg.result().asInstanceOf[DirectBuffer]
      val resultHist = BinaryHistogram.BinHistogram(result).toHistogram

      // sum of sample 1 (base=10) and sample 2 (base=20) for each bucket b: (10+b*5) + (20+b*5) = 30+10b
      (0 until 8).foreach { b =>
        resultHist.bucketValue(b) shouldEqual (30.0 + 10.0 * b)
      }
    }
  }

  describe("HistogramLastAggregator") {
    it("should keep last histogram without timestamp") {
      val agg = new HistogramLastAggregator
      val hist1 = createHistogramBuffer(Seq((1.0, 5L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 99L)))

      agg.add(hist1)
      agg.add(hist2)

      // Last added should win
      val resultAgg = agg.asInstanceOf[HistogramLastAggregator]
      resultAgg.getCurrentHistogram shouldBe defined
      resultAgg.getCurrentHistogram.get.bucketValue(0) shouldEqual 99.0
    }

    it("should keep histogram with latest timestamp") {
      val agg = new HistogramLastAggregator
      val hist1 = createHistogramBuffer(Seq((1.0, 10L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 20L)))
      val hist3 = createHistogramBuffer(Seq((1.0, 30L)))

      agg.addWithTimestamp(hist1, 1000L)
      agg.addWithTimestamp(hist3, 3000L) // Latest
      agg.addWithTimestamp(hist2, 2000L) // Older, should not replace

      val resultAgg = agg.asInstanceOf[HistogramLastAggregator]
      resultAgg.getCurrentHistogram.get.bucketValue(0) shouldEqual 30.0
    }

    it("should replace histogram with equal timestamp") {
      val agg = new HistogramLastAggregator
      val hist1 = createHistogramBuffer(Seq((1.0, 10L)))
      val hist2 = createHistogramBuffer(Seq((1.0, 20L)))

      agg.addWithTimestamp(hist1, 1000L)
      agg.addWithTimestamp(hist2, 1000L) // Same timestamp, should update

      val resultAgg = agg.asInstanceOf[HistogramLastAggregator]
      resultAgg.getCurrentHistogram.get.bucketValue(0) shouldEqual 20.0
    }

    it("should return serialized buffer from result()") {
      val agg = new HistogramLastAggregator
      val hist = createHistogramBuffer(Seq((1.0, 5L)))
      agg.add(hist)

      val result = agg.result()
      result shouldBe a[DirectBuffer]
    }

    it("should return empty histogram for no data") {
      val agg = new HistogramLastAggregator
      val result = agg.result()
      result shouldBe a[DirectBuffer]
    }

    it("should reset correctly") {
      val agg = new HistogramLastAggregator
      val hist = createHistogramBuffer(Seq((1.0, 5L)))
      agg.add(hist)
      agg.reset()

      agg.asInstanceOf[HistogramLastAggregator].getCurrentHistogram shouldEqual None
    }

    it("should ignore non-histogram values") {
      val agg = new HistogramLastAggregator
      agg.add(42.0)
      agg.add("string")

      agg.asInstanceOf[HistogramLastAggregator].getCurrentHistogram shouldEqual None
    }
  }
}
