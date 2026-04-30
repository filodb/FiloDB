package filodb.core.memstore.aggregation

import org.agrona.DirectBuffer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}

class AggregatorSpec extends AnyFunSpec with Matchers {

  // Helper to create a serialized histogram as DirectBuffer
  // Serialize into a fresh buffer each call to avoid shared BinaryHistogram.histBuf being overwritten
  private def createHistogramBuffer(bucketCounts: Seq[(Double, Long)]): DirectBuffer = {
    val boundaries = bucketCounts.map(_._1).toArray :+ Double.PositiveInfinity
    val counts = bucketCounts.map(_._2).toArray :+ 0L
    LongHistogram(CustomBuckets(boundaries), counts)
      .serialize(Some(new org.agrona.concurrent.UnsafeBuffer(new Array[Byte](4096))))
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

    it("should create independent copies") {
      val agg1 = new SumAggregator
      agg1.add(10.0)
      val agg2 = agg1.copy()
      agg2.add(20.0)
      agg1.result() shouldEqual 10.0
      agg2.result() shouldEqual 20.0
    }
  }

  describe("AvgAggregator") {
    it("should compute average correctly") {
      val agg = new AvgAggregator
      agg.add(10.0)
      agg.add(20.0)
      agg.add(30.0)
      agg.result() shouldEqual 20.0
    }

    it("should handle single value") {
      val agg = new AvgAggregator
      agg.add(42.0)
      agg.result() shouldEqual 42.0
    }

    it("should return NaN for empty aggregator") {
      val agg = new AvgAggregator
      agg.result().asInstanceOf[Double].isNaN shouldEqual true
    }

    it("should ignore NaN values") {
      val agg = new AvgAggregator
      agg.add(10.0)
      agg.add(Double.NaN)
      agg.add(20.0)
      agg.result() shouldEqual 15.0
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
      Aggregator.create(AggregationType.Avg) shouldBe a[AvgAggregator]
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
      AggregationType.parse("avg") shouldEqual Some(AggregationType.Avg)
      AggregationType.parse("average") shouldEqual Some(AggregationType.Avg)
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
      AggregationType.parse("Avg") shouldEqual Some(AggregationType.Avg)
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

    it("should create independent copies") {
      val agg = new HistogramAggregator
      val copy = agg.copy()
      copy shouldBe a[HistogramAggregator]
      copy should not be theSameInstanceAs(agg)
    }

    it("should ignore non-histogram values") {
      val agg = new HistogramAggregator
      agg.add(42.0)
      agg.add("string")
      agg.add(null)

      agg.asInstanceOf[HistogramAggregator].getAccumulator shouldEqual None
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

    it("should create independent copies") {
      val agg = new HistogramLastAggregator
      val copy = agg.copy()
      copy shouldBe a[HistogramLastAggregator]
      copy should not be theSameInstanceAs(agg)
    }

    it("should ignore non-histogram values") {
      val agg = new HistogramLastAggregator
      agg.add(42.0)
      agg.add("string")

      agg.asInstanceOf[HistogramLastAggregator].getCurrentHistogram shouldEqual None
    }
  }

  describe("AggregationConfig") {
    it("should create valid config") {
      val config = AggregationConfig(1, AggregationType.Sum, 30000L, 60000L)
      config.columnIndex shouldEqual 1
      config.aggType shouldEqual AggregationType.Sum
      config.intervalMs shouldEqual 30000L
      config.oooToleranceMs shouldEqual 60000L
    }

    it("should validate interval > 0") {
      intercept[IllegalArgumentException] {
        AggregationConfig(1, AggregationType.Sum, 0L, 60000L)
      }
    }

    it("should validate tolerance >= 0") {
      intercept[IllegalArgumentException] {
        AggregationConfig(1, AggregationType.Sum, 30000L, -1L)
      }
    }

    it("should validate column index >= 0") {
      intercept[IllegalArgumentException] {
        AggregationConfig(-1, AggregationType.Sum, 30000L, 60000L)
      }
    }
  }
}
