package filodb.memory.format.vectors

import org.agrona.ExpandableArrayBuffer

object HistogramTest {
  val bucketScheme = GeometricBuckets(1.0, 2.0, 8)
  val customScheme = CustomBuckets(Array(0.25, 0.5, 1.0, 2.5, 5.0, 10, Double.PositiveInfinity))
  val rawHistBuckets = Seq(
    Array[Double](10, 15, 17, 20, 25, 34, 76, 82),
    Array[Double](6, 16, 26, 26, 36, 38, 56, 59),
    Array[Double](11, 16, 26, 27, 33, 42, 46, 55),
    Array[Double](4, 4, 5, 33, 35, 67, 91, 121)
  )
  val rawLongBuckets = rawHistBuckets.map(_.map(_.toLong))
  val mutableHistograms = rawHistBuckets.map { buckets =>
    // Create a new copy here, so that mutations don't affect original array
    MutableHistogram(bucketScheme, buckets.toList.toArray)
  }

  val incrHistBuckets = rawHistBuckets.scanLeft(Array.fill(8)(0.0)) { case (acc, h) =>
                          acc.zip(h).map { case (a, b) => a + b }
                        }.drop(1)

  val customHistograms = rawHistBuckets.map { buckets =>
    LongHistogram(customScheme, buckets.take(customScheme.numBuckets).map(_.toLong))
  }

  val correction1 = LongHistogram(bucketScheme, Array(1, 2, 3, 4, 5, 6, 7, 8))
  val correction2 = LongHistogram(bucketScheme, Array(2, 4, 6, 8, 10, 12, 14, 18))

  val quantile50Result = Seq(37.333333333333336, 10.8, 8.666666666666666, 28.75)
}

import BinaryHistogram._

class HistogramTest extends NativeVectorTest {
  val writeBuf = new ExpandableArrayBuffer()

  import HistogramTest._

  describe("HistogramBuckets") {
    it("can list out bucket definition LE values properly for Geometric and Geometric_1") {
      val buckets1 = GeometricBuckets(5.0, 3.0, 4)
      buckets1.allBucketTops shouldEqual Array(5.0, 15.0, 45.0, 135.0)

      val buckets2 = GeometricBuckets(2.0, 2.0, 8, minusOne = true)
      buckets2.allBucketTops shouldEqual Array(1.0, 3.0, 7.0, 15.0, 31.0, 63.0, 127.0, 255.0)
    }

    it("can serialize and deserialize properly") {
      val buckets1 = GeometricBuckets(5.0, 2.0, 4)
      buckets1.serialize(writeBuf, 0) shouldEqual 2+2+8+8
      HistogramBuckets(writeBuf, HistFormat_Geometric_Delta) shouldEqual buckets1

      val buckets2 = GeometricBuckets(2.0, 2.0, 8, minusOne = true)
      buckets2.serialize(writeBuf, 0) shouldEqual 2+2+8+8
      HistogramBuckets(writeBuf, HistFormat_Geometric1_Delta) shouldEqual buckets2

      customScheme.serialize(writeBuf, 0) shouldEqual 26
      HistogramBuckets(writeBuf, HistFormat_Custom_Delta) shouldEqual customScheme
    }
  }

  describe("Histogram") {
      it("should add two histograms with the same bucket scheme correctly") {
        val hist1 = LongHistogram(bucketScheme, Array(1, 2, 3, 4, 5, 6, 7, 8))
        val hist2 = LongHistogram(bucketScheme, Array(2, 4, 6, 8, 10, 12, 14, 18))
        hist1.add(hist2)

        hist1.values shouldEqual Array(3, 6, 9, 12, 15, 18, 21, 26)
      }

      it("should not add histograms with different bucket schemes") {
        val hist1 = LongHistogram(bucketScheme, Array(1, 2, 3, 4, 5, 6, 7, 8))
        val histWithDifferentBuckets = LongHistogram(customScheme, Array(1, 2, 3, 4, 5, 6, 7))

        val thrown = intercept[IllegalArgumentException] {
          hist1.add(histWithDifferentBuckets)
        }

        thrown.getMessage shouldEqual s"Mismatch in bucket sizes. Cannot add histograms with different bucket configurations. " +
          s"Expected: ${hist1.buckets}, Found: ${histWithDifferentBuckets.buckets}"
      }
    it("should calculate quantile correctly") {
      mutableHistograms.zip(quantile50Result).foreach { case (h, res) =>
        val quantile = h.quantile(0.50)
        info(s"For histogram ${h.values.toList} -> quantile = $quantile")
        quantile shouldEqual res
      }

      // Cannot return anything more than 2nd-to-last bucket (ie 64)
      mutableHistograms(0).quantile(0.95) shouldEqual 64
    }

    it("should calculate more accurate quantile with MaxMinHistogram using max column") {
      val h = MaxMinHistogram(mutableHistograms(0), 90)
      h.quantile(0.95) shouldEqual 72.2 +- 0.1   // more accurate due to max!

      // Not just last bucket, but should always be clipped at max regardless of bucket scheme
      val values = Array[Double](10, 15, 17, 20, 25, 25, 25, 25)
      val h2 = MaxMinHistogram(MutableHistogram(bucketScheme, values), 10)
      h2.quantile(0.95) shouldEqual 9.5 +- 0.1   // more accurate due to max!

      val values3 = Array[Double](1, 1, 1, 1, 1, 4, 7, 7, 9, 9) ++ Array.fill(54)(12.0)
      val h3 = MaxMinHistogram(MutableHistogram(HistogramBuckets.binaryBuckets64, values3), 1617.0)
      h3.quantile(0.99) shouldEqual 1593.2 +- 0.1
      h3.quantile(0.90) shouldEqual 1379.4 +- 0.1
    }

    it("should calculate more accurate quantile with MaxMinHistogram using max and min column") {
      val h = MaxMinHistogram(mutableHistograms(0), 100, 10)
      h.quantile(0.95) shouldEqual 75.39 +- 0.1 // more accurate due to max, min!

      // Not just last bucket, but should always be clipped at max regardless of bucket scheme
      val values = Array[Double](10, 15, 17, 20, 25, 25, 25, 25)
      val h2 = MaxMinHistogram(MutableHistogram(bucketScheme, values), 10, 2)
      h2.quantile(0.95) shouldEqual 9.5 +- 0.1 // more accurate due to max, min!

      val values3 = Array[Double](1, 1, 1, 1, 1, 4, 7, 7, 9, 9) ++ Array.fill(54)(12.0)
      val h3 = MaxMinHistogram(MutableHistogram(HistogramBuckets.binaryBuckets64, values3), 1617.0, 1.0)
      h3.quantile(0.99) shouldEqual 1593.2 +- 0.1
      h3.quantile(0.90) shouldEqual 1379.4 +- 0.1
      h3.quantile(0.01) shouldEqual 1.0 +- 0.1 // must use the starting reference from min
    }

    it("should serialize to and from BinaryHistograms and compare correctly") {
      val binHistograms = mutableHistograms.map { h =>
        val buf = new ExpandableArrayBuffer()
        h.serialize(Some(buf))
      }

      binHistograms.zip(mutableHistograms).foreach { case (binHistBuf, mutHist) =>
        val binHist = BinaryHistogram.BinHistogram(binHistBuf).toHistogram
        binHist shouldEqual mutHist
        binHist.hashCode shouldEqual mutHist.hashCode
        println(binHist)
      }
    }

    it("should serialize to and from BinHistograms for MutableHistograms with doubles") {
      val dblHist = MutableHistogram(mutableHistograms.head.buckets,
                                     mutableHistograms.head.values.map(_ + .5))
      val buf = new ExpandableArrayBuffer()
      dblHist.serialize(Some(buf))

      val deserHist = BinaryHistogram.BinHistogram(buf).toHistogram
      deserHist shouldEqual dblHist
    }

    it("should serialize to and from BinaryHistograms with custom buckets") {
      val longHist = LongHistogram(customScheme, Array[Long](10, 15, 17, 20, 25, 34, 76))
      val buf = new ExpandableArrayBuffer()
      longHist.serialize(Some(buf))

      val hist2 = BinaryHistogram.BinHistogram(buf).toHistogram
      hist2 shouldEqual longHist
      hist2.hashCode shouldEqual longHist.hashCode
    }

    it("should serialize to and from an empty Histogram") {
      val binEmptyHist = BinaryHistogram.BinHistogram(Histogram.empty.serialize())
      binEmptyHist.numBuckets shouldEqual 0
      binEmptyHist.toHistogram shouldEqual Histogram.empty
      Histogram.empty.toString shouldEqual "{}"
    }

    it("should compare different histograms correctly") {
      mutableHistograms(0) shouldEqual mutableHistograms(0)
      mutableHistograms(0) should not equal ("boofoo")

      mutableHistograms(0).compare(mutableHistograms(1)) should be > 0
    }

    it("should copy and not be affected by mutation to original") {
      val addedBuckets = rawHistBuckets(0).zip(rawHistBuckets(1)).map { case(a,b) => a + b }.toArray
      val hist = mutableHistograms(0).copy
      val hist2 = hist.copy
      hist shouldEqual hist2
      hist.add(mutableHistograms(1))
      hist should not equal (hist2)
      hist.values shouldEqual addedBuckets
    }

    it("should correctly add & makeMonotonic histograms containing NaNs") {
      val hist1 = mutableHistograms(0).copy.asInstanceOf[MutableHistogram]
      val histWNans = mutableHistograms(1)
      histWNans.values(0) = Double.NaN
      histWNans.values(2) = Double.NaN

      hist1.add(histWNans)
      var current = 0d
      hist1.valueArray.foreach { d =>
        d should be >= (current)
        current = d
      }
    }

    // Test this later when different schemas are supported
    ignore("should add histogram w/ diff bucket scheme and result in monotonically increasing histogram") {
      val hist1 = mutableHistograms(0).copy.asInstanceOf[MutableHistogram]

      val scheme2 = GeometricBuckets(2.0, 6.0, 3)
      val hist2 = LongHistogram(scheme2, Array(10L, 20L, 40L))

      hist1.add(hist2)
      hist1.makeMonotonic()

      hist1.buckets shouldEqual bucketScheme   // scheme does not change - for now

      // New buckets should be increasing, and values should not be less than before
      var current = 0d
      hist1.valueArray.zipWithIndex.foreach { case (d, i) =>
        d should be >= (current)
        d should be >= rawHistBuckets(0)(i)
        current = d
      }
    }

    it("should create OTel exponential buckets and add them correctly") {
      val b1 = OTelExpHistogramBuckets(3, -5, 5) // 0.707 to 1.68
      b1.numBuckets shouldEqual 11
      b1.startBucketTop shouldBe 0.7071067811865475 +- 0.0001
      b1.endBucketTop shouldBe 1.6817928305074294 +- 0.0001

      b1.bucketIndexToArrayIndex(-5) shouldEqual 0
      b1.bucketIndexToArrayIndex(5) shouldEqual 10
      b1.bucketTop(b1.bucketIndexToArrayIndex(-1)) shouldEqual 1.0

      val b2 = OTelExpHistogramBuckets(2, -2, 4) // 0.8408 to 2.378
      val b3 = OTelExpHistogramBuckets(2, -4, 4) // 0.594 to 2.378
      b1.canAccommodate(b2) shouldEqual false
      b2.canAccommodate(b1) shouldEqual false
      b3.canAccommodate(b1) shouldEqual true
      b3.canAccommodate(b2) shouldEqual true

      val bAdd = b1.add(b2)
      bAdd.numBuckets shouldEqual 9
      bAdd.scale shouldEqual 2
      bAdd.startBucketTop shouldBe 0.5946035575013606 +- 0.0001
      bAdd.endBucketTop shouldBe 2.378414245732675 +- 0.0001
      bAdd.canAccommodate(b1) shouldEqual true
      bAdd.canAccommodate(b2) shouldEqual true

      val bAddValues = new Array[Double](bAdd.numBuckets)
      bAdd.addValues(bAddValues, b1, MutableHistogram(b1, Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)))
      bAddValues.toSeq shouldEqual Seq(0.0, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 11.0, 11.0)
      bAdd.addValues(bAddValues, b2, MutableHistogram(b2, Array(1, 2, 3, 4, 5, 6, 7)))
      bAddValues.toSeq shouldEqual Seq(0.0, 1.0, 4.0, 7.0, 10.0, 13.0, 16.0, 17.0, 18.0)

      val b4 = OTelExpHistogramBuckets(5, 15, 50) // 1.414 to 3.018
      b4.numBuckets shouldEqual 36
      b4.startBucketTop shouldBe 1.414213562373094 +- 0.0001
      b4.endBucketTop shouldBe 3.0183288551868377 +- 0.0001

      val b5 = OTelExpHistogramBuckets(3, 10, 15)
      b5.numBuckets shouldEqual 6
      b5.startBucketTop shouldBe 2.59367910930202 +- 0.0001
      b5.endBucketTop shouldBe 4.000000000000002 +- 0.0001

      val bAdd2 = bAdd.add(b4).add(b5)
      val bAdd2Values = new Array[Double](bAdd2.numBuckets)
      bAdd2.addValues(bAdd2Values, bAdd, MutableHistogram(bAdd, bAddValues))
      bAdd2Values.toSeq shouldEqual Seq(0.0, 1.0, 4.0, 7.0, 10.0, 13.0, 16.0, 17.0, 18.0, 18.0, 18.0, 18.0, 18.0)

      bAdd2.addValues(bAdd2Values, b4, MutableHistogram(b4, (0 until 36 map (i => i.toDouble)).toArray))
      bAdd2Values.toSeq shouldEqual Seq(0.0, 1.0, 4.0, 7.0, 10.0, 13.0, 24.0, 33.0, 42.0, 50.0, 53.0, 53.0, 53.0)

      bAdd2.addValues(bAdd2Values, b5, MutableHistogram(b5, Array(10.0, 11, 12, 13, 14, 15)))
      bAdd2Values.toSeq shouldEqual Seq(0.0, 1.0, 4.0, 7.0, 10.0, 13.0, 24.0, 33.0, 42.0, 61.0, 66.0, 68.0, 68.0)

    }

    it("should create non-overlapping OTel exponential buckets and add them correctly") {
      val b1 = OTelExpHistogramBuckets(3, -5, 5)
      val b2 = OTelExpHistogramBuckets(3, 15, 25)
      val b3 = b1.add(b2)
      b3.numBuckets shouldEqual 31
      b3.startIndexPositiveBuckets shouldEqual -5
      b3.endIndexPositiveBuckets shouldEqual 25
    }

  }
}