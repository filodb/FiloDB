package filodb.memory.format.vectors

import org.agrona.ExpandableArrayBuffer

object HistogramTest {
  val bucketScheme = GeometricBuckets(1.0, 2.0, 8)
  val rawHistBuckets = Seq(
    Array[Double](10, 15, 17, 20, 25, 34, 76, 82),
    Array[Double](6, 16, 26, 26, 36, 38, 56, 59),
    Array[Double](11, 16, 26, 27, 33, 42, 46, 55),
    Array[Double](4, 4, 5, 33, 35, 67, 91, 121)
  )
  val rawLongBuckets = rawHistBuckets.map(_.map(_.toLong))
  val mutableHistograms = rawHistBuckets.map { buckets =>
    MutableHistogram(bucketScheme, buckets)
  }

  val quantile50Result = Seq(37.333333333333336, 10.8, 8.666666666666666, 28.75)
}

import BinaryHistogram._

class HistogramTest extends NativeVectorTest {
  val writeBuf = new ExpandableArrayBuffer()

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
    }
  }

  import HistogramTest._

  describe("Histogram") {
    it("should calculate quantile correctly") {
      mutableHistograms.zip(quantile50Result).foreach { case (h, res) =>
        val quantile = h.quantile(0.50)
        info(s"For histogram ${h.values.toList} -> quantile = $quantile")
        quantile shouldEqual res
      }
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
      val hist = mutableHistograms(0)
      val hist2 = hist.copy
      hist shouldEqual hist2
      hist.add(mutableHistograms(1))
      hist should not equal (hist2)
      hist.values shouldEqual addedBuckets
    }
  }
}