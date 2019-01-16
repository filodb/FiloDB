package filodb.memory.format.vectors

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

class HistogramTest extends NativeVectorTest {
  describe("HistogramBuckets") {
    it("can list out bucket definition LE values properly for Geometric and Geometric_1") {
      val buckets1 = GeometricBuckets(5.0, 3.0, 4)
      buckets1.allBucketTops shouldEqual Array(5.0, 15.0, 45.0, 135.0)

      val buckets2 = GeometricBuckets_1(2.0, 2.0, 8)
      buckets2.allBucketTops shouldEqual Array(1.0, 3.0, 7.0, 15.0, 31.0, 63.0, 127.0, 255.0)
    }

    it("can serialize and deserialize properly") {
      val buckets1 = GeometricBuckets(5.0, 2.0, 4)
      HistogramBuckets(buckets1.toByteArray) shouldEqual buckets1

      val buckets2 = GeometricBuckets_1(2.0, 2.0, 8)
      HistogramBuckets(buckets2.toByteArray) shouldEqual buckets2
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
  }
}