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

  val rawHistBuckets2 = Seq(
    Array[Double](0, 15, 17, 20, 25, 34, 76, 82),
    Array[Double](0, 16, 26, 26, 36, 38, 56, 59),
    Array[Double](0, 16, 26, 27, 33, 42, 46, 55),
    Array[Double](0, 4, 5, 33, 35, 67, 91, 121)
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

  val otelExpBuckets = Base2ExpHistogramBuckets(3, -3, 7)
  val otelExpHistograms = rawHistBuckets2.map { buckets =>
    LongHistogram(otelExpBuckets, buckets.take(otelExpBuckets.numBuckets).map(_.toLong))
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

      val buckets3 = Base2ExpHistogramBuckets(3, -5, 16)
      buckets3.serialize(writeBuf, 0) shouldEqual 18
      HistogramBuckets(writeBuf, HistFormat_OtelExp_Delta) shouldEqual buckets3

      // bugfix: accommodate large offsets that exceed short-int
      val buckets4 = Base2ExpHistogramBuckets(3, -9037032, 150)
      buckets4.serialize(writeBuf, 0) shouldEqual 18
      HistogramBuckets(writeBuf, HistFormat_OtelExp_Delta) shouldEqual buckets4

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

      thrown.getMessage shouldEqual s"Cannot add histograms with different bucket configurations. " +
        s"Expected: ${hist1.buckets}, Found: ${histWithDifferentBuckets.buckets}"
    }

    it("should calculate quantile correctly for custom and geometric bucket histograms") {
      mutableHistograms.zip(quantile50Result).foreach { case (h, res) =>
        val quantile = h.quantile(0.50)
        info(s"For histogram ${h.values.toList} -> quantile = $quantile")
        quantile shouldEqual res
      }

      // Cannot return anything more than 2nd-to-last bucket (ie 64) when last bucket is infinity
      customHistograms(0).quantile(0.95) shouldEqual 10
    }

    it("should calculate quantile correctly for exponential bucket histograms") {
      val bucketScheme = Base2ExpHistogramBuckets(3, -5, 11) // 0.707 to 1.68
      val hist = MutableHistogram(bucketScheme, Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

      // multiple buckets with interpolation
      hist.quantile(0.5) shouldEqual 1.0 +- 0.00001
      hist.quantile(0.75) shouldEqual 1.2968395546510099 +- 0.00001
      hist.quantile(0.25) shouldEqual 0.7711054127039704 +- 0.00001
      hist.quantile(0.99) shouldEqual 1.6643974694230492 +- 0.00001
      hist.quantile(0.01) shouldEqual 0.0 // zero bucket
      hist.quantile(0.085) shouldEqual 0.014142135623730961 +- 0.00001
    }

    it("quantile for exponential histogram with real data should match expected") {
      val bucketScheme = Base2ExpHistogramBuckets(3, -78, 126)
      val str = "0.0=0.0, 0.0012664448775888738=0.0, 0.0013810679320049727=0.0, 0.001506065259187439=0.0, " +
        "0.0016423758110424079=0.0, 0.0017910235218841198=0.0, 0.001953124999999996=0.0, 0.002129897915361827=0.0, " +
        "0.002322670146489685=0.0, 0.0025328897551777484=0.0, 0.002762135864009946=0.0, 0.0030121305183748786=0.0, " +
        "0.0032847516220848166=0.0, 0.0035820470437682404=0.0, 0.003906249999999993=0.0, 0.004259795830723655=0.0, " +
        "0.004645340292979371=0.0, 0.005065779510355498=0.0, 0.005524271728019893=0.0, 0.006024261036749759=0.0, " +
        "0.006569503244169634=0.0, 0.007164094087536483=0.0, 0.007812499999999988=0.0, 0.008519591661447312=0.0, " +
        "0.009290680585958744=0.0, 0.010131559020710997=0.0, 0.011048543456039788=0.0, 0.012048522073499521=0.0, " +
        "0.013139006488339272=0.0, 0.014328188175072969=0.0, 0.01562499999999998=0.0, 0.017039183322894627=0.0, " +
        "0.018581361171917492=0.0, 0.020263118041422=0.0, 0.022097086912079584=0.0, 0.024097044146999046=0.0, " +
        "0.026278012976678547=0.0, 0.028656376350145944=0.0, 0.031249999999999965=0.0, 0.03407836664578927=0.0, " +
        "0.037162722343835=0.0, 0.04052623608284401=0.0, 0.044194173824159175=0.0, 0.0481940882939981=0.0, " +
        "0.05255602595335711=0.0, 0.0573127527002919=0.0, 0.062499999999999944=0.0, 0.06815673329157855=0.0, " +
        "0.07432544468767001=0.0, 0.08105247216568803=0.0, 0.08838834764831838=0.0, 0.09638817658799623=0.0, " +
        "0.10511205190671424=0.0, 0.11462550540058382=0.0, 0.12499999999999992=0.0, 0.13631346658315713=1.0, " +
        "0.14865088937534005=1.0, 0.16210494433137612=1.0, 0.17677669529663678=1.0, 0.1927763531759925=1.0, " +
        "0.21022410381342854=1.0, 0.2292510108011677=1.0, 0.2499999999999999=2.0, 0.2726269331663143=2.0, " +
        "0.29730177875068015=3.0, 0.3242098886627523=3.0, 0.3535533905932736=3.0, 0.3855527063519851=3.0, " +
        "0.42044820762685714=4.0, 0.4585020216023355=5.0, 0.4999999999999999=5.0, 0.5452538663326287=5.0, " +
        "0.5946035575013604=6.0, 0.6484197773255047=6.0, 0.7071067811865475=8.0, 0.7711054127039704=8.0, " +
        "0.8408964152537145=9.0, 0.9170040432046712=9.0, 1.0=11.0, 1.0905077326652577=12.0, 1.189207115002721=14.0, " +
        "1.2968395546510099=15.0, 1.4142135623730951=17.0, 1.542210825407941=19.0, 1.6817928305074294=20.0, " +
        "1.8340080864093429=22.0, 2.0000000000000004=23.0, 2.181015465330516=26.0, 2.378414230005443=28.0, " +
        "2.59367910930202=31.0, 2.8284271247461907=34.0, 3.084421650815883=37.0, 3.3635856610148593=41.0, " +
        "3.6680161728186866=45.0, 4.000000000000002=48.0, 4.3620309306610325=53.0, 4.756828460010887=58.0, " +
        "5.187358218604041=64.0, 5.656854249492383=70.0, 6.168843301631767=76.0, 6.7271713220297205=84.0, " +
        "7.336032345637374=90.0, 8.000000000000005=99.0, 8.724061861322067=108.0, 9.513656920021775=118.0, " +
        "10.374716437208086=129.0, 11.31370849898477=140.0, 12.337686603263537=152.0, 13.454342644059444=167.0, " +
        "14.672064691274752=182.0, 16.000000000000014=199.0, 17.448123722644137=217.0, 19.027313840043554=237.0, " +
        "20.749432874416176=258.0, 22.627416997969544=282.0, 24.675373206527077=308.0, 26.908685288118896=336.0, " +
        "29.34412938254951=367.0, 32.000000000000036=400.0, 34.89624744528828=435.0, 38.05462768008712=474.0, " +
        "41.49886574883236=517.0, 45.254833995939094=565.0, 49.35074641305417=617.0, 53.8173705762378=672.0, " +
        "58.688258765099036=732.0, 64.00000000000009=749.0"
      val counts = str.split(", ").map { s =>
        val kv = s.split("=")
        kv(1).toDouble
      }
      val hist = MutableHistogram(bucketScheme, counts)
      hist.quantile(0.5) shouldEqual 29.927691427444305 +- 0.00001
      hist.quantile(0.99) shouldEqual 61.602904581469566 +- 0.00001
      hist.quantile(0.01) shouldEqual 0.6916552392692796 +- 0.00001
      hist.quantile(0.99, min=0.03, max=59.87) shouldEqual 59.34643429268522 +- 0.00001
    }

    it("should calculate histogram_fraction correctly for exponential histograms using exponential interpolation") {
      val bucketScheme = Base2ExpHistogramBuckets(3, -5, 11) // 0.707 to 1.68
      val hist = MutableHistogram(bucketScheme, Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

      // multiple buckets with interpolation
      hist.histogramFraction(0.8, 1.2) shouldEqual 0.3899750004807707 +- 0.00001

      // multiple buckets with interpolation using min and max leads to improved accuracy
      hist.histogramFraction(0.8, 1.2, 0.76, 1.21) shouldEqual 0.42472117149283567 +- 0.00001

      // multiple buckets without interpolation
      hist.histogramFraction(bucketScheme.bucketTop(3),
        bucketScheme.bucketTop(7)) shouldEqual ((hist.bucketValue(7) - hist.bucketValue(3)) / hist.topBucketValue) +- 0.00001

      // zero bucket
      hist.histogramFraction(0, 0) shouldEqual 0.0833333 +- 0.00001

      // beyond last bucket
      hist.histogramFraction(2.0, 2.1) shouldEqual 0.0

      // one bucket
      hist.histogramFraction(1.0, 1.09) shouldEqual 0.08288542333480116 +- 0.00001

      // all buckets
      hist.histogramFraction(0, 2) shouldEqual 1.0
    }

    it("should calculate histogram_fraction correctly for custom bucket histograms using linear interpolation") {
      val bucketScheme = CustomBuckets(Array(1,2,3,4,5,6,7,8,9,10,11,Double.PositiveInfinity))
      val hist = MutableHistogram(bucketScheme, Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

      // multiple buckets with linear interpolation
      hist.histogramFraction(4.5, 6.6) shouldEqual 0.175 +- 0.00001

      // multiple buckets with interpolation using min and max leads to improved accuracy
      hist.histogramFraction(4.5, 6.6, 4.1, 6.8) shouldEqual 0.19212962962962962 +- 0.00001

      // multiple buckets without interpolation
      hist.histogramFraction(bucketScheme.bucketTop(3),
        bucketScheme.bucketTop(7)) shouldEqual ((hist.bucketValue(7) - hist.bucketValue(3)) / hist.topBucketValue) +- 0.00001

      // beyond last bucket
      hist.histogramFraction(11.1, 12.1) shouldEqual 0.0

      // one bucket
      hist.histogramFraction(1.0, 1.09) shouldEqual 0.0075 +- 0.00001

      // all buckets
      hist.histogramFraction(0, Double.PositiveInfinity) shouldEqual 1.0
    }

    it("should calculate more accurate quantile with MaxMinHistogram using max column") {
      val h = mutableHistograms(0)
      h.quantile(0.95, 0 ,90) shouldEqual 72.2 +- 0.1   // more accurate due to max!

      // Not just last bucket, but should always be clipped at max regardless of bucket scheme
      val values = Array[Double](10, 15, 17, 20, 25, 25, 25, 25)
      val h2 = MutableHistogram(bucketScheme, values)
      h2.quantile(0.95, 0, 10) shouldEqual 9.5 +- 0.1   // more accurate due to max!

      val values3 = Array[Double](1, 1, 1, 1, 1, 4, 7, 7, 9, 9) ++ Array.fill(54)(12.0)
      val h3 = MutableHistogram(HistogramBuckets.binaryBuckets64, values3)
      h3.quantile(0.99, 0, 1617.0) shouldEqual 1593.2 +- 0.1
      h3.quantile(0.90, 0, 1617.0) shouldEqual 1379.4 +- 0.1
    }

    it("should calculate more accurate quantile with MaxMinHistogram using max and min column") {
      val h = mutableHistograms(0)
      h.quantile(0.95, 10, 100) shouldEqual 75.39 +- 0.1 // more accurate due to max, min!

      // Not just last bucket, but should always be clipped at max regardless of bucket scheme
      val values = Array[Double](10, 15, 17, 20, 25, 25, 25, 25)
      val h2 = MutableHistogram(bucketScheme, values)
      h2.quantile(0.95, 2, 10) shouldEqual 9.5 +- 0.1 // more accurate due to max, min!

      val values3 = Array[Double](1, 1, 1, 1, 1, 4, 7, 7, 9, 9) ++ Array.fill(54)(12.0)
      val h3 = MutableHistogram(HistogramBuckets.binaryBuckets64, values3)
      h3.quantile(0.99) shouldEqual 2006.0399999999995 +- 0.0001 // without min/max
      h3.quantile(0.99, 0, 0) shouldEqual 2006.0399999999995 +- 0.0001 // with potentially wrong min max
      h3.quantile(0.99, 1.0, 1617.0) shouldEqual 1593.2 +- 0.1
      h3.quantile(0.90, 1.0, 1617.0) shouldEqual 1379.4 +- 0.1
      h3.quantile(0.01, 1.0, 1617.0) shouldEqual 1.0 +- 0.1 // must use the starting reference from min
    }

    it("should calculate more accurate quantile for otel exponential histogram using max and min column") {
      // bucket boundaries for scale 3, range -3 to 3, 7 buckets
      // zeroBucket: 0.0 -3: 0.840896, -2: 0.917004, -1: 1.000000, 0; 1.090508, 1: 1.189207, 2: 1.296840, 3: 1.414214
      val expected95thWithoutMinMax = Seq(1.3329136609345256, 1.2987136172035367, 1.3772644080792311, 1.3897175222720994)
      otelExpHistograms.map { h =>
        h.quantile(0.95)
      }.zip(expected95thWithoutMinMax).foreach { case (q, e) => q shouldEqual e +- 0.0001 }

      // notice how the quantiles are calculated within the max now, unlike before
      val expected95thWithMinMax = Seq(1.2978395301558558, 1.296892165727062, 1.2990334920692943, 1.2993620241156902)
      otelExpHistograms.map { h =>
        h.quantile(0.95, 0.78, 1.3) // notice 1.3 max is less than last bucket
      }.zip(expected95thWithMinMax).foreach { case (q, e) => q shouldEqual e +- 0.0001 }

      val expected5thWithoutMinMax = Seq(0.22984502016934866, 0.15504027656240363, 0.14452907137173218, 0.9199883517494387)
      otelExpHistograms.map { h =>
        h.quantile(0.05)
      }.zip(expected5thWithoutMinMax).foreach { case (q, e) => q shouldEqual e +- 0.0001 }

      // notice how the quantiles are calculated within the min now, unlike before
      val expected5thWithMinMax = Seq(0.7961930120386448, 0.7908863115573832, 0.7901434789531481, 0.9199883517494387)
      otelExpHistograms.map { h =>
        // notice 0.78 min is less than first non-zero bucketTop,
        // but bigger than previous otel bucketTop if it existed
        h.quantile(0.05, 0.78, 1.3)
      }.zip(expected5thWithMinMax).foreach { case (q, e) => q shouldEqual e +- 0.0001 }
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

    it("should return correct values for Base2ExpHistogramBuckets.bucketIndexToArrayIndex(index: Int): Int") {
      val b1 = Base2ExpHistogramBuckets(3, -5, 11) // 0.707 to 1.68
      b1.bucketIndexToArrayIndex(-5) shouldEqual 1
      b1.bucketIndexToArrayIndex(-4) shouldEqual 2
      b1.bucketIndexToArrayIndex(0) shouldEqual 6
      b1.bucketIndexToArrayIndex(5) shouldEqual 11
    }

    it("should create OTel exponential buckets and add them correctly") {
      val b1 = Base2ExpHistogramBuckets(3, -5, 11) // 0.707 to 1.68
      b1.numBuckets shouldEqual 12
      b1.startBucketTop shouldBe 0.7071067811865475 +- 0.0001
      b1.endBucketTop shouldBe 1.6817928305074294 +- 0.0001

      b1.bucketIndexToArrayIndex(-5) shouldEqual 1
      b1.bucketIndexToArrayIndex(5) shouldEqual 11
      b1.bucketTop(b1.bucketIndexToArrayIndex(-1)) shouldEqual 1.0

      val b2 = Base2ExpHistogramBuckets(2, -2, 7) // 0.8408 to 2.378
      val b3 = Base2ExpHistogramBuckets(2, -4, 9) // 0.594 to 2.378
      b1.canAccommodate(b2) shouldEqual false
      b2.canAccommodate(b1) shouldEqual false
      b3.canAccommodate(b1) shouldEqual true
      b3.canAccommodate(b2) shouldEqual true

      val bAdd = b1.add(b2)
      bAdd.numBuckets shouldEqual 10
      bAdd.scale shouldEqual 2
      bAdd.startBucketTop shouldBe 0.5946035575013606 +- 0.0001
      bAdd.endBucketTop shouldBe 2.378414245732675 +- 0.0001
      bAdd.canAccommodate(b1) shouldEqual true
      bAdd.canAccommodate(b2) shouldEqual true

      val bAddValues = new Array[Double](bAdd.numBuckets)
      bAdd.addValues(bAddValues, b1, MutableHistogram(b1, Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)))
      bAddValues.toSeq shouldEqual Seq(0.0, 0.0, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 11.0, 11.0)
      bAdd.addValues(bAddValues, b2, MutableHistogram(b2, Array(0, 1, 2, 3, 4, 5, 6, 7)))
      bAddValues.toSeq shouldEqual Seq(0.0, 0.0, 1.0, 4.0, 7.0, 10.0, 13.0, 16.0, 17.0, 18.0)

      val b4 = Base2ExpHistogramBuckets(5, 15, 36) // 1.414 to 3.018
      b4.numBuckets shouldEqual 37
      b4.startBucketTop shouldBe 1.414213562373094 +- 0.0001
      b4.endBucketTop shouldBe 3.0183288551868377 +- 0.0001

      val b5 = Base2ExpHistogramBuckets(3, 10, 6)
      b5.numBuckets shouldEqual 7
      b5.startBucketTop shouldBe 2.59367910930202 +- 0.0001
      b5.endBucketTop shouldBe 4.000000000000002 +- 0.0001

      val bAdd2 = bAdd.add(b4).add(b5)
      val bAdd2Values = new Array[Double](bAdd2.numBuckets)
      bAdd2.addValues(bAdd2Values, bAdd, MutableHistogram(bAdd, bAddValues))
      bAdd2Values.toSeq shouldEqual Seq(0.0, 0.0, 1.0, 4.0, 7.0, 10.0, 13.0, 16.0, 17.0, 18.0, 18.0, 18.0, 18.0, 18.0)

      bAdd2.addValues(bAdd2Values, b4, MutableHistogram(b4, (0 until 37 map (i => i.toDouble)).toArray))
      bAdd2Values.toSeq shouldEqual Seq(0.0, 0.0, 1.0, 4.0, 7.0, 10.0, 14.0, 25.0, 34.0, 43.0, 51.0, 54.0, 54.0, 54.0)

      bAdd2.addValues(bAdd2Values, b5, MutableHistogram(b5, Array(0.0, 10.0, 11, 12, 13, 14, 15)))
      bAdd2Values.toSeq shouldEqual Seq(0.0, 0.0, 1.0, 4.0, 7.0, 10.0, 14.0, 25.0, 34.0, 43.0, 62.0, 67.0, 69.0, 69.0)

    }

    it("should create non-overlapping OTel exponential buckets and add them correctly") {
      val b1 = Base2ExpHistogramBuckets(3, -5, 11)
      val b2 = Base2ExpHistogramBuckets(3, 15, 11)
      val b3 = b1.add(b2)
      b3.numBuckets shouldEqual 32
      b3.startIndexPositiveBuckets shouldEqual -5
    }

    it("should reduce scale when more than 120 buckets to keep memory and compute in check") {
      val b1 = Base2ExpHistogramBuckets(6, -50, 21)
      val b2 = Base2ExpHistogramBuckets(6, 100, 26)
      val add1 = b1.add(b2, maxBuckets = 128)
      add1 shouldEqual Base2ExpHistogramBuckets(5, -26, 90)
      add1.canAccommodate(b1) shouldEqual true
      add1.canAccommodate(b2) shouldEqual true

      val add2 = b1.add(b2, maxBuckets = 64)
      add2 shouldEqual Base2ExpHistogramBuckets(4, -14, 46)
      add2.canAccommodate(b1) shouldEqual true
      add2.canAccommodate(b2) shouldEqual true
    }


  }
}