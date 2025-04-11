package filodb.memory.format.vectors

import filodb.memory.format._
import filodb.memory.format.vectors.BinaryHistogram.{BinHistogram, HistFormat_OtelExp_XOR}
import org.agrona.concurrent.UnsafeBuffer

import java.nio.ByteBuffer

class ExpHistogramVectorTest extends NativeVectorTest {
  import HistogramTest._

  it("should throw exceptions trying to query empty HistogramVector") {
    val appender = HistogramVector.appendingExp(memFactory, 1024)

    appender.length shouldEqual 0
    appender.isAllNA shouldEqual true
    val reader = appender.reader.asInstanceOf[RowExpHistogramReader]

    reader.length(acc, appender.addr) shouldEqual 0
    intercept[IllegalArgumentException] { reader(0) }
  }

  val buffer = new UnsafeBuffer(new Array[Byte](4096))

  def verifyHistogram(h: Histogram, itemNo: Int,
                      rawBuckets: Seq[Array[Double]] = rawHistBuckets,
                      bucketScheme: HistogramBuckets = bucketScheme ): Unit = {
    h.numBuckets shouldEqual bucketScheme.numBuckets
    for { i <- 0 until bucketScheme.numBuckets } {
      h.bucketTop(i) shouldEqual bucketScheme.bucketTop(i)
      h.bucketValue(i) shouldEqual rawBuckets(itemNo)(i)
    }
  }

  val otelExpHistograms = Seq(
    LongHistogram(Base2ExpHistogramBuckets(3, -3, 1), Array(0L, 3L)),
    LongHistogram(Base2ExpHistogramBuckets(20, -3, 9), Array(0L, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
    LongHistogram(Base2ExpHistogramBuckets(20, -888388, 1), Array(0L, 5L)),
  )

  it("should accept BinaryHistograms of the different exp schema and be able to query them") {
    val appender = HistogramVector.appendingExp(memFactory, 1024)

    otelExpHistograms.foreach { h =>
      BinaryHistogram.writeDelta(h.buckets, h.values, buffer)
      appender.addData(buffer) shouldEqual Ack
    }
    appender.length shouldEqual otelExpHistograms.length

    val reader = appender.reader.asInstanceOf[RowExpHistogramReader]
    reader.length shouldEqual otelExpHistograms.length

    (0 until otelExpHistograms.length).foreach { i =>
      val h = reader(i)
      h.compare(otelExpHistograms(i)) shouldEqual 0
    }

    reader.iterate(acc, 0, 0).asInstanceOf[Iterator[Histogram]]
          .zipWithIndex.foreach { case (h, i) => h.compare(otelExpHistograms(i)) shouldEqual 0 }
  }

  // This test confirms that we can store 1-minutely exponential histogram samples of 160 buckets
  // easily in a histogram appender / write buffer, and we can still hold samples for 1-hr flush interval
  it("should be able to store sufficient hist samples in one 15k byte vector, the histogram appender default size") {
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

    val appender = HistogramVector.appendingExp(memFactory, 15000) // 15k bytes is default blob size
    val bucketScheme = Base2ExpHistogramBuckets(3, -78, 126)
    var counts = str.split(", ").map { s =>
      val kv = s.split("=")
      kv(1).toDouble.toLong
    }

    (0 until 159).foreach { buckets =>
      val hist = LongHistogram(bucketScheme, counts)
      hist.serialize(Some(buffer))
      if (buckets < 159) appender.addData(buffer) shouldEqual Ack
      // should fail for 159th histogram because it crosses the size of write buffer
      if (buckets >= 159) appender.addData(buffer) shouldEqual VectorTooSmall(73,42)
      counts = counts.map(_ + 10)
    }

    val reader = appender.reader.asInstanceOf[RowExpHistogramReader]
    reader.length shouldEqual 159
  }

  it ("should be able to create a vector of one observation histograms of large bucket offset") {
    val appender = HistogramVector.appendingExp(memFactory, 15000) // 15k bytes is default blob size
    val bucketScheme = Base2ExpHistogramBuckets(20, 9037032, 1)
    val counts = Array(0L, 1L)

    (0 until 576).foreach { i =>
      val hist = LongHistogram(bucketScheme, counts)
      hist.serialize(Some(buffer))
      println(i)
      if (i < 575) appender.addData(buffer) shouldEqual Ack
      // should fail for 206th histogram because it crosses the size of write buffer
      if (i >= 575) appender.addData(buffer) shouldEqual VectorTooSmall(26,6)
    }
    val reader = appender.reader.asInstanceOf[RowExpHistogramReader]
    reader.length shouldEqual 575
  }

  it("should optimize histograms and be able to query optimized vectors") {
    val appender = HistogramVector.appendingExp(memFactory, 1024)
    otelExpHistograms.foreach { h =>
      BinaryHistogram.writeDelta(h.buckets, h.values, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual otelExpHistograms.length

    val reader = appender.reader.asInstanceOf[RowExpHistogramReader]
    reader.length shouldEqual otelExpHistograms.length
    (0 until otelExpHistograms.length).foreach { i =>
      val h = reader(i)
      h.compare(otelExpHistograms(i)) shouldEqual 0
    }

    val optimized = appender.optimize(memFactory)
    val optReader = HistogramVector(BinaryVector.asBuffer(optimized))

    optReader.length(acc, optimized) shouldEqual otelExpHistograms.length
    (0 until otelExpHistograms.length).foreach { i =>
      val h = optReader(i)
      h.compare(otelExpHistograms(i)) shouldEqual 0
    }

    appender.reset()
    appender.length shouldEqual 0
  }

  it("should be able to read from on-heap Histogram Vector") {
    val appender = HistogramVector.appendingExp(memFactory, 1024)
    otelExpHistograms.foreach { h =>
      BinaryHistogram.writeDelta(h.buckets, h.values, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    val optimized = appender.optimize(memFactory)
    val optReader = HistogramVector(BinaryVector.asBuffer(optimized)).asInstanceOf[RowExpHistogramReader]
    val bytes = optReader.toBytes(acc, optimized)
    val onHeapAcc = Seq(
      MemoryReader.fromArray(bytes),
      MemoryReader.fromByteBuffer(BinaryVector.asBuffer(optimized)),
      MemoryReader.fromByteBuffer(ByteBuffer.wrap(bytes))
    )

    onHeapAcc.foreach { a =>
      val readerH = HistogramVector(a, 0)
      (0 until otelExpHistograms.length).foreach { i =>
        val h = readerH(i)
        h.compare(otelExpHistograms(i)) shouldEqual 0
      }
    }
  }

   it("histogram should support sum and sum result should have right formatCode") {
     val appender = HistogramVector.appendingExp(memFactory, 1024)
     otelExpHistograms.foreach { h =>
       BinaryHistogram.writeDelta(h.buckets, h.values, buffer)
       appender.addData(buffer) shouldEqual Ack
     }
     appender.length shouldEqual otelExpHistograms.length

    val sumHist = appender.reader.asHistReader.sum(0, 2)
    sumHist.buckets shouldEqual Base2ExpHistogramBuckets(3, -8, 9)
    sumHist.values shouldEqual Array(0.0, 0.0, 5.0, 5.0, 5.0, 5.0, 8.0, 8.0, 14.0, 20.0)
    BinHistogram(sumHist.serialize()).formatCode shouldEqual HistFormat_OtelExp_XOR
  }

  it("should reject initially invalid BinaryHistogram") {
    val appender = HistogramVector.appendingExp(memFactory, 1024)

    // Create some garbage
    buffer.putStringWithoutLengthAscii(0, "monkeying")

    appender.addData(buffer) shouldEqual InvalidHistogram
    appender.length shouldEqual 0

    // Reject null histograms also
    buffer.putShort(0, 1)
    buffer.putByte(2, 0)
    appender.addData(buffer) shouldEqual InvalidHistogram
    appender.length shouldEqual 0
  }

  // Test for Section reader code in RowHistogramReader, that we can jump back and forth
  it("should be able to randomly look up any element in long HistogramVector") {
    val numElements = 150
    val appender = HistogramVector.appendingExp(memFactory, numElements * 30)
    (0 until numElements).foreach { i =>
      val h = otelExpHistograms(i % otelExpHistograms.length)
      BinaryHistogram.writeDelta(h.buckets, h.values, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    val optimized = appender.optimize(memFactory)
    val optReader = HistogramVector(BinaryVector.asBuffer(optimized))

    for { _ <- 0 to 50 } {
      val elemNo = scala.util.Random.nextInt(numElements)
      val h = optReader(elemNo % otelExpHistograms.length)
      h.compare(otelExpHistograms(elemNo % otelExpHistograms.length)) shouldEqual 0
    }
  }

}
