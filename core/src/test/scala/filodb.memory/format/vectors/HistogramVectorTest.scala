package filodb.memory.format.vectors

import java.nio.ByteBuffer
import org.agrona.concurrent.UnsafeBuffer
import filodb.memory.format._
import filodb.memory.format.vectors.BinaryHistogram.{BinHistogram, HistFormat_Geometric1_Delta}

class HistogramVectorTest extends NativeVectorTest {
  import HistogramTest._

  it("should throw exceptions trying to query empty HistogramVector") {
    val appender = HistogramVector.appending(memFactory, 1024)

    appender.length shouldEqual 0
    appender.isAllNA shouldEqual true
    val reader = appender.reader.asInstanceOf[RowHistogramReader]

    reader.length(acc, appender.addr) shouldEqual 0
    reader.numBuckets shouldEqual 0
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

  it("should accept BinaryHistograms of the same schema and be able to query them") {
    val appender = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual rawHistBuckets.length

    val reader = appender.reader.asInstanceOf[RowHistogramReader]
    reader.length shouldEqual rawHistBuckets.length

    (0 until rawHistBuckets.length).foreach { i =>
      val h = reader(i)
      verifyHistogram(h, i)
    }

    reader.iterate(acc, 0, 0).asInstanceOf[Iterator[Histogram]]
          .zipWithIndex.foreach { case (h, i) => verifyHistogram(h, i) }
  }

  it("should accept LongHistograms with custom scheme and query them back") {
    val appender = HistogramVector.appending(memFactory, 1024)
    customHistograms.foreach { custHist =>
      custHist.serialize(Some(buffer))
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual customHistograms.length

    val reader = appender.reader.asInstanceOf[RowHistogramReader]
    reader.length shouldEqual customHistograms.length

    (0 until customHistograms.length).foreach { i =>
      reader(i) shouldEqual customHistograms(i)
    }
  }

  it("should accept LongHistograms with otel exp scheme and query them back") {
    val appender = HistogramVector.appending(memFactory, 1024)
    otelExpHistograms.foreach { custHist =>
      custHist.serialize(Some(buffer))
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual otelExpHistograms.length

    val reader = appender.reader.asInstanceOf[RowHistogramReader]
    reader.length shouldEqual otelExpHistograms.length

    (0 until otelExpHistograms.length).foreach { i =>
      reader(i) shouldEqual otelExpHistograms(i)
    }
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

    val appender = HistogramVector.appending(memFactory, 15000) // 15k bytes is default blob size
    val bucketScheme = Base2ExpHistogramBuckets(3, -78, 126)
    var counts = str.split(", ").map { s =>
      val kv = s.split("=")
      kv(1).toDouble.toLong
    }

    (0 until 206).foreach { buckets =>
      val hist = LongHistogram(bucketScheme, counts)
      hist.serialize(Some(buffer))
      if (buckets < 205) appender.addData(buffer) shouldEqual Ack
      // should fail for 206th histogram because it crosses the size of write buffer
      if (buckets >= 205) appender.addData(buffer) shouldEqual VectorTooSmall(73,42)
      counts = counts.map(_ + 10)
    }

    val reader = appender.reader.asInstanceOf[RowHistogramReader]
    reader.length shouldEqual 205

  }

  it ("should be able to create a vector of one observation histograms of large bucket offset") {
    val appender = HistogramVector.appending(memFactory, 15000) // 15k bytes is default blob size
    val bucketScheme = Base2ExpHistogramBuckets(20, 9037032, 1)
    val counts = Array(0L, 1L)

    (0 until 2958).foreach { i =>
      val hist = LongHistogram(bucketScheme, counts)
      hist.serialize(Some(buffer))
      if (i < 2957) appender.addData(buffer) shouldEqual Ack
      // should fail for 206th histogram because it crosses the size of write buffer
      if (i >= 2957) appender.addData(buffer) shouldEqual VectorTooSmall(5,0)
    }
    val reader = appender.reader.asInstanceOf[RowHistogramReader]
    reader.length shouldEqual 2957
  }

  it("should accept MutableHistograms with rate doubles that have fractional value with" +
    " otel exp scheme and query them back") {
    val mutHistograms = otelExpHistograms.map(h => MutableHistogram(h.buckets, h.valueArray.map(_ + 4.6992d)))
    mutHistograms.foreach { custHist =>
      custHist.serialize(Some(buffer))
      val hist2 = BinaryHistogram.BinHistogram(buffer).toHistogram
      hist2 shouldEqual custHist
    }
  }

  it("should optimize histograms and be able to query optimized vectors") {
    val appender = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual rawHistBuckets.length

    val reader = appender.reader.asInstanceOf[RowHistogramReader]
    reader.length shouldEqual rawHistBuckets.length

    (0 until rawHistBuckets.length).foreach { i =>
      val h = reader(i)
      verifyHistogram(h, i)
    }

    val optimized = appender.optimize(memFactory)
    val optReader = HistogramVector(BinaryVector.asBuffer(optimized))
    optReader.length(acc, optimized) shouldEqual rawHistBuckets.length
    (0 until rawHistBuckets.length).foreach { i =>
      val h = optReader(i)
      verifyHistogram(h, i)
    }

    val sum = optReader.sum(0, rawHistBuckets.length - 1)  // should not crash
    val expected = (0 until 8).map { b => rawHistBuckets.map(_(b)).sum }.toArray
    sum.values shouldEqual expected

    appender.reset()
    appender.length shouldEqual 0
  }

  it("should be able to read from on-heap Histogram Vector") {
    val appender = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual rawHistBuckets.length

    val reader = appender.reader.asInstanceOf[RowHistogramReader]
    reader.length shouldEqual rawHistBuckets.length

    (0 until rawHistBuckets.length).foreach { i =>
      val h = reader(i)
      verifyHistogram(h, i)
    }

    val optimized = appender.optimize(memFactory)
    val optReader = HistogramVector(BinaryVector.asBuffer(optimized))

    val bytes = optReader.toBytes(acc, optimized)

    val onHeapAcc = Seq(MemoryReader.fromArray(bytes),
      MemoryReader.fromByteBuffer(BinaryVector.asBuffer(optimized)),
      MemoryReader.fromByteBuffer(ByteBuffer.wrap(bytes)))

    onHeapAcc.foreach { a =>
      val readerH = HistogramVector(a, 0)
      (0 until rawHistBuckets.length).foreach { i =>
        val h = readerH(i)
        verifyHistogram(h, i)
      }
    }
  }

  val lastIncrHist = LongHistogram(bucketScheme, incrHistBuckets.last.map(_.toLong))

  it("should append, optimize, and query SectDelta histograms") {
    val appender = HistogramVector.appendingSect(memFactory, 1024)
    incrHistBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets.map(_.toLong), buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual incrHistBuckets.length

    val reader = appender.reader.asInstanceOf[SectDeltaHistogramReader]
    reader.length shouldEqual rawHistBuckets.length
    println(reader.dumpAllSections)  //  XXX

    (0 until incrHistBuckets.length).foreach { i =>
      verifyHistogram(reader(i), i, incrHistBuckets)
    }

    val optimized = appender.optimize(memFactory)
    val optReader = HistogramVector(BinaryVector.asBuffer(optimized))
    optReader.length(acc, optimized) shouldEqual incrHistBuckets.length
    (0 until incrHistBuckets.length).foreach { i =>
      val h = optReader(i)
      verifyHistogram(h, i, incrHistBuckets)
    }

    val oReader2 = optReader.asInstanceOf[CounterHistogramReader]
    oReader2.updateCorrection(acc, optimized, NoCorrection) shouldEqual
      HistogramCorrection(lastIncrHist, LongHistogram.empty(bucketScheme))
    oReader2.updateCorrection(acc, optimized, HistogramCorrection(lastIncrHist, correction1.copy)) shouldEqual
      HistogramCorrection(lastIncrHist, correction1)
  }

  it("should work for the downsampling round trip of a histogram vector") {

    val myBucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    // time, sum, count, histogram
    val bucketData = Seq(
      Array(0d, 0d, 1d),
      Array(0d, 2d, 3d),
      Array(2d, 5d, 6d),
      Array(2d, 5d, 9d),
      Array(2d, 5d, 10d),
      Array(2d, 8d, 14d),
      Array(0d, 0d, 2d),
      Array(1d, 7d, 9d),
      Array(1d, 15d, 19d),
      Array(2d, 16d, 21d),
      Array(0d, 1d, 1d),
      Array(0d, 15d, 15d),
      Array(1d, 16d, 19d),
      Array(4d, 20d, 25d)
    )

    val appender = HistogramVector.appendingSect(memFactory, 1024)
    bucketData.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(myBucketScheme, rawBuckets.map(_.toLong), buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual bucketData.length

    val reader = appender.reader.asInstanceOf[SectDeltaHistogramReader]
    reader.length shouldEqual bucketData.length

    reader.dropPositions(MemoryAccessor.nativePtrAccessor, appender.addr) shouldEqual debox.Buffer(6, 10)

    (0 until bucketData.length).foreach { i =>
      verifyHistogram(reader(i), i, bucketData, myBucketScheme)
    }

    val optimized = appender.optimize(memFactory)

    val optReader = HistogramVector(BinaryVector.asBuffer(optimized))

    val bytes = optReader.toBytes(acc, optimized)

    val onHeapAcc = Seq(MemoryReader.fromArray(bytes),
      MemoryReader.fromByteBuffer(BinaryVector.asBuffer(optimized)),
      MemoryReader.fromByteBuffer(ByteBuffer.wrap(bytes)))

    onHeapAcc.foreach { a =>
      val readerH = HistogramVector(a, 0).asInstanceOf[SectDeltaHistogramReader]
      readerH.dropPositions(a, 0) shouldEqual debox.Buffer(6, 10)
      (0 until bucketData.length).foreach { i =>
        val h = readerH(i)
        verifyHistogram(h, i, bucketData, myBucketScheme)
      }
    }
  }

  it("SectDelta vectors should detect and handle drops correctly") {
    val appender = HistogramVector.appendingSect(memFactory, 1024)
    incrHistBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets.map(_.toLong), buffer)
      appender.addData(buffer) shouldEqual Ack
    }
    incrHistBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets.map(_.toLong), buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual incrHistBuckets.length*2

    val reader = appender.reader.asInstanceOf[SectDeltaHistogramReader]
    // One normal section, one dropped section
    reader.iterateSections.toSeq.map(_.sectionType(acc)) shouldEqual Seq(SectionType(0), SectionType(1))

    (0 until incrHistBuckets.length).foreach { i =>
      verifyHistogram(reader(i), i, incrHistBuckets)
      verifyHistogram(reader(4 + i), i, incrHistBuckets)
    }

    // Now, verify updateCorrection will propagate correction correctly
    reader.updateCorrection(acc, appender.addr, NoCorrection) shouldEqual
      HistogramCorrection(lastIncrHist, lastIncrHist)
    val corr2 = correction1.copy
    corr2.add(lastIncrHist)
    reader.updateCorrection(acc, appender.addr, HistogramCorrection(lastIncrHist, correction1.copy)) shouldEqual
      HistogramCorrection(lastIncrHist, corr2)
  }

  it("should reject BinaryHistograms of schema different from first schema ingested") {
    val appender = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual rawHistBuckets.length

    // A record using a different schema
    BinaryHistogram.writeDelta(HistogramBuckets.binaryBuckets64, Array.fill(64)(1L), buffer)
    appender.addData(buffer) shouldEqual BucketSchemaMismatch
  }
  
   it("histogram should have right formatCode after sum operation is applied") {
    val appender = HistogramVector.appending(memFactory, 1024)
    BinaryHistogram.writeNonIncreasing(HistogramBuckets.binaryBuckets64, Array.fill(64)(1L), buffer)
    BinaryHistogram.writeNonIncreasing(HistogramBuckets.binaryBuckets64, Array.fill(64)(1L), buffer)
    appender.addData(buffer) shouldEqual Ack
    val mutableHisto = appender.reader.asHistReader.sum(0,0)
    BinHistogram(mutableHisto.serialize()).formatCode shouldEqual HistFormat_Geometric1_Delta
  }

  it("should reject initially invalid BinaryHistogram") {
    val appender = HistogramVector.appending(memFactory, 1024)

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

  it("should reject new adds when vector is full") {
    val appender = HistogramVector.appending(memFactory, 76)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.numBytes shouldEqual HistogramVector.OffsetBucketDef + 2+8+8 + 4 + (12+11+8+11)
    appender.length shouldEqual rawHistBuckets.length

    appender.addData(buffer) shouldBe a[VectorTooSmall]
  }

  // Test for Section reader code in RowHistogramReader, that we can jump back and forth
  it("should be able to randomly look up any element in long HistogramVector") {
    val numElements = 150
    val appender = HistogramVector.appending(memFactory, numElements * 20)
    (0 until numElements).foreach { i =>
      BinaryHistogram.writeDelta(bucketScheme, rawLongBuckets(i % rawLongBuckets.length), buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    val optimized = appender.optimize(memFactory)
    val optReader = HistogramVector(BinaryVector.asBuffer(optimized))

    for { _ <- 0 to 50 } {
      val elemNo = scala.util.Random.nextInt(numElements)
      verifyHistogram(optReader(elemNo), elemNo % (rawLongBuckets.length))
    }
  }

  // ── DeltaHistogramReader.sum() tests ───────────────────────────────
  // Tests the native-accelerated sum() used by: rate(delta_hist[5m]) → SumOverTimeChunkedFunctionH
  // Each test compares native (SIMD enabled) vs JVM (SIMD disabled) to ensure correctness.

  it("native sum should match JVM sum for geometric bucket histograms") {
    val appender = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }
    val reader = appender.reader.asHistReader

    SimdNativeMethods.deltaHistogramSumEnabled = false
    val jvmSum = reader.sum(0, rawHistBuckets.length - 1)

    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val nativeSum = reader.sum(0, rawHistBuckets.length - 1)
      val expected = (0 until 8).map { b => rawHistBuckets.map(_(b)).sum }.toArray
      nativeSum.values shouldEqual expected
      nativeSum.values shouldEqual jvmSum.values
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("native sum of single histogram should match JVM") {
    val appender = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }
    val reader = appender.reader.asHistReader

    SimdNativeMethods.deltaHistogramSumEnabled = false
    val jvmSum = reader.sum(2, 2)

    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val nativeSum = reader.sum(2, 2)
      nativeSum.values shouldEqual jvmSum.values
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("native sum of sub-range should match JVM") {
    val appender = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }
    val reader = appender.reader.asHistReader

    SimdNativeMethods.deltaHistogramSumEnabled = false
    val jvmSum = reader.sum(1, 2)

    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val nativeSum = reader.sum(1, 2)
      nativeSum.values shouldEqual jvmSum.values
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("native sum should work across section boundaries (>64 histograms)") {
    val numElements = 100
    val appender = HistogramVector.appending(memFactory, numElements * 30)
    (0 until numElements).foreach { i =>
      BinaryHistogram.writeDelta(bucketScheme, rawLongBuckets(i % rawLongBuckets.length), buffer)
      appender.addData(buffer) shouldEqual Ack
    }
    val reader = appender.reader.asHistReader

    SimdNativeMethods.deltaHistogramSumEnabled = false
    val jvmSum = reader.sum(50, 80)

    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val nativeSum = reader.sum(50, 80)
      nativeSum.values shouldEqual jvmSum.values
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("native sum should work with custom bucket schemes") {
    val appender = HistogramVector.appending(memFactory, 1024)
    customHistograms.foreach { custHist =>
      custHist.serialize(Some(buffer))
      appender.addData(buffer) shouldEqual Ack
    }
    val reader = appender.reader.asHistReader

    SimdNativeMethods.deltaHistogramSumEnabled = false
    val jvmSum = reader.sum(0, customHistograms.length - 1)

    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val nativeSum = reader.sum(0, customHistograms.length - 1)
      nativeSum.values shouldEqual jvmSum.values
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("native sum should handle zero-valued histograms") {
    val zeroBuckets = Array.fill(8)(0L)
    val appender = HistogramVector.appending(memFactory, 1024)
    (0 until 10).foreach { _ =>
      BinaryHistogram.writeDelta(bucketScheme, zeroBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }
    val reader = appender.reader.asHistReader

    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val nativeSum = reader.sum(0, 9)
      (0 until 8).foreach { b => nativeSum.bucketValue(b) shouldEqual 0.0 }
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("native sum should fallback gracefully when SIMD disabled") {
    val appender = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }
    val reader = appender.reader.asHistReader

    // With SIMD disabled, DeltaHistogramReader should use super.sum() (JVM path)
    SimdNativeMethods.deltaHistogramSumEnabled = false
    val sum = reader.sum(0, rawHistBuckets.length - 1)
    val expected = (0 until 8).map { b => rawHistBuckets.map(_(b)).sum }.toArray
    sum.values shouldEqual expected
  }

  // ── Intra-chunk / Inter-chunk correctness tests ────────────────────
  // Verifies that native sum() (intra-chunk) produces results that combine
  // correctly with MutableHistogram.add() (inter-chunk) when chunks have
  // DIFFERENT bucket schemes — simulating a bucket schema change over time.

  it("intra-chunk native sum should produce correct results independently per chunk") {
    // Chunk A: 8-bucket geometric scheme, 4 histograms
    val schemeA = GeometricBuckets(1.0, 2.0, 8)
    val appenderA = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(schemeA, rawBuckets, buffer)
      appenderA.addData(buffer) shouldEqual Ack
    }

    // Chunk B: 5-bucket custom scheme, 3 histograms
    val schemeB = CustomBuckets(Array(0.25, 0.5, 1.0, 2.5, Double.PositiveInfinity))
    val appenderB = HistogramVector.appending(memFactory, 1024)
    val dataBuckets5 = Seq(Array(5L, 10L, 15L, 20L, 25L), Array(3L, 8L, 12L, 18L, 22L), Array(7L, 14L, 21L, 28L, 35L))
    dataBuckets5.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(schemeB, rawBuckets, buffer)
      appenderB.addData(buffer) shouldEqual Ack
    }

    // Get DeltaHistogramReaders via HistogramVector.apply (off-heap, native pointer)
    val readerA = HistogramVector(MemoryReader.nativePtrReader, appenderA.addr).asHistReader
    val readerB = HistogramVector(MemoryReader.nativePtrReader, appenderB.addr).asHistReader

    readerA.getClass.getSimpleName shouldEqual "DeltaHistogramReader"
    readerB.getClass.getSimpleName shouldEqual "DeltaHistogramReader"

    // Native intra-chunk sum for each chunk independently
    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val sumA = readerA.sum(0, rawHistBuckets.length - 1)
      val sumB = readerB.sum(0, dataBuckets5.length - 1)

      // Verify chunk A sum matches expected (8 buckets)
      val expectedA = (0 until 8).map { b => rawHistBuckets.map(_(b)).sum }.toArray
      sumA.values shouldEqual expectedA
      sumA.numBuckets shouldEqual 8

      // Verify chunk B sum matches expected (5 buckets)
      val expectedB = (0 until 5).map { b => dataBuckets5.map(_(b).toDouble).sum }.toArray
      sumB.values shouldEqual expectedB
      sumB.numBuckets shouldEqual 5
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("inter-chunk add of native sum results with DIFFERENT bucket schemes should produce NaN") {
    // This simulates SumOverTimeChunkedFunctionH.addTimeChunks() combining results
    // from two chunks where the bucket scheme changed between chunks.
    // MutableHistogram.add() → addNoCorrection() checks similarForMath and sets NaN.

    val schemeA = GeometricBuckets(1.0, 2.0, 8)
    val appenderA = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(schemeA, rawBuckets, buffer)
      appenderA.addData(buffer) shouldEqual Ack
    }

    val schemeB = GeometricBuckets(1.0, 3.0, 8)  // different ratio → different bucket tops
    val appenderB = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(schemeB, rawBuckets, buffer)
      appenderB.addData(buffer) shouldEqual Ack
    }

    val readerA = HistogramVector(MemoryReader.nativePtrReader, appenderA.addr).asHistReader
    val readerB = HistogramVector(MemoryReader.nativePtrReader, appenderB.addr).asHistReader

    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val sumA = readerA.sum(0, rawHistBuckets.length - 1)
      val sumB = readerB.sum(0, rawHistBuckets.length - 1)

      // Simulate inter-chunk combination (what SumOverTimeChunkedFunctionH does):
      //   h = sumA.copy; h.add(sumB)
      // Since schemes differ, add() → addNoCorrection() → similarForMath returns false
      // → values set to NaN
      val combined = sumA.copy.asInstanceOf[MutableHistogram]
      combined.add(sumB)

      // All values should be NaN because bucket schemes are incompatible
      (0 until 8).foreach { b =>
        java.lang.Double.isNaN(combined.bucketValue(b)) shouldBe true
      }
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("inter-chunk add of native sum results with SAME bucket scheme should combine correctly") {
    // Two chunks with the same scheme but different data — simulates normal operation
    // where bucket scheme is stable across chunks.

    val scheme = GeometricBuckets(1.0, 2.0, 8)
    val appenderA = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(scheme, rawBuckets, buffer)
      appenderA.addData(buffer) shouldEqual Ack
    }

    // Second chunk with different data, same scheme
    val data2 = Seq(Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L),
                    Array(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L))
    val appenderB = HistogramVector.appending(memFactory, 1024)
    data2.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(scheme, rawBuckets, buffer)
      appenderB.addData(buffer) shouldEqual Ack
    }

    val readerA = HistogramVector(MemoryReader.nativePtrReader, appenderA.addr).asHistReader
    val readerB = HistogramVector(MemoryReader.nativePtrReader, appenderB.addr).asHistReader

    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val sumA = readerA.sum(0, rawHistBuckets.length - 1)
      val sumB = readerB.sum(0, data2.length - 1)

      // Simulate inter-chunk combination
      val combined = sumA.copy.asInstanceOf[MutableHistogram]
      combined.add(sumB)

      // Combined should be sumA + sumB per bucket
      val expectedA = (0 until 8).map { b => rawHistBuckets.map(_(b)).sum }.toArray
      val expectedB = (0 until 8).map { b => data2.map(_(b).toDouble).sum }.toArray
      (0 until 8).foreach { b =>
        combined.bucketValue(b) shouldEqual (expectedA(b) + expectedB(b))
      }
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("native and JVM sum should produce identical results for inter-chunk combination") {
    // End-to-end: two chunks, sum each with native AND JVM, combine, compare results

    val scheme = GeometricBuckets(1.0, 2.0, 8)
    val appenderA = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(scheme, rawBuckets, buffer)
      appenderA.addData(buffer) shouldEqual Ack
    }

    val data2 = Seq(Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L),
                    Array(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L))
    val appenderB = HistogramVector.appending(memFactory, 1024)
    data2.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(scheme, rawBuckets, buffer)
      appenderB.addData(buffer) shouldEqual Ack
    }

    val readerA = HistogramVector(MemoryReader.nativePtrReader, appenderA.addr).asHistReader
    val readerB = HistogramVector(MemoryReader.nativePtrReader, appenderB.addr).asHistReader

    // JVM path
    SimdNativeMethods.deltaHistogramSumEnabled = false
    val jvmSumA = readerA.sum(0, rawHistBuckets.length - 1)
    val jvmSumB = readerB.sum(0, data2.length - 1)
    val jvmCombined = jvmSumA.copy.asInstanceOf[MutableHistogram]
    jvmCombined.add(jvmSumB)

    // Native path
    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val natSumA = readerA.sum(0, rawHistBuckets.length - 1)
      val natSumB = readerB.sum(0, data2.length - 1)
      val natCombined = natSumA.copy.asInstanceOf[MutableHistogram]
      natCombined.add(natSumB)

      // Results must be identical
      (0 until 8).foreach { b =>
        natCombined.bucketValue(b) shouldEqual jvmCombined.bucketValue(b)
      }
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  // ── NibblePack Scala vs Rust unpacking comparison tests ────────────
  // These tests pack data with Scala NibblePack, then sum via both
  // JVM (Scala unpack) and native (Rust unpack), comparing results
  // to verify the Rust NibblePack port is bit-for-bit correct.

  /** Helper: builds a SIMPLE histogram vector with given data, returns a
   *  DeltaHistogramReader backed by off-heap native memory. */
  private def buildDeltaReader(scheme: HistogramBuckets, data: Seq[Array[Long]])
    : HistogramReader = {
    val appender = HistogramVector.appending(memFactory, data.size * 100)
    data.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(scheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }
    HistogramVector(MemoryReader.nativePtrReader, appender.addr).asHistReader
  }

  /** Helper: compares native vs JVM sum for range [start, end] */
  private def compareNativeVsJvm(reader: HistogramReader,
                                  start: Int, end: Int): Unit = {
    SimdNativeMethods.deltaHistogramSumEnabled = false
    val jvmSum = reader.sum(start, end)

    SimdNativeMethods.deltaHistogramSumEnabled = true
    try {
      val nativeSum = reader.sum(start, end)
      nativeSum.numBuckets shouldEqual jvmSum.numBuckets
      (0 until jvmSum.numBuckets).foreach { b =>
        nativeSum.bucketValue(b) shouldEqual jvmSum.bucketValue(b)
      }
    } finally {
      SimdNativeMethods.deltaHistogramSumEnabled = false
    }
  }

  it("Rust unpack should match Scala for all-zero buckets") {
    val scheme = GeometricBuckets(1.0, 2.0, 8)
    val data = (0 until 30).map(_ => Array.fill(8)(0L))
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 29)
  }

  it("Rust unpack should match Scala for constant bucket values") {
    // All histograms identical — NibblePack should produce similar encodings
    val scheme = GeometricBuckets(1.0, 2.0, 8)
    val row = Array(100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L)
    val data = (0 until 30).map(_ => row.clone())
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 29)
  }

  it("Rust unpack should match Scala for small delta values (typical case)") {
    // Small deltas between cumulative buckets — typical for real histograms
    val scheme = GeometricBuckets(1.0, 2.0, 20)
    val rng = new scala.util.Random(123)
    val data = (0 until 30).map { _ =>
      val raw = (0 until 20).map(_ => rng.nextInt(100).toLong).toArray
      (1 until 20).foreach { i => raw(i) += raw(i - 1) }
      raw
    }
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 29)
  }

  it("Rust unpack should match Scala for large values near Long.MaxValue") {
    val scheme = GeometricBuckets(1.0, 2.0, 8)
    val data = (0 until 10).map { i =>
      val base = Long.MaxValue / 2 + i * 1000L
      (0 until 8).map(b => base + b * 100L).toArray
    }
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 9)
  }

  it("Rust unpack should match Scala for single-element buckets (1 nibble)") {
    // Values 0-15 fit in a single nibble — tests minimal bit width encoding
    val scheme = GeometricBuckets(1.0, 2.0, 8)
    val data = (0 until 30).map { i =>
      (0 until 8).map(b => ((i + b) % 16).toLong).toArray.
        scanLeft(0L)(_ + _).tail.toArray  // make cumulative
    }
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 29)
  }

  it("Rust unpack should match Scala for values requiring many nibbles") {
    // Large spread values requiring 12+ nibbles — tests wide bit packing
    val scheme = GeometricBuckets(1.0, 2.0, 8)
    val data = (0 until 20).map { i =>
      Array(i.toLong * 1000000,
            i.toLong * 1000000 + 500000,
            i.toLong * 2000000,
            i.toLong * 3000000,
            i.toLong * 5000000,
            i.toLong * 8000000,
            i.toLong * 13000000,
            i.toLong * 21000000)
    }
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 19)
  }

  it("Rust unpack should match Scala for non-power-of-8 bucket counts") {
    // 3 buckets — tests remainder handling in unpack8 (3 values + 5 zeros)
    val scheme = CustomBuckets(Array(1.0, 10.0, Double.PositiveInfinity))
    val data = (0 until 30).map { i =>
      Array(i.toLong * 10, i.toLong * 50, i.toLong * 100)
    }
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 29)
  }

  it("Rust unpack should match Scala for exactly 8 buckets") {
    // Exactly 8 = one pack8 call, no remainder
    val scheme = GeometricBuckets(1.0, 2.0, 8)
    val rng = new scala.util.Random(456)
    val data = (0 until 50).map { _ =>
      val raw = (0 until 8).map(_ => rng.nextInt(10000).toLong).toArray
      (1 until 8).foreach { i => raw(i) += raw(i - 1) }
      raw
    }
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 49)
  }

  it("Rust unpack should match Scala for 16 buckets (exactly 2 pack8 calls)") {
    val scheme = GeometricBuckets(1.0, 2.0, 16)
    val rng = new scala.util.Random(789)
    val data = (0 until 30).map { _ =>
      val raw = (0 until 16).map(_ => rng.nextInt(5000).toLong).toArray
      (1 until 16).foreach { i => raw(i) += raw(i - 1) }
      raw
    }
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 29)
  }

  it("Rust unpack should match Scala for many histograms crossing section boundary") {
    // 100 histograms in SIMPLE format = 2 sections (64 + 36)
    // Tests that Rust section walking matches Scala locate()
    val scheme = GeometricBuckets(1.0, 2.0, 8)
    val rng = new scala.util.Random(101)
    val data = (0 until 100).map { _ =>
      val raw = (0 until 8).map(_ => rng.nextInt(1000).toLong).toArray
      (1 until 8).foreach { i => raw(i) += raw(i - 1) }
      raw
    }
    val reader = buildDeltaReader(scheme, data)

    // Full range
    compareNativeVsJvm(reader, 0, 99)
    // Entirely within section 0
    compareNativeVsJvm(reader, 10, 50)
    // Spans section boundary (section 0 ends at 63)
    compareNativeVsJvm(reader, 55, 75)
    // Entirely within section 1
    compareNativeVsJvm(reader, 70, 95)
    // Single element from each section
    compareNativeVsJvm(reader, 63, 63)
    compareNativeVsJvm(reader, 64, 64)
  }

  it("Rust unpack should match Scala for sparse histograms (mostly zero buckets)") {
    // Only first and last bucket have values — tests bitmask with few bits set
    val scheme = GeometricBuckets(1.0, 2.0, 20)
    val data = (0 until 30).map { i =>
      val raw = Array.fill(20)(0L)
      raw(0) = i.toLong * 5
      raw(19) = i.toLong * 5 + 100
      // make cumulative
      (1 until 20).foreach { b => raw(b) = Math.max(raw(b), raw(b - 1)) }
      raw
    }
    val reader = buildDeltaReader(scheme, data)
    compareNativeVsJvm(reader, 0, 29)
  }

  it("Rust unpack should match Scala with ScalaCheck random data") {
    import org.scalacheck.Gen
    import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

    ScalaCheckPropertyChecks.forAll(
      Gen.choose(3, 30),     // numBuckets
      Gen.choose(5, 50),     // numHistograms
      Gen.choose(0L, 10000L) // maxBucketValue
    ) { (numBuckets, numHistograms, maxVal) =>
      val scheme = GeometricBuckets(1.0, 2.0, numBuckets)
      val rng = new scala.util.Random(numBuckets * 1000 + numHistograms)
      val data = (0 until numHistograms).map { _ =>
        val raw = (0 until numBuckets).map(_ => (rng.nextDouble() * maxVal).toLong).toArray
        (1 until numBuckets).foreach { i => raw(i) += raw(i - 1) }
        raw
      }
      val reader = buildDeltaReader(scheme, data)
      compareNativeVsJvm(reader, 0, numHistograms - 1)
    }
  }

  val incrAppender = HistogramVector.appendingSect(memFactory, 1024)
  incrHistBuckets.foreach { rawBuckets =>
    BinaryHistogram.writeDelta(bucketScheme, rawBuckets.map(_.toLong), buffer)
    incrAppender.addData(buffer) shouldEqual Ack
  }
  val incrAddr = incrAppender.addr
  val incrReader = incrAppender.reader.asInstanceOf[CounterHistogramReader]

  it("should detect drop correctly at beginning of chunk and adjust CorrectionMeta") {
    incrReader.detectDropAndCorrection(acc, incrAddr, NoCorrection) shouldEqual NoCorrection

    // No drop in first value, correction should be returned unchanged
    val meta1 = HistogramCorrection(correction1, correction2.copy)
    incrReader.detectDropAndCorrection(acc, incrAddr, meta1) shouldEqual meta1

    // Drop in first value, correction should be done
    val meta2 = HistogramCorrection(lastIncrHist, correction2.copy)
    val corr3 = correction2.copy
    corr3.add(lastIncrHist)
    incrReader.detectDropAndCorrection(acc, incrAddr, meta2) shouldEqual
      HistogramCorrection(lastIncrHist, corr3)
  }

  it("should return correctedValue with correction adjustment even if vector has no drops") {
    val incr1 = LongHistogram(bucketScheme, incrHistBuckets(1).map(_.toLong))
    incrReader.correctedValue(1, NoCorrection) shouldEqual incr1

    val meta1 = HistogramCorrection(correction1, correction2.copy)
    val adjustedHist = correction2.copy
    adjustedHist.add(incr1)
    incrReader.correctedValue(1, meta1) shouldEqual adjustedHist
  }

  it("should return correctedValue with vector with drops") {
    val appender2 = HistogramVector.appendingSect(memFactory, 1024)
    incrHistBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets.map(_.toLong), buffer)
      appender2.addData(buffer) shouldEqual Ack
    }
    incrHistBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets.map(_.toLong + 15L), buffer)
      appender2.addData(buffer) shouldEqual Ack
    }
    val reader = appender2.reader.asInstanceOf[CounterHistogramReader]

    // now check corrected hist
    val incr5 = LongHistogram(bucketScheme, incrHistBuckets(1).map(_.toLong + 15L))
    incr5.add(lastIncrHist)
    reader.correctedValue(5, NoCorrection) shouldEqual incr5

    // value @5 plus correction plus carryover correction from meta
    val meta1 = HistogramCorrection(correction1, correction2.copy)
    incr5.add(correction2)
    reader.correctedValue(5, meta1) shouldEqual incr5
  }
}
