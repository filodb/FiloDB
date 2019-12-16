package filodb.memory.format.vectors

import java.nio.ByteBuffer

import org.agrona.concurrent.UnsafeBuffer

import filodb.memory.format._

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
                      bucketScheme: HistogramBuckets = bucketScheme
                     ): Unit = {
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