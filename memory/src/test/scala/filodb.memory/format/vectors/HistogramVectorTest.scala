package filodb.memory.format.vectors

import org.agrona.concurrent.UnsafeBuffer

import filodb.memory.format._

class HistogramVectorTest extends NativeVectorTest {
  import HistogramTest._

  it("should throw exceptions trying to query empty HistogramVector") {
    val appender = HistogramVector.appending(memFactory, 1024)

    appender.length shouldEqual 0
    appender.isAllNA shouldEqual true
    val reader = appender.reader.asInstanceOf[RowHistogramReader]

    reader.length(appender.addr) shouldEqual 0
    reader.numBuckets shouldEqual 0
    intercept[IllegalArgumentException] { reader(0) }
  }

  val buffer = new UnsafeBuffer(new Array[Byte](4096))

  def verifyHistogram(h: Histogram, itemNo: Int): Unit = {
    h.numBuckets shouldEqual bucketScheme.numBuckets
    for { i <- 0 until bucketScheme.numBuckets } {
      h.bucketTop(i) shouldEqual bucketScheme.bucketTop(i)
      h.bucketValue(i) shouldEqual rawHistBuckets(itemNo)(i)
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

    reader.iterate(0, 0).asInstanceOf[Iterator[Histogram]]
          .zipWithIndex.foreach { case (h, i) => verifyHistogram(h, i) }
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
    optReader.length(optimized) shouldEqual rawHistBuckets.length
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

  it("should reject BinaryHistograms of schema different from first schema ingested") {
    val appender = HistogramVector.appending(memFactory, 1024)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeDelta(bucketScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual rawHistBuckets.length

    // A record using a different schema
    BinaryHistogram.writeDelta(HistogramBuckets.binaryBuckets64, Array[Long](0, 1, 2, 0), buffer)
    appender.addData(buffer) shouldEqual BucketSchemaMismatch
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
}