package filodb.memory.format.vectors

import org.agrona.concurrent.UnsafeBuffer

import filodb.memory.format._

class HistogramVectorTest extends NativeVectorTest {
  import HistogramTest._

  it("should throw exceptions trying to query empty HistogramVector") {
    val appender = HistogramVector.appendingColumnar(memFactory, 8, 100)

    appender.length shouldEqual 0
    appender.isAllNA shouldEqual true
    val reader = appender.reader.asInstanceOf[ColumnarHistogramReader]

    reader.length(appender.addr) shouldEqual 0
    reader.numBuckets shouldEqual 0
    intercept[IllegalArgumentException] { reader(0) }
  }

  val buffer = new UnsafeBuffer(new Array[Byte](4096))
  val binScheme = bucketScheme.toByteArray

  def verifyHistogram(h: Histogram, itemNo: Int): Unit = {
    h.numBuckets shouldEqual bucketScheme.numBuckets
    for { i <- 0 until bucketScheme.numBuckets } {
      h.bucketTop(i) shouldEqual bucketScheme.bucketTop(i)
      h.bucketValue(i) shouldEqual rawHistBuckets(itemNo)(i)
    }
  }

  it("should accept BinaryHistograms of the same schema and be able to query them") {
    val appender = HistogramVector.appendingColumnar(memFactory, 8, 50)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeBinHistogram(binScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual rawHistBuckets.length

    val reader = appender.reader.asInstanceOf[ColumnarHistogramReader]
    reader.length shouldEqual rawHistBuckets.length

    (0 until rawHistBuckets.length).foreach { i =>
      val h = reader(i)
      verifyHistogram(h, i)
    }

    reader.iterate(0, 0).asInstanceOf[Iterator[Histogram]]
          .zipWithIndex.foreach { case (h, i) => verifyHistogram(h, i) }
  }

  it("should reject BinaryHistograms of schema different from first schema ingested") {
    val appender = HistogramVector.appendingColumnar(memFactory, 8, 50)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeBinHistogram(binScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual rawHistBuckets.length

    // A record using a different schema
    BinaryHistogram.writeBinHistogram(HistogramBuckets.binaryBuckets64Bytes, Array[Long](0, 1, 2, 0), buffer)
    appender.addData(buffer) shouldEqual BucketSchemaMismatch
  }

  it("should reject new adds when vector is full") {
    val appender = HistogramVector.appendingColumnar(memFactory, 8, 4)
    rawLongBuckets.foreach { rawBuckets =>
      BinaryHistogram.writeBinHistogram(binScheme, rawBuckets, buffer)
      appender.addData(buffer) shouldEqual Ack
    }

    appender.length shouldEqual rawHistBuckets.length

    appender.addData(buffer) shouldEqual VectorTooSmall(0, 0)
  }

  it("should calculate sum of multiple histograms correctly") (pending)
}