package filodb.memory.format

import org.agrona.{DirectBuffer, ExpandableArrayBuffer}
import org.agrona.concurrent.UnsafeBuffer

import org.scalatest._
import org.scalatest.prop.PropertyChecks

class NibblePackTest extends FunSpec with Matchers with PropertyChecks {
  it("should NibblePack 8 words partial non-zero even nibbles") {
    // All 8 are nonzero, even # nibbles
    val buf = new ExpandableArrayBuffer()
    val inputs = Array(0L,
                       0x0000003322110000L, 0x0000004433220000L,
                       0x0000005544330000L, 0x0000006655440000L,
                       0L, 0L, 0L)
    val outpos = NibblePack.pack8(inputs, buf, 0)

    // Expected result:
    val expectedBuf = Array[Byte](
        0x1e,    // 0b0001_1110u8,   // only some bits on
        0x54,        // six nibbles wide, four zero nibbles trailing
        0x11, 0x22, 0x33, 0x22, 0x33, 0x44,
        0x33, 0x44, 0x55, 0x44, 0x55, 0x66)

    outpos shouldEqual expectedBuf.length
    buf.byteArray.take(expectedBuf.length) shouldEqual expectedBuf
  }

  it("should NibblePack 8 words partial non-zero odd nibbles") {
    // All 8 are nonzero, even # nibbles
    val buf = new ExpandableArrayBuffer()
    val inputs = Array(0L,
                       0x0000003322100000L, 0x0000004433200000L,
                       0x0000005544300000L, 0x0000006655400000L,
                       0x0000007654300000L, 0L, 0L)
    val outpos = NibblePack.pack8(inputs, buf, 0)

    // Expected result:
    val expectedBuf = Array[Byte](
        0x3e,  // 0b0011_1110u8,    // only some bits on
        0x45,          // five nibbles wide, five zero nibbles trailing
        0x21, 0x32, 0x23, 0x33, 0x44, // First two values
        0x43, 0x54, 0x45, 0x55, 0x66,
        0x43, 0x65, 0x07
    )
    outpos shouldEqual expectedBuf.length
    buf.byteArray.take(expectedBuf.length) shouldEqual expectedBuf
  }

  it("should correctly unpack partial 8 words odd nibbles") {
    val compressed = Array[Byte](
        0x3e,  // 0b0011_1110u8,    // only some bits on
        0x45,          // five nibbles wide, five zero nibbles trailing
        0x21, 0x32, 0x23, 0x33, 0x44, // First two values
        0x43, 0x54, 0x45, 0x55, 0x66,
        0x43, 0x65, 0x07
    )

    val expected = Array(0L,
                       0x0000003322100000L, 0x0000004433200000L,
                       0x0000005544300000L, 0x0000006655400000L,
                       0x0000007654300000L, 0L, 0L)

    val inbuf = new UnsafeBuffer(compressed)
    val outarray = new Array[Long](8)
    val res = NibblePack.unpack8(inbuf, outarray)
    res shouldEqual NibblePack.Ok

    inbuf.capacity shouldEqual 0
    outarray shouldEqual expected
  }

  it("should pack and unpack delta values") {
    val inputs = Array(0L, 1000, 1001, 1002, 1003, 2005, 2010, 3034, 4045, 5056, 6067, 7078)
    val buf = new ExpandableArrayBuffer()
    val bytesWritten = NibblePack.packDelta(inputs, buf, 0)

    val sink = NibblePack.DeltaSink(new Array[Long](inputs.size))
    val bufSlice = new UnsafeBuffer(buf, 0, bytesWritten)
    val res = NibblePack.unpackToSink(bufSlice, sink, inputs.size)

    res shouldEqual NibblePack.Ok
    sink.outArray shouldEqual inputs

    val inputs2 = Array(10000, 1032583228027L)
    val written2 = NibblePack.packDelta(inputs2, buf, 0)
    val sink2 = NibblePack.DeltaSink(new Array[Long](inputs2.size))
    bufSlice.wrap(buf, 0, written2)
    val res2 = NibblePack.unpackToSink(bufSlice, sink2, inputs2.size)

    res2 shouldEqual NibblePack.Ok
    sink2.outArray shouldEqual inputs2
  }

  it("should pack and unpack double values") {
    val inputs = Array(0.0, 2.5, 5.0, 7.5, 8, 13.2, 18.9, 89, 101.1, 102.3)
    val buf = new ExpandableArrayBuffer()
    val bytesWritten = NibblePack.packDoubles(inputs, buf, 0)

    val bufSlice = new UnsafeBuffer(buf, 0, bytesWritten)
    val out = new Array[Double](inputs.size)
    val res = NibblePack.unpackDoubleXOR(bufSlice, out)

    res shouldEqual NibblePack.Ok
    out shouldEqual inputs
  }

  def unpackAndCompare(inbuf: DirectBuffer, orig: Array[Long]): Unit = {
    val sink2 = NibblePack.DeltaSink(new Array[Long](orig.size))
    val res = NibblePack.unpackToSink(inbuf, sink2, orig.size)
    res shouldEqual NibblePack.Ok
    sink2.outArray shouldEqual orig
  }

  def unpackAndCompare(buf: DirectBuffer, index: Int, numBytes: Int, orig: Array[Long]): Unit = {
    val slice = new UnsafeBuffer(buf, index, numBytes)
    unpackAndCompare(slice, orig)
  }

  it("should repack increasing deltas to diffs using DeltaDiffPackSink") {
    val inputs = Seq(Array(0L, 1000, 1001, 1002, 1003, 2005, 2010, 3034, 4045, 5056, 6067, 7078),
                     Array(3L, 1004, 1006, 1008, 1009, 2010, 2020, 3056, 4070, 5090, 6101, 7150),
                     Array(7L, 1010, 1016, 1018, 1019, 2020, 2030, 3078, 4101, 5112, 6134, 7195))
    val diffs = inputs.sliding(2).map { case twoInputs =>
      twoInputs.last.clone.zipWithIndex.map { case (num, i) => num - twoInputs.head(i) }
    }.toSeq

    val writeBuf = new ExpandableArrayBuffer()

    // Compress each individual input into its own buffer
    val bufsAndSize = inputs.map { in =>
      val buf = new ExpandableArrayBuffer()
      val bytesWritten = NibblePack.packDelta(in, buf, 0)
      (buf, bytesWritten)
    }

    // Now, use DeltaDiffPackSink to recompress to deltas from initial inputs
    val sink = NibblePack.DeltaDiffPackSink(new Array[Long](inputs.head.size), writeBuf)

    // Verify delta on first one (empty diffs) yields back the original
    val (firstCompBuf, firstBufSize) = bufsAndSize.head
    val bufSlice0 = new UnsafeBuffer(firstCompBuf, 0, firstBufSize)
    val res0 = NibblePack.unpackToSink(bufSlice0, sink, inputs.head.size)
    res0 shouldEqual NibblePack.Ok

    val finalWritten0 = sink.finish
    finalWritten0 shouldEqual firstBufSize
    unpackAndCompare(writeBuf, 0, finalWritten0, inputs.head)

    // Verify delta on subsequent ones yields diff
    bufsAndSize.drop(1).zip(diffs).foreach { case ((origCompressedBuf, origSize), diff) =>
      val bufSlice = new UnsafeBuffer(origCompressedBuf, 0, origSize)
      val res = NibblePack.unpackToSink(bufSlice, sink, inputs.head.size)
      res shouldEqual NibblePack.Ok

      val finalWritten = sink.finish
      unpackAndCompare(writeBuf, 0, finalWritten, diff)
    }
  }

  import org.scalacheck._

  // Generate a list of increasing integers, every time bound it slightly differently
  // (to test different int compression techniques)
  def increasingLongList: Gen[Seq[Long]] =
    for {
      maxVal <- Gen.oneOf(1000, 5000, 30000, Math.pow(2L, 40).toLong)
      seqList <- Gen.containerOf[Seq, Long](Gen.choose(10, maxVal))
    } yield { seqList.scanLeft(10000L)(_ + Math.abs(_)) }

  it("should pack and unpack random list of increasing Longs via delta") {
    val buf = new ExpandableArrayBuffer()
    forAll(increasingLongList) { longs =>

      val inputs = longs.toArray
      val bytesWritten = NibblePack.packDelta(inputs, buf, 0)

      val sink = NibblePack.DeltaSink(new Array[Long](inputs.size))
      val bufSlice = new UnsafeBuffer(buf, 0, bytesWritten)
      val res = NibblePack.unpackToSink(bufSlice, sink, inputs.size)

      res shouldEqual NibblePack.Ok
      sink.outArray shouldEqual inputs
    }
  }

  def increasingDoubleList: Gen[Seq[Double]] = increasingLongList.map(_.map(_.toDouble)).filter(_.length > 0)

  it("should pack and unpack random list of increasing Doubles via XOR") {
    val buf = new ExpandableArrayBuffer()
    forAll(increasingDoubleList) { doubles =>
      val inputs = doubles.toArray
      val bytesWritten = NibblePack.packDoubles(inputs, buf, 0)

      val bufSlice = new UnsafeBuffer(buf, 0, bytesWritten)
      val out = new Array[Double](inputs.size)
      val res = NibblePack.unpackDoubleXOR(bufSlice, out)

      res shouldEqual NibblePack.Ok
      out shouldEqual inputs
    }
  }
}