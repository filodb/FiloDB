package filodb.memory.format

import org.agrona.ExpandableArrayBuffer
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
}