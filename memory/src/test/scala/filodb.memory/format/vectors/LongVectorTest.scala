package filodb.memory.format.vectors

import java.nio.ByteBuffer

import debox.Buffer
import filodb.memory.format.{BinaryVector, GrowableVector, MemoryReader, WireFormat}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class LongVectorTest extends NativeVectorTest with ScalaCheckPropertyChecks {
  def maxPlus(i: Int): Long = Int.MaxValue.toLong + i

  describe("LongMaskedAppendableVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      builder.addNA
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val ptr = builder.freeze(memFactory)
      LongBinaryVector(acc, ptr).length(acc, ptr) shouldEqual 1
      LongBinaryVector(acc, ptr)(acc, ptr, 0)   // Just to make sure this does not throw an exception
      // LongBinaryVector(ptr).isAvailable(0) should equal (false)
      LongBinaryVector(acc, ptr).toBuffer(acc, ptr) shouldEqual Buffer.empty[Long]
    }

    it("should encode a mix of NAs and Longs and decode iterate and skip NAs") {
      val cb = LongBinaryVector.appendingVector(memFactory, 5)
      cb.addNA
      cb.addData(101)
      cb.addData(maxPlus(102))
      cb.addData(103)
      cb.addNA
      cb.isAllNA should be (false)
      cb.noNAs should be (false)
      val ptr = cb.freeze(memFactory)
      val reader = LongBinaryVector(acc, ptr)

      reader.length(acc, ptr) shouldEqual 5
      // reader.isAvailable(0) should equal (false)
      // reader.isAvailable(1) should equal (true)
      // reader.isAvailable(4) should equal (false)
      reader(acc, ptr, 1) shouldEqual 101
      reader.toBuffer(acc, ptr) shouldEqual Buffer(101L, maxPlus(102), 103L)
    }

    it("should be able to append lots of longs off-heap and grow vector") {
      val numInts = 1000
      val builder = LongBinaryVector.appendingVector(memFactory, numInts / 2)
      (0 until numInts).map(_.toLong).foreach(builder.addData)
      builder.length should equal (numInts)
      builder.isAllNA should be (false)
      builder.noNAs should be (true)

      val optimized = builder.optimize(memFactory)
      LongBinaryVector(acc, optimized).sum(acc, optimized, 0, numInts - 1) shouldEqual (0 until numInts).sum.toDouble
    }

    it("should be able to read from on-heap array or byte buffer based Long Binary Vector") {
      val numInts = 1000
      val builder = LongBinaryVector.appendingVector(memFactory, numInts / 2)
      (0 until numInts).map(_.toLong).foreach(builder.addData)
      builder.length should equal (numInts)
      val optimized = builder.optimize(memFactory)
      val bytes = LongBinaryVector(acc, optimized).toBytes(acc, optimized)

      val onHeapAcc = Seq(MemoryReader.fromArray(bytes),
        MemoryReader.fromByteBuffer(BinaryVector.asBuffer(optimized)),
        MemoryReader.fromByteBuffer(ByteBuffer.wrap(bytes)))

      onHeapAcc.foreach { a =>
        LongBinaryVector(a, 0).sum(a, 0, 0, numInts - 1) shouldEqual (0 until numInts).sum.toDouble
      }
    }

    it("should be able to return minMax accurately with NAs") {
      val cb = LongBinaryVector.appendingVector(memFactory, 5)
      cb.addNA
      cb.addData(-maxPlus(100))
      cb.addData(102)
      cb.addData(maxPlus(103))
      cb.addNA
      val inner = cb.asInstanceOf[GrowableVector[Long]].inner.asInstanceOf[MaskedLongAppendingVector]
      inner.minMax should equal ((-maxPlus(100), maxPlus(103)))
    }

    it("should be able to freeze() and minimize bytes used") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      // Test numBytes to make sure it's accurate
      builder.numBytes should equal (12 + 16 + 8)   // 2 long words needed for 100 bits
      (0 to 4).map(_.toLong).foreach(builder.addData)
      builder.numBytes should equal (12 + 16 + 8 + 40)
      val frozen = builder.freeze(memFactory)
      BinaryVector.totalBytes(acc, frozen) should equal (12 + 8 + 8 + 40)  // bitmask truncated

      LongBinaryVector(acc, frozen).length(acc, frozen) shouldEqual 5
      LongBinaryVector(acc, frozen).toBuffer(acc, frozen) shouldEqual Buffer.fromIterable((0 to 4).map(_.toLong))
    }

    it("should be able to optimize longs with fewer nbits off-heap to DeltaDeltaVector") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      val orig = Seq(0, 2, 1, 4, 3).map(_.toLong)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      LongBinaryVector(acc, optimized).length(acc, optimized) should equal (5)
      LongBinaryVector(acc, optimized)(acc, optimized, 0) should equal (0L)
      LongBinaryVector(acc, optimized).toBuffer(acc, optimized).toList shouldEqual orig
      BinaryVector.totalBytes(acc, optimized) shouldEqual (28 + 3)   // nbits=4, 3 bytes of data + 28 overhead for DDV

      LongBinaryVector(acc, optimized).sum(acc, optimized, 0, 4) shouldEqual (2+1+4+3)
    }

    it("should be able to optimize longs with fewer nbits off-heap NoNAs to DeltaDeltaVector") {
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      // Be sure to make this not an increasing sequence so it doesn't get delta-delta encoded
      val orig = Seq(0, 2, 1, 4, 3).map(_.toLong)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      BinaryVector.majorVectorType(acc, optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      LongBinaryVector(acc, optimized).length(acc, optimized) shouldEqual 5
      LongBinaryVector(acc, optimized)(acc, optimized, 0) shouldEqual 0L
      LongBinaryVector(acc, optimized).toBuffer(acc, optimized).toList shouldEqual orig
      BinaryVector.totalBytes(acc, optimized) shouldEqual (28 + 3)   // nbits=4, 3 bytes of data + 28 overhead for DDV
    }

    it("should automatically use Delta-Delta encoding for increasing numbers") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(_ * 10000 + start)   // simulate 10 second samples
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual (8 + 51 * 8)
      val optimized = builder.optimize(memFactory)
      BinaryVector.majorVectorType(acc, optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      BinaryVector.totalBytes(acc, optimized) shouldEqual 24   // DeltaDeltaConstVector
      val readVect = LongBinaryVector(BinaryVector.asBuffer(optimized))
      readVect.toBuffer(acc, optimized).toList shouldEqual orig

      readVect.sum(acc, optimized, 0, 13) shouldEqual orig.take(14).sum.toDouble
    }

    it("should not allow sum() with out of bound indices") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(_ * 10000 + start)   // simulate 10 second samples
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual (8 + 51 * 8)
      val optimized = builder.optimize(memFactory)

      builder.reader.asLongReader.sum(acc, builder.addr, 0, 13) shouldEqual orig.take(14).sum.toDouble

      intercept[IllegalArgumentException] {
        builder.reader.asLongReader.sum(acc, builder.addr, 25, 52)
      }

      val readVect = LongBinaryVector(acc, optimized)
      intercept[IllegalArgumentException] {
        readVect.asLongReader.sum(acc, optimized, 25, 52)
      }
    }

    it("should iterate with startElement > 0") {
      val orig = Seq(1000L, 2001L, 2999L, 5123L, 5250L, 6004L, 7678L)
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, orig.length)
      orig.foreach(builder.addData)
      builder.length shouldEqual orig.length
      val frozen = builder.optimize(memFactory)
      LongBinaryVector(acc, frozen) shouldEqual DeltaDeltaDataReader
      (2 to 5).foreach { start =>
        LongBinaryVector(acc, frozen).toBuffer(acc, frozen, start).toList shouldEqual orig.drop(start)
      }
    }

    it("should automatically use Delta-Delta encoding for decreasing numbers") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(start - _ * 100)

      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual (8 + 51 * 8)
      val optimized = builder.optimize(memFactory)
      BinaryVector.majorVectorType(acc, optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      BinaryVector.totalBytes(acc, optimized) shouldEqual 24   // DeltaDeltaConstVector
      val readVect = LongBinaryVector(BinaryVector.asBuffer(optimized))
      readVect.toBuffer(acc, optimized).toList shouldEqual orig
    }

    it("should binarySearch both appending and DeltaDelta vectors") {
      val orig = Seq(1000L, 2001L, 2999L, 5123L, 5250L, 6004L, 7678L)
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, orig.length)
      orig.foreach(builder.addData)
      builder.length shouldEqual orig.length

      val appendReader = builder.reader.asLongReader
      appendReader.binarySearch(acc, builder.addr, 0L) shouldEqual 0x80000000 | 0   // first elem, not equal
      appendReader.binarySearch(acc, builder.addr, 999L) shouldEqual 0x80000000 | 0   // first elem, not equal
      appendReader.binarySearch(acc, builder.addr, 1000L) shouldEqual 0               // first elem, equal
      appendReader.binarySearch(acc, builder.addr, 3000L) shouldEqual 0x80000000 | 3
      appendReader.binarySearch(acc, builder.addr, 7677L) shouldEqual 0x80000000 | 6
      appendReader.binarySearch(acc, builder.addr, 7678L) shouldEqual 6
      appendReader.binarySearch(acc, builder.addr, 7679L) shouldEqual 0x80000000 | 7

      appendReader.ceilingIndex(acc, builder.addr, 0L) shouldEqual -1
      appendReader.ceilingIndex(acc, builder.addr, 999L) shouldEqual -1
      appendReader.ceilingIndex(acc, builder.addr, 1000L) shouldEqual 0
      appendReader.ceilingIndex(acc, builder.addr, 3000L) shouldEqual 2
      appendReader.ceilingIndex(acc, builder.addr, 7677L) shouldEqual 5
      appendReader.ceilingIndex(acc, builder.addr, 7678L) shouldEqual 6
      appendReader.ceilingIndex(acc, builder.addr, 7679L) shouldEqual 6

      val optimized = builder.optimize(memFactory)
      val binReader = LongBinaryVector(acc, optimized)
      LongBinaryVector(acc, optimized) shouldEqual DeltaDeltaDataReader
      binReader.binarySearch(acc, optimized, 0L) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader.binarySearch(acc, optimized, 999L) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader.binarySearch(acc, optimized, 1000L) shouldEqual 0               // first elem, equal
      binReader.binarySearch(acc, optimized, 3000L) shouldEqual 0x80000000 | 3
      binReader.binarySearch(acc, optimized, 5123L) shouldEqual 3
      binReader.binarySearch(acc, optimized, 5250L) shouldEqual 4
      binReader.binarySearch(acc, optimized, 6003L) shouldEqual 0x80000000 | 5
      binReader.binarySearch(acc, optimized, 6004L) shouldEqual 5
      binReader.binarySearch(acc, optimized, 7677L) shouldEqual 0x80000000 | 6
      binReader.binarySearch(acc, optimized, 7678L) shouldEqual 6
      binReader.binarySearch(acc, optimized, 7679L) shouldEqual 0x80000000 | 7

      binReader.ceilingIndex(acc, optimized, 7679L) shouldEqual 6
    }

    it("should binarySearch DDV vectors with slope=0") {
      // Test vector with slope=0
      val builder2 = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      (0 until 16).foreach(x => builder2.addData(1000L + (x / 5)))
      val optimized2 = builder2.optimize(memFactory)
      val binReader2 = LongBinaryVector(acc, optimized2)
      binReader2 shouldEqual DeltaDeltaDataReader
      DeltaDeltaDataReader.slope(acc, optimized2) shouldEqual 0
      binReader2.binarySearch(acc, optimized2,  999L) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader2.binarySearch(acc, optimized2, 1000L) shouldEqual 0               // first elem, equal
      binReader2.binarySearch(acc, optimized2, 1001L) shouldEqual 9
    }

    it("should binarySearch DeltaDeltaConst vectors") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(_ * 10000 + start)   // simulate 10 second samples
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      BinaryVector.totalBytes(acc, optimized) shouldEqual 24   // DeltaDeltaConstVector
      val binReader = LongBinaryVector(acc, optimized)
      binReader.binarySearch(acc, optimized, start - 1) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader.binarySearch(acc, optimized, start) shouldEqual 0               // first elem, equal
      binReader.binarySearch(acc, optimized, start + 1) shouldEqual 0x80000000 | 1
      binReader.binarySearch(acc, optimized, start + 100001) shouldEqual 0x80000000 | 11
      binReader.binarySearch(acc, optimized, start + orig.length*10000) shouldEqual 0x80000000 | orig.length

      // Test vector with slope=0
      val builder2 = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      (0 until 16).foreach(x => builder2.addData(1000L))
      val optimized2 = builder2.optimize(memFactory)
      BinaryVector.totalBytes(acc, optimized2) shouldEqual 24   // DeltaDeltaConstVector
      val binReader2 = LongBinaryVector(acc, optimized2)
      DeltaDeltaConstDataReader.slope(acc, optimized2) shouldEqual 0
      binReader2.binarySearch(acc, optimized2,  999L) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader2.binarySearch(acc, optimized2, 1000L) shouldEqual 0               // first elem, equal
      binReader2.binarySearch(acc, optimized2, 1001L) shouldEqual 0x80000000 | 16
    }

    import org.scalacheck._

    // Generate a list of increasing integers, every time bound it slightly differently
    // (to test different int compression techniques)
    def increasingIntList: Gen[Seq[Long]] =
      for {
        maxVal <- Gen.oneOf(1000, 5000, 30000)   // must be greater than 250ms so not within approximation
        seqList <- Gen.containerOf[Seq, Int](Gen.choose(10, maxVal))
      } yield { seqList.scanLeft(10000L)(_ + Math.abs(_)) }

    it("should binarySearch Long/DDV vector correctly for random elements and searches") {
      try {
        forAll(increasingIntList) { longs =>
          val builder = LongBinaryVector.appendingVectorNoNA(memFactory, longs.length)
          longs.foreach(builder.addData)
          val optimized = builder.optimize(memFactory)
          val reader = LongBinaryVector(acc, optimized)
          forAll(Gen.choose(0, longs.last * 3)) { num =>
            val out = reader.binarySearch(acc, optimized, num)
            (out & 0x7fffffff) should be >= 0
            val posMatch = longs.indexWhere(_ >= num)
            (out & 0x7fffffff) should be < 100000
            if (posMatch >= 0) {
              if (longs(posMatch) == num) {    // exact match, or within 250ms
                out shouldEqual posMatch
              } else {  // not match, but # at pos is first # greater than num.  So insert here.
                out shouldEqual (posMatch | 0x80000000)
              }
            } else {   // our # is greater than all numbers; ie _ >= num never true.
              out shouldEqual (0x80000000 | longs.length)
            }
          }
        }
      } finally {
        memFactory.freeAll()
      }
    }

    it("should not use Delta-Delta for short vectors, NAs, etc.") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      builder.addData(1L)
      builder.addData(5L)
      // Only 2 items, don't use DDV
      val opt1 = builder.optimize(memFactory)
      BinaryVector.majorVectorType(acc, opt1) shouldEqual WireFormat.VECTORTYPE_BINSIMPLE
      LongBinaryVector(acc, opt1).toBuffer(acc, opt1) shouldEqual Buffer(1L, 5L)

      builder.addNA()   // add NA(), now should not use DDV
      val opt2 = builder.optimize(memFactory)
      BinaryVector.majorVectorType(acc, opt2) shouldEqual WireFormat.VECTORTYPE_BINSIMPLE
      LongBinaryVector(acc, opt2).toBuffer(acc, opt2) shouldEqual Buffer(1L, 5L)
    }

    it("should be able to optimize constant longs to a DeltaDeltaConstVector") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      val longVal = Int.MaxValue.toLong + 100
      (0 to 4).foreach(n => builder.addData(longVal))
      val ptr = builder.optimize(memFactory)
      BinaryVector.majorVectorType(acc, ptr) shouldEqual WireFormat.VECTORTYPE_DELTA2
      val readVect = LongBinaryVector(acc, ptr)
      readVect shouldEqual DeltaDeltaConstDataReader
      BinaryVector.totalBytes(acc, ptr) shouldEqual 24   // DeltaDeltaConstVector
      readVect.toBuffer(acc, ptr) shouldEqual Buffer(longVal, longVal, longVal, longVal, longVal)
    }

    it("should support resetting and optimizing AppendableVector multiple times") {
      val cb = LongBinaryVector.appendingVector(memFactory, 5)
      // Use large numbers on purpose so cannot optimized to less than 32 bits
      val orig = Seq(100000, 200001, 300002).map(Long.MaxValue - _)
      cb.addNA()
      orig.foreach(cb.addData)
      cb.copyToBuffer.toList shouldEqual orig
      val optimized = cb.optimize(memFactory)
      val readVect1 = LongBinaryVector(acc, optimized)
      readVect1.toBuffer(acc, optimized).toList shouldEqual orig

      // Now the optimize should not have damaged original vector
      cb.copyToBuffer.toList shouldEqual orig
      cb.reset()
      val orig2 = orig.map(_ * 2)
      orig2.foreach(cb.addData)
      val opt2 = cb.optimize(memFactory)
      LongBinaryVector(acc, opt2).toBuffer(acc, opt2).toList shouldEqual orig2
      cb.copyToBuffer.toList shouldEqual orig2
    }
  }

  describe("TimestampAppendingVector") {
    it("should use DeltaDeltaConstVector for values falling within 250ms of slope line") {
      val start = System.currentTimeMillis
      val deltas = Seq(20, -20, 249, 2, -10, -220, 100, 0, -50, 23)
      val orig = deltas.zipWithIndex.map { case (dd, i) => start + 10000 * i + dd }
      val builder = LongBinaryVector.timestampVector(memFactory, 50)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual (8 + orig.length * 8)
      val optimized = builder.optimize(memFactory)
      BinaryVector.majorVectorType(acc, optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      BinaryVector.totalBytes(acc, optimized) shouldEqual 24   // DeltaDeltaConstVector
      val readVect = LongBinaryVector(BinaryVector.asBuffer(optimized))
      val values = readVect.toBuffer(acc, optimized).toList
      values.zip(orig).foreach { case (encoded, origValue) =>
        Math.abs(encoded - origValue) should be < 250L
      }
    }

    it("should not use DeltaDeltaConstVector if values larger than 250ms from slope") {
      val start = System.currentTimeMillis
      val deltas = Seq(20, -20, 249, 2, -10, -251, 100, 0, -50, 23)
      val orig = deltas.zipWithIndex.map { case (dd, i) => start + 10000 * i + dd }
      val builder = LongBinaryVector.timestampVector(memFactory, 50)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual (8 + orig.length * 8)
      val optimized = builder.optimize(memFactory)
      BinaryVector.majorVectorType(acc, optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      val readVect = LongBinaryVector(BinaryVector.asBuffer(optimized))
      readVect shouldEqual DeltaDeltaDataReader
      readVect.toBuffer(acc, optimized).toList shouldEqual orig
    }

    it("should not return -1 when binarySearching") {
      val orig = List(290, 264, 427, 746, 594, 216, 864, 85, 747, 897, 821).scanLeft(10000L)(_ + _)
      val builder = LongBinaryVector.timestampVector(memFactory, orig.length)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      val reader = LongBinaryVector(acc, optimized)
      val out = reader.binarySearch(acc, optimized, 9603)  // less than first item, should return 0x80000000
      out shouldEqual 0x80000000
    }
  }

  it("should do changes on DeltaDeltaConstVector") {
    val builder = LongBinaryVector.appendingVector(memFactory, 100)
    val longVal = Int.MaxValue.toLong + 100
    (0 to 4).foreach(n => builder.addData(longVal))
    val ptr = builder.optimize(memFactory)
    BinaryVector.majorVectorType(acc, ptr) shouldEqual WireFormat.VECTORTYPE_DELTA2
    val readVect = LongBinaryVector(acc, ptr)
    readVect shouldEqual DeltaDeltaConstDataReader
    val changesResult = readVect.changes(acc, ptr,0, 4,0, true)
    changesResult._1 shouldEqual(0)
    changesResult._2 shouldEqual(Int.MaxValue.toLong + 100)
  }

  it("should do changes on DeltaDeltaDataReader") {
    val orig = Seq(1000L, 2001L, 5123L, 5123L, 5250L, 6004L, 6004L)
    val builder = LongBinaryVector.appendingVectorNoNA(memFactory, orig.length)
    orig.foreach(builder.addData)
    builder.length shouldEqual orig.length
    val ptr = builder.optimize(memFactory)
    val readVect = LongBinaryVector(acc, ptr)
    readVect shouldEqual DeltaDeltaDataReader
    val changesResult = LongBinaryVector(acc, ptr).changes(acc, ptr, 0, 6, 0, true)
    changesResult._1 shouldEqual(4)
    changesResult._2 shouldEqual(6004)
  }
}
