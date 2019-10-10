package filodb.memory.format.vectors

import debox.Buffer
import org.scalatest.prop.PropertyChecks

import filodb.memory.format.{BinaryVector, GrowableVector, WireFormat}

class LongVectorTest extends NativeVectorTest with PropertyChecks {
  def maxPlus(i: Int): Long = Int.MaxValue.toLong + i

  describe("LongMaskedAppendableVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      builder.addNA
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val ptr = builder.freeze(memFactory)
      LongBinaryVector(ptr).length(ptr) shouldEqual 1
      LongBinaryVector(ptr)(ptr, 0)   // Just to make sure this does not throw an exception
      // LongBinaryVector(ptr).isAvailable(0) should equal (false)
      LongBinaryVector(ptr).toBuffer(ptr) shouldEqual Buffer.empty[Long]
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
      val reader = LongBinaryVector(ptr)

      reader.length(ptr) shouldEqual 5
      // reader.isAvailable(0) should equal (false)
      // reader.isAvailable(1) should equal (true)
      // reader.isAvailable(4) should equal (false)
      reader(ptr, 1) shouldEqual 101
      reader.toBuffer(ptr) shouldEqual Buffer(101L, maxPlus(102), 103L)
    }

    it("should be able to append lots of longs off-heap and grow vector") {
      val numInts = 1000
      val builder = LongBinaryVector.appendingVector(memFactory, numInts / 2)
      (0 until numInts).map(_.toLong).foreach(builder.addData)
      builder.length should equal (numInts)
      builder.isAllNA should be (false)
      builder.noNAs should be (true)

      val optimized = builder.optimize(memFactory)
      LongBinaryVector(optimized).sum(optimized, 0, numInts - 1) shouldEqual (0 until numInts).sum.toDouble
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
      BinaryVector.totalBytes(frozen) should equal (12 + 8 + 8 + 40)  // bitmask truncated

      LongBinaryVector(frozen).length(frozen) shouldEqual 5
      LongBinaryVector(frozen).toBuffer(frozen) shouldEqual Buffer.fromIterable((0 to 4).map(_.toLong))
    }

    it("should be able to optimize longs with fewer nbits off-heap to DeltaDeltaVector") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      val orig = Seq(0, 2, 1, 4, 3).map(_.toLong)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      LongBinaryVector(optimized).length(optimized) should equal (5)
      LongBinaryVector(optimized)(optimized, 0) should equal (0L)
      LongBinaryVector(optimized).toBuffer(optimized).toList shouldEqual orig
      BinaryVector.totalBytes(optimized) shouldEqual (28 + 3)   // nbits=4, 3 bytes of data + 28 overhead for DDV

      LongBinaryVector(optimized).sum(optimized, 0, 4) shouldEqual (2+1+4+3)
    }

    it("should be able to optimize longs with fewer nbits off-heap NoNAs to DeltaDeltaVector") {
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      // Be sure to make this not an increasing sequence so it doesn't get delta-delta encoded
      val orig = Seq(0, 2, 1, 4, 3).map(_.toLong)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      BinaryVector.majorVectorType(optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      LongBinaryVector(optimized).length(optimized) shouldEqual 5
      LongBinaryVector(optimized)(optimized, 0) shouldEqual 0L
      LongBinaryVector(optimized).toBuffer(optimized).toList shouldEqual orig
      BinaryVector.totalBytes(optimized) shouldEqual (28 + 3)   // nbits=4, 3 bytes of data + 28 overhead for DDV
    }

    it("should automatically use Delta-Delta encoding for increasing numbers") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(_ * 10000 + start)   // simulate 10 second samples
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual (8 + 51 * 8)
      val optimized = builder.optimize(memFactory)
      BinaryVector.majorVectorType(optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      BinaryVector.totalBytes(optimized) shouldEqual 24   // DeltaDeltaConstVector
      val readVect = LongBinaryVector(BinaryVector.asBuffer(optimized))
      readVect.toBuffer(optimized).toList shouldEqual orig

      readVect.sum(optimized, 0, 13) shouldEqual orig.take(14).sum.toDouble
    }

    it("should not allow sum() with out of bound indices") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(_ * 10000 + start)   // simulate 10 second samples
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual (8 + 51 * 8)
      val optimized = builder.optimize(memFactory)

      builder.reader.asLongReader.sum(builder.addr, 0, 13) shouldEqual orig.take(14).sum.toDouble

      intercept[IllegalArgumentException] {
        builder.reader.asLongReader.sum(builder.addr, 25, 52)
      }

      val readVect = LongBinaryVector(optimized)
      intercept[IllegalArgumentException] {
        readVect.asLongReader.sum(optimized, 25, 52)
      }
    }

    it("should iterate with startElement > 0") {
      val orig = Seq(1000L, 2001L, 2999L, 5123L, 5250L, 6004L, 7678L)
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, orig.length)
      orig.foreach(builder.addData)
      builder.length shouldEqual orig.length
      val frozen = builder.optimize(memFactory)
      LongBinaryVector(frozen) shouldEqual DeltaDeltaDataReader
      (2 to 5).foreach { start =>
        LongBinaryVector(frozen).toBuffer(frozen, start).toList shouldEqual orig.drop(start)
      }
    }

    it("should automatically use Delta-Delta encoding for decreasing numbers") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(start - _ * 100)

      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual (8 + 51 * 8)
      val optimized = builder.optimize(memFactory)
      BinaryVector.majorVectorType(optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      BinaryVector.totalBytes(optimized) shouldEqual 24   // DeltaDeltaConstVector
      val readVect = LongBinaryVector(BinaryVector.asBuffer(optimized))
      readVect.toBuffer(optimized).toList shouldEqual orig
    }

    it("should binarySearch both appending and DeltaDelta vectors") {
      val orig = Seq(1000L, 2001L, 2999L, 5123L, 5250L, 6004L, 7678L)
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, orig.length)
      orig.foreach(builder.addData)
      builder.length shouldEqual orig.length

      val appendReader = builder.reader.asLongReader
      appendReader.binarySearch(builder.addr, 0L) shouldEqual 0x80000000 | 0   // first elem, not equal
      appendReader.binarySearch(builder.addr, 999L) shouldEqual 0x80000000 | 0   // first elem, not equal
      appendReader.binarySearch(builder.addr, 1000L) shouldEqual 0               // first elem, equal
      appendReader.binarySearch(builder.addr, 3000L) shouldEqual 0x80000000 | 3
      appendReader.binarySearch(builder.addr, 7677L) shouldEqual 0x80000000 | 6
      appendReader.binarySearch(builder.addr, 7678L) shouldEqual 6
      appendReader.binarySearch(builder.addr, 7679L) shouldEqual 0x80000000 | 7

      appendReader.ceilingIndex(builder.addr, 0L) shouldEqual -1
      appendReader.ceilingIndex(builder.addr, 999L) shouldEqual -1
      appendReader.ceilingIndex(builder.addr, 1000L) shouldEqual 0
      appendReader.ceilingIndex(builder.addr, 3000L) shouldEqual 2
      appendReader.ceilingIndex(builder.addr, 7677L) shouldEqual 5
      appendReader.ceilingIndex(builder.addr, 7678L) shouldEqual 6
      appendReader.ceilingIndex(builder.addr, 7679L) shouldEqual 6

      val optimized = builder.optimize(memFactory)
      val binReader = LongBinaryVector(optimized)
      LongBinaryVector(optimized) shouldEqual DeltaDeltaDataReader
      binReader.binarySearch(optimized, 0L) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader.binarySearch(optimized, 999L) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader.binarySearch(optimized, 1000L) shouldEqual 0               // first elem, equal
      binReader.binarySearch(optimized, 3000L) shouldEqual 0x80000000 | 3
      binReader.binarySearch(optimized, 5123L) shouldEqual 3
      binReader.binarySearch(optimized, 5250L) shouldEqual 4
      binReader.binarySearch(optimized, 6003L) shouldEqual 0x80000000 | 5
      binReader.binarySearch(optimized, 6004L) shouldEqual 5
      binReader.binarySearch(optimized, 7677L) shouldEqual 0x80000000 | 6
      binReader.binarySearch(optimized, 7678L) shouldEqual 6
      binReader.binarySearch(optimized, 7679L) shouldEqual 0x80000000 | 7

      binReader.ceilingIndex(optimized, 7679L) shouldEqual 6
    }

    it("should binarySearch DDV vectors with slope=0") {
      // Test vector with slope=0
      val builder2 = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      (0 until 16).foreach(x => builder2.addData(1000L + (x / 5)))
      val optimized2 = builder2.optimize(memFactory)
      val binReader2 = LongBinaryVector(optimized2)
      binReader2 shouldEqual DeltaDeltaDataReader
      DeltaDeltaDataReader.slope(optimized2) shouldEqual 0
      binReader2.binarySearch(optimized2,  999L) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader2.binarySearch(optimized2, 1000L) shouldEqual 0               // first elem, equal
      binReader2.binarySearch(optimized2, 1001L) shouldEqual 9
    }

    it("should binarySearch DeltaDeltaConst vectors") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(_ * 10000 + start)   // simulate 10 second samples
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      BinaryVector.totalBytes(optimized) shouldEqual 24   // DeltaDeltaConstVector
      val binReader = LongBinaryVector(optimized)
      binReader.binarySearch(optimized, start - 1) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader.binarySearch(optimized, start) shouldEqual 0               // first elem, equal
      binReader.binarySearch(optimized, start + 1) shouldEqual 0x80000000 | 1
      binReader.binarySearch(optimized, start + 100001) shouldEqual 0x80000000 | 11
      binReader.binarySearch(optimized, start + orig.length*10000) shouldEqual 0x80000000 | orig.length

      // Test vector with slope=0
      val builder2 = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      (0 until 16).foreach(x => builder2.addData(1000L))
      val optimized2 = builder2.optimize(memFactory)
      BinaryVector.totalBytes(optimized2) shouldEqual 24   // DeltaDeltaConstVector
      val binReader2 = LongBinaryVector(optimized2)
      DeltaDeltaConstDataReader.slope(optimized2) shouldEqual 0
      binReader2.binarySearch(optimized2,  999L) shouldEqual 0x80000000 | 0   // first elem, not equal
      binReader2.binarySearch(optimized2, 1000L) shouldEqual 0               // first elem, equal
      binReader2.binarySearch(optimized2, 1001L) shouldEqual 0x80000000 | 16
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
          val reader = LongBinaryVector(optimized)
          forAll(Gen.choose(0, longs.last * 3)) { num =>
            val out = reader.binarySearch(optimized, num)
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
      BinaryVector.majorVectorType(opt1) shouldEqual WireFormat.VECTORTYPE_BINSIMPLE
      LongBinaryVector(opt1).toBuffer(opt1) shouldEqual Buffer(1L, 5L)

      builder.addNA()   // add NA(), now should not use DDV
      val opt2 = builder.optimize(memFactory)
      BinaryVector.majorVectorType(opt2) shouldEqual WireFormat.VECTORTYPE_BINSIMPLE
      LongBinaryVector(opt2).toBuffer(opt2) shouldEqual Buffer(1L, 5L)
    }

    it("should be able to optimize constant longs to a DeltaDeltaConstVector") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      val longVal = Int.MaxValue.toLong + 100
      (0 to 4).foreach(n => builder.addData(longVal))
      val ptr = builder.optimize(memFactory)
      BinaryVector.majorVectorType(ptr) shouldEqual WireFormat.VECTORTYPE_DELTA2
      val readVect = LongBinaryVector(ptr)
      readVect shouldEqual DeltaDeltaConstDataReader
      BinaryVector.totalBytes(ptr) shouldEqual 24   // DeltaDeltaConstVector
      readVect.toBuffer(ptr) shouldEqual Buffer(longVal, longVal, longVal, longVal, longVal)
    }

    it("should support resetting and optimizing AppendableVector multiple times") {
      val cb = LongBinaryVector.appendingVector(memFactory, 5)
      // Use large numbers on purpose so cannot optimized to less than 32 bits
      val orig = Seq(100000, 200001, 300002).map(Long.MaxValue - _)
      cb.addNA()
      orig.foreach(cb.addData)
      cb.copyToBuffer.toList shouldEqual orig
      val optimized = cb.optimize(memFactory)
      val readVect1 = LongBinaryVector(optimized)
      readVect1.toBuffer(optimized).toList shouldEqual orig

      // Now the optimize should not have damaged original vector
      cb.copyToBuffer.toList shouldEqual orig
      cb.reset()
      val orig2 = orig.map(_ * 2)
      orig2.foreach(cb.addData)
      val opt2 = cb.optimize(memFactory)
      LongBinaryVector(opt2).toBuffer(opt2).toList shouldEqual orig2
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
      BinaryVector.majorVectorType(optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      BinaryVector.totalBytes(optimized) shouldEqual 24   // DeltaDeltaConstVector
      val readVect = LongBinaryVector(BinaryVector.asBuffer(optimized))
      val values = readVect.toBuffer(optimized).toList
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
      BinaryVector.majorVectorType(optimized) shouldEqual WireFormat.VECTORTYPE_DELTA2
      val readVect = LongBinaryVector(BinaryVector.asBuffer(optimized))
      readVect shouldEqual DeltaDeltaDataReader
      readVect.toBuffer(optimized).toList shouldEqual orig
    }

    it("should not return -1 when binarySearching") {
      val orig = List(290, 264, 427, 746, 594, 216, 864, 85, 747, 897, 821).scanLeft(10000L)(_ + _)
      val builder = LongBinaryVector.timestampVector(memFactory, orig.length)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      val reader = LongBinaryVector(optimized)
      val out = reader.binarySearch(optimized, 9603)  // less than first item, should return 0x80000000
      out shouldEqual 0x80000000
    }
  }

  it("should do changes on DeltaDeltaConstVector") {
    val builder = LongBinaryVector.appendingVector(memFactory, 100)
    val longVal = Int.MaxValue.toLong + 100
    (0 to 4).foreach(n => builder.addData(longVal))
    val ptr = builder.optimize(memFactory)
    BinaryVector.majorVectorType(ptr) shouldEqual WireFormat.VECTORTYPE_DELTA2
    val readVect = LongBinaryVector(ptr)
    readVect shouldEqual DeltaDeltaConstDataReader
    val changesResult = readVect.changes(ptr,0, 4,0, true)
    changesResult._1 shouldEqual(0)
    changesResult._2 shouldEqual(Int.MaxValue.toLong + 100)
  }

  it("should do changes on DeltaDeltaDataReader") {
    val orig = Seq(1000L, 2001L, 5123L, 5123L, 5250L, 6004L, 6004L)
    val builder = LongBinaryVector.appendingVectorNoNA(memFactory, orig.length)
    orig.foreach(builder.addData)
    builder.length shouldEqual orig.length
    val ptr = builder.optimize(memFactory)
    val readVect = LongBinaryVector(ptr)
    readVect shouldEqual DeltaDeltaDataReader
    val changesResult = LongBinaryVector(ptr).changes(ptr, 0, 6, 0, true)
    changesResult._1 shouldEqual(4)
    changesResult._2 shouldEqual(6004)
  }
}