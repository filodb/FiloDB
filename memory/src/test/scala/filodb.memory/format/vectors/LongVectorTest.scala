package filodb.memory.format.vectors

import org.scalatest.{FunSpec, Matchers}

import filodb.memory.NativeMemoryManager
import filodb.memory.format.{BinaryVector, FiloVector, GrowableVector}

class LongVectorTest extends FunSpec with Matchers {
  def maxPlus(i: Int): Long = Int.MaxValue.toLong + i
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)

  describe("LongMaskedAppendableVector") {

    it("should append a list of all NAs and read all NAs back") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      builder.addNA
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val sc = builder.freeze(memFactory)
      sc.length should equal (1)
      sc(0)   // Just to make sure this does not throw an exception
      sc.isAvailable(0) should equal (false)
      sc.toList should equal (Nil)
      sc.optionIterator.toSeq should equal (Seq(None))
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
      val sc = cb.freeze(memFactory)

      sc.length should equal (5)
      sc.isAvailable(0) should equal (false)
      sc.isAvailable(1) should equal (true)
      sc.isAvailable(4) should equal (false)
      sc(1) should equal (101)
      sc.boxed(2) should equal (maxPlus(102))
      //noinspection ScalaStyle
      sc.boxed(2) shouldBe a[java.lang.Long]
      sc.get(0) should equal (None)
      sc.get(-1) should equal (None)
      sc.get(2) should equal (Some(maxPlus(102)))
      sc.toList should equal (List(101, maxPlus(102), 103))
    }

    it("should be able to append lots of longs and grow vector") {
      val numInts = 1000
      val builder = LongBinaryVector.appendingVector(memFactory, numInts / 2)
      (0 until numInts).map(_.toLong).foreach(builder.addData)
      builder.length should equal (numInts)
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
    }

    it("should be able to append lots of longs off-heap and grow vector") {
      val numInts = 1000
      val builder = LongBinaryVector.appendingVector(memFactory, numInts / 2)
      (0 until numInts).map(_.toLong).foreach(builder.addData)
      builder.length should equal (numInts)
      builder.isOffheap shouldEqual true
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
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
      builder.numBytes should equal (4 + 16 + 4)   // 2 long words needed for 100 bits
      (0 to 4).map(_.toLong).foreach(builder.addData)
      builder.numBytes should equal (4 + 16 + 4 + 40)
      val frozen = builder.freeze(memFactory)
      frozen.numBytes should equal (4 + 8 + 4 + 40)  // bitmask truncated

      frozen.length should equal (5)
      frozen.toSeq should equal (0 to 4)
    }

    it("should toFiloBuffer and read back using FiloVector.apply") {
      val cb = LongBinaryVector.appendingVector(memFactory, 5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(maxPlus(104))
      cb.addNA
      val buffer = cb.optimize(memFactory).toFiloBuffer
      val readVect = FiloVector[Long](buffer)
      readVect shouldBe a[MaskedLongBinaryVector]
      readVect.asInstanceOf[BinaryVector[Long]].maybeNAs should equal (true)
      readVect.toSeq should equal (Seq(101, 102, maxPlus(104)))
    }

    it("should be able to optimize longs with fewer nbits off-heap to DeltaDeltaVector") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      // Be sure to make this not an increasing sequence so it doesn't get delta-delta encoded
      val orig = Seq(0, 2, 1, 4, 3).map(_.toLong)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      optimized.length should equal (5)
      optimized.maybeNAs should equal (false)
      optimized(0) should equal (0L)
      optimized.toSeq should equal (orig)
      optimized.numBytes should equal (16 + 3)   // nbits=4, 3 bytes of data + 16 overhead for DDV
      val readVect = FiloVector[Long](optimized.toFiloBuffer)
      readVect shouldBe a[DeltaDeltaVector]
      readVect.toSeq should equal (orig)
    }

    it("should be able to optimize longs with fewer nbits off-heap NoNAs to DeltaDeltaVector") {
      val builder = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      // Be sure to make this not an increasing sequence so it doesn't get delta-delta encoded
      val orig = Seq(0, 2, 1, 4, 3).map(_.toLong)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      optimized.length shouldEqual 5
      optimized.isOffheap shouldEqual true
      optimized.maybeNAs shouldEqual false
      optimized(0) shouldEqual 0L
      optimized.toSeq should equal (orig)
      optimized.numBytes should equal (16 + 3)   // nbits=4, 3 bytes of data + 16 overhead for DDV
      optimized shouldBe a[DeltaDeltaVector]
      val readVect = FiloVector[Long](optimized.toFiloBuffer)
      readVect shouldBe a[DeltaDeltaVector]
      readVect.toSeq should equal (orig)
    }

    it("should automatically use Delta-Delta encoding for increasing numbers") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(_ * 10000 + start)   // simulate 10 second samples
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual 424
      val optimized = builder.optimize(memFactory)
      optimized.isOffheap shouldEqual true
      optimized.numBytes shouldEqual 16   // DeltaDeltaConstVector
      val buf = optimized.toFiloBuffer
      val readVect = FiloVector[Long](buf)
      readVect shouldBe a[DeltaDeltaConstVector]
      readVect.toSeq should equal (orig)
      readVect.asInstanceOf[BinaryVector[Long]].maybeNAs should equal (false)
      // The # of bytes below is MUCH less than the original 51 * 8 + 4
      readVect.asInstanceOf[BinaryVector[Long]].numBytes shouldEqual 16
    }

    it("should automatically use Delta-Delta encoding for decreasing numbers") {
      val start = System.currentTimeMillis
      val orig = (0 to 50).map(start - _ * 100)
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      orig.foreach(builder.addData)
      builder.frozenSize shouldEqual 424
      val optimized = builder.optimize(memFactory)
      optimized.isOffheap shouldEqual true
      optimized.numBytes shouldEqual 16   // DeltaDeltaConstVector
      optimized shouldBe a[DeltaDeltaConstVector]
      val buf = optimized.toFiloBuffer
      val readVect = FiloVector[Long](buf)
      readVect shouldBe a[DeltaDeltaConstVector]
      readVect.toSeq should equal (orig)
      readVect.asInstanceOf[BinaryVector[Long]].maybeNAs should equal (false)
      // The # of bytes below is MUCH less than the original 51 * 8 + 4
      readVect.asInstanceOf[BinaryVector[Long]].numBytes shouldEqual 16
    }

    it("should not use Delta-Delta for short vectors, NAs, etc.") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      builder.addData(1L)
      builder.addData(5L)
      // Only 2 items, don't use DDV
      val opt1 = builder.optimize(memFactory)
      opt1.isOffheap shouldEqual true
      opt1 shouldBe a[LongBinaryVector]
      opt1.toSeq shouldEqual (Seq(1L, 5L))

      builder.addNA()   // add NA(), now should not use DDV
      val opt2 = builder.optimize(memFactory)
      opt2 shouldBe a[MaskedLongBinaryVector]
      opt2.toSeq shouldEqual (Seq(1L, 5L))
    }

    it("should be able to optimize constant longs to a DeltaDeltaConstVector") {
      val builder = LongBinaryVector.appendingVector(memFactory, 100)
      val longVal = Int.MaxValue.toLong + 100
      (0 to 4).foreach(n => builder.addData(longVal))
      val buf = builder.optimize(memFactory).toFiloBuffer
      val readVect = FiloVector[Long](buf)
      readVect shouldBe a[DeltaDeltaConstVector]
      readVect.asInstanceOf[BinaryVector[Long]].numBytes shouldEqual 16
      readVect.asInstanceOf[BinaryVector[Long]].maybeNAs should equal (false)
      readVect.toSeq should equal (Seq(longVal, longVal, longVal, longVal, longVal))
    }

    it("should support resetting and optimizing AppendableVector multiple times") {
      val cb = LongBinaryVector.appendingVector(memFactory, 5)
      // Use large numbers on purpose so cannot optimized to less than 32 bits
      val orig = Seq(100000, 200001, 300002).map(Long.MaxValue - _)
      cb.addNA()
      orig.foreach(cb.addData)
      cb.toSeq should equal (orig)
      val optimized = cb.optimize(memFactory)
      //assert(optimized.base != cb.base)   // just compare instances
      val readVect1 = FiloVector[Long](optimized.toFiloBuffer)
      readVect1.toSeq should equal (orig)

      // Now the optimize should not have damaged original vector
      cb.toSeq should equal (orig)
      cb.reset()
      val orig2 = orig.map(_ * 2)
      orig2.foreach(cb.addData)
      val readVect2 = FiloVector[Long](cb.optimize(memFactory).toFiloBuffer)
      readVect2.toSeq should equal (orig2)
      cb.toSeq should equal (orig2)
    }
  }
}