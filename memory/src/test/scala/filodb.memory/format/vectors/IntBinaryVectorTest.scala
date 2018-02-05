package filodb.memory.format.vectors

import org.scalatest.{FunSpec, Matchers}

import filodb.memory.NativeMemoryManager
import filodb.memory.format.{FiloVector, GrowableVector, VectorTooSmall}

class IntBinaryVectorTest extends FunSpec with Matchers {
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)

  describe("IntAppendingVector") {
    it("should append a mix of Ints and read them all back") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 4)
      val orig = Seq(1, 2, -5, 101)
      orig.foreach(builder.addData)
      builder.length should equal (4)
      builder.freeze(memFactory).toSeq should equal (orig)
    }

    it("should append 16-bit Ints and read them back") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 5)
      val orig = Seq(1, 0, -127, Short.MaxValue, Short.MinValue)
      orig.foreach(builder.addData)
      builder.length should equal (5)
      builder.freeze(memFactory).toSeq should equal (orig)
    }

    it("should append bytes and read them back") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 4)
      val orig = Seq(1, 0, -128, 127)
      orig.foreach(builder.addData)
      builder.length should equal (4)
      builder.freeze(memFactory).toSeq should equal (orig)
    }

    it("should be able to create new FiloVector from frozen appending vector") {
      // Make sure it can freeze when primaryMaxBytes is much greater.
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 1000)
      val orig = Seq(1, 0, -128, 127)
      orig.foreach(builder.addData)
      val readVect = IntBinaryVector(builder.base, builder.offset, builder.numBytes, builder.dispose)
      readVect.length should equal (4)
      readVect.toSeq should equal (orig)

      builder.frozenSize should equal (20)
      val frozen = builder.freeze(memFactory)
      frozen.length should equal (4)
      frozen.toSeq should equal (orig)
    }

    it("should throw error if not enough space to add new items") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 4)
      val orig = Seq(1, 2, -5, 101)
      orig.foreach(builder.addData)
      intercept[VectorTooSmall] { builder.addNA() }
    }
  }

  describe("IntBinaryVector 2/4 bit") {
    it("should append and read back list with nbits=4") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 10, nbits=4, signed=false)
      builder.length should equal (0)
      builder.addData(2)
      builder.numBytes should equal (5)
      builder.toSeq should equal (Seq(2))
      builder.addData(4)
      builder.addData(3)
      builder.length should equal (3)
      builder.toSeq should equal (Seq(2, 4, 3))
      builder.frozenSize should equal (6)
      val frozen = builder.freeze(memFactory)
      frozen.length should equal (3)
      frozen.toSeq should equal (Seq(2, 4, 3))

      val intVect = FiloVector[Int](builder.toFiloBuffer)
      intVect.toSeq should equal (Seq(2, 4, 3))
    }

    it("should append and read back list with nbits=2") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 10, nbits=2, signed=false)
      val orig = Seq(0, 2, 1, 3, 2)
      orig.foreach(builder.addData)
      builder.toSeq should equal (orig)
      builder.numBytes should equal (6)

      val intVect = FiloVector[Int](builder.toFiloBuffer)
      intVect.toSeq should equal (orig)
    }
  }

  describe("MaskedIntAppendingVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = IntBinaryVector.appendingVector(memFactory, 100)
      builder.addNA
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val sc = builder.optimize(memFactory)
      //sc.base should not equal (builder.base)
      sc.length should equal (1)
      sc(0)   // Just to make sure this does not throw an exception
      sc.isAvailable(0) should equal (false)
      sc.toList should equal (Nil)
      sc.optionIterator.toSeq should equal (Seq(None))
    }

    it("should encode a mix of NAs and Ints and decode iterate and skip NAs") {
      val cb = IntBinaryVector.appendingVector(memFactory, 5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      cb.isAllNA should be (false)
      cb.noNAs should be (false)
      val sc = cb.optimize(memFactory)
      //sc.base should not equal (cb.base)

      sc.length should equal (5)
      sc.isAvailable(0) should equal (false)
      sc.isAvailable(1) should equal (true)
      sc.isAvailable(4) should equal (false)
      sc(1) should equal (101)
      sc.boxed(2) should equal (102)
      //noinspection ScalaStyle
      sc.boxed(2) shouldBe a [java.lang.Integer]
      sc.get(0) should equal (None)
      sc.get(-1) should equal (None)
      sc.get(2) should equal (Some(102))
      sc.toList should equal (List(101, 102, 103))
    }

    it("should be able to append lots of ints and grow vector") {
      val numInts = 1000
      val builder = IntBinaryVector.appendingVector(memFactory, numInts / 2)
      (0 until numInts).foreach(builder.addData)
      builder.length should equal (numInts)
      //builder.isOffheap shouldEqual false
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
    }

    it("should be able to append lots of ints off-heap and grow vector") {
      val numInts = 1000
      val builder = IntBinaryVector.appendingVector(memFactory, numInts / 2)
      (0 until numInts).foreach(builder.addData)
      builder.length should equal (numInts)
      builder.isOffheap shouldEqual true
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
    }

    it("should be able to grow vector even if adding all NAs") {
      val numInts = 1000
      val builder = IntBinaryVector.appendingVector(memFactory, numInts / 2)
      (0 until numInts).foreach(i => builder.addNA)
      builder.length should equal (numInts)
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
    }

    it("should be able to return minMax accurately with NAs") {
      val cb = IntBinaryVector.appendingVector(memFactory, 5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      val inner = cb.asInstanceOf[GrowableVector[Int]].inner.asInstanceOf[MaskedIntAppendingVector]
      inner.minMax should equal ((101, 103))
    }

    it("should be able to freeze() and minimize bytes used") {
      val builder = IntBinaryVector.appendingVector(memFactory, 100)
      // Test numBytes to make sure it's accurate
      builder.numBytes should equal (4 + 16 + 4)   // 2 long words needed for 100 bits
      (0 to 4).foreach(builder.addData)
      builder.numBytes should equal (4 + 16 + 4 + 20)
      val frozen = builder.freeze(memFactory)
      frozen.numBytes should equal (4 + 8 + 4 + 20)  // bitmask truncated

      frozen.length shouldEqual 5
      frozen.toSeq should equal (0 to 4)
    }

    it("should toFiloBuffer and read back using FiloVector.apply") {
      val cb = IntBinaryVector.appendingVector(memFactory, 5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      val buffer = cb.optimize(memFactory).toFiloBuffer
      val readVect = FiloVector[Int](buffer)
      readVect.toSeq should equal (Seq(101, 102, 103))
    }

    it("should toFiloBuffer from offheap and read back using FiloVector.apply") {
      val cb = IntBinaryVector.appendingVector(memFactory, 5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      val buffer = cb.optimize(memFactory).toFiloBuffer
      val readVect = FiloVector[Int](buffer)
      readVect.toSeq should equal (Seq(101, 102, 103))
    }

    it("should support resetting and optimizing AppendableVector multiple times") {
      val cb = IntBinaryVector.appendingVector(memFactory, 5)
      // Use large numbers on purpose so cannot optimized to less than 32 bits
      val orig = Seq(100000, 200001, 300002)
      cb.addNA()
      orig.foreach(cb.addData)
      cb.toSeq should equal (orig)
      val optimized = cb.optimize(memFactory)
      //assert(optimized.base != cb.base)   // just compare instances
      val readVect1 = FiloVector[Int](optimized.toFiloBuffer)
      readVect1.toSeq should equal (orig)

      // Now the optimize should not have damaged original vector
      cb.toSeq should equal (orig)
      cb.reset()
      val orig2 = orig.map(_ * 2)
      orig2.foreach(cb.addData)
      val readVect2 = FiloVector[Int](cb.optimize(memFactory).toFiloBuffer)
      readVect2.toSeq should equal (orig2)
      cb.toSeq should equal (orig2)
    }

    it("should be able to optimize a 32-bit appending vector to smaller size") {
      val builder = IntBinaryVector.appendingVector(memFactory, 100)
      (0 to 4).foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      optimized.length shouldEqual 5
      optimized.toSeq should equal (0 to 4)
      optimized.numBytes should equal (4 + 3)   // nbits=4, so only 3 extra bytes
    }

    it("should be able to optimize a 32-bit offheap vector to smaller size") {
      val builder = IntBinaryVector.appendingVector(memFactory, 100)
      (0 to 4).foreach(builder.addData)
      val optimized = builder.optimize(memFactory)
      optimized.length shouldEqual 5
      optimized.isOffheap shouldEqual true
      optimized.toSeq should equal (0 to 4)
      optimized.numBytes should equal (4 + 3)   // nbits=4, so only 3 extra bytes
    }

    it("should be able to optimize constant ints to an IntConstVector") {
      val builder = IntBinaryVector.appendingVector(memFactory, 100)
      (0 to 4).foreach(n => builder.addData(999))
      val buf = builder.optimize(memFactory).toFiloBuffer
      val readVect = FiloVector[Int](buf)
      readVect shouldBe a[IntConstVector]
      readVect.toSeq should equal (Seq(999, 999, 999, 999, 999))
    }
  }
}