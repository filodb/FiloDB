package filodb.memory.format.vectors

import filodb.memory.format.{FiloVector, GrowableVector}
import org.scalatest.{FunSpec, Matchers}

class DoubleVectorTest extends FunSpec with Matchers {
  describe("DoubleMaskedAppendableVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = DoubleVector.appendingVector(100)
      builder.addNA
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val sc = builder.freeze()
      sc.length should equal (1)
      sc(0)   // Just to make sure this does not throw an exception
      sc.isAvailable(0) should equal (false)
      sc.toList should equal (Nil)
      sc.optionIterator.toSeq should equal (Seq(None))
    }

    it("should encode a mix of NAs and Doubles and decode iterate and skip NAs") {
      val cb = DoubleVector.appendingVector(5)
      cb.addNA
      cb.addData(101)
      cb.addData(102.5)
      cb.addData(103)
      cb.addNA
      cb.isAllNA should be (false)
      cb.noNAs should be (false)
      val sc = cb.freeze()

      sc.length should equal (5)
      sc.isAvailable(0) should equal (false)
      sc.isAvailable(1) should equal (true)
      sc.isAvailable(4) should equal (false)
      sc(1) should equal (101)
      sc.boxed(2) should equal (102.5)
      //noinspection ScalaStyle
      sc.boxed(2) shouldBe a [java.lang.Double]
      sc.get(0) should equal (None)
      sc.get(-1) should equal (None)
      sc.get(2) should equal (Some(102.5))
      sc.toList should equal (List(101, 102.5, 103))
    }

    it("should be able to append lots of Doubles and grow vector") {
      val numDoubles = 1000
      val builder = DoubleVector.appendingVector(numDoubles / 2)
      (0 until numDoubles).map(_.toDouble).foreach(builder.addData)
      builder.length should equal (numDoubles)
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
    }

    it("should be able to append lots of Doubles off-heap and grow vector") {
      val numDoubles = 1000
      val builder = DoubleVector.appendingVector(numDoubles / 2, offheap=true)
      (0 until numDoubles).map(_.toDouble).foreach(builder.addData)
      builder.length should equal (numDoubles)
      builder.isOffheap shouldEqual true
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
    }

    it("should be able to return minMax accurately with NAs") {
      val cb = DoubleVector.appendingVector(5)
      cb.addNA
      cb.addData(10.1)
      cb.addData(102)
      cb.addData(1.03E9)
      cb.addNA
      val inner = cb.asInstanceOf[GrowableVector[Double]].inner.asInstanceOf[MaskedDoubleAppendingVector]
      inner.minMax should equal ((10.1, 1.03E9))
    }

    it("should be able to freeze() and minimize bytes used") {
      val builder = DoubleVector.appendingVector(100)
      // Test numBytes to make sure it's accurate
      builder.numBytes should equal (4 + 16 + 4)   // 2 long words needed for 100 bits
      (0 to 4).map(_.toDouble).foreach(builder.addData)
      builder.numBytes should equal (4 + 16 + 4 + 40)
      val frozen = builder.freeze()
      frozen.numBytes should equal (4 + 8 + 4 + 40)  // bitmask truncated

      frozen.length should equal (5)
      frozen.toSeq should equal (0 to 4)
    }

    it("should toFiloBuffer and read back using FiloVector.apply") {
      val cb = DoubleVector.appendingVector(5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103.7)
      cb.addNA
      val buffer = cb.optimize().toFiloBuffer
      val readVect = FiloVector[Double](buffer)
      readVect.toSeq should equal (Seq(101.0, 102.0, 103.7))
    }

    it("should be able to optimize all integral vector to IntBinaryVector") {
      val builder = DoubleVector.appendingVector(100)
      (0 to 4).map(_.toDouble).foreach(builder.addData)
      val optimized = builder.optimize()
      optimized.length should equal (5)
      optimized.toSeq should equal (0 to 4)
      optimized(0) should equal (0.0)
      optimized.numBytes should equal (4 + 3)   // nbits=4, so only 3 extra bytes
      val readVect = FiloVector[Double](optimized.toFiloBuffer)
      readVect.toSeq should equal (0 to 4)
    }

    it("should be able to optimize off-heap all integral vector to IntBinaryVector") {
      val builder = DoubleVector.appendingVector(100, offheap=true)
      (0 to 4).map(_.toDouble).foreach(builder.addData)
      val optimized = builder.optimize()
      optimized.length shouldEqual 5
      optimized.isOffheap shouldEqual true
      optimized.toSeq should equal (0 to 4)
      optimized(0) should equal (0.0)
      optimized.numBytes should equal (4 + 3)   // nbits=4, so only 3 extra bytes
      val readVect = FiloVector[Double](optimized.toFiloBuffer)
      readVect.toSeq should equal (0 to 4)
    }

    it("should be able to optimize constant Doubles to an IntConstVector") {
      val builder = DoubleVector.appendingVector(100)
      (0 to 4).foreach(n => builder.addData(99.9))
      val buf = builder.optimize().toFiloBuffer
      val readVect = FiloVector[Double](buf)
      readVect shouldBe a[DoubleConstVector]
      readVect.toSeq should equal (Seq(99.9, 99.9, 99.9, 99.9, 99.9))
    }

    it("should support resetting and optimizing AppendableVector multiple times") {
      val cb = DoubleVector.appendingVector(5)
      // Use large numbers on purpose so cannot optimize to Doubles or const
      val orig = Seq(11.11E101, -2.2E-176, 1.77E88)
      cb.addNA()
      orig.foreach(cb.addData)
      cb.toSeq should equal (orig)
      val optimized = cb.optimize()
      //bases will be equal in offheap
      //assert(optimized.base != cb.base)   // just compare instances
      val readVect1 = FiloVector[Double](optimized.toFiloBuffer)
      readVect1.toSeq should equal (orig)

      // Now the optimize should not have damaged original vector
      cb.toSeq should equal (orig)
      cb.reset()
      val orig2 = orig.map(_ * 2)
      orig2.foreach(cb.addData)
      val readVect2 = FiloVector[Double](cb.optimize().toFiloBuffer)
      readVect2.toSeq should equal (orig2)
      cb.toSeq should equal (orig2)
    }
  }
}