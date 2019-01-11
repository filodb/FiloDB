package filodb.memory.format.vectors

import debox.Buffer

import filodb.memory.format.{BinaryVector, GrowableVector}

class DoubleVectorTest extends NativeVectorTest {
  describe("DoubleMaskedAppendableVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = DoubleVector.appendingVector(memFactory, 100)
      builder.addNA
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val sc = builder.freeze(memFactory)
      DoubleVector(sc).length(sc) shouldEqual 1
      DoubleVector(sc)(sc, 0)   // Just to make sure this does not throw an exception
      DoubleVector(sc).toBuffer(sc) shouldEqual Buffer.empty[Double]
    }

    it("should encode a mix of NAs and Doubles and decode iterate and skip NAs") {
      val cb = DoubleVector.appendingVector(memFactory, 5)
      cb.addNA
      cb.addData(101)
      cb.addData(102.5)
      cb.addData(103)
      cb.addNA
      cb.isAllNA should be (false)
      cb.noNAs should be (false)
      val sc = cb.freeze(memFactory)

      DoubleVector(sc).length(sc) shouldEqual 5
      DoubleVector(sc)(sc, 1) shouldEqual 101.0
      DoubleVector(sc).toBuffer(sc) shouldEqual Buffer(101, 102.5, 103)
    }

    it("should be able to append lots of Doubles off-heap and grow vector") {
      val numDoubles = 1000
      val builder = DoubleVector.appendingVector(memFactory, numDoubles / 2)
      (0 until numDoubles).map(_.toDouble).foreach(builder.addData)
      builder.length should equal (numDoubles)
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
    }

    it("should be able to return minMax accurately with NAs") {
      val cb = DoubleVector.appendingVector(memFactory, 5)
      cb.addNA
      cb.addData(10.1)
      cb.addData(102)
      cb.addData(1.03E9)
      cb.addNA
      val inner = cb.asInstanceOf[GrowableVector[Double]].inner.asInstanceOf[MaskedDoubleAppendingVector]
      inner.minMax should equal ((10.1, 1.03E9))
    }

    it("should be able to freeze() and minimize bytes used") {
      val builder = DoubleVector.appendingVector(memFactory, 100)
      // Test numBytes to make sure it's accurate
      builder.numBytes should equal (12 + 16 + 8)   // 2 long words needed for 100 bits
      (0 to 4).map(_.toDouble).foreach(builder.addData)
      builder.numBytes should equal (12 + 16 + 8 + 40)
      val frozen = builder.freeze(memFactory)
      BinaryVector.totalBytes(frozen) should equal (12 + 8 + 8 + 40)  // bitmask truncated

      DoubleVector(frozen).length(frozen) shouldEqual 5
      DoubleVector(frozen).toBuffer(frozen) shouldEqual Buffer.fromIterable((0 to 4).map(_.toDouble))
    }

    it("should be able to optimize all integral vector to DeltaDeltaVector") {
      val orig = (0 to 9).map(_.toDouble)
      val builder = DoubleVector(memFactory, orig)
      val optimized = builder.optimize(memFactory)
      DoubleVector(optimized).length(optimized) shouldEqual 10
      DoubleVector(optimized).toBuffer(optimized).toList shouldEqual orig
      DoubleVector(optimized)(optimized, 0) shouldEqual 0.0
      DoubleVector(optimized)(optimized, 2) shouldEqual 2.0
      BinaryVector.totalBytes(optimized) shouldEqual 24   // Const DeltaDeltaVector (since this is linearly increasing)
    }

    it("should be able to optimize off-heap No NA integral vector to DeltaDeltaVector") {
      val builder = DoubleVector.appendingVectorNoNA(memFactory, 100)
      // Use higher numbers to verify they can be encoded efficiently too
      (100000 to 100004).map(_.toDouble).foreach(builder.addData)
      val optimized = builder.optimize(memFactory)

      DoubleVector(optimized).length(optimized) shouldEqual 5
      DoubleVector(optimized).toBuffer(optimized) shouldEqual Buffer.fromIterable((100000 to 100004).map(_.toDouble))
      DoubleVector(optimized)(optimized, 2) shouldEqual 100002.0
      BinaryVector.totalBytes(optimized) shouldEqual 24   // Const DeltaDeltaVector (since this is linearly increasing)
    }

    it("should iterate with startElement > 0") {
      val orig = Seq(1000, 2001.1, 2999.99, 5123.4, 5250, 6004, 7678)
      val builder = DoubleVector.appendingVectorNoNA(memFactory, orig.length)
      orig.foreach(builder.addData)
      builder.length shouldEqual orig.length
      val frozen = builder.optimize(memFactory)
      (2 to 5).foreach { start =>
        DoubleVector(frozen).toBuffer(frozen, start).toList shouldEqual orig.drop(start)
      }
    }

    it("should sum uncompressed double vectors and ignore NaN values") {
      val orig = Seq(1000, 2001.1, 2999.99, 5123.4, 5250, 6004, 7678)
      val builder = DoubleVector.appendingVectorNoNA(memFactory, orig.length + 2)
      orig.foreach(builder.addData)

      val reader = builder.reader.asDoubleReader
      reader.sum(builder.addr, 2, orig.length - 1) shouldEqual (orig.drop(2).sum)

      // Now add a NaN
      builder.addData(Double.NaN)
      builder.length shouldEqual (orig.length + 1)
      reader.sum(builder.addr, 2, orig.length) shouldEqual (orig.drop(2).sum)
    }

    it("should support resetting and optimizing AppendableVector multiple times") {
      val cb = DoubleVector.appendingVector(memFactory, 5)
      // Use large numbers on purpose so cannot optimize to Doubles or const
      val orig = Seq(11.11E101, -2.2E-176, 1.77E88)
      cb.addNA()
      orig.foreach(cb.addData)
      cb.copyToBuffer.toList shouldEqual orig
      val optimized = cb.optimize(memFactory)
      DoubleVector(optimized).toBuffer(optimized).toList shouldEqual orig

      // Now the optimize should not have damaged original vector
      cb.copyToBuffer.toList shouldEqual orig
      cb.reset()
      val orig2 = orig.map(_ * 2)
      orig2.foreach(cb.addData)
      val opt2 = cb.optimize(memFactory)
      DoubleVector(opt2).toBuffer(opt2).toList shouldEqual orig2
      cb.copyToBuffer.toList shouldEqual orig2
    }
  }
}