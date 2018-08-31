package filodb.memory.format.vectors

import debox.Buffer

import filodb.memory.format._

class IntBinaryVectorTest extends NativeVectorTest {
  describe("IntAppendingVector") {
    it("should append a mix of Ints and read them all back") {
      val orig = Seq(1, 2, -5, 101)
      val builder = IntBinaryVector(memFactory, orig)
      builder.length shouldEqual 4
      val frozen = builder.freeze(memFactory)
      IntBinaryVector(frozen).toBuffer(frozen).toList shouldEqual orig
    }

    it("should append 16-bit Ints and read them back") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 5)
      val orig = Seq(1, 0, -127, Short.MaxValue, Short.MinValue)
      orig.foreach(x => builder.addData(x) shouldEqual Ack)
      builder.length should equal (5)
      val frozen = builder.freeze(memFactory)
      IntBinaryVector(frozen).toBuffer(frozen).toList should equal (orig)
    }

    it("should append bytes and read them back") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 4)
      val orig = Seq(1, 0, -128, 127)
      orig.foreach(x => builder.addData(x) shouldEqual Ack)
      builder.length should equal (4)
      val frozen = builder.freeze(memFactory)
      IntBinaryVector(frozen).toBuffer(frozen).toList should equal (orig)
    }

    it("should iterate with startElement > 0") {
      val orig = Seq(1, 0, -128, 127, 2, 4, 3, 7)
      val builder = IntBinaryVector(memFactory, orig)
      builder.length shouldEqual orig.length
      val frozen = builder.freeze(memFactory)
      (2 to 5).foreach { start =>
        IntBinaryVector(frozen).toBuffer(frozen, start).toList shouldEqual orig.drop(start)
      }
    }

    it("should be able to create new FiloVector from frozen appending vector") {
      // Make sure it can freeze when primaryMaxBytes is much greater.
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 1000)
      val orig = Seq(1, 0, -128, 127)
      orig.foreach(x => builder.addData(x) shouldEqual Ack)
      val readVect = IntBinaryVector(builder.addr)
      readVect.length(builder.addr) shouldEqual 4
      readVect.toBuffer(builder.addr).toList shouldEqual orig

      builder.frozenSize should equal (24)
      val frozen = builder.freeze(memFactory)
      IntBinaryVector(frozen).length(frozen) shouldEqual 4
      IntBinaryVector(frozen).toBuffer(frozen).toList shouldEqual orig
    }

    it("should return VectorTooSmall if not enough space to add new items") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 4)
      val orig = Seq(1, 2, -5, 101)
      orig.foreach(x => builder.addData(x) shouldEqual Ack)
      builder.addNA() shouldEqual VectorTooSmall(25, 24)
      builder.length shouldEqual 4
    }
  }

  describe("IntBinaryVector 2/4 bit") {
    it("should append and read back list with nbits=4") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 10, nbits=4, signed=false)
      builder.length should equal (0)
      builder.addData(2) shouldEqual Ack
      builder.numBytes should equal (8 + 1)
      builder.reader.toBuffer(builder.addr).toList shouldEqual Seq(2)
      builder.addData(4) shouldEqual Ack
      builder.addData(3) shouldEqual Ack
      builder.length should equal (3)
      builder.reader.toBuffer(builder.addr).toList shouldEqual Seq(2, 4, 3)
      builder.frozenSize should equal (8 + 2)
      val frozen = builder.freeze(memFactory)
      IntBinaryVector(frozen).length(frozen) shouldEqual 3
      IntBinaryVector(frozen).toBuffer(frozen).toList shouldEqual Seq(2, 4, 3)
    }

    it("should append and read back list with nbits=2") {
      val builder = IntBinaryVector.appendingVectorNoNA(memFactory, 10, nbits=2, signed=false)
      val orig = Seq(0, 2, 1, 3, 2)
      orig.foreach(x => builder.addData(x) shouldEqual Ack)
      builder.reader.toBuffer(builder.addr).toList shouldEqual orig
      builder.numBytes shouldEqual 10

      val frozen = builder.freeze(memFactory)
      IntBinaryVector(frozen).toBuffer(frozen).toList shouldEqual orig
    }

    it("should optimize even with NoNA vectors to less nbits") {
      val orig = Seq(0, 2, 1, 3, 2)
      val builder1 = IntBinaryVector(memFactory, orig)
      val intVect = builder1.optimize(memFactory)
      IntBinaryVector(intVect).toBuffer(intVect).toList shouldEqual orig
      BinaryVector.totalBytes(intVect) shouldEqual 10

      val builder2 = IntBinaryVector.appendingVectorNoNA(memFactory, 10)
      orig.foreach(x => builder2.addData(x) shouldEqual Ack)
      BinaryVector.totalBytes(builder2.optimize(memFactory)) shouldEqual 10
    }
  }

  describe("MaskedIntAppendingVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = IntBinaryVector.appendingVector(memFactory, 100)
      builder.addNA shouldEqual Ack
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val sc = builder.optimize(memFactory)
      IntBinaryVector(sc).length(sc) should equal (1)
      IntBinaryVector(sc)(sc, 0)   // Just to make sure this does not throw an exception
      // IntBinaryVector(sc).isAvailable(0) should equal (false)
      IntBinaryVector(sc).toBuffer(sc) shouldEqual Buffer.empty[Int]
      // IntBinaryVector(sc).optionIterator.toSeq should equal (Seq(None))
    }

    it("should encode a mix of NAs and Ints and decode iterate and skip NAs") {
      val cb = IntBinaryVector.appendingVector(memFactory, 5)
      cb.addNA shouldEqual Ack
      cb.addData(101) shouldEqual Ack
      cb.addData(102) shouldEqual Ack
      cb.addData(103) shouldEqual Ack
      cb.addNA shouldEqual Ack
      cb.isAllNA should be (false)
      cb.noNAs should be (false)
      val sc = cb.optimize(memFactory)
      val reader = IntBinaryVector(sc)

      reader.length(sc) shouldEqual 5
      // reader.isAvailable(0) should equal (false)
      // reader.isAvailable(1) should equal (true)
      // reader.isAvailable(4) should equal (false)
      reader(sc, 1) should equal (101)
      // reader.get(0) should equal (None)
      // reader.get(-1) should equal (None)
      // reader.get(2) should equal (Some(102))
      reader.toBuffer(sc) shouldEqual Buffer(101, 102, 103)
    }

    it("should be able to append lots of ints off-heap and grow vector") {
      val numInts = 1000
      val builder = IntBinaryVector.appendingVector(memFactory, numInts / 2)
      (0 until numInts).foreach(x => builder.addData(x) shouldEqual Ack)
      builder.length should equal (numInts)
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
    }

    it("should be able to grow vector even if adding all NAs") {
      val numInts = 1000
      val builder = IntBinaryVector.appendingVector(memFactory, numInts / 2)
      builder shouldBe a[GrowableVector[_]]
      (0 until numInts).foreach(i => builder.addNA shouldEqual Ack)
      builder.length should equal (numInts)
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
    }

    it("should be able to return minMax accurately with NAs") {
      val cb = IntBinaryVector.appendingVector(memFactory, 5)
      cb.addNA shouldEqual Ack
      cb.addData(101) shouldEqual Ack
      cb.addData(102) shouldEqual Ack
      cb.addData(103) shouldEqual Ack
      cb.addNA shouldEqual Ack
      val inner = cb.asInstanceOf[GrowableVector[Int]].inner.asInstanceOf[MaskedIntAppendingVector]
      inner.minMax should equal ((101, 103))
    }

    it("should be able to freeze() and minimize bytes used") {
      val builder = IntBinaryVector.appendingVector(memFactory, 100)
      // Test numBytes to make sure it's accurate
      builder.numBytes should equal (12 + 16 + 8)   // 2 long words needed for 100 bits
      (0 to 4).foreach(x => builder.addData(x) shouldEqual Ack)
      builder.numBytes should equal (12 + 16 + 8 + 20)
      val frozen = builder.freeze(memFactory)
      BinaryVector.totalBytes(frozen) should equal (12 + 8 + 8 + 20)  // bitmask truncated

      IntBinaryVector(frozen).length(frozen) shouldEqual 5
      IntBinaryVector(frozen).toBuffer(frozen).toList should equal (0 to 4)
    }

    it("should optimize and parse back using IntBinaryVector.apply") {
      val cb = IntBinaryVector.appendingVector(memFactory, 5)
      cb.addNA shouldEqual Ack
      cb.addData(101) shouldEqual Ack
      cb.addData(102) shouldEqual Ack
      cb.addData(103) shouldEqual Ack
      cb.addNA shouldEqual Ack
      val buffer = cb.optimize(memFactory)
      val readVect = IntBinaryVector(buffer)
      readVect.toBuffer(buffer) shouldEqual Buffer(101, 102, 103)
    }

    it("should support resetting and optimizing AppendableVector multiple times") {
      val cb = IntBinaryVector.appendingVector(memFactory, 5)
      // Use large numbers on purpose so cannot optimized to less than 32 bits
      val orig = Seq(100000, 200001, 300002)
      cb.addNA() shouldEqual Ack
      orig.foreach(x => cb.addData(x) shouldEqual Ack)
      cb.copyToBuffer.toList shouldEqual orig
      val optimized = cb.optimize(memFactory)
      val readVect1 = IntBinaryVector(optimized)
      readVect1.toBuffer(optimized).toList shouldEqual orig

      // Now the optimize should not have damaged original vector
      cb.copyToBuffer shouldEqual Buffer.fromIterable(orig)
      cb.reset()
      val orig2 = orig.map(_ * 2)
      orig2.foreach(x => cb.addData(x) shouldEqual Ack)
      val frozen2 = cb.optimize(memFactory)
      val readVect2 = IntBinaryVector(BinaryVector.asBuffer(frozen2))
      readVect2.toBuffer(frozen2).toList shouldEqual orig2
      cb.copyToBuffer.toList shouldEqual orig2
    }

    it("should be able to optimize a 32-bit appending vector to smaller size") {
      val builder = IntBinaryVector.appendingVector(memFactory, 100)
      (0 to 4).foreach(x => builder.addData(x) shouldEqual Ack)
      val optimized = builder.optimize(memFactory)
      IntBinaryVector(optimized).length(optimized) shouldEqual 5
      IntBinaryVector(optimized).toBuffer(optimized) shouldEqual Buffer.fromIterable(0 to 4)
      BinaryVector.totalBytes(optimized) shouldEqual (8 + 3)   // nbits=4, so only 3 extra bytes
    }

    it("should be able to optimize constant ints to an IntConstVector") {
      val builder = IntBinaryVector.appendingVector(memFactory, 100)
      (0 to 4).foreach(n => builder.addData(999))
      val buf = builder.optimize(memFactory)
      val readVect = IntBinaryVector(buf)
      readVect shouldEqual IntConstVector
      readVect.toBuffer(buf) shouldEqual Buffer(999, 999, 999, 999, 999)
    }
  }
}