package filodb.memory.format.vectors

import java.nio.ByteBuffer

import debox.Buffer

import filodb.memory.format._

//noinspection ScalaStyle
class UTF8VectorTest extends NativeVectorTest {
  import filodb.memory.format.Encodings._

  describe("UTF8AppendableVector") {
    it("should be able to append all NAs") {
      val utf8vect = UTF8Vector.appendingVector(memFactory, 5, 1024)
      utf8vect.addNA() shouldEqual Ack
      utf8vect.addNA() shouldEqual Ack
      utf8vect.length should equal (2)
      utf8vect.frozenSize should equal (12 + 2*4)
      utf8vect.isAvailable(0) shouldEqual false
      utf8vect.isAvailable(1) shouldEqual false

      // should be able to apply read back NA values and get back empty string
      utf8vect(0).length should equal (0)
      utf8vect.isAllNA should equal (true)
      utf8vect.noNAs should equal (false)
    }

    it("should be able to append mix of strings and NAs") {
      val strs = Seq("apple", "", "Charlie").map(ZeroCopyUTF8String.apply)
      val utf8vect = UTF8Vector.appendingVector(memFactory, 5, 1024)
      utf8vect.addNA()
      strs.foreach(s => utf8vect.addData(s) shouldEqual Ack)
      utf8vect.addNA()

      utf8vect.length should equal (5)
      utf8vect.copyToBuffer.toList shouldEqual strs
      utf8vect.isAllNA should equal (false)
      utf8vect.noNAs should equal (false)
      utf8vect.isAvailable(0) should equal (false)
      utf8vect.isAvailable(1) should equal (true)
      utf8vect.isAvailable(2) should equal (true)
      utf8vect.numBytes should equal (12 + 5*4 + 12)
      utf8vect.frozenSize should equal (12 + 5*4 + 12)
    }

    it("should be able to calculate min, max # bytes for all elements") {
      val utf8vect = UTF8Vector.appendingVector(memFactory, 5, 1024)
      Seq("apple", "zoe", "bananas").foreach(s => utf8vect.addData(ZeroCopyUTF8String(s)) shouldEqual Ack)
      utf8vect.addNA()   // NA or empty string should not affect min/max len
      val inner = utf8vect.asInstanceOf[GrowableVector[_]].inner.asInstanceOf[UTF8AppendableVector]
      inner.minMaxStrLen should equal ((3, 7))

      val utf8vect2 = UTF8Vector.appendingVector(memFactory, 5, 1024)
      Seq("apple", "", "bananas").foreach(s => utf8vect2.addData(ZeroCopyUTF8String(s)) shouldEqual Ack)
      utf8vect2.noNAs should equal (true)
      val inner2 = utf8vect2.asInstanceOf[GrowableVector[_]].inner.asInstanceOf[UTF8AppendableVector]
      inner2.minMaxStrLen should equal ((0, 7))
    }

    it("should be able to freeze and minimize bytes used") {
      val strs = Seq("apple", "zoe", "bananas").map(ZeroCopyUTF8String.apply)
      val utf8vect = UTF8Vector.appendingVector(memFactory, 10, 1024)
      strs.foreach(s => utf8vect.addData(s) shouldEqual Ack)
      utf8vect.length should equal (3)
      utf8vect.noNAs should equal (true)
      utf8vect.frozenSize should equal (12 + 12 + 5 + 3 + 7)

      val frozen = utf8vect.freeze(memFactory)
      UTF8Vector(acc, frozen).length(acc, frozen) shouldEqual 3
      UTF8Vector(acc, frozen).toBuffer(acc, frozen).toList shouldEqual strs
      BinaryVector.totalBytes(acc, frozen) should equal (12 + 12 + 5 + 3 + 7)
    }


    it("should be able to read from on-heap IntBinaryVector") {
      val strs = Seq("apple", "zoe", "bananas").map(ZeroCopyUTF8String.apply)
      val utf8vect = UTF8Vector.appendingVector(memFactory, 10, 1024)
      strs.foreach(s => utf8vect.addData(s) shouldEqual Ack)
      val frozen = utf8vect.freeze(memFactory)
      val bytes = UTF8Vector(acc, frozen).toBytes(acc, frozen)

      val onHeapAcc = Seq(MemoryAccessor.fromArray(bytes),
        MemoryAccessor.fromByteBuffer(BinaryVector.asBuffer(frozen)),
        MemoryAccessor.fromByteBuffer(ByteBuffer.wrap(bytes)))

      onHeapAcc.foreach { a =>
        UTF8Vector(a, 0).toBuffer(a, 0).toList shouldEqual strs
      }
    }

    it("should be able to parse with UTF8Vector() a DirectBuffer") {
      val strs = Seq("apple", "zoe", "bananas").map(ZeroCopyUTF8String.apply)
      val ptr = UTF8Vector(memFactory, strs).optimize(memFactory)
      val readVect2 = UTF8Vector(BinaryVector.asBuffer(ptr))
      readVect2.toBuffer(acc, ptr).toList shouldEqual strs
    }

    it("should be able to grow the UTF8Vector if run out of initial maxBytes") {
      // Purposefully test when offsets grow beyond 32k
      val strs = (1 to 10000).map(i => ZeroCopyUTF8String("string" + i))
      val utf8vect = UTF8Vector.appendingVector(memFactory, 50, 16384)
      strs.foreach(s => utf8vect.addData(s) shouldEqual Ack)
      val ptr = utf8vect.optimize(memFactory)
      UTF8Vector(acc, ptr).toBuffer(acc, ptr) shouldEqual Buffer.fromIterable(strs)

      val vect2 = UTF8Vector.appendingVector(memFactory, 50, 8192)
      vect2 shouldBe a[GrowableVector[_]]
      vect2.asInstanceOf[GrowableVector[_]].inner shouldBe a[UTF8AppendableVector]
      strs.foreach(s => vect2.addData(s) shouldEqual Ack)
      val ptr2 = vect2.optimize(memFactory)
      UTF8Vector(acc, ptr2).toBuffer(acc, ptr2) shouldEqual Buffer.fromIterable(strs)
    }
  }

  describe("UTF8Appendable.optimize") {
    it("should produce a UTF8Vector if strings mostly same length") {
      val strs = Seq("apple", "zoe", "jack").map(ZeroCopyUTF8String.apply)
      val ptr = UTF8Vector(memFactory, strs).optimize(memFactory)
      val reader = UTF8Vector(acc, ptr)
      reader shouldEqual UTF8FlexibleVectorDataReader
      reader.toBuffer(acc, ptr).toList shouldEqual strs
    }

    it("should produce a UTF8Vector if one string much longer") {
      val strs = Seq("apple", "zoe", "jacksonhole, wyoming").map(ZeroCopyUTF8String.apply)
      val ptr = UTF8Vector(memFactory, strs).optimize(memFactory)
      val reader = UTF8Vector(acc, ptr)
      reader shouldEqual UTF8FlexibleVectorDataReader
      reader.toBuffer(acc, ptr).toList shouldEqual strs
    }

    it("should produce a UTF8ConstVector if all strings the same") {
      val strs = Seq.fill(50)("apple").map(ZeroCopyUTF8String.apply)
      val ptr = UTF8Vector(memFactory, strs).optimize(memFactory)
      val reader = UTF8Vector(acc, ptr)
      reader shouldEqual UTF8ConstVector
      reader.toBuffer(acc, ptr).toList shouldEqual strs
    }
  }

  describe("DictUTF8Vector") {
    it("shouldMakeDict when source strings are mostly repeated") {
      val strs = Seq("apple", "zoe", "grape").permutations.flatten.toList.map(ZeroCopyUTF8String.apply)
      val dictInfo = DictUTF8Vector.shouldMakeDict(memFactory, UTF8Vector(memFactory, strs), samplingRate=0.5)
      dictInfo should be ('defined)
      dictInfo.get.codeMap.size should equal (3)
      dictInfo.get.dictStrings.length should equal (4)
    }

    it("should not makeDict when source strings are all unique") {
      val strs = (0 to 9).map(_.toString).map(ZeroCopyUTF8String.apply)
      val dictInfo = DictUTF8Vector.shouldMakeDict(memFactory, UTF8Vector(memFactory, strs))
      dictInfo should be ('empty)
    }

    it("should optimize UTF8Vector to DictVector with NAs and read it back") {
      val strs = ZeroCopyUTF8String.NA +:
                 Seq("apple", "zoe", "grape").permutations.flatten.toList.map(ZeroCopyUTF8String.apply)
      val ptr = UTF8Vector(memFactory, strs).optimize(memFactory, AutoDictString(samplingRate=0.5))
      val reader = UTF8Vector(acc, ptr)
      reader shouldEqual UTF8DictVectorDataReader
      reader.length(acc, ptr) shouldEqual strs.length
      reader.toBuffer(acc, ptr).toList shouldEqual strs.drop(1)
      reader(acc, ptr, 0) // shouldEqual ZeroCopyUTF8String("")
    }

    // Negative byte values might not get converted to ints properly, leading
    // to an ArrayOutOfBoundsException.
    it("should ensure proper conversion when there are 128-255 unique strings") {
      val orig = (0 to 130).map(_.toString).map(ZeroCopyUTF8String.apply)
      val ptr = UTF8Vector(memFactory, orig).optimize(memFactory,
                                             AutoDictString(spaceThreshold = 1.1))
      val reader = UTF8Vector(acc, ptr)
      reader shouldEqual UTF8DictVectorDataReader

      reader.length(acc, ptr) shouldEqual orig.length
      reader.toBuffer(acc, ptr).toList shouldEqual orig
    }
  }
}