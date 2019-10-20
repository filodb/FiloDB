package filodb.memory.format.vectors

import debox.Buffer

import filodb.memory.format._

class DoubleVectorTest extends NativeVectorTest {

  describe("DoubleMaskedAppendableVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = DoubleVector.appendingVector(memFactory, 100)
      builder.addNA
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val sc = builder.freeze(memFactory)
      DoubleVector(acc, sc).length(acc, sc) shouldEqual 1
      // Just to make sure this does not throw an exception
      DoubleVector(acc, sc)(acc, sc, 0)
      DoubleVector(acc, sc)
        .toBuffer(acc, sc) shouldEqual Buffer.empty[Double]
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

      DoubleVector(acc, sc)
        .length(acc, sc) shouldEqual 5
      DoubleVector(acc, sc)(acc, sc, 1) shouldEqual 101.0
      DoubleVector(acc, sc)
        .toBuffer(acc, sc) shouldEqual Buffer(101, 102.5, 103)
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
      BinaryVector.totalBytes(acc, frozen) should equal (12 + 8 + 8 + 40)  // bitmask truncated

      DoubleVector(acc, frozen).length(acc, frozen) shouldEqual 5
      DoubleVector(acc, frozen)
        .toBuffer(acc, frozen) shouldEqual Buffer.fromIterable((0 to 4).map(_.toDouble))
    }

    it("should be able to optimize all integral vector to DeltaDeltaVector") {
      val orig = (0 to 9).map(_.toDouble)
      val builder = DoubleVector(memFactory, orig)
      val optimized = builder.optimize(memFactory)
      DoubleVector(acc, optimized).length(acc, optimized) shouldEqual 10
      DoubleVector(acc, optimized).toBuffer(acc, optimized).toList shouldEqual orig
      DoubleVector(acc, optimized)(acc, optimized, 0) shouldEqual 0.0
      DoubleVector(acc, optimized)(acc, optimized, 2) shouldEqual 2.0
      // Const DeltaDeltaVector (since this is linearly increasing)
      BinaryVector.totalBytes(acc, optimized) shouldEqual 24
    }

    it("should encode some edge cases correctly to DDV") {
      val orig = Seq(55.0, 60.0) ++ Seq.fill(10)(60.0)
      val appender = DoubleVector.appendingVectorNoNA(memFactory, 100)
      orig.foreach(appender.addData)
      appender.length shouldEqual orig.length

      val optimized = appender.optimize(memFactory)
      DoubleVector(acc, optimized).length(acc, optimized) shouldEqual orig.length
      DoubleVector(acc, optimized).toBuffer(acc, optimized).toList shouldEqual orig
      BinaryVector.totalBytes(acc, optimized) should be > (24)   // Not const DDV!
    }

    it("should be able to optimize off-heap No NA integral vector to DeltaDeltaVector") {
      val builder = DoubleVector.appendingVectorNoNA(memFactory, 100)
      // Use higher numbers to verify they can be encoded efficiently too
      (100000 to 100004).map(_.toDouble).foreach(builder.addData)
      val optimized = builder.optimize(memFactory)

      DoubleVector(acc, optimized).length(acc, optimized) shouldEqual 5
      DoubleVector(acc, optimized)
        .toBuffer(acc, optimized) shouldEqual Buffer.fromIterable((100000 to 100004).map(_.toDouble))
      DoubleVector(acc, optimized)(acc, optimized, 2) shouldEqual 100002.0
      // Const DeltaDeltaVector (since this is linearly increasing)
      BinaryVector.totalBytes(acc, optimized) shouldEqual 24
    }

    it("should iterate with startElement > 0") {
      val orig = Seq(1000, 2001.1, 2999.99, 5123.4, 5250, 6004, 7678)
      val builder = DoubleVector.appendingVectorNoNA(memFactory, orig.length)
      orig.foreach(builder.addData)
      builder.length shouldEqual orig.length
      val frozen = builder.optimize(memFactory)
      (2 to 5).foreach { start =>
        DoubleVector(acc, frozen).toBuffer(acc, frozen, start).toList shouldEqual orig.drop(start)
      }
    }

    it("should sum uncompressed double vectors and ignore NaN values") {
      val orig = Seq(1000, 2001.1, 2999.99, 5123.4, 5250, 6004, 7678)
      val builder = DoubleVector.appendingVectorNoNA(memFactory, orig.length + 2)
      orig.foreach(builder.addData)

      val reader = builder.reader.asDoubleReader
      reader.sum(acc, builder.addr, 2, orig.length - 1) shouldEqual (orig.drop(2).sum)

      // Now add a NaN
      builder.addData(Double.NaN)
      builder.length shouldEqual (orig.length + 1)
      reader.sum(acc, builder.addr, 2, orig.length) shouldEqual (orig.drop(2).sum)
    }

    it("should not allow sum() with out of bound indices") {
      val orig = Seq(1000, 2001.1, 2999.99, 5123.4, 5250, 6004, 7678)
      val builder = DoubleVector.appendingVectorNoNA(memFactory, orig.length + 2)
      orig.foreach(builder.addData)
      val optimized = builder.optimize(memFactory)

      builder.reader.asDoubleReader.sum(acc, builder.addr, 0, 4) shouldEqual orig.take(5).sum

      intercept[IllegalArgumentException] {
        builder.reader.asDoubleReader.sum(acc, builder.addr, 1, orig.length)
      }

      val readVect = DoubleVector(acc, optimized)
      intercept[IllegalArgumentException] {
        readVect.sum(acc, optimized, 1, orig.length)
      }
    }

    it("should support resetting and optimizing AppendableVector multiple times") {
      val cb = DoubleVector.appendingVector(memFactory, 5)
      // Use large numbers on purpose so cannot optimize to Doubles or const
      val orig = Seq(11.11E101, -2.2E-176, 1.77E88)
      cb.addNA()
      orig.foreach(cb.addData)
      cb.copyToBuffer.toList shouldEqual orig
      val optimized = cb.optimize(memFactory)
      DoubleVector(acc, optimized).toBuffer(acc, optimized).toList shouldEqual orig

      // Now the optimize should not have damaged original vector
      cb.copyToBuffer.toList shouldEqual orig
      cb.reset()
      val orig2 = orig.map(_ * 2)
      orig2.foreach(cb.addData)
      val opt2 = cb.optimize(memFactory)
      DoubleVector(acc, opt2).toBuffer(acc, opt2).toList shouldEqual orig2
      cb.copyToBuffer.toList shouldEqual orig2
    }
  }

  describe("bugs") {
    it("should enumerate same samples regardless of where start enumeration from") {
      val data = scala.io.Source.fromURL(getClass.getResource("/timeseries_bug1.txt"))
                      .getLines.map(_.split(' '))
                      .map(ArrayStringRowReader).toSeq
      val origValues = data.map(_.getDouble(1))
      val timestampAppender = LongBinaryVector.appendingVectorNoNA(memFactory, data.length)
      val valuesAppender = DoubleVector.appendingVectorNoNA(memFactory, data.length)
      data.foreach { reader =>
        timestampAppender.addData(reader.getLong(0))
        valuesAppender.addData(reader.getDouble(1))
      }

      val tsEncoded = timestampAppender.optimize(memFactory)
      val valuesEncoded = valuesAppender.optimize(memFactory)
      val tsReader = LongBinaryVector(acc, tsEncoded)
      val dReader = DoubleVector(acc, valuesEncoded)

      val samples = new collection.mutable.ArrayBuffer[Double]
      for { i <- 0 until timestampAppender.length by 10 } {
        samples.clear()
        val iter = dReader.iterate(acc, valuesEncoded, i)
        (i until timestampAppender.length).foreach(_ => samples.append(iter.next))
        samples shouldEqual origValues.drop(i)
      }
    }
  }

  describe("counter correction") {
    val orig = Seq(1000, 2001.1, 2999.99, 5123.4, 5250, 6004, 7678)
    val builder = DoubleVector.appendingVectorNoNA(memFactory, orig.length)
    orig.foreach(builder.addData)
    builder.length shouldEqual orig.length
    val frozen = builder.optimize(memFactory)
    val reader = DoubleVector(acc, frozen)

    it("should detect drop correctly at beginning of chunk and adjust CorrectionMeta") {
      reader.detectDropAndCorrection(acc, frozen, NoCorrection) shouldEqual NoCorrection

      // No drop in first value, correction should be returned unchanged
      val corr1 = DoubleCorrection(999.9, 100.0)
      reader.detectDropAndCorrection(acc, frozen, corr1) shouldEqual corr1

      // Drop in first value, correction should be done
      val corr2 = DoubleCorrection(1201.2, 100.0)
      reader.detectDropAndCorrection(acc, frozen, corr2) shouldEqual DoubleCorrection(1201.2, 100.0 + 1201.2)
    }

    it("should return correctedValue with correction adjustment even if vector has no drops") {
      reader.correctedValue(acc, frozen, 1, NoCorrection) shouldEqual 2001.1

      val corr1 = DoubleCorrection(999.9, 100.0)
      reader.correctedValue(acc, frozen, 3, corr1) shouldEqual 5223.4
    }

    it("should updateCorrections correctly") {
      reader.updateCorrection(acc, frozen, NoCorrection) shouldEqual DoubleCorrection(7678, 0.0)

      val corr1 = DoubleCorrection(999.9, 50.0)
      reader.updateCorrection(acc, frozen, corr1) shouldEqual DoubleCorrection(7678, 50.0)
    }

    it("should detect drops with DoubleCounterAppender and carry flag to optimized version") {
      val cb = DoubleVector.appendingVectorNoNA(memFactory, 10, detectDrops = true)
      cb.addData(101)
      cb.addData(102.5)

      // So far, no drops
      PrimitiveVectorReader.dropped(acc, cb.addr) shouldEqual false

      // Add dropped value, flag should be set to true
      cb.addData(9)
      PrimitiveVectorReader.dropped(acc, cb.addr) shouldEqual true

      // Add more values, no drops, flag should still be true
      cb.addData(13.3)
      cb.addData(21.1)
      PrimitiveVectorReader.dropped(acc, cb.addr) shouldEqual true

      // Optimize, flag should still be true in optimized version
      val sc = cb.optimize(memFactory)
      PrimitiveVectorReader.dropped(acc, sc) shouldEqual true

      DoubleVector(acc, sc).toBuffer(acc, sc) shouldEqual Buffer(101, 102.5, 9, 13.3, 21.1)

      // Make sure return correcting version of reader as well
      cb.reader shouldBe a[CorrectingDoubleVectorReader]
      DoubleVector(acc, sc) shouldBe a[CorrectingDoubleVectorReader]
    }

    it("should read out corrected values properly") {
      val orig = Seq(101, 102.5, 9, 13.3, 21.1)
      val cb = DoubleVector.appendingVectorNoNA(memFactory, 10, detectDrops = true)
      orig.foreach(cb.addData)
      cb.length shouldEqual orig.length
      val sc = cb.optimize(memFactory)
      val reader = DoubleVector(acc, sc)
      reader shouldBe a[CorrectingDoubleVectorReader]

      reader.correctedValue(acc, sc, 1, NoCorrection) shouldEqual 102.5
      reader.correctedValue(acc, sc, 2, NoCorrection) shouldEqual 111.5
      reader.correctedValue(acc, sc, 4, NoCorrection) shouldEqual (21.1 + 102.5)

      val corr1 = DoubleCorrection(999.9, 50.0)
      reader.correctedValue(acc, sc, 1, corr1) shouldEqual 152.5
      reader.correctedValue(acc, sc, 2, corr1) shouldEqual 161.5
      reader.correctedValue(acc, sc, 4, corr1) shouldEqual (21.1 + 102.5 + 50)

      reader.updateCorrection(acc, sc, corr1) shouldEqual DoubleCorrection(21.1, 102.5 + 50.0)
    }

    it("should read out length and values correctly for corrected vectors") {
      val orig = Seq(4419.00, 4511.00, 4614.00, 4724.00, 4909.00, 948.00, 1000.00, 1095.00, 1102.00, 1201.00)
      val cb = DoubleVector.appendingVectorNoNA(memFactory, 10, detectDrops = true)
      orig.foreach(cb.addData)
      cb.length shouldEqual orig.length
      val sc = cb.optimize(memFactory)
      val reader = DoubleVector(acc, sc)
      reader shouldBe a[CorrectingDoubleVectorReader]

      reader.length(acc, sc) shouldEqual orig.length
      reader.toBuffer(acc, sc).toList shouldEqual orig
    }
  }
}