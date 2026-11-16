package filodb.memory.format.vectors

import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr

/**
 * Range min/max on vector readers must equal folding over iterate() for every encoding and every range.
 */
class VectorMinMaxTest extends NativeVectorTest {

  // Reference: the original per element loop of MinOverTimeChunkedFunctionD / MaxOverTimeChunkedFunctionD.
  // MinDownsampler / MaxDownsampler return the range min/max directly, so this covers them too.
  private def refMinMax(reader: DoubleVectorDataReader, vect: BinaryVectorPtr, start: Int, end: Int) = {
    var min = Double.NaN
    var max = Double.NaN
    val it = reader.iterate(acc, vect, start)
    for { _ <- start to end } {
      val v = it.next
      if (min != min) min = v else if (v != v) {} else if (v <= min) min = v
      if (max != max) max = v else if (v != v) {} else if (v >= max) max = v
    }
    (min, max)
  }

  // Compares doubles exactly: NaN equals NaN, and 0.0 differs from -0.0
  private def sameDouble(a: Double, b: Double): Boolean = java.lang.Double.compare(a, b) == 0

  private def checkDouble(reader: DoubleVectorDataReader, vect: BinaryVectorPtr): Unit = {
    val len = reader.length(acc, vect)
    // empty ranges, including [len, len - 1], which the chunked min/max_over_time functions produce
    // when startRowNum == length
    for { start <- 1 to len } {
      withClue(s"${reader.getClass.getSimpleName} empty range [$start, ${start - 1}]: ") {
        reader.min(acc, vect, start, start - 1).isNaN shouldEqual true
        reader.max(acc, vect, start, start - 1).isNaN shouldEqual true
      }
    }
    // out of bounds ranges must not read past the vector
    withClue(s"${reader.getClass.getSimpleName} out of bounds: ") {
      intercept[IllegalArgumentException] { reader.min(acc, vect, 0, len) }
      intercept[IllegalArgumentException] { reader.max(acc, vect, 0, len) }
      intercept[IllegalArgumentException] { reader.min(acc, vect, -1, 0) }
      intercept[IllegalArgumentException] { reader.max(acc, vect, -1, 0) }
    }
    for { start <- 0 until len; end <- start until len } {
      val (expMin, expMax) = refMinMax(reader, vect, start, end)
      val actMin = reader.min(acc, vect, start, end)
      val actMax = reader.max(acc, vect, start, end)
      withClue(s"${reader.getClass.getSimpleName} range [$start, $end]: min $actMin vs $expMin, " +
               s"max $actMax vs $expMax") {
        sameDouble(actMin, expMin) shouldEqual true
        sameDouble(actMax, expMax) shouldEqual true
      }
    }
  }

  private def checkLong(reader: LongVectorDataReader, vect: BinaryVectorPtr): Unit = {
    val len = reader.length(acc, vect)
    // empty and out of bounds ranges are rejected, rather than returning a value that is not in the range
    withClue(s"${reader.getClass.getSimpleName} invalid ranges: ") {
      intercept[IllegalArgumentException] { reader.min(acc, vect, 5, 4) }
      intercept[IllegalArgumentException] { reader.max(acc, vect, 5, 4) }
      intercept[IllegalArgumentException] { reader.min(acc, vect, 0, len) }
      intercept[IllegalArgumentException] { reader.max(acc, vect, 0, len) }
    }
    for { start <- 0 until len; end <- start until len } {
      val it = reader.iterate(acc, vect, start)
      val values = (start to end).map(_ => it.next)
      withClue(s"${reader.getClass.getSimpleName} range [$start, $end]: ") {
        reader.min(acc, vect, start, end) shouldEqual values.min
        reader.max(acc, vect, start, end) shouldEqual values.max
      }
    }
  }

  private val rnd = new scala.util.Random(17)

  describe("DoubleVectorDataReader min/max") {
    it("should match iterate for 64-bit doubles with NaN, all-NaN runs and signed zeros") {
      val values = (0 until 40).map { i =>
        if (i >= 10 && i < 15) Double.NaN                      // a run of NaNs, so some ranges are all NaN
        else if (i % 7 == 0) Double.NaN
        else if (i % 11 == 0) -0.0
        else if (i % 13 == 0) 0.0
        else rnd.nextDouble() * 200 - 100
      }
      val appender = DoubleVector.appendingVectorNoNA(memFactory, 100)
      values.foreach(appender.addData)
      val optimized = appender.optimize(memFactory)
      val reader = DoubleVector(acc, optimized)
      reader shouldEqual DoubleVectorDataReader64
      checkDouble(reader, optimized)
    }

    it("should match iterate for masked doubles with NA values") {
      val appender = DoubleVector.appendingVector(memFactory, 100)
      (0 until 30).foreach { i => if (i % 4 == 0) appender.addNA() else appender.addData(rnd.nextDouble() * 50) }
      val reader = DoubleVector(acc, appender.addr)   // dispatch on wire format; appender.reader is the inner one
      reader shouldEqual MaskedDoubleDataReader
      checkDouble(reader, appender.addr)
    }

    it("should match iterate for integral doubles stored as DeltaDelta longs") {
      val appender = DoubleVector.appendingVectorNoNA(memFactory, 100)
      (0 until 40).foreach { i => appender.addData((i * 37 + rnd.nextInt(500) - 250).toDouble) }
      val optimized = appender.optimize(memFactory)
      val reader = DoubleVector(acc, optimized)
      reader shouldEqual DoubleLongWrapDataReader
      LongBinaryVector(acc, optimized) shouldEqual DeltaDeltaDataReader
      checkDouble(reader, optimized)
    }

    it("should match iterate for integral doubles on a line (DeltaDeltaConst), increasing, decreasing and flat") {
      Seq(15, -9, 0).foreach { slope =>
        val appender = DoubleVector.appendingVectorNoNA(memFactory, 100)
        (0 until 30).foreach { i => appender.addData((1000 + i * slope).toDouble) }
        val optimized = appender.optimize(memFactory)
        val reader = DoubleVector(acc, optimized)
        reader shouldEqual DoubleLongWrapDataReader
        LongBinaryVector(acc, optimized) shouldEqual DeltaDeltaConstDataReader
        checkDouble(reader, optimized)
      }
    }

    it("should match iterate for counters with drops (CorrectingDoubleVectorReader)") {
      val appender = DoubleVector.appendingVectorNoNA(memFactory, 100, detectDrops = true)
      Seq(10.5, 20.25, 30.5, 5.75, 15.5, 2.25, 40.5, 41.0).foreach(appender.addData)
      val optimized = appender.optimize(memFactory)
      val reader = DoubleVector(acc, optimized)
      reader shouldBe a[CorrectingDoubleVectorReader]
      checkDouble(reader, optimized)
    }
  }

  describe("LongVectorDataReader min/max") {
    it("should match iterate for 64-bit longs") {
      val appender = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      (0 until 30).foreach { _ => appender.addData(rnd.nextLong()) }   // full range, can't be delta encoded
      val optimized = appender.optimize(memFactory)
      val reader = LongBinaryVector(acc, optimized)
      reader shouldEqual LongVectorDataReader64
      checkLong(reader, optimized)
    }

    it("should match iterate for masked longs") {
      val appender = LongBinaryVector.appendingVector(memFactory, 100)
      (0 until 30).foreach { i => if (i % 5 == 0) appender.addNA() else appender.addData(rnd.nextInt(1000) - 500L) }
      val reader = LongBinaryVector(acc, appender.addr)
      reader shouldEqual MaskedLongDataReader
      checkLong(reader, appender.addr)
    }

    it("should match iterate for DeltaDelta and DeltaDeltaConst longs") {
      val ddAppender = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
      (0 until 40).foreach { i => ddAppender.addData(1000000L + i * 10000L + rnd.nextInt(2000) - 1000) }
      val dd = ddAppender.optimize(memFactory)
      LongBinaryVector(acc, dd) shouldEqual DeltaDeltaDataReader
      checkLong(LongBinaryVector(acc, dd), dd)

      Seq(10000, -3, 0).foreach { slope =>
        val constAppender = LongBinaryVector.appendingVectorNoNA(memFactory, 100)
        (0 until 30).foreach { i => constAppender.addData(5000000L + i * slope) }
        val c = constAppender.optimize(memFactory)
        LongBinaryVector(acc, c) shouldEqual DeltaDeltaConstDataReader
        checkLong(LongBinaryVector(acc, c), c)
      }
    }
  }
}
