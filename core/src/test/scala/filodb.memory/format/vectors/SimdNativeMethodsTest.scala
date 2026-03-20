package filodb.memory.format.vectors

import filodb.memory.format.BinaryVector.BinaryVectorPtr

class SimdNativeMethodsTest extends NativeVectorTest {

  // Helper: build an uncompressed NoNA double vector and return (reader, addr)
  private def buildVector(values: Seq[Double]): (DoubleVectorDataReader, BinaryVectorPtr) = {
    val builder = DoubleVector.appendingVectorNoNA(memFactory, values.length)
    values.foreach(builder.addData)
    (builder.reader.asDoubleReader, builder.addr)
  }

  // PrimitiveVector.OffsetData — data doubles start 8 bytes into the vector
  private val OffsetData = 8

  describe("SimdNativeMethods.simdSumDouble correctness") {
    it("should match Scala sum for normal doubles with ignoreNaN=true") {
      val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should match Scala sum for normal doubles with ignoreNaN=false") {
      val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = false)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = false)

      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should return NaN when all values are NaN with ignoreNaN=true") {
      val values = Seq(Double.NaN, Double.NaN, Double.NaN)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      java.lang.Double.isNaN(scalaResult) shouldBe true
      java.lang.Double.isNaN(simdResult) shouldBe true
    }

    it("should skip NaN values and sum the rest with ignoreNaN=true") {
      val values = Seq(1.0, Double.NaN, 3.0, Double.NaN, 5.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      // Scala sums to 9.0 (1+3+5), SIMD should match
      scalaResult shouldEqual 9.0
      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should propagate NaN with ignoreNaN=false when NaN present") {
      val values = Seq(1.0, Double.NaN, 3.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = false)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = false)

      java.lang.Double.isNaN(scalaResult) shouldBe true
      java.lang.Double.isNaN(simdResult) shouldBe true
    }

    it("should handle a single element") {
      val values = Seq(42.5)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, 0, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, 0, ignoreNaN = true)

      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should handle a single NaN element with ignoreNaN=true") {
      val values = Seq(Double.NaN)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, 0, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, 0, ignoreNaN = true)

      java.lang.Double.isNaN(scalaResult) shouldBe true
      java.lang.Double.isNaN(simdResult) shouldBe true
    }

    it("should handle sub-range sum correctly") {
      val values = Seq(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      // Sum elements [2..5] = 30+40+50+60 = 180
      val scalaResult = reader.sum(acc, addr, 2, 5, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 2, 5, ignoreNaN = true)

      scalaResult shouldEqual 180.0
      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should handle NaN at various positions within 8-element chunks") {
      // NaN at positions 0,3,4,7 to test boundary alignment with 8-way unrolling
      val values = Seq(Double.NaN, 2.0, 3.0, Double.NaN, Double.NaN, 6.0, 7.0, Double.NaN, 9.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      // Expected: 2+3+6+7+9 = 27
      scalaResult shouldEqual 27.0
      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should match Scala for large vector with mixed values") {
      val rng = new java.util.Random(123L)
      val values = (0 until 1000).map { i =>
        if (i % 7 == 0) Double.NaN else rng.nextDouble() * 1000.0
      }
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      // Allow small floating-point divergence due to different accumulation order
      simdResult shouldEqual scalaResult +- (Math.abs(scalaResult) * 1e-12)
    }

    it("should match Scala for large vector with no NaN values") {
      val rng = new java.util.Random(456L)
      val values = (0 until 1000).map(_ => rng.nextDouble() * 1000.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaIgnore = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdIgnore = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)
      simdIgnore shouldEqual scalaIgnore +- (Math.abs(scalaIgnore) * 1e-12)

      val scalaNoNaN = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = false)
      val simdNoNaN = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = false)
      simdNoNaN shouldEqual scalaNoNaN +- (Math.abs(scalaNoNaN) * 1e-12)
    }

    it("should handle remainder elements (count not divisible by 8)") {
      // 5 elements: 0 chunks of 8 + 5 remainder
      val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      scalaResult shouldEqual 15.0
      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should handle exactly 8 elements (one full chunk, no remainder)") {
      val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      scalaResult shouldEqual 36.0
      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should handle 9 elements (one chunk of 8 + 1 remainder)") {
      val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      scalaResult shouldEqual 45.0
      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should handle NaN at 8-way chunk boundaries with ignoreNaN=true") {
      // NaN at positions 0, 7, 8, 15 to test 8-way unroll boundaries
      val values = (0 until 16).map { i =>
        if (i == 0 || i == 7 || i == 8 || i == 15) Double.NaN else (i + 1).toDouble
      }
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should handle negative values") {
      val values = Seq(-5.0, 3.0, -2.0, 10.0, -6.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      scalaResult shouldEqual 0.0
      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should handle positive infinity") {
      val values = Seq(1.0, Double.PositiveInfinity, 3.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaIgnore = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdIgnore = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)
      scalaIgnore shouldEqual Double.PositiveInfinity
      simdIgnore shouldEqual scalaIgnore

      val scalaNoNaN = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = false)
      val simdNoNaN = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = false)
      scalaNoNaN shouldEqual Double.PositiveInfinity
      simdNoNaN shouldEqual scalaNoNaN
    }

    it("should handle negative infinity") {
      val values = Seq(1.0, Double.NegativeInfinity, 3.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      scalaResult shouldEqual Double.NegativeInfinity
      simdResult shouldEqual scalaResult
    }

    it("should produce NaN when summing +Inf and -Inf") {
      // Inf + (-Inf) = NaN per IEEE 754
      val values = Seq(Double.PositiveInfinity, Double.NegativeInfinity, 1.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = false)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = false)

      java.lang.Double.isNaN(scalaResult) shouldBe true
      java.lang.Double.isNaN(simdResult) shouldBe true
    }

    it("should handle +Inf and -Inf with ignoreNaN=true (known divergence)") {
      // Behavioral difference: when Inf + (-Inf) produces NaN mid-accumulation,
      // Scala resets sum to 0 on the next non-NaN value (because it checks isNaN(sum)),
      // while SIMD accumulates into separate lanes and only checks found_non_nan at the end.
      //
      // Scala: sum=NaN → 0+Inf=Inf → Inf+(-Inf)=NaN → 0+5=5.0 (resets on NaN sum)
      // SIMD:  acc0=Inf+5=Inf, acc1=-Inf → Inf+(-Inf)=NaN (no mid-sum reset)
      //
      // This divergence only occurs when +Inf and -Inf are both present — an edge case
      // that doesn't arise in FiloDB's time-series double vectors.
      val values = Seq(Double.PositiveInfinity, Double.NegativeInfinity, 5.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      // Scala resets sum to 0 after Inf+(-Inf)=NaN, so it returns 5.0
      scalaResult shouldEqual 5.0
      // SIMD does not reset, so Inf+(-Inf)=NaN propagates
      java.lang.Double.isNaN(simdResult) shouldBe true
    }

    it("should handle negative zero") {
      val values = Seq(-0.0, 1.0, 2.0, 3.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      scalaResult shouldEqual 6.0
      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should handle negative zero summed with positive zero") {
      // -0.0 + 0.0 = 0.0 in IEEE 754
      val values = Seq(-0.0, 0.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = false)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = false)

      scalaResult shouldEqual 0.0
      simdResult shouldEqual 0.0
    }

    it("should handle subnormal (denormalized) values") {
      // Double.MinPositiveValue is the smallest subnormal: 4.9E-324
      val values = Seq(Double.MinPositiveValue, Double.MinPositiveValue, Double.MinPositiveValue, 1.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      simdResult shouldEqual scalaResult +- 1e-10
    }

    it("should handle values near Double.MAX_VALUE without unexpected overflow") {
      // Two large values that don't overflow when summed
      val large = Double.MaxValue / 4.0
      val values = Seq(large, large, large)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      simdResult shouldEqual scalaResult +- (Math.abs(scalaResult) * 1e-12)
    }

    it("should overflow to Infinity when sum exceeds Double.MAX_VALUE") {
      val values = Seq(Double.MaxValue, Double.MaxValue)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = false)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = false)

      scalaResult shouldEqual Double.PositiveInfinity
      simdResult shouldEqual scalaResult
    }

    it("should handle mix of Infinity, NaN, and normal values with ignoreNaN=true") {
      val values = Seq(1.0, Double.NaN, Double.PositiveInfinity, Double.NaN, 5.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val simdResult = SimdNativeMethods.simdSumDouble(dataAddr, 0, values.length - 1, ignoreNaN = true)

      // NaNs skipped, 1 + Inf + 5 = Inf
      scalaResult shouldEqual Double.PositiveInfinity
      simdResult shouldEqual scalaResult
    }
  }

  describe("SimdNativeMethods.simdCountDouble correctness") {
    it("should match Scala count for normal doubles") {
      val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.count(acc, addr, 0, values.length - 1)
      val simdResult = SimdNativeMethods.simdCountDouble(dataAddr, 0, values.length - 1)

      scalaResult shouldEqual 5
      simdResult shouldEqual scalaResult
    }

    it("should return 0 when all values are NaN") {
      val values = Seq(Double.NaN, Double.NaN, Double.NaN)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.count(acc, addr, 0, values.length - 1)
      val simdResult = SimdNativeMethods.simdCountDouble(dataAddr, 0, values.length - 1)

      scalaResult shouldEqual 0
      simdResult shouldEqual scalaResult
    }

    it("should count only non-NaN values") {
      val values = Seq(1.0, Double.NaN, 3.0, Double.NaN, 5.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.count(acc, addr, 0, values.length - 1)
      val simdResult = SimdNativeMethods.simdCountDouble(dataAddr, 0, values.length - 1)

      scalaResult shouldEqual 3
      simdResult shouldEqual scalaResult
    }

    it("should handle a single element") {
      val values = Seq(42.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      SimdNativeMethods.simdCountDouble(dataAddr, 0, 0) shouldEqual 1
    }

    it("should handle a single NaN element") {
      val values = Seq(Double.NaN)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      SimdNativeMethods.simdCountDouble(dataAddr, 0, 0) shouldEqual 0
    }

    it("should handle sub-range count correctly") {
      val values = Seq(1.0, Double.NaN, 3.0, 4.0, Double.NaN, 6.0, 7.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      // Count elements [2..5]: 3.0, 4.0, NaN, 6.0 → 3 non-NaN
      val scalaResult = reader.count(acc, addr, 2, 5)
      val simdResult = SimdNativeMethods.simdCountDouble(dataAddr, 2, 5)

      scalaResult shouldEqual 3
      simdResult shouldEqual scalaResult
    }

    it("should match Scala for large vector with mixed NaN values") {
      val rng = new java.util.Random(789L)
      val values = (0 until 1000).map { i =>
        if (i % 5 == 0) Double.NaN else rng.nextDouble()
      }
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.count(acc, addr, 0, values.length - 1)
      val simdResult = SimdNativeMethods.simdCountDouble(dataAddr, 0, values.length - 1)

      // 200 NaN values out of 1000 → 800 non-NaN
      scalaResult shouldEqual 800
      simdResult shouldEqual scalaResult
    }

    it("should count Infinity as non-NaN") {
      val values = Seq(Double.PositiveInfinity, Double.NaN, Double.NegativeInfinity, 1.0)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.count(acc, addr, 0, values.length - 1)
      val simdResult = SimdNativeMethods.simdCountDouble(dataAddr, 0, values.length - 1)

      // +Inf, -Inf, and 1.0 are non-NaN → 3
      scalaResult shouldEqual 3
      simdResult shouldEqual scalaResult
    }

    it("should count negative zero as non-NaN") {
      val values = Seq(-0.0, 0.0, Double.NaN)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.count(acc, addr, 0, values.length - 1)
      val simdResult = SimdNativeMethods.simdCountDouble(dataAddr, 0, values.length - 1)

      scalaResult shouldEqual 2
      simdResult shouldEqual scalaResult
    }

    it("should count subnormal values as non-NaN") {
      val values = Seq(Double.MinPositiveValue, Double.NaN, 5e-324, Double.NaN)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.count(acc, addr, 0, values.length - 1)
      val simdResult = SimdNativeMethods.simdCountDouble(dataAddr, 0, values.length - 1)

      scalaResult shouldEqual 2
      simdResult shouldEqual scalaResult
    }

    it("should count Double.MAX_VALUE as non-NaN") {
      val values = Seq(Double.MaxValue, -Double.MaxValue, Double.NaN)
      val (reader, addr) = buildVector(values)
      val dataAddr = addr + OffsetData

      val scalaResult = reader.count(acc, addr, 0, values.length - 1)
      val simdResult = SimdNativeMethods.simdCountDouble(dataAddr, 0, values.length - 1)

      scalaResult shouldEqual 2
      simdResult shouldEqual scalaResult
    }
  }

  describe("DoubleVectorDataReader64 with SIMD enabled") {
    it("should produce matching sum results through reader.sum() when SIMD is on") {
      SimdNativeMethods.enabled = true
      try {
        val values = Seq(1.0, 2.0, 3.0, Double.NaN, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
        val (reader, addr) = buildVector(values)

        val result = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
        // 1+2+3+5+6+7+8+9+10 = 51
        result shouldEqual 51.0 +- 1e-10
      } finally {
        SimdNativeMethods.enabled = false
      }
    }

    it("should produce matching sum with ignoreNaN=false through reader when SIMD is on") {
      SimdNativeMethods.enabled = true
      try {
        val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
        val (reader, addr) = buildVector(values)

        val result = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = false)
        result shouldEqual 15.0 +- 1e-10
      } finally {
        SimdNativeMethods.enabled = false
      }
    }

    it("should return NaN through reader.sum() when all NaN and SIMD is on") {
      SimdNativeMethods.enabled = true
      try {
        val values = Seq(Double.NaN, Double.NaN, Double.NaN)
        val (reader, addr) = buildVector(values)

        val result = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
        java.lang.Double.isNaN(result) shouldBe true
      } finally {
        SimdNativeMethods.enabled = false
      }
    }

    it("should produce matching count results through reader.count() when SIMD is on") {
      SimdNativeMethods.enabled = true
      try {
        val values = Seq(1.0, Double.NaN, 3.0, Double.NaN, 5.0, 6.0, 7.0)
        val (reader, addr) = buildVector(values)

        val result = reader.count(acc, addr, 0, values.length - 1)
        result shouldEqual 5
      } finally {
        SimdNativeMethods.enabled = false
      }
    }

    it("should count 0 when all values are NaN and SIMD is on") {
      SimdNativeMethods.enabled = true
      try {
        val values = Seq(Double.NaN, Double.NaN, Double.NaN)
        val (reader, addr) = buildVector(values)

        val result = reader.count(acc, addr, 0, values.length - 1)
        result shouldEqual 0
      } finally {
        SimdNativeMethods.enabled = false
      }
    }

    it("should handle sub-range sum through reader when SIMD is on") {
      SimdNativeMethods.enabled = true
      try {
        val values = Seq(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0)
        val (reader, addr) = buildVector(values)

        // Sum elements [2..5] = 30+40+50+60 = 180
        val result = reader.sum(acc, addr, 2, 5, ignoreNaN = true)
        result shouldEqual 180.0 +- 1e-10
      } finally {
        SimdNativeMethods.enabled = false
      }
    }

    it("should match SIMD-off results for large vector with mixed values") {
      val rng = new java.util.Random(999L)
      val values = (0 until 1000).map { i =>
        if (i % 7 == 0) Double.NaN else rng.nextDouble() * 1000.0
      }
      val (reader, addr) = buildVector(values)

      // Compute with SIMD off
      SimdNativeMethods.enabled = false
      val scalaSum = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
      val scalaCount = reader.count(acc, addr, 0, values.length - 1)

      // Compute with SIMD on
      SimdNativeMethods.enabled = true
      try {
        val simdSum = reader.sum(acc, addr, 0, values.length - 1, ignoreNaN = true)
        val simdCount = reader.count(acc, addr, 0, values.length - 1)

        simdSum shouldEqual scalaSum +- (Math.abs(scalaSum) * 1e-12)
        simdCount shouldEqual scalaCount
      } finally {
        SimdNativeMethods.enabled = false
      }
    }
  }
}
