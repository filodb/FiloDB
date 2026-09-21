package filodb.memory.format.vectors

import java.nio.file.Files

import org.apache.commons.lang3.SystemUtils

/**
 * JNI entry points for SIMD-accelerated double vector operations.
 *
 * These native methods operate on raw off-heap memory pointers. The `dataAddr` parameter
 * should point to the start of the double data (i.e., vector + PrimitiveVector.OffsetData).
 *
 * The native library is the same `libfilodb_core` used by TantivyNativeMethods.
 */
object SimdNativeMethods {

  /** Minimum number of elements for SIMD to be worthwhile.
   *  Below this threshold, JNI call overhead exceeds the SIMD savings.
   *  Benchmarked: 30 elements (5m@10s) shows regression at long query ranges,
   *  90 elements (15m@10s) shows clear SIMD wins. 64 is the safe default crossover.
   *  Configurable via `filodb.simd.threshold` in config or system property. */
  @volatile var simdThreshold: Int =
    Integer.getInteger("filodb.simd.threshold", 64)

  /** Runtime toggle for SIMD-accelerated sum/count in DoubleVectorDataReader64.
   *  Initialized from system property `filodb.simd.enabled` (default false).
   *  Can be set programmatically or via GlobalConfig. */
  @volatile var enabled: Boolean =
    java.lang.Boolean.getBoolean("filodb.simd.enabled")

  /** Toggle for native-accelerated delta histogram sum in DeltaHistogramReader.
   *  When true, sum() uses Rust NibblePack decompression via histogramBatchSum JNI call.
   *  When false, falls back to JVM-based RowHistogramReader.sum().
   *  Initialized from system property (default false), also set by GlobalConfig
   *  via `filodb.simd.delta-histogram-sum-optimized-enabled`. */
  @volatile var deltaHistogramSumEnabled: Boolean =
    java.lang.Boolean.getBoolean("filodb.simd.delta-histogram-sum-optimized-enabled")

  // Load the native library. TantivyNativeMethods may have already loaded it,
  // so we catch UnsatisfiedLinkError to avoid duplicate loading.
  private lazy val ensureLoaded: Unit = {
    try { loadLibrary() }
    catch { case _: UnsatisfiedLinkError => () }
  }

  ensureLoaded

  private def loadLibrary(): Unit = {
    val tempDir = Files.createTempDirectory("filodb-native-")

    val lib = System.mapLibraryName("filodb_core")

    val arch = SystemUtils.OS_ARCH
    val kernel = if (SystemUtils.IS_OS_LINUX) {
      "linux"
    } else if (SystemUtils.IS_OS_MAC) {
      "darwin"
    } else if (SystemUtils.IS_OS_WINDOWS) {
      "windows"
    } else {
      sys.error(s"Unhandled platform ${SystemUtils.OS_NAME}")
    }

    val resourcePath: String = "/native/" + kernel + "/" + arch + "/" + lib
    val resourceStream = Option(SimdNativeMethods.getClass.getResourceAsStream(resourcePath))
      .getOrElse(throw new IllegalStateException(s"Native library not found at $resourcePath"))

    val finalPath = tempDir.resolve(lib)
    Files.copy(resourceStream, finalPath)

    System.load(finalPath.toAbsolutePath.toString)
  }

  /**
   * Sum doubles from native memory with optional NaN skipping.
   *
   * @param dataAddr  pointer to first double element (vector + OffsetData)
   * @param start     first element index (inclusive)
   * @param end       last element index (inclusive)
   * @param ignoreNaN if true, skip NaN values; returns NaN if all values are NaN
   * @return the sum, or NaN if all values are NaN (when ignoreNaN=true)
   */
  @native def simdSumDouble(dataAddr: Long, start: Int, end: Int, ignoreNaN: Boolean): Double

  /**
   * Count non-NaN doubles from native memory.
   *
   * @param dataAddr pointer to first double element (vector + OffsetData)
   * @param start    first element index (inclusive)
   * @param end      last element index (inclusive)
   * @return count of non-NaN values
   */
  @native def simdCountDouble(dataAddr: Long, start: Int, end: Int): Int

  /**
   * Batch sum of histogram bucket values for SUBTYPE_H_SIMPLE (delta histogram) vectors.
   * Walks sections, unpacks NibblePack delta-encoded blobs, and accumulates bucket sums
   * — all in one JNI call.
   *
   * @param vectorAddr pointer to HistogramVector (start of i32 numBytes header)
   * @param startRow   first histogram index (0-based, inclusive)
   * @param endRow     last histogram index (inclusive)
   * @param outAddr    pointer to pre-allocated off-heap Double[numBuckets] output buffer
   * @param numBuckets number of histogram buckets
   * @return number of histograms summed, or -1 on error
   */
  @native def histogramBatchSum(vectorAddr: Long, startRow: Int, endRow: Int,
                                outAddr: Long, numBuckets: Int): Int
}
