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

  /** Runtime toggle for SIMD-accelerated sum/count in DoubleVectorDataReader64.
   *  Initialized from system property `filodb.simd.enabled` (default false).
   *  Can be set programmatically or via GlobalConfig. */
  @volatile var enabled: Boolean =
    java.lang.Boolean.getBoolean("filodb.simd.enabled")

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
    val resourceStream = Option(SimdNativeMethods.getClass.getResourceAsStream(resourcePath)).get

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
}
