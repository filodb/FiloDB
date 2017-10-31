package filodb.memory.format.vectors

import java.nio.ByteBuffer
import java.util.HashMap

import filodb.memory.format._
import scalaxy.loops._

import filodb.memory.MemFactory

case class DictUTF8Info(codeMap: HashMap[ZeroCopyUTF8String, Int],
                        dictStrings: BinaryAppendableVector[ZeroCopyUTF8String],
                        codes: BinaryAppendableVector[Int])

object DictUTF8Vector {
  /**
   * Determines if it is worth it to do dictionary encoding (which takes longer).  Tries to use up
   * minimal amount of time to make this determination by sampling or going through only a portion
   * of the source vector, and stops building the expensive hash and dictionary if its not worth it.
   * This approach might not work for source vectors that are very biased but the sampling rate is
   * adjustable.
   *
   * @param sourceVector the source UTF8 vector.  Recommended this be a UTF8PtrAppendable.
   * @param spaceThreshold a number between 0.0 and 1.0, the fraction of the original
   *                       space below which the DictUTF8Vector should be sized to be
   *                       worth doing dictionary encoding for. Make this >1.0 if you want to force it
   * @param samplingRate the fraction (0.0 <= n < 1.0) of the source vector to use to determine
   *                     if dictionary encoding will be worth it
   * @param maxDictSize the max number of bytes that the dictionary coukd grow to
   * @return Option[DictUTF8Info] contains info for building the dictionary if it is worth it
   */
  def shouldMakeDict(memFactory: MemFactory,
                     sourceVector: BinaryAppendableVector[ZeroCopyUTF8String],
                     spaceThreshold: Double = 0.6,
                     samplingRate: Double = 0.3,
                     maxDictSize: Int = 10000): Option[DictUTF8Info] = {
    val sourceLen = sourceVector.length
    val codeMap = new HashMap[ZeroCopyUTF8String, Int](sourceLen, 0.5F)
    val sampleSize = (sourceLen * samplingRate).toInt
    // The max size for the dict we will tolerate given the sample size and orig vector size
    // Above this, cardinality is not likely to be low enough for dict encoding
    val dictThreshold = (sampleSize * spaceThreshold).toInt
    val dictVect = UTF8Vector.flexibleAppending(memFactory, sourceLen + 1, maxDictSize)
    val codeVect = IntBinaryVector.appendingVectorNoNA(memFactory, sourceLen)
    dictVect.addNA()   // first code point 0 == NA

    for { i <- 0 until sourceLen optimized } {
      val item = sourceVector(i)
      // scalastyle:off
      if (item != null) {
        val newCode = codeMap.size + 1
        val orig = codeMap.putIfAbsent(item, newCode)  // Just one hashcode/compare
        if (orig == 0) {
          dictVect.addData(item)
          codeVect.addData(newCode)
        } else {
          codeVect.addData(orig)
        }
      } else {
        codeVect.addData(0)
      }
      // scalastyle:on
      // Now check if we are over the threshold already
      if (i <= sampleSize && dictVect.length > dictThreshold) return None
    }
    Some(DictUTF8Info(codeMap, dictVect, codeVect))
  }

  /**
   * Creates the dictionary-encoding frozen vector from intermediate data.
   */
  def makeVector(memFactory: MemFactory, info: DictUTF8Info): DictUTF8Vector = {
    // Estimate and allocate enough space for the UTF8Vector
    val (nbits, signed) = IntBinaryVector.minMaxToNbitsSigned(0, info.codeMap.size)
    val codeVectSize = IntBinaryVector.noNAsize(info.codes.length, nbits)
    val dictVectSize = info.dictStrings.frozenSize
    val bytesRequired = 8 + dictVectSize + codeVectSize
    val (base, off, nBytes) = memFactory.allocateWithMagicHeader(bytesRequired)
    val dispose = () => memFactory.freeMemory(off)
    // Copy over the dictionary strings
    // TODO: optimize in future to FIXED UTF8 vector?
    info.dictStrings.freeze(Some((base, off + 8)))

    // Fill up the codes - directly in the allocated space for the DictUTF8Vector
    val codeVect = IntBinaryVector.appendingVectorNoNA(base,
                                                       off + 8 + dictVectSize,
                                                       codeVectSize,
                                                       nbits, signed, dispose)
    codeVect.addVector(info.codes)

    // Write 8 bytes of metadata at beginning
    UnsafeUtils.setInt(base, off,     WireFormat.SUBTYPE_UTF8)
    UnsafeUtils.setInt(base, off + 4, 8 + dictVectSize)

    new DictUTF8Vector(base, off, bytesRequired, dispose)
  }

  /**
   * Wraps bytes with a DictUTF8Vector so it can be read.
   */
  def apply(buffer: ByteBuffer): DictUTF8Vector = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new DictUTF8Vector(base, off, len, BinaryVector.NoOpDispose)
  }
}

/**
 * Dictionary-encoding UTF8 string BinaryVector
 * Layout:
 * +0   Int    WireFormat vector subtype of dictionary
 * +4   Int    relative offset to integer vector for dictionary codes
 * +8          String dictionary, either UTF8Vector or FixedMaxUTF8Vector
 * +....
 *
 * The code zero is used to mark NA.  Thus the first entry of the string dictionary is also NA.
 * Unlike the FlatBuffer-based DictStringVector, this one does not need to cache because there is no
 * string deserialization to be done, thus the code is much much simpler.
 */
class DictUTF8Vector(val base: Any,
                     val offset: Long,
                     val numBytes: Int,
                     val dispose: () => Unit) extends BinaryVector[ZeroCopyUTF8String] {
  val vectMajorType = WireFormat.VECTORTYPE_BINDICT
  val vectSubType = WireFormat.SUBTYPE_UTF8
  val maybeNAs = true
  private val dictSubtype = UnsafeUtils.getInt(base, offset)
  private val codeVectOffset = UnsafeUtils.getInt(base, offset + 4)

  private final val dict = dictSubtype match {
    case WireFormat.SUBTYPE_UTF8 => UTF8Vector(base, offset + 8, codeVectOffset - 8, dispose)
  }

  private final val codes = IntBinaryVector(base, offset + codeVectOffset, numBytes - codeVectOffset, dispose)

  override final def length: Int = codes.length
  final def isAvailable(i: Int): Boolean = codes(i) != 0
  final def apply(i: Int): ZeroCopyUTF8String = dict(codes(i))
}