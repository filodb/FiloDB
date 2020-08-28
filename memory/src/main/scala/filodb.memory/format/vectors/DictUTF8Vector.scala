package filodb.memory.format.vectors

import java.util.HashMap

import spire.syntax.cfor._

import filodb.memory.MemFactory
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr

case class DictUTF8Info(codeMap: HashMap[ZeroCopyUTF8String, Int],
                        dictStrings: BinaryAppendableVector[ZeroCopyUTF8String],
                        codes: BinaryAppendableVector[Int])

object DictUTF8Vector {
  import WireFormat._

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
    val dictVect = UTF8Vector.appendingVector(memFactory, sourceLen + 1, maxDictSize)
    val codeVect = IntBinaryVector.appendingVectorNoNA(memFactory, sourceLen)
    dictVect.addNA()   // first code point 0 == NA

    cforRange { 0 until sourceLen } { i =>
      if (sourceVector.isAvailable(i)) {
        val item = sourceVector(i)
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
      // Now check if we are over the threshold already
      if (i <= sampleSize && dictVect.length > dictThreshold) return None
    }
    Some(DictUTF8Info(codeMap, dictVect, codeVect))
  }

  /**
   * Creates the dictionary-encoding frozen vector from intermediate data.
   */
  def makeVector(memFactory: MemFactory, info: DictUTF8Info): BinaryVectorPtr = {
    // Estimate and allocate enough space for the UTF8Vector
    val (nbits, signed) = IntBinaryVector.minMaxToNbitsSigned(0, info.codeMap.size)
    val codeVectSize = IntBinaryVector.noNAsize(info.codes.length, nbits)
    val dictVectSize = info.dictStrings.frozenSize
    val bytesRequired = 12 + dictVectSize + codeVectSize
    val addr = memFactory.allocateOffheap(bytesRequired)
    // Copy over the dictionary strings
    // TODO: optimize in future to FIXED UTF8 vector?
    info.dictStrings.freeze(Some(addr + 12))

    // Fill up the codes - directly in the allocated space for the DictUTF8Vector
    val codeVect = IntBinaryVector.appendingVectorNoNA(addr + 12 + dictVectSize,
                                                       codeVectSize,
                                                       nbits, signed, () => {})
    codeVect.addVector(info.codes)

    // Write 12 bytes of metadata at beginning
    UnsafeUtils.setInt(addr, bytesRequired - 4)
    UnsafeUtils.setInt(addr + 4, WireFormat(VECTORTYPE_BINDICT, SUBTYPE_UTF8))
    UnsafeUtils.setInt(addr + 8, 12 + dictVectSize)
    addr
  }
}

/**
 * Dictionary-encoding UTF8 string BinaryVector
 * Layout:
 * +0   Int    number of bytes of rest of vector
 * +4   Int    WireFormat (VECTORTYPE_BINDICT, SUBTYPE_UTF8)
 * +8   Int    relative offset to integer vector for dictionary codes
 * +12         String dictionary vector (includes its own header bytes)
 * +....
 *
 * The code zero is used to mark NA.  Thus the first entry of the string dictionary is also NA.
 * Unlike the FlatBuffer-based DictStringVector, this one does not need to cache because there is no
 * string deserialization to be done, thus the code is much much simpler.
 */
object UTF8DictVectorDataReader extends UTF8VectorDataReader {
  final def codeVectAddr(acc: MemoryReader, vector: BinaryVectorPtr): BinaryVectorPtr =
    vector + acc.getInt(vector + 8)
  final def length(acc: MemoryReader, vector: BinaryVectorPtr): Int =
    IntBinaryVector(acc, codeVectAddr(acc, vector)).length(acc, codeVectAddr(acc, vector))
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): ZeroCopyUTF8String = {
    val code = IntBinaryVector(acc, codeVectAddr(acc, vector))(acc, codeVectAddr(acc, vector), n)
    UTF8FlexibleVectorDataReader(acc, vector + 12, code)
  }

  def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): UTF8Iterator = new UTF8Iterator {
    private final val codeIt = IntBinaryVector(acc, codeVectAddr(acc, vector))
                        .iterate(acc, codeVectAddr(acc, vector), startElement)
    def next: ZeroCopyUTF8String = UTF8FlexibleVectorDataReader(acc, vector + 12, codeIt.next)
  }

  override def iterateAvailable(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): BooleanIterator =
    new BooleanIterator {
      private final val codeIt = IntBinaryVector(acc, codeVectAddr(acc, vector))
        .iterate(acc, codeVectAddr(acc, vector), startElement)
      final def next: Boolean = codeIt.next != 0
    }
}
