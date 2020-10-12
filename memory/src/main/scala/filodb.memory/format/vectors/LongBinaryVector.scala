package filodb.memory.format.vectors

import java.nio.ByteBuffer

import debox.Buffer
import spire.syntax.cfor._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.Encodings._
import filodb.memory.format.MemoryReader._


object LongBinaryVector {
  /**
   * Creates a new MaskedLongAppendingVector, allocating a byte array of the right size for the max #
   * of elements plus a bit mask.
   * @param maxElements initial maximum number of elements this vector will hold. Will automatically grow.
   */
  def appendingVector(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Long] = {
    val bytesRequired = 12 + BitmapMask.numBytesRequired(maxElements) + 8 + 8 * maxElements
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose = () => memFactory.freeMemory(addr)
    GrowableVector(memFactory, new MaskedLongAppendingVector(addr, bytesRequired, maxElements, dispose))
  }

  /**
   * Creates a LongAppendingVector - does not grow and does not have bit mask. All values are marked
   * as available.
   */
  def appendingVectorNoNA(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Long] = {
    val bytesRequired = 8 + 8 * maxElements
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose = () => memFactory.freeMemory(addr)
    new LongAppendingVector(addr, bytesRequired, dispose)
  }

  /**
   * Creates a TimestampAppendingVector - does not grow and does not have bit mask. All values are marked
   * as available.  Uses approximate DeltaDeltaVector for better timestamp compression
   */
  def timestampVector(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Long] = {
    val bytesRequired = 8 + 8 * maxElements
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose = () => memFactory.freeMemory(addr)
    new TimestampAppendingVector(addr, bytesRequired, dispose)
  }

  def apply(buffer: ByteBuffer): LongVectorDataReader = {
    apply(MemoryReader.fromByteBuffer(buffer), 0)
  }

  import WireFormat._

  /**
   * Parses the type of vector from the WireFormat word at address+4 and returns the appropriate
   * LongVectorDataReader object for parsing it
   */
  def apply(acc: MemoryReader, vector: BinaryVectorPtr): LongVectorDataReader = {
    BinaryVector.vectorType(acc, vector) match {
      case x if x == WireFormat(VECTORTYPE_DELTA2, SUBTYPE_INT_NOMASK) => DeltaDeltaDataReader
      case x if x == WireFormat(VECTORTYPE_DELTA2, SUBTYPE_REPEATED)   => DeltaDeltaConstDataReader
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE)  => MaskedLongDataReader
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE_NOMASK) => LongVectorDataReader64
    }
  }

  /**
   * Produces a smaller, frozen BinaryVector if possible.
   * Here are the things tried:
   *  1. Try DeltaDelta.  That covers a huge range of optimizations: fitting in int with a slope line, fitting in
   *     smaller nbits
   *  4. If all values are filled (no NAs) then the bitmask is dropped
   */
  def optimize(memFactory: MemFactory, vector: OptimizingPrimitiveAppender[Long]): BinaryVectorPtr = {
    // Try delta-delta encoding
    DeltaDeltaVector.fromLongVector(memFactory, vector)
                    .getOrElse {
                      if (vector.noNAs) {
                        vector.dataVect(memFactory)
                      } else {
                        vector.getVect(memFactory)
                      }
                    }
  }
}

/**
 * An iterator optimized for speed and type-specific to avoid boxing.
 * It has no hasNext() method - because it is guaranteed to visit every element, and this way
 * you can avoid another method call for performance.
 */
trait LongIterator extends TypedIterator {
  def next: Long
}

/**
 * A VectorDataReader object that supports fast extraction of Long data BinaryVectors
 * +0000   4-byte length word
 * +0004   4-byte WireFormat
 * +0008   2-byte nbits  (unused for Longs)
 * +0010   1 byte Boolean signed
 * +0011   1 byte (actually 3 bits) bitshift when nbits < 8
 * +0012   start of packed Long data
 */
trait LongVectorDataReader extends VectorDataReader {
  /**
   * Retrieves the element at position/row n, where n=0 is the first element of the vector.
   */
  def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Long

  /**
   * Returns the number of elements in this vector
   */
  def length(acc: MemoryReader, vector: BinaryVectorPtr): Int =
    (numBytes(acc, vector) - PrimitiveVector.HeaderLen) / 8

  /**
   * Returns a LongIterator to efficiently go through the elements of the vector.  The user is responsible for
   * knowing how many elements to process.  There is no hasNext.
   * All elements are iterated through, even those designated as "not available".
   * Costs an allocation for the iterator but allows potential performance gains too.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param startElement the starting element # in the vector, by default 0 (the first one)
   */
  def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): LongIterator

  def debugString(acc: MemoryReader, vector: BinaryVectorPtr, sep: String = ","): String = {
    val it = iterate(acc, vector)
    val size = length(acc, vector)
    (0 to size).map(_ => it.next).mkString(sep)
  }

  /**
   * Sums up the Long values in the vector from position start to position end.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param start the starting element # in the vector to sum, 0 == first element
   * @param end the ending element # in the vector to sum, inclusive
   * @return a Double, since Longs might possibly overflow
   */
  def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Double

  /**
   * Efficiently searches for the first element # where the vector element is greater than or equal to item.
   * Good for finding the startElement for iterate() above where time is at least item.
   * Assumes all the elements of vector are in increasing numeric order.
   * @return bits 0-30: the position/element #.
   *           If all the elements in vector are less than item, then the vector length is returned.
   *         bit 31   : set if element did not match exactly / no match
   */
  def binarySearch(acc: MemoryReader, vector: BinaryVectorPtr, item: Long): Int

  def changes(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int,
              prev: Long, ignorePrev: Boolean = false): (Long, Long)
  /**
   * Searches for the last element # whose element is <= the item, assuming all elements are increasing.
   * Typically used to find the last timestamp <= item.
   * Uses binarySearch.  TODO: maybe optimize by comparing item to first item.
   * @return integer row or element #.  -1 means item is less than the first item in vector.
   */
  final def ceilingIndex(acc: MemoryReader, vector: BinaryVectorPtr, item: Long): Int =
    binarySearch(acc, vector, item) match {
      // if endTime does not match, we want last row such that timestamp < endTime
      // Note if we go past end of timestamps, it will never match, so this should make sure we don't go too far
      case row if row < 0 => (row & 0x7fffffff) - 1
      // otherwise if timestamp == endTime, just use that row number
      case row            => row
    }

  /**
   * Converts the BinaryVector to an unboxed Buffer.
   * Only returns elements that are "available".
   */
  // NOTE: I know this code is repeated but I don't want to have to debug specialization/unboxing/traits right now
  def toBuffer(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): Buffer[Long] = {
    val newBuf = Buffer.empty[Long]
    val dataIt = iterate(acc, vector, startElement)
    val availIt = iterateAvailable(acc, vector, startElement)
    val len = length(acc, vector)
    cforRange { startElement until len } { n =>
      val item = dataIt.next
      if (availIt.next) newBuf += item
    }
    newBuf
  }
}

/**
 * VectorDataReader for a Long BinaryVector using full 64-bits for a Long value
 */
object LongVectorDataReader64 extends LongVectorDataReader {
  import PrimitiveVector.OffsetData

  // Put addr in constructor to make accesses much faster
  class Long64Iterator(acc: MemoryReader, var addr: Long) extends LongIterator {
    final def next: Long = {
      val data = acc.getLong(addr)
      addr += 8
      data
    }
  }

  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Long =
    acc.getLong(vector + OffsetData + n * 8)
  def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): LongIterator =
    new Long64Iterator(acc, vector + OffsetData + startElement * 8)

  // end is inclusive
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Double = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is out of bounds, " +
      s"length=${length(acc, vector)}")
    var addr = vector + OffsetData + start * 8
    val untilAddr = vector + OffsetData + end * 8 + 8   // one past the end
    var sum: Double = 0d
    while (addr < untilAddr) {
      sum += acc.getLong(addr)
      addr += 8
    }
    sum
  }

  /**
   * Default O(log n) binary search implementation assuming fast random access, which is true here.
   * Everything should be intrinsic and registers so should be super fast
   */
  def binarySearch(acc: MemoryReader, vector: BinaryVectorPtr, item: Long): Int = {
    var len = length(acc, vector)
    if (len == 0) return 0x80000000
    var first = 0
    var element = 0L
    while (len > 0) {
      val half = len >>> 1
      val middle = first + half
      element = acc.getLong(vector + OffsetData + middle * 8)
      if (element == item) {
        return middle
      } else if (element < item) {
        first = middle + 1
        len = len - half - 1
      } else {
        len = half
      }
    }
    if (element == item) first else first | 0x80000000
  }

  final def changes(acc: MemoryReader, vector: BinaryVectorPtr,
                    start: Int, end: Int, prev: Long, ignorePrev: Boolean = false):
  (Long, Long) = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is out of bounds, " +
      s"length=${length(acc, vector)}")
    var prevVector: Long = prev
    val startAddr = vector + OffsetData + start * 8
    val untilAddr = vector + OffsetData + end * 8 + 8   // one past the end
    var changes = 0
    var addr = startAddr
    while (addr < untilAddr) {
      val cur = acc.getLong(addr)
      if (addr == startAddr) prevVector = cur
      if (prevVector != cur) changes += 1
      prevVector = cur
      addr += 8
    }
    (changes, prevVector)
  }
}

/**
 * VectorDataReader for a masked (NA bit) Long BinaryVector, uses underlying DataReader for subvector
 */
object MaskedLongDataReader extends LongVectorDataReader with BitmapMaskVector {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Long = {
    val subvect = subvectAddr(acc, vector)
    LongBinaryVector(acc, subvect).apply(acc, subvect, n)
  }

  override def length(acc: MemoryReader, vector: BinaryVectorPtr): Int =
    LongBinaryVector(acc, subvectAddr(acc, vector)).length(acc, subvectAddr(acc, vector))

  override def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): LongIterator =
    LongBinaryVector(acc, subvectAddr(acc, vector)).iterate(acc, subvectAddr(acc, vector), startElement)

  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Double =
    LongBinaryVector(acc, subvectAddr(acc, vector)).sum(acc, subvectAddr(acc, vector), start, end)

  def binarySearch(acc: MemoryReader, vector: BinaryVectorPtr, item: Long): Int =
    LongBinaryVector(acc, subvectAddr(acc, vector)).binarySearch(acc, subvectAddr(acc, vector), item)

   def changes(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int,
               prev: Long, ignorePrev: Boolean = false): (Long, Long) =
     LongBinaryVector(acc, subvectAddr(acc, vector)).changes(acc, subvectAddr(acc, vector), start, end, prev)
}

class LongAppendingVector(addr: BinaryRegion.NativePointer, maxBytes: Int, val dispose: () => Unit)
extends PrimitiveAppendableVector[Long](addr, maxBytes, 64, true) {
  final def addNA(): AddResponse = addData(0L)
  final def addData(data: Long): AddResponse = checkOffset() match {
    case Ack =>
      UnsafeUtils.setLong(writeOffset, data)
      incWriteOffset(8)
      Ack
    case other: AddResponse => other
  }

  final def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = addData(reader.getLong(col))

  private final val readVect = LongBinaryVector(nativePtrReader, addr)
  final def apply(index: Int): Long = readVect.apply(nativePtrReader, addr, index)
  final def reader: VectorDataReader = LongVectorDataReader64
  def copyToBuffer: Buffer[Long] = LongVectorDataReader64.toBuffer(nativePtrReader, addr)

  final def minMax: (Long, Long) = {
    var min = Long.MaxValue
    var max = Long.MinValue
    cforRange { 0 until length } { index =>
      val data = apply(index)
      if (data < min) min = data
      if (data > max) max = data
    }
    (min, max)
  }

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr =
    LongBinaryVector.optimize(memFactory, this)
}

/**
 * TimestampAppendingVector is just like LongAppendingVector EXCEPT that it uses
 * "approximate" Delta Delta encoding.  If the values are all within 250ms of the DDV "slope" line, which is
 * true most of the time in constantly emitting time series, then we use a "constant" DDV to save space.
 */
class TimestampAppendingVector(addr: BinaryRegion.NativePointer, maxBytes: Int, dispose: () => Unit)
extends LongAppendingVector(addr, maxBytes, dispose) {
  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr =
    DeltaDeltaVector.fromLongVector(memFactory, this, approxConst = true)
                    .getOrElse {
                      if (noNAs) dataVect(memFactory) else getVect(memFactory)
                    }
}

class MaskedLongAppendingVector(addr: BinaryRegion.NativePointer,
                                val maxBytes: Int,
                                val maxElements: Int,
                                val dispose: () => Unit) extends
// +8: offset to LongBinaryVector
BitmapMaskAppendableVector[Long](addr, maxElements) with OptimizingPrimitiveAppender[Long] {
  def vectMajorType: Int = WireFormat.VECTORTYPE_BINSIMPLE
  def vectSubType: Int = WireFormat.SUBTYPE_PRIMITIVE
  def nbits: Short = 64

  val subVect = new LongAppendingVector(addr + subVectOffset, maxBytes - subVectOffset, dispose)
  def copyToBuffer: Buffer[Long] = MaskedLongDataReader.toBuffer(nativePtrReader, addr)

  final def minMax: (Long, Long) = {
    var min = Long.MaxValue
    var max = Long.MinValue
    cforRange { 0 until length } { index =>
      if (isAvailable(index)) {
        val data = subVect.apply(index)
        if (data < min) min = data
        if (data > max) max = data
      }
    }
    (min, max)
  }

  final def dataVect(memFactory: MemFactory): BinaryVectorPtr = subVect.freeze(memFactory)

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr =
    LongBinaryVector.optimize(memFactory, this)

  override def newInstance(memFactory: MemFactory, growFactor: Int = 2): BinaryAppendableVector[Long] = {
    val newAddr = memFactory.allocateOffheap(maxBytes * growFactor)
    val dispose = () => memFactory.freeMemory(newAddr)
    new MaskedLongAppendingVector(newAddr, maxBytes * growFactor, maxElements * growFactor, dispose)
  }
}
