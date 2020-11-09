package filodb.memory.format.vectors

import java.nio.ByteBuffer

import debox.Buffer
import spire.syntax.cfor._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.Encodings._
import filodb.memory.format.MemoryReader._

object DoubleVector {
  /**
   * Creates a new MaskedDoubleAppendingVector, allocating a byte array of the right size for the max #
   * of elements plus a bit mask.
   * @param maxElements initial maximum number of elements this vector will hold. Will automatically grow.
   */
  def appendingVector(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Double] = {
    val bytesRequired = 12 + BitmapMask.numBytesRequired(maxElements) + 8 + 8 * maxElements
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose = () => memFactory.freeMemory(addr)
    GrowableVector(memFactory, new MaskedDoubleAppendingVector(addr, bytesRequired, maxElements, dispose))
  }

  /**
   * Creates a DoubleAppendingVector - does not grow and does not have bit mask. All values are marked
   * as available.
   * @param maxElements the max number of elements the vector will hold.  Not expandable
   * @param detectDrops if true, then use a DoubleCounterAppender instead to detect drops
   */
  def appendingVectorNoNA(memFactory: MemFactory, maxElements: Int, detectDrops: Boolean = false):
  BinaryAppendableVector[Double] = {
    val bytesRequired = 8 + 8 * maxElements
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose = () => memFactory.freeMemory(addr)
    if (detectDrops) new DoubleCounterAppender(addr, bytesRequired, dispose)
    else             new DoubleAppendingVector(addr, bytesRequired, dispose)
  }

  /**
   * Quickly create a DoubleVector from a sequence of Doubles which can be optimized.
   */
  def apply(memFactory: MemFactory, data: Seq[Double]): BinaryAppendableVector[Double] = {
    val vect = appendingVectorNoNA(memFactory, data.length)
    data.foreach(vect.addData)
    vect
  }

  def apply(buffer: ByteBuffer): DoubleVectorDataReader = {
    require(buffer.isDirect)
    apply(MemoryReader.fromByteBuffer(buffer), 0)
  }

  import WireFormat._

  /**
   * Parses the type of vector from the WireFormat word at address+4 and returns the appropriate
   * DoubleVectorDataReader object for parsing it
   */
  def apply(acc: MemoryReader, vector: BinaryVectorPtr): DoubleVectorDataReader = {
    val reader = BinaryVector.vectorType(acc, vector) match {
      case x if x == WireFormat(VECTORTYPE_DELTA2, SUBTYPE_INT_NOMASK)    => DoubleLongWrapDataReader
      case x if x == WireFormat(VECTORTYPE_DELTA2, SUBTYPE_REPEATED)      => DoubleLongWrapDataReader
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE)  => MaskedDoubleDataReader
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE_NOMASK) => DoubleVectorDataReader64
    }
    if (PrimitiveVectorReader.dropped(acc, vector)) new CorrectingDoubleVectorReader(reader, acc, vector)
    else                                       reader
  }

  /**
   * For now, since most Prometheus double data is in fact integral, we take a really simple approach:
   * 1. First if all doubles are integral, use DeltaDeltaVector to encode
   * 2. If not, don't compress
   *
   * In future, try some schemes that work for doubles:
   * - XOR of initial value, bitshift, store using less bits (similar to Gorilla but not differential)
   * - Delta2/slope double diff first, shift doubles by offset, then use XOR technique
   *    (XOR works better when numbers are same sign plus smaller exponent range)
   * - slightly lossy version of any of above
   * - http://vis.cs.ucdavis.edu/vis2014papers/TVCG/papers/2674_20tvcg12-lindstrom-2346458.pdf
   * - (slightly lossy) normalize exponents and convert to fixed point, then compress using int/long techniques
   */
  def optimize(memFactory: MemFactory, vector: OptimizingPrimitiveAppender[Double]): BinaryVectorPtr = {
    val longWrapper = new LongDoubleWrapper(vector)
    if (longWrapper.allIntegrals) {
      DeltaDeltaVector.fromLongVector(memFactory, longWrapper)
        .getOrElse {
          if (vector.noNAs) vector.dataVect(memFactory) else vector.getVect(memFactory)
        }
    } else {
      if (vector.noNAs) vector.dataVect(memFactory) else vector.getVect(memFactory)
    }
  }
}

final case class DoubleCorrection(lastValue: Double, correction: Double = 0.0) extends CorrectionMeta

/**
 * An iterator optimized for speed and type-specific to avoid boxing.
 * It has no hasNext() method - because it is guaranteed to visit every element, and this way
 * you can avoid another method call for performance.
 */
trait DoubleIterator extends TypedIterator {
  def next: Double
}

/**
 * A VectorDataReader object that supports fast extraction of Double data BinaryVectors
 * +0000   4-byte length word
 * +0004   2-byte WireFormat
 * +0006   2-byte nbits / signed / bitshift
 * +0008   start of packed Double data
 */
trait DoubleVectorDataReader extends CounterVectorReader {
  /**
   * Retrieves the element at position/row n, where n=0 is the first element of the vector.
   */
  def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Double

  // This length method works assuming nbits is divisible into 32
  def length(acc: MemoryReader, vector: BinaryVectorPtr): Int =
    (numBytes(acc, vector) - PrimitiveVector.HeaderLen) / 8

  /**
   * Returns a DoubleIterator to efficiently go through the elements of the vector.  The user is responsible for
   * knowing how many elements to process.  There is no hasNext.
   * All elements are iterated through, even those designated as "not available".
   * Costs an allocation for the iterator but allows potential performance gains too.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param startElement the starting element # in the vector, by default 0 (the first one)
   */
  def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): DoubleIterator

  def debugString(acc: MemoryReader, vector: BinaryVectorPtr, sep: String = ","): String = {
    val it = iterate(acc, vector)
    val size = length(acc, vector)
    (0 to size).map(_ => it.next).mkString(sep)
  }

  /**
   * Sums up the Double values in the vector from position start to position end.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param start the starting element # in the vector to sum, 0 == first element
   * @param end the ending element # in the vector to sum, inclusive
   * @param ignoreNaN if true, ignore samples which have NaN value (sometimes used for special purposes)
   */
  def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int, ignoreNaN: Boolean = true): Double

  /**
   * Counts the values excluding NaN / not available bits
   */
  def count(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Int

  def changes(acc: MemoryReader, vector: BinaryVectorPtr,
              start: Int, end: Int, prev: Double, ignorePrev: Boolean = false): (Double, Double)

  /**
   * Converts the BinaryVector to an unboxed Buffer.
   * Only returns elements that are "available".
   */
  // NOTE: I know this code is repeated but I don't want to have to debug specialization/unboxing/traits right now
  def toBuffer(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): Buffer[Double] = {
    val newBuf = Buffer.empty[Double]
    val dataIt = iterate(acc, vector, startElement)
    val availIt = iterateAvailable(acc, vector, startElement)
    val len = length(acc, vector)
    cforRange { startElement until len } { n =>
      val item = dataIt.next
      if (availIt.next) newBuf += item
    }
    newBuf
  }

  def detectDropAndCorrection(acc: MemoryReader,
                              vector: BinaryVectorPtr,
                              meta: CorrectionMeta): CorrectionMeta = meta match {
    case NoCorrection =>   meta    // No last value, cannot compare.  Just pass it on.
    case DoubleCorrection(lastValue, correction) =>
      val firstValue = apply(acc, vector, 0)
      // Last value is the new delta correction
      if (firstValue < lastValue) DoubleCorrection(lastValue, correction + lastValue)
      else                        meta
  }

  // Default implementation for vectors with no correction
  def updateCorrection(acc: MemoryReader, vector: BinaryVectorPtr, meta: CorrectionMeta): CorrectionMeta =
    meta match {
      // Return the last value and simply pass on the previous correction value
      case DoubleCorrection(_, corr) => DoubleCorrection(apply(acc, vector, length(acc, vector) - 1), corr)
      case NoCorrection              => DoubleCorrection(apply(acc, vector, length(acc, vector) - 1), 0.0)
    }

  /**
   * Retrieves the element at position/row n, with counter correction, taking into account a previous
   * correction factor.  Calling this method with increasing n should result in nondecreasing
   * values starting no lower than the initial correction factor in correctionMeta.
   * NOTE: this is a default implementation for vectors having no correction
   */
  def correctedValue(acc: MemoryReader, vector: BinaryVectorPtr, n: Int, meta: CorrectionMeta): Double = meta match {
    // Since this is a vector that needs no correction, simply add the correction amount to the original value
    case DoubleCorrection(_, corr) => apply(acc, vector, n) + corr
    case NoCorrection              => apply(acc, vector, n)
  }

  // Default implementation with no drops detected
  def dropPositions(acc2: MemoryReader, vector: BinaryVectorPtr): debox.Buffer[Int] = debox.Buffer.empty[Int]
}

/**
 * VectorDataReader for a Double BinaryVector using full 64-bits for a Double value
 * Right now this is for non-corrected ingested vectors.
 */
object DoubleVectorDataReader64 extends DoubleVectorDataReader {
  import PrimitiveVector.OffsetData

  class Double64Iterator(acc: MemoryReader, var addr: BinaryRegion.NativePointer) extends DoubleIterator {
    final def next: Double = {
      val data = acc.getDouble(addr)
      addr += 8
      data
    }
  }

  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Double =
    acc.getDouble(vector + OffsetData + n * 8)
  def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): DoubleIterator =
    new Double64Iterator(acc, vector + OffsetData + startElement * 8)

  // end is inclusive
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr,
                start: Int, end: Int, ignoreNaN: Boolean = true): Double = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is out of bounds, " +
      s"length=${length(acc, vector)}")
    var addr = vector + OffsetData + start * 8
    val untilAddr = vector + OffsetData + end * 8 + 8   // one past the end
    var sum: Double = Double.NaN
    if (ignoreNaN) {
      while (addr < untilAddr) {
        val nextDbl = acc.getDouble(addr)
        // There are many possible values of NaN.  Use a function to ignore them reliably.
        if (!java.lang.Double.isNaN(nextDbl)) {
          if (sum.isNaN) sum = 0d
          sum += nextDbl
        }
        addr += 8
      }
    } else {
      sum = 0d
      while (addr < untilAddr) {
        sum += acc.getDouble(addr)
        addr += 8
      }
    }
    sum
  }

  final def count(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Int = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is out of bounds, " +
      s"length=${length(acc, vector)}")
    var addr = vector + OffsetData + start * 8
    val untilAddr = vector + OffsetData + end * 8 + 8   // one past the end
    var count = 0

    while (addr < untilAddr) {
      val nextDbl = acc.getDouble(addr)
      // There are many possible values of NaN.  Use a function to ignore them reliably.
      if (!java.lang.Double.isNaN(nextDbl)) count += 1
      addr += 8
    }
    count
  }

  final def changes(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int,
                    prev: Double, ignorePrev: Boolean = false):
  (Double, Double) = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is out of bounds, " +
      s"length=${length(acc, vector)}")
    var addr = vector + OffsetData + start * 8
    val untilAddr = vector + OffsetData + end * 8 + 8   // one past the end
    var changes = 0d
    var prevVector : Double = prev
    while (addr < untilAddr) {
      val nextDbl = acc.getDouble(addr)
      // There are many possible values of NaN.  Use a function to ignore them reliably.
      if (!java.lang.Double.isNaN(nextDbl) && prevVector != nextDbl && !java.lang.Double.isNaN(prevVector)) {
        changes += 1
      }
      addr += 8
      prevVector = nextDbl
    }
    (changes, prevVector)
  }
}

// Corrects and caches ONE underlying chunk.
// The algorithm is naive - just go through and correct all values.  Total correction for whole vector is passed.
// Works fine for randomly accessible vectors.
class CorrectingDoubleVectorReader(inner: DoubleVectorDataReader, acc: MemoryReader, vect: BinaryVectorPtr)
extends DoubleVectorDataReader {
  override def length(acc2: MemoryReader, vector: BinaryVectorPtr): Int = inner.length(acc2, vector)
  def apply(acc2: MemoryReader, vector: BinaryVectorPtr, n: Int): Double = inner(acc2, vector, n)
  def iterate(acc2: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): DoubleIterator =
    inner.iterate(acc2, vector, startElement)
  def sum(acc2: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int, ignoreNaN: Boolean = true): Double =
    inner.sum(acc2, vector, start, end, ignoreNaN)
  def count(acc2: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Int =
    inner.count(acc2, vector, start, end)
  def changes(acc2: MemoryReader, vector: BinaryVectorPtr,
              start: Int, end: Int, prev: Double, ignorePrev: Boolean = false): (Double, Double) =
    inner.changes(acc2, vector, start, end, prev)

  private var _correction = 0.0
  private val _drops = debox.Buffer.empty[Int] // to track counter drop positions
  // Lazily correct - not all queries want corrected data
  lazy val corrected = {
    // if asked, lazily create corrected values and resets list
    val _corrected = new Array[Double](length(acc, vect))
    val it = iterate(acc, vect, 0)
    var last = Double.MinValue
    cforRange { 0 until length(acc, vect) } { pos =>
      val nextVal = it.next
      if (nextVal < last) {   // reset!
        _correction += last
        _drops += pos
      }
      _corrected(pos) = nextVal + _correction
      last = nextVal
    }
    _corrected
  }

  override def dropPositions(acc2: MemoryReader, vector: BinaryVectorPtr): debox.Buffer[Int] = {
    assert(vector == vect && acc == acc2)
    corrected // access it since it is lazy
    _drops
  }

  override def correctedValue(acc2: MemoryReader, vector: BinaryVectorPtr,
                              n: Int, correctionMeta: CorrectionMeta): Double = {
    assert(vector == vect && acc == acc2)
    correctionMeta match {
      // corrected value + any carryover correction
      case DoubleCorrection(_, corr) => corrected(n) + corr
      case NoCorrection              => corrected(n)
    }
  }

  override def updateCorrection(acc2: MemoryReader, vector: BinaryVectorPtr, meta: CorrectionMeta): CorrectionMeta = {
    assert(vector == vect && acc == acc2)
    val lastValue = apply(acc2, vector, length(acc2, vector) - 1)
    // Return the last (original) value and all corrections onward
    meta match {
      case DoubleCorrection(_, corr) => DoubleCorrection(lastValue, corr + _correction)
      case NoCorrection              => DoubleCorrection(lastValue, _correction)
    }
  }
}

/**
 * VectorDataReader for a masked (NA bit) Double BinaryVector, uses underlying DataReader for subvector
 */
object MaskedDoubleDataReader extends DoubleVectorDataReader with BitmapMaskVector {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Double = {
    val subvect = subvectAddr(acc, vector)
    DoubleVector(acc, subvect).apply(acc, subvect, n)
  }

  override def length(acc: MemoryReader, vector: BinaryVectorPtr): Int = super.length(acc, subvectAddr(acc, vector))

  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int, ignoreNaN: Boolean = true): Double =
    DoubleVector(acc, subvectAddr(acc, vector)).sum(acc, subvectAddr(acc, vector), start, end, ignoreNaN)

  final def count(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Int =
    DoubleVector(acc, subvectAddr(acc, vector)).count(acc, subvectAddr(acc, vector), start, end)

  override def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): DoubleIterator =
    DoubleVector(acc, subvectAddr(acc, vector)).iterate(acc, subvectAddr(acc, vector), startElement)

  override def changes(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int,
                       prev: Double, ignorePrev: Boolean = false): (Double, Double) =
    DoubleVector(acc, subvectAddr(acc, vector)).changes(acc, subvectAddr(acc, vector), start, end, prev)
}

class DoubleAppendingVector(addr: BinaryRegion.NativePointer, maxBytes: Int, val dispose: () => Unit)
extends PrimitiveAppendableVector[Double](addr, maxBytes, 64, true) {
  final def addNA(): AddResponse = addData(0.0)
  def addData(data: Double): AddResponse = checkOffset() match {
    case Ack =>
      UnsafeUtils.setDouble(writeOffset, data)
      incWriteOffset(8)
      Ack
    case other: AddResponse => other
  }

  final def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = addData(reader.getDouble(col))

  final def minMax: (Double, Double) = {
    var min = Double.MaxValue
    var max = Double.MinValue
    cforRange { 0 until length } { index =>
      val data = apply(index)
      if (data < min) min = data
      if (data > max) max = data
    }
    (min, max)
  }

  private final val readVect = DoubleVector(nativePtrReader, addr)
  final def apply(index: Int): Double = readVect.apply(nativePtrReader, addr, index)
  def reader: VectorDataReader = readVect
  final def copyToBuffer: Buffer[Double] = DoubleVectorDataReader64.toBuffer(nativePtrReader, addr)

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr =
    DoubleVector.optimize(memFactory, this)
}

/**
 * A Double appender for incrementing counters that detects if there is a drop in ingested value,
 * and if so, marks the Reset/Drop bit (bit 15 of the NBits/Offset u16).  Also marks this bit in
 * encoded chunks as well.
 */
class DoubleCounterAppender(addr: BinaryRegion.NativePointer, maxBytes: Int, dispose: () => Unit)
extends DoubleAppendingVector(addr, maxBytes, dispose) {
  private var last = Double.MinValue
  override final def addData(data: Double): AddResponse = {
    if (data < last) PrimitiveVectorReader.markDrop(MemoryAccessor.nativePtrAccessor, addr)
    last = data
    super.addData(data)
  }

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr = {
    val newChunk = DoubleVector.optimize(memFactory, this)
    if (PrimitiveVectorReader.dropped(nativePtrReader, addr))
      PrimitiveVectorReader.markDrop(MemoryAccessor.nativePtrAccessor, newChunk)
    newChunk
  }

  override def reader: VectorDataReader = DoubleVector(nativePtrReader, addr)
}

class MaskedDoubleAppendingVector(addr: BinaryRegion.NativePointer,
                                  val maxBytes: Int,
                                  val maxElements: Int,
                                  val dispose: () => Unit) extends
// First four bytes: offset to DoubleBinaryVector
BitmapMaskAppendableVector[Double](addr, maxElements) with OptimizingPrimitiveAppender[Double] {
  def vectMajorType: Int = WireFormat.VECTORTYPE_BINSIMPLE
  def vectSubType: Int = WireFormat.SUBTYPE_PRIMITIVE
  def nbits: Short = 64

  val subVect = new DoubleAppendingVector(addr + subVectOffset, maxBytes - subVectOffset, dispose)
  def copyToBuffer: Buffer[Double] = MaskedDoubleDataReader.toBuffer(nativePtrReader, addr)

  final def minMax: (Double, Double) = {
    var min = Double.MaxValue
    var max = Double.MinValue
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
    DoubleVector.optimize(memFactory, this)

  override def newInstance(memFactory: MemFactory, growFactor: Int = 2): BinaryAppendableVector[Double] = {
    val newAddr = memFactory.allocateOffheap(maxBytes * growFactor)
    val dispose = () => memFactory.freeMemory(newAddr)
    new MaskedDoubleAppendingVector(newAddr, maxBytes * growFactor, maxElements * growFactor, dispose)
  }
}

/**
 * A wrapper around Double appenders that returns Longs.  Designed to feed into the Long/DeltaDelta optimizers
 * so that an optimized int representation of double vector can be produced in one pass without
 * appending to another Long based AppendingVector first.
 */
private[vectors] class LongDoubleWrapper(val inner: OptimizingPrimitiveAppender[Double])
extends AppendableVectorWrapper[Long, Double] {
  val MaxLongDouble = Long.MaxValue.toDouble
  final def nonIntegrals: Int = {
    var nonInts = 0
    cforRange { 0 until length } { index =>
      if (inner.isAvailable(index)) {
        val data = inner.apply(index)
        if (data > MaxLongDouble || (Math.rint(data) != data)) nonInts += 1
      }
    }
    nonInts
  }

  val allIntegrals: Boolean = (nonIntegrals == 0)

  final def addData(value: Long): AddResponse = inner.addData(value.toDouble)
  final def apply(index: Int): Long = inner(index).toLong
}

/**
 * A wrapper to return Doubles from a Long vector... for when one can compress Double vectors as DDVs
 */
object DoubleLongWrapDataReader extends DoubleVectorDataReader {
  class DoubleLongWrapIterator(innerIt: LongIterator) extends DoubleIterator {
    final def next: Double = innerIt.next.toDouble
  }

  override def length(acc: MemoryReader, vector: BinaryVectorPtr): Int =
    LongBinaryVector(acc, vector).length(acc, vector)
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Double =
    LongBinaryVector(acc, vector)(acc, vector, n).toDouble
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int, ignoreNaN: Boolean = true): Double =
    LongBinaryVector(acc, vector).sum(acc, vector, start, end)   // Long vectors cannot contain NaN, ignore it
  final def count(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Int = end - start + 1
  final def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): DoubleIterator =
    new DoubleLongWrapIterator(LongBinaryVector(acc, vector).iterate(acc, vector, startElement))

  final def changes(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int,
                    prev: Double, ignorePrev: Boolean = false):
  (Double, Double) = {
    val ignorePrev = if (prev.isNaN) true
    else false
    val changes = LongBinaryVector(acc, vector).changes(acc, vector, start, end, prev.toLong, ignorePrev)
    (changes._1.toDouble, changes._2.toDouble)
  }
}
