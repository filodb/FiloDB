package filodb.memory.format.vectors

import java.nio.ByteBuffer

import debox.Buffer
import scalaxy.loops._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.Encodings._


object DoubleVector {
  /**
   * Creates a new MaskedDoubleAppendingVector, allocating a byte array of the right size for the max #
   * of elements plus a bit mask.
   * @param maxElements initial maximum number of elements this vector will hold. Will automatically grow.
   */
  def appendingVector(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Double] = {
    val bytesRequired = 12 + BitmapMask.numBytesRequired(maxElements) + 8 + 8 * maxElements
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose =  () => memFactory.freeMemory(addr)
    GrowableVector(memFactory,new MaskedDoubleAppendingVector(addr, bytesRequired, maxElements, dispose))
  }

  /**
   * Creates a DoubleAppendingVector - does not grow and does not have bit mask. All values are marked
   * as available.
   * @param maxElements the max number of elements the vector will hold.  Not expandable
   */
  def appendingVectorNoNA(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Double] = {
    val bytesRequired = 8 + 8 * maxElements
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose =  () => memFactory.freeMemory(addr)
    new DoubleAppendingVector(addr, bytesRequired, dispose)
  }

  /**
   * Quickly create a DoubleVector from a sequence of Doubles which can be optimized.
   */
  def apply(memFactory: MemFactory, data: Seq[Double]): BinaryAppendableVector[Double] = {
    val vect = appendingVectorNoNA(memFactory, data.length)
    data.foreach(vect.addData)
    vect
  }

  def apply(buffer: ByteBuffer): DoubleVectorDataReader = apply(UnsafeUtils.addressFromDirectBuffer(buffer))

  import WireFormat._

  /**
   * Parses the type of vector from the WireFormat word at address+4 and returns the appropriate
   * DoubleVectorDataReader object for parsing it
   */
  def apply(vector: BinaryVectorPtr): DoubleVectorDataReader = BinaryVector.vectorType(vector) match {
    case x if x == WireFormat(VECTORTYPE_DELTA2,    SUBTYPE_INT_NOMASK) => DoubleLongWrapDataReader
    case x if x == WireFormat(VECTORTYPE_DELTA2,    SUBTYPE_REPEATED)   => DoubleLongWrapDataReader
    case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE)  => MaskedDoubleDataReader
    case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE_NOMASK) => DoubleVectorDataReader64
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
trait DoubleVectorDataReader extends VectorDataReader {
  /**
   * Retrieves the element at position/row n, where n=0 is the first element of the vector.
   */
  def apply(vector: BinaryVectorPtr, n: Int): Double

  // This length method works assuming nbits is divisible into 32
  def length(vector: BinaryVectorPtr): Int = (numBytes(vector) - PrimitiveVector.HeaderLen) / 8

  /**
   * Returns a DoubleIterator to efficiently go through the elements of the vector.  The user is responsible for
   * knowing how many elements to process.  There is no hasNext.
   * All elements are iterated through, even those designated as "not available".
   * Costs an allocation for the iterator but allows potential performance gains too.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param startElement the starting element # in the vector, by default 0 (the first one)
   */
  def iterate(vector: BinaryVectorPtr, startElement: Int = 0): DoubleIterator

  /**
   * Sums up the Double values in the vector from position start to position end.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param start the starting element # in the vector to sum, 0 == first element
   * @param end the ending element # in the vector to sum, inclusive
   * @param ignoreNan if true, ignore samples which have NaN value (sometimes used for special purposes)
   */
  def sum(vector: BinaryVectorPtr, start: Int, end: Int, ignoreNaN: Boolean = true): Double

  /**
   * Counts the values excluding NaN / not available bits
   */
  def count(vector: BinaryVectorPtr, start: Int, end: Int): Int

  /**
   * Converts the BinaryVector to an unboxed Buffer.
   * Only returns elements that are "available".
   */
  // NOTE: I know this code is repeated but I don't want to have to debug specialization/unboxing/traits right now
  def toBuffer(vector: BinaryVectorPtr, startElement: Int = 0): Buffer[Double] = {
    val newBuf = Buffer.empty[Double]
    val dataIt = iterate(vector, startElement)
    val availIt = iterateAvailable(vector, startElement)
    val len = length(vector)
    for { n <- startElement until len optimized } {
      val item = dataIt.next
      if (availIt.next) newBuf += item
    }
    newBuf
  }
}

/**
 * VectorDataReader for a Double BinaryVector using full 64-bits for a Double value
 */
object DoubleVectorDataReader64 extends DoubleVectorDataReader {
  import PrimitiveVector.OffsetData

  class Double64Iterator(var addr: BinaryRegion.NativePointer) extends DoubleIterator {
    final def next: Double = {
      val data = UnsafeUtils.getDouble(addr)
      addr += 8
      data
    }
  }

  final def apply(vector: BinaryVectorPtr, n: Int): Double = UnsafeUtils.getDouble(vector + OffsetData + n * 8)
  def iterate(vector: BinaryVectorPtr, startElement: Int = 0): DoubleIterator =
    new Double64Iterator(vector + OffsetData + startElement * 8)

  // end is inclusive
  final def sum(vector: BinaryVectorPtr, start: Int, end: Int, ignoreNaN: Boolean = true): Double = {
    var addr = vector + OffsetData + start * 8
    val untilAddr = vector + OffsetData + end * 8 + 8   // one past the end
    var sum: Double = 0d
    if (ignoreNaN) {
      while (addr < untilAddr) {
        val nextDbl = UnsafeUtils.getDouble(addr)
        // There are many possible values of NaN.  Use a function to ignore them reliably.
        if (!java.lang.Double.isNaN(nextDbl)) sum += nextDbl
        addr += 8
      }
    } else {
      while (addr < untilAddr) {
        sum += UnsafeUtils.getDouble(addr)
        addr += 8
      }
    }
    sum
  }

  final def count(vector: BinaryVectorPtr, start: Int, end: Int): Int = {
    var addr = vector + OffsetData + start * 8
    val untilAddr = vector + OffsetData + end * 8 + 8   // one past the end
    var count = 0
    while (addr < untilAddr) {
      val nextDbl = UnsafeUtils.getDouble(addr)
      // There are many possible values of NaN.  Use a function to ignore them reliably.
      if (!java.lang.Double.isNaN(nextDbl)) count += 1
      addr += 8
    }
    count
  }
}

/**
 * VectorDataReader for a masked (NA bit) Double BinaryVector, uses underlying DataReader for subvector
 */
object MaskedDoubleDataReader extends DoubleVectorDataReader with BitmapMaskVector {
  final def apply(vector: BinaryVectorPtr, n: Int): Double = {
    val subvect = subvectAddr(vector)
    DoubleVector(subvect).apply(subvect, n)
  }

  override def length(vector: BinaryVectorPtr): Int = super.length(subvectAddr(vector))

  final def sum(vector: BinaryVectorPtr, start: Int, end: Int, ignoreNaN: Boolean = true): Double =
    DoubleVector(subvectAddr(vector)).sum(subvectAddr(vector), start, end, ignoreNaN)

  final def count(vector: BinaryVectorPtr, start: Int, end: Int): Int =
    DoubleVector(subvectAddr(vector)).count(subvectAddr(vector), start, end)

  override def iterate(vector: BinaryVectorPtr, startElement: Int = 0): DoubleIterator =
    DoubleVector(subvectAddr(vector)).iterate(subvectAddr(vector), startElement)
}


class DoubleAppendingVector(addr: BinaryRegion.NativePointer, maxBytes: Int, val dispose: () => Unit)
extends PrimitiveAppendableVector[Double](addr, maxBytes, 64, true) {
  final def addNA(): AddResponse = addData(0.0)
  final def addData(data: Double): AddResponse = checkOffset() match {
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
    for { index <- 0 until length optimized } {
      val data = apply(index)
      if (data < min) min = data
      if (data > max) max = data
    }
    (min, max)
  }

  private final val readVect = DoubleVector(addr)
  final def apply(index: Int): Double = readVect.apply(addr, index)
  final def reader: VectorDataReader = DoubleVectorDataReader64
  final def copyToBuffer: Buffer[Double] = DoubleVectorDataReader64.toBuffer(addr)

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr =
    DoubleVector.optimize(memFactory, this)
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
  def copyToBuffer: Buffer[Double] = MaskedDoubleDataReader.toBuffer(addr)

  final def minMax: (Double, Double) = {
    var min = Double.MaxValue
    var max = Double.MinValue
    for { index <- 0 until length optimized } {
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
    for { index <- 0 until length optimized } {
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

  override def length(vector: BinaryVectorPtr): Int = LongBinaryVector(vector).length(vector)
  final def apply(vector: BinaryVectorPtr, n: Int): Double =
    LongBinaryVector(vector)(vector, n).toDouble
  final def sum(vector: BinaryVectorPtr, start: Int, end: Int, ignoreNaN: Boolean = true): Double =
    LongBinaryVector(vector).sum(vector, start, end)   // Long vectors cannot contain NaN, ignore it
  final def count(vector: BinaryVectorPtr, start: Int, end: Int): Int = end - start + 1
  final def iterate(vector: BinaryVectorPtr, startElement: Int = 0): DoubleIterator =
    new DoubleLongWrapIterator(LongBinaryVector(vector).iterate(vector, startElement))
}
