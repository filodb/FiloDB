package filodb.memory.format.vectors

import java.nio.ByteBuffer

import scalaxy.loops._

import filodb.memory.MemFactory
import filodb.memory.format._
import filodb.memory.format.Encodings._


object DoubleVector {
  /**
   * Creates a new MaskedDoubleAppendingVector, allocating a byte array of the right size for the max #
   * of elements plus a bit mask.
   * @param maxElements initial maximum number of elements this vector will hold. Will automatically grow.
   * @param offheap if true, allocate the space for the vector off heap.  User will have to dispose.
   */
  def appendingVector(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Double] = {
    val bytesRequired = 8 + BitmapMask.numBytesRequired(maxElements) + 8 * maxElements
    val (base, off, nBytes) = memFactory.allocateWithMagicHeader(bytesRequired)
    val dispose = () => memFactory.freeMemory(off)
    GrowableVector(memFactory,new MaskedDoubleAppendingVector(base, off, nBytes, maxElements, dispose))
  }

  /**
   * Creates a DoubleAppendingVector - does not grow and does not have bit mask. All values are marked
   * as available.
   * @param offheap if true, allocate the space for the vector off heap.  User will have to dispose.
   */
  def appendingVectorNoNA(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Double] = {
    val bytesRequired = 4 + 8 * maxElements
    val (base, off, nBytes) = memFactory.allocateWithMagicHeader(bytesRequired)
    val dispose = () => memFactory.freeMemory(off)
    new DoubleAppendingVector(base, off, nBytes, dispose)
  }

  /**
   * Quickly create a DoubleVector from a sequence of Doubles which can be optimized.
   */
  def apply(memFactory: MemFactory, data: Seq[Double]): BinaryAppendableVector[Double] = {
    val vect = appendingVectorNoNA(memFactory, data.length)
    data.foreach(vect.addData)
    vect
  }

  def apply(buffer: ByteBuffer): BinaryVector[Double] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    DoubleBinaryVector(base, off, len, BinaryVector.NoOpDispose)
  }

  def masked(buffer: ByteBuffer): MaskedDoubleBinaryVector = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new MaskedDoubleBinaryVector(base, off, len, BinaryVector.NoOpDispose)
  }

  def const(buffer: ByteBuffer): BinaryVector[Double] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new DoubleConstVector(base, off, len, BinaryVector.NoOpDispose)
  }

  def fromDDVBuf(buf: ByteBuffer): BinaryVector[Double] =
    new DoubleLongWrapper(DeltaDeltaVector(buf))
  def fromConstDDVBuf(buf: ByteBuffer): BinaryVector[Double] =
    new DoubleLongWrapper(DeltaDeltaVector.const(buf))

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
  def optimize(memFactory: MemFactory, vector: OptimizingPrimitiveAppender[Double]): BinaryVector[Double] = {
    val longWrapper = new LongDoubleWrapper(vector)
    if (longWrapper.allIntegrals) {
      DeltaDeltaVector.fromLongVector(memFactory, longWrapper)
        .map(new DoubleLongWrapper(_))
        .getOrElse {
          if (vector.noNAs) vector.dataVect(memFactory) else vector.getVect(memFactory)
        }
    } else {
      if (vector.noNAs) vector.dataVect(memFactory) else vector.getVect(memFactory)
    }
  }
}

final case class DoubleBinaryVector(base: Any,
                                    offset: Long,
                                    numBytes: Int,
                                    val dispose: () => Unit) extends PrimitiveVector[Double] {
  override val length: Int = (numBytes - 4) / 8
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Double = UnsafeUtils.getDouble(base, offset + 4 + index * 8)
}

class MaskedDoubleBinaryVector(val base: Any,
                               val offset: Long,
                               val numBytes: Int,
                               val dispose: () => Unit) extends PrimitiveMaskVector[Double] {
  val bitmapOffset = offset + 4L
  val subVectOffset = UnsafeUtils.getInt(base, offset)
  private val dblVect = DoubleBinaryVector(base, offset + subVectOffset, numBytes - subVectOffset, dispose)

  override final def length: Int = dblVect.length
  final def apply(index: Int): Double = dblVect.apply(index)
}

class DoubleAppendingVector(base: Any, offset: Long, maxBytes: Int, val dispose: () => Unit)
extends PrimitiveAppendableVector[Double](base, offset, maxBytes, 64, true) {
  final def addNA(): AddResponse = addData(0.0)
  final def addData(data: Double): AddResponse = checkOffset() match {
    case Ack =>
      UnsafeUtils.setDouble(base, writeOffset, data)
      writeOffset += 8
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

  private final val readVect = new DoubleBinaryVector(base, offset, maxBytes, dispose)
  final def apply(index: Int): Double = readVect.apply(index)

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVector[Double] =
    DoubleVector.optimize(memFactory, this)

  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Double] =
    new DoubleBinaryVector(newBase, newOff, numBytes, dispose)
}

class MaskedDoubleAppendingVector(base: Any,
                                  val offset: Long,
                                  val maxBytes: Int,
                                  val maxElements: Int,
                                  val dispose: () => Unit) extends
// First four bytes: offset to DoubleBinaryVector
BitmapMaskAppendableVector[Double](base, offset + 4L, maxElements) with OptimizingPrimitiveAppender[Double] {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE
  val nbits: Short = 64

  val subVect = new DoubleAppendingVector(base, offset + subVectOffset, maxBytes - subVectOffset, dispose)

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

  final def dataVect(memFactory: MemFactory): BinaryVector[Double] = subVect.freeze(memFactory)

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVector[Double] =
    DoubleVector.optimize(memFactory, this)

  override def newInstance(memFactory: MemFactory, growFactor: Int = 2): BinaryAppendableVector[Double] = {
    val (newbase, newoff, nBytes) = memFactory.allocateWithMagicHeader(maxBytes * growFactor)
    val dispose = () => memFactory.freeMemory(newoff)
    new MaskedDoubleAppendingVector(newbase, newoff, maxBytes * growFactor, maxElements * growFactor, dispose)
  }

  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Double] = {
    // Don't forget to write the new subVectOffset
    UnsafeUtils.setInt(newBase, newOff, (bitmapOffset + bitmapBytes - offset).toInt)
    new MaskedDoubleBinaryVector(newBase, newOff, 4 + bitmapBytes + subVect.numBytes, dispose)
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

class DoubleConstVector(base: Any, offset: Long, numBytes: Int, val dispose: () => Unit) extends
ConstVector[Double](base, offset, numBytes) {
  private final val const = UnsafeUtils.getDouble(base, dataOffset)
  final def apply(i: Int): Double = const
}

class DoubleConstAppendingVect(val dispose: () => Unit, value: Double, initLen: Int = 0) extends
ConstAppendingVector(value, 8, initLen) {
  def fillBytes(base: Any, offset: Long): Unit = UnsafeUtils.setDouble(base, offset, value)
  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Double] =
    new DoubleConstVector(newBase, newOff, numBytes, dispose)
}

/**
 * A wrapper to return Doubles from a Long vector... for when one can compress Double vectors as DDVs
 */
class DoubleLongWrapper(inner: BinaryVector[Long]) extends PrimitiveVector[Double] {
  val base = inner.base
  val offset = inner.offset
  val numBytes = inner.numBytes
  override val vectMajorType = inner.vectMajorType
  override val vectSubType = inner.vectSubType

  final def apply(i: Int): Double = inner(i).toDouble
  final def isAvailable(i: Int): Boolean = inner.isAvailable(i)
  override final def length: Int = inner.length
  override val maybeNAs = inner.maybeNAs
  override def dispose: () => Unit = inner.dispose
}
