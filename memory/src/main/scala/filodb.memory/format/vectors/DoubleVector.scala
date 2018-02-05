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
    val vect = appendingVector(memFactory, data.length)
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

  def fromIntBuf(buf: ByteBuffer): BinaryVector[Double] =
    new DoubleIntWrapper(IntBinaryVector(buf))
  def fromMaskedIntBuf(buf: ByteBuffer): BinaryVector[Double] =
    new DoubleIntWrapper(IntBinaryVector.masked(buf))

  /**
   * Produces a smaller BinaryVector if possible given combination of minimal nbits as well as
   * if all values are not NA.
   * Here are the things tried:
   *  1. If min and max are the same, then a DoubleConstVector is produced.
   *  2. If all values are integral, then IntBinaryVector is produced (and integer optimization done)
   *  3. If all values are filled (no NAs) then the bitmask is dropped
   */
  def optimize(memFactory: MemFactory, vector: MaskedDoubleAppendingVector): BinaryVector[Double] = {
    val intWrapper = new IntDoubleWrapper(vector)

    if (intWrapper.binConstVector) {
      (new DoubleConstAppendingVect(vector.dispose, vector(0), vector.length)).optimize(memFactory)
    // Check if all integrals. use the wrapper to avoid an extra pass
    } else if (intWrapper.allIntegrals) {
      new DoubleIntWrapper(IntBinaryVector.optimize(memFactory, intWrapper))
    } else if (vector.noNAs) {
      vector.subVect.freeze(memFactory)
    } else {
      vector.freeze(memFactory)
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
  final def addNA(): Unit = addData(0.0)
  final def addData(data: Double): Unit = {
    checkOffset()
    UnsafeUtils.setDouble(base, writeOffset, data)
    writeOffset += 8
  }

  private final val readVect = new DoubleBinaryVector(base, offset, maxBytes, dispose)
  final def apply(index: Int): Double = readVect.apply(index)

  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Double] =
    new DoubleBinaryVector(newBase, newOff, numBytes, dispose)
}

class MaskedDoubleAppendingVector(base: Any,
                                  val offset: Long,
                                  val maxBytes: Int,
                                  val maxElements: Int,
                                  val dispose: () => Unit) extends
// First four bytes: offset to DoubleBinaryVector
BitmapMaskAppendableVector[Double](base, offset + 4L, maxElements) {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE

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
 * A wrapper around MaskedDoubleAppendingVector that returns Ints.  Designed to feed into IntVector
 * optimizer so that an optimized int representation of double vector can be produced in one pass without
 * appending to another Int based AppendingVector first.
 * If it turns out the optimizer needs the original 32-bit vector, then it calls dataVect / getVect.
 */
private[vectors] class IntDoubleWrapper(val inner: MaskedDoubleAppendingVector) extends MaskedIntAppending
with AppendableVectorWrapper[Int, Double] {
  val (min, max) = inner.minMax
  def minMax: (Int, Int) = (min.toInt, max.toInt)
  val nbits: Short = 64

  final def nonIntegrals: Int = {
    var nonInts = 0
    for { index <- 0 until length optimized } {
      if (inner.isAvailable(index)) {
        val data = inner.subVect.apply(index)
        if (Math.rint(data) != data) nonInts += 1
      }
    }
    nonInts
  }

  val allIntegrals: Boolean =
    (nonIntegrals == 0) && min >= Int.MinValue.toDouble && max <= Int.MaxValue.toDouble

  val binConstVector = (min == max) && inner.noNAs

  final def addData(value: Int): Unit = inner.addData(value.toDouble)
  final def apply(index: Int): Int = inner(index).toInt

  def dataVect(memFactory: MemFactory): BinaryVector[Int] = {
    val vect = IntBinaryVector.appendingVectorNoNA(memFactory, inner.length)
    for { index <- 0 until length optimized } {
      vect.addData(inner(index).toInt)
    }
    vect.freeze(None)
  }

  override def getVect(memFactory: MemFactory): BinaryVector[Int] = {
    val vect = IntBinaryVector.appendingVector(memFactory, inner.length)
    for { index <- 0 until length optimized } {
      if (inner.isAvailable(index)) vect.addData(inner(index).toInt) else vect.addNA()
    }
    vect.freeze(None)
  }
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
 * A wrapper to return Doubles from an Int vector... for when one can compress Double vectors as IntVectors
 */
class DoubleIntWrapper(inner: BinaryVector[Int]) extends PrimitiveVector[Double] {
  val base = inner.base
  val offset = inner.offset
  val numBytes = inner.numBytes
  override val vectSubType = inner.vectSubType

  final def apply(i: Int): Double = inner(i).toDouble
  final def isAvailable(i: Int): Boolean = inner.isAvailable(i)
  override final def length: Int = inner.length
  override val maybeNAs = inner.maybeNAs
  override def dispose: () => Unit = inner.dispose
}
