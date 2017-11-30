package filodb.memory.format.vectors

import java.nio.ByteBuffer

import scalaxy.loops._

import filodb.memory.MemFactory
import filodb.memory.format._
import filodb.memory.format.Encodings._

object LongBinaryVector {
  /**
   * Creates a new MaskedLongAppendingVector, allocating a byte array of the right size for the max #
   * of elements plus a bit mask.
   * @param maxElements initial maximum number of elements this vector will hold. Will automatically grow.
   */
  def appendingVector(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Long] = {
    val bytesRequired = 8 + BitmapMask.numBytesRequired(maxElements) + 8 * maxElements
    val (base, off, nBytes) = memFactory.allocateWithMagicHeader(bytesRequired)
    val dispose =  () => memFactory.freeMemory(off)
    GrowableVector(memFactory, new MaskedLongAppendingVector(base, off, nBytes, maxElements, dispose))
  }

  /**
   * Creates a LongAppendingVector - does not grow and does not have bit mask. All values are marked
   * as available.
   */
  def appendingVectorNoNA(memFactory: MemFactory, maxElements: Int): BinaryAppendableVector[Long] = {
    val bytesRequired = 4 + 8 * maxElements
    val (base, off, nBytes) = memFactory.allocateWithMagicHeader(bytesRequired)
    val dispose =  () => memFactory.freeMemory(off)
    new LongAppendingVector(base, off, nBytes, dispose)
  }


  def apply(buffer: ByteBuffer): BinaryVector[Long] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    LongBinaryVector(base, off, len, BinaryVector.NoOpDispose)
  }

  def masked(buffer: ByteBuffer): MaskedLongBinaryVector = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new MaskedLongBinaryVector(base, off, len, BinaryVector.NoOpDispose)
  }

  def const(buffer: ByteBuffer): BinaryVector[Long] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new LongConstVector(base, off, len, BinaryVector.NoOpDispose)
  }

  def fromIntBuf(buf: ByteBuffer): BinaryVector[Long] =
    new LongIntWrapper(IntBinaryVector(buf))

  def fromMaskedIntBuf(buf: ByteBuffer): BinaryVector[Long] =
    new LongIntWrapper(IntBinaryVector.masked(buf))

  /**
   * Produces a smaller, frozen BinaryVector if possible given combination of minimal nbits as well as
   * if all values are not NA.
   * Here are the things tried:
   *  1. If min and max are the same, then a LongConstVector is produced.
   *  2. If delta-delta encoding can be used (for timestamps, counters) do that.
   *  3. Check the nbits, if it can fit in smaller # of bits do that, possibly creating int vector
   *  4. If all values are filled (no NAs) then the bitmask is dropped
   */
  def optimize(memFactory: MemFactory, vector: MaskedLongAppendingVector): BinaryVector[Long] = {
    val intWrapper = new IntLongWrapper(vector)

    if (intWrapper.binConstVector) {
      (new LongConstAppendingVect(vector.dispose, vector(0), vector.length)).optimize(memFactory)
    } else {
      // Try delta-delta encoding
      DeltaDeltaVector.fromLongVector(MemFactory.onHeapFactory, vector, intWrapper.min, intWrapper.max)
                      .map(_.optimize(memFactory, Encodings.AutoDetectDispose))
                      .getOrElse(optimize2(memFactory, intWrapper, vector))
    }
  }

  private def optimize2(memFactory: MemFactory, intWrapper: IntLongWrapper,
                        vector: MaskedLongAppendingVector): BinaryVector[Long] = {
    // Check if all integrals. use the wrapper to avoid an extra pass
    if (intWrapper.fitInInt) {
      new LongIntWrapper(IntBinaryVector.optimize(memFactory, intWrapper))
    } else if (vector.noNAs) {
      vector.subVect.freeze(memFactory)
    } else {
      vector.freeze(memFactory)
    }
  }
}

final case class LongBinaryVector(base: Any, offset: Long, numBytes: Int, val dispose: () => Unit)
  extends PrimitiveVector[Long] {
  override val length: Int = (numBytes - 4) / 8
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Long = UnsafeUtils.getLong(base, offset + 4 + index * 8)
}

class MaskedLongBinaryVector(val base: Any, val offset: Long, val numBytes: Int, val dispose: () => Unit) extends
PrimitiveMaskVector[Long] {
  val bitmapOffset = offset + 4L
  val subVectOffset = UnsafeUtils.getInt(base, offset)
  private val longVect = LongBinaryVector(base, offset + subVectOffset, numBytes - subVectOffset, dispose)

  override final def length: Int = longVect.length
  final def apply(index: Int): Long = longVect.apply(index)
}

class LongAppendingVector(base: Any, offset: Long, maxBytes: Int, val dispose: () => Unit)
extends PrimitiveAppendableVector[Long](base, offset, maxBytes, 64, true) {
  final def addNA(): Unit = addData(0L)
  final def addData(data: Long): Unit = {
    checkOffset()
    UnsafeUtils.setLong(base, writeOffset, data)
    writeOffset += 8
  }

  private final val readVect = new LongBinaryVector(base, offset, maxBytes, dispose)
  final def apply(index: Int): Long = readVect.apply(index)

  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Long] =
    new LongBinaryVector(newBase, newOff, numBytes, dispose)
}

class MaskedLongAppendingVector(base: Any,
                                val offset: Long,
                                val maxBytes: Int,
                                val maxElements: Int,
                                val dispose: () => Unit) extends
// First four bytes: offset to LongBinaryVector
BitmapMaskAppendableVector[Long](base, offset + 4L, maxElements) {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE

  val subVect = new LongAppendingVector(base, offset + subVectOffset, maxBytes - subVectOffset, dispose)

  final def minMax: (Long, Long) = {
    var min = Long.MaxValue
    var max = Long.MinValue
    for { index <- 0 until length optimized } {
      if (isAvailable(index)) {
        val data = subVect.apply(index)
        if (data < min) min = data
        if (data > max) max = data
      }
    }
    (min, max)
  }

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVector[Long] =
    LongBinaryVector.optimize(memFactory, this)

  override def newInstance(memFactory: MemFactory, growFactor: Int = 2): BinaryAppendableVector[Long] = {
    val (newbase, newoff, nBytes) = memFactory.allocateWithMagicHeader(maxBytes * growFactor)
    val dispose = () => memFactory.freeMemory(newoff)
    new MaskedLongAppendingVector(newbase, newoff, maxBytes * growFactor, maxElements * growFactor, dispose)
  }

  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Long] = {
    // Don't forget to write the new subVectOffset
    UnsafeUtils.setInt(newBase, newOff, (bitmapOffset + bitmapBytes - offset).toInt)
    new MaskedLongBinaryVector(newBase, newOff, 4 + bitmapBytes + subVect.numBytes, dispose)
  }
}

/**
 * A wrapper around MaskedLongAppendingVector that returns Ints.  Designed to feed into IntVector
 * optimizer so that an optimized int representation of Long vector can be produced in one pass without
 * appending to another Int based AppendingVector first.
 * If it turns out the optimizer needs the original 32-bit vector, then it calls dataVect / getVect.
 */
private[vectors] class IntLongWrapper(val inner: MaskedLongAppendingVector) extends MaskedIntAppending
with AppendableVectorWrapper[Int, Long] {
  val (min, max) = inner.minMax
  def minMax: (Int, Int) = (min.toInt, max.toInt)
  val nbits: Short = 64

  val fitInInt = min >= Int.MinValue.toLong && max <= Int.MaxValue.toLong

  val binConstVector = (min == max) && inner.noNAs

  final def addData(value: Int): Unit = inner.addData(value.toLong)
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

class LongConstVector(base: Any, offset: Long, numBytes: Int, val dispose: () => Unit) extends
ConstVector[Long](base, offset, numBytes) {
  private final val const = UnsafeUtils.getLong(base, dataOffset)
  final def apply(i: Int): Long = const
}

class LongConstAppendingVect(val dispose: () => Unit, value: Long, initLen: Int = 0) extends
ConstAppendingVector(value, 8, initLen) {
  def fillBytes(base: Any, offset: Long): Unit = UnsafeUtils.setLong(base, offset, value)
  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Long] =
    new LongConstVector(newBase, newOff, numBytes, dispose)
}

/**
 * A wrapper to return Longs from an Int vector... for when one can compress Long vectors as IntVectors
 */
class LongIntWrapper(inner: BinaryVector[Int]) extends PrimitiveVector[Long] {
  val base = inner.base
  val offset = inner.offset
  val numBytes = inner.numBytes
  override val vectSubType = inner.vectSubType

  final def apply(i: Int): Long = inner(i).toLong
  final def isAvailable(i: Int): Boolean = inner.isAvailable(i)
  override final def length: Int = inner.length
  override val maybeNAs = inner.maybeNAs
  override def dispose: () => Unit = inner.dispose
}
