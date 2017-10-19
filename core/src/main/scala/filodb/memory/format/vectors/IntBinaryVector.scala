package filodb.memory.format.vectors

import java.nio.ByteBuffer

import filodb.memory.format._

import scalaxy.loops._

object IntBinaryVector {
  /**
   * Creates a new MaskedIntAppendingVector, allocating a byte array of the right size for the max #
   * of elements.
   * @param maxElements initial maximum number of elements this vector will hold. Will automatically grow.
   * @param offheap if true, allocate the space for the vector off heap.  User will have to dispose.
   */
  def appendingVector(maxElements: Int,
                      nbits: Short = 32,
                      signed: Boolean = true,
                      offheap: Boolean = true): BinaryAppendableVector[Int] = {
    val bytesRequired = 4 + BitmapMask.numBytesRequired(maxElements) + noNAsize(maxElements, nbits)
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(bytesRequired, offheap)
    GrowableVector(new MaskedIntAppendingVector(base, off, nBytes, maxElements, nbits, signed))
  }

  /**
   * Returns the number of bytes required for a NoNA appending vector of given max length and nbits
   * This accounts for when nbits < 8 and we need extra byte
   */
  def noNAsize(maxElements: Int, nbits: Short): Int =
    4 + ((maxElements * nbits + Math.max(8 - nbits, 0)) / 8)

  /**
   * Same as appendingVector but uses a SimpleAppendingVector with no ability to hold NA mask
   */
  def appendingVectorNoNA(maxElements: Int,
                          nbits: Short = 32,
                          signed: Boolean = true,
                          offheap: Boolean = false): IntAppendingVector = {
    val bytesRequired = noNAsize(maxElements, nbits)
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(bytesRequired, offheap)
    appendingVectorNoNA(base, off, nBytes, nbits, signed)
  }

  def appendingVectorNoNA(base: Any,
                          offset: Long,
                          maxBytes: Int,
                          nbits: Short,
                          signed: Boolean): IntAppendingVector = nbits match {
    case 32 => new IntAppendingVector(base, offset, maxBytes, nbits, signed) {
      final def addData(v: Int): Unit = {
        checkOffset()
        UnsafeUtils.setInt(base, offset + numBytes, v)
        writeOffset += 4
      }
    }
    case 16 => new IntAppendingVector(base, offset, maxBytes, nbits, signed) {
      final def addData(v: Int): Unit = {
        checkOffset()
        UnsafeUtils.setShort(base, offset + numBytes, v.toShort)
        writeOffset += 2
      }
    }
    case 8 => new IntAppendingVector(base, offset, maxBytes, nbits, signed) {
      final def addData(v: Int): Unit = {
        checkOffset()
        UnsafeUtils.setByte(base, offset + numBytes, v.toByte)
        writeOffset += 1
      }
    }
    case 4 => new IntAppendingVector(base, offset, maxBytes, nbits, signed) {
      final def addData(v: Int): Unit = {
        checkOffset()
        val origByte = UnsafeUtils.getByte(base, writeOffset)
        val newByte = (origByte | (v << bitShift)).toByte
        UnsafeUtils.setByte(base, writeOffset, newByte)
        bumpBitShift()
      }
    }
    case 2 => new IntAppendingVector(base, offset, maxBytes, nbits, signed) {
      final def addData(v: Int): Unit = {
        checkOffset()
        val origByte = UnsafeUtils.getByte(base, writeOffset)
        val newByte = (origByte | (v << bitShift)).toByte
        UnsafeUtils.setByte(base, writeOffset, newByte)
        bumpBitShift()
      }
    }
  }

  /**
   * Creates a BinaryVector[Int] with no NAMask
   */
  def apply(base: Any, offset: Long, numBytes: Int): BinaryVector[Int] = {
    val nbits = UnsafeUtils.getShort(base, offset)
    // offset+2: nonzero = signed integral vector
    if (UnsafeUtils.getByte(base, offset + 2) != 0) {
      nbits match {
        case 32 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = UnsafeUtils.getInt(base, bufOffset + index * 4)
        }
        case 16 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = UnsafeUtils.getShort(base, bufOffset + index * 2).toInt
        }
        case 8 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = UnsafeUtils.getByte(base, bufOffset + index).toInt
        }
      }
    } else {
      nbits match {
        case 32 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = UnsafeUtils.getInt(base, bufOffset + index * 4)
        }
        case 16 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = (UnsafeUtils.getShort(base, bufOffset + index * 2) & 0x0ffff).toInt
        }
        case 8 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = (UnsafeUtils.getByte(base, bufOffset + index) & 0x00ff).toInt
        }
        case 4 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int =
            (UnsafeUtils.getByte(base, bufOffset + index/2) >> ((index & 0x01) * 4)).toInt & 0x0f
        }
        case 2 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int =
            (UnsafeUtils.getByte(base, bufOffset + index/4) >> ((index & 0x03) * 2)).toInt & 0x03
        }
      }
    }
  }

  def apply(buffer: ByteBuffer): BinaryVector[Int] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    apply(base, off, len)
  }

  def masked(buffer: ByteBuffer): MaskedIntBinaryVector = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new MaskedIntBinaryVector(base, off, len)
  }

  def const(buffer: ByteBuffer): BinaryVector[Int] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new IntConstVector(base, off, len)
  }

  /**
   * Given the min and max values in an IntVector, determines the most optimal (smallest)
   * nbits and the signed flag to use.  Typically used in a workflow where you use
   * `IntBinaryVector.appendingVector` first, then further optimize to the smallest IntVector
   * available.
   */
  def minMaxToNbitsSigned(min: Int, max: Int): (Short, Boolean) = {
    if (min >= 0 && max < 4) {
      (2, false)
    } else if (min >= 0 && max < 16) {
      (4, false)
    } else if (min >= Byte.MinValue && max <= Byte.MaxValue) {
      (8, true)
    } else if (min >= 0 && max < 256) {
      (8, false)
    } else if (min >= Short.MinValue && max <= Short.MaxValue) {
      (16, true)
    } else if (min >= 0 && max < 65536) {
      (16, false)
    } else {
      (32, true)
    }
  }

  /**
   * Produces a smaller BinaryVector if possible given combination of minimal nbits as well as
   * if all values are not NA.
   * The output is a frozen BinaryVector with optimized nbits and without mask if appropriate.
   */
  def optimize(vector: MaskedIntAppending): BinaryVector[Int] = {
    // Get nbits and signed
    val (min, max) = vector.minMax
    val (nbits, signed) = minMaxToNbitsSigned(min, max)

    if (vector.noNAs) {
      if (min == max) {
        (new IntConstAppendingVect(vector(0), vector.length)).optimize()
      // No NAs?  Use just the PrimitiveAppendableVector
      } else if (nbits == vector.nbits) { vector.dataVect }
      else {
        val newVect = IntBinaryVector.appendingVectorNoNA(vector.length, nbits, signed, vector.isOffheap)
        newVect.addVector(vector)
        newVect.freeze(copy = false)  // we're already creating a new copy
      }
    } else {
      // Some NAs and same number of bits?  Just keep NA mask
      if (nbits == vector.nbits) { vector.getVect }
      // Some NAs and different number of bits?  Create new vector and copy data over
      else {
        val newVect = IntBinaryVector.appendingVector(vector.length, nbits, signed, vector.isOffheap)
        newVect.addVector(vector)
        newVect.freeze(copy = false)
      }
    }
  }
}

abstract class IntBinaryVector(val base: Any,
                               val offset: Long,
                               val numBytes: Int,
                               nbits: Short) extends PrimitiveVector[Int] {
  override val vectSubType = WireFormat.SUBTYPE_INT_NOMASK

  final val bufOffset = offset + 4
  private final val bitShift = UnsafeUtils.getByte(base, offset + 3) & 0x07
  // This length method works assuming nbits is divisible into 32
  override val length: Int = ((numBytes - 4) * 8 + (if (bitShift != 0) bitShift - 8 else 0)) / nbits
  final def isAvailable(index: Int): Boolean = true
}

class MaskedIntBinaryVector(val base: Any, val offset: Long, val numBytes: Int) extends
PrimitiveMaskVector[Int] {
  override val vectSubType = WireFormat.SUBTYPE_INT

  // First four bytes: offset to SimpleIntBinaryVector
  val bitmapOffset = offset + 4L
  val intVectOffset = UnsafeUtils.getInt(base, offset)
  private val intVect = IntBinaryVector(base, offset + intVectOffset, numBytes - intVectOffset)

  override final def length: Int = intVect.length
  final def apply(index: Int): Int = intVect.apply(index)
}

abstract class IntAppendingVector(base: Any,
                                  offset: Long,
                                  maxBytes: Int,
                                  nbits: Short,
                                  signed: Boolean)
extends PrimitiveAppendableVector[Int](base, offset, maxBytes, nbits, signed) {
  override val vectSubType = WireFormat.SUBTYPE_INT_NOMASK

  final def addNA(): Unit = addData(0)
  private final val readVect = IntBinaryVector(base, offset, maxBytes)
  final def apply(index: Int): Int = readVect.apply(index)

  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Int] =
    IntBinaryVector(newBase, newOff, numBytes)
}

trait MaskedIntAppending extends BinaryAppendableVector[Int] {
  def minMax: (Int, Int)
  def nbits: Short
  def dataVect: BinaryVector[Int]
  def getVect: BinaryVector[Int] = freeze()
}

import filodb.memory.format.Encodings._

class MaskedIntAppendingVector(base: Any,
                               val offset: Long,
                               val maxBytes: Int,
                               maxElements: Int,
                               val nbits: Short,
                               signed: Boolean) extends
// First four bytes: offset to SimpleIntBinaryVector
BitmapMaskAppendableVector[Int](base, offset + 4L, maxElements) with MaskedIntAppending {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_INT

  val subVect = IntBinaryVector.appendingVectorNoNA(base, offset + subVectOffset,
                                                    maxBytes - subVectOffset,
                                                    nbits, signed)

  def dataVect: BinaryVector[Int] = subVect.freeze()

  final def minMax: (Int, Int) = {
    var min = Int.MaxValue
    var max = Int.MinValue
    for { index <- 0 until length optimized } {
      if (isAvailable(index)) {
        val data = subVect.apply(index)
        if (data < min) min = data
        if (data > max) max = data
      }
    }
    (min, max)
  }

  override def optimize(hint: EncodingHint = AutoDetect): BinaryVector[Int] =
    IntBinaryVector.optimize(this)

  override def newInstance(growFactor: Int = 2): BinaryAppendableVector[Int] = {
    val (newbase, newoff, nBytes) = BinaryVector.reAlloc(base, maxBytes * growFactor)
    new MaskedIntAppendingVector(newbase, newoff, maxBytes * growFactor, maxElements * growFactor,
                                 nbits, signed)
  }

  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Int] = {
    // Don't forget to write the new subVectOffset
    UnsafeUtils.setInt(newBase, newOff, (bitmapOffset + bitmapBytes - offset).toInt)
    new MaskedIntBinaryVector(newBase, newOff, 4 + bitmapBytes + subVect.numBytes)
  }
}

class IntConstVector(base: Any, offset: Long, numBytes: Int) extends
ConstVector[Int](base, offset, numBytes) {
  def apply(i: Int): Int = UnsafeUtils.getInt(base, dataOffset)
}

class IntConstAppendingVect(value: Int, initLen: Int = 0) extends
ConstAppendingVector(value, 4, initLen) {
  def fillBytes(base: Any, offset: Long): Unit = UnsafeUtils.setInt(base, offset, value)
  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Int] =
    new IntConstVector(newBase, newOff, numBytes)
}