package filodb.memory.format.vectors

import java.nio.ByteBuffer

import debox.Buffer
import spire.syntax.cfor._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.Encodings._
import filodb.memory.format.MemoryReader._
import filodb.memory.format.UnsafeUtils.ZeroPointer

object IntBinaryVector {
  /**
   * Creates a new MaskedIntAppendingVector, allocating a byte array of the right size for the max #
   * of elements.
   * @param maxElements initial maximum number of elements this vector will hold. Will automatically grow.
   */
  def appendingVector(memFactory: MemFactory,
                      maxElements: Int,
                      nbits: Short = 32,
                      signed: Boolean = true): BinaryAppendableVector[Int] = {
    val bytesRequired = 12 + BitmapMask.numBytesRequired(maxElements) + noNAsize(maxElements, nbits)
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose = () => memFactory.freeMemory(addr)
    GrowableVector(memFactory, new MaskedIntAppendingVector(addr, bytesRequired, maxElements, nbits, signed, dispose))
  }

  /**
   * Returns the number of bytes required for a NoNA appending vector of given max length and nbits
   * This accounts for when nbits < 8 and we need extra byte
   */
  def noNAsize(maxElements: Int, nbits: Short): Int =
    8 + ((maxElements * nbits + Math.max(8 - nbits, 0)) / 8)

  /**
   * Same as appendingVector but uses a SimpleAppendingVector with no ability to hold NA mask
   */
  def appendingVectorNoNA(memFactory: MemFactory,
                          maxElements: Int,
                          nbits: Short = 32,
                          signed: Boolean = true): IntAppendingVector = {
    val bytesRequired = noNAsize(maxElements, nbits)
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose = () => memFactory.freeMemory(addr)
    appendingVectorNoNA(addr, bytesRequired, nbits, signed, dispose)
  }

  // scalastyle:off method.length
  def appendingVectorNoNA(addr: BinaryRegion.NativePointer,
                          maxBytes: Int,
                          nbits: Short,
                          signed: Boolean,
                          dispose: () => Unit): IntAppendingVector = nbits match {
    case 32 => new IntAppendingVector(addr, maxBytes, nbits, signed, dispose) {
      final def addData(v: Int): AddResponse = checkOffset() match {
        case Ack =>
          UnsafeUtils.setInt(addr + numBytes, v)
          incWriteOffset(4)
          Ack
        case other: AddResponse => other
      }
    }
    case 16 => new IntAppendingVector(addr, maxBytes, nbits, signed, dispose) {
      final def addData(v: Int): AddResponse = checkOffset() match {
        case Ack =>
          UnsafeUtils.setShort(addr + numBytes, v.toShort)
          incWriteOffset(2)
          Ack
        case other: AddResponse => other
      }
    }
    case 8 => new IntAppendingVector(addr, maxBytes, nbits, signed, dispose) {
      final def addData(v: Int): AddResponse = checkOffset() match {
        case Ack =>
          UnsafeUtils.setByte(addr + numBytes, v.toByte)
          incWriteOffset(1)
          Ack
        case other: AddResponse => other
      }
    }
    case 4 => new IntAppendingVector(addr, maxBytes, nbits, signed, dispose) {
      final def addData(v: Int): AddResponse = checkOffset() match {
        case Ack =>
          val origByte = if (bitShift == 0) 0 else nativePtrReader.getByte(writeOffset)
          val newByte = (origByte | (v << bitShift)).toByte
          UnsafeUtils.setByte(writeOffset, newByte)
          bumpBitShift()
          Ack
        case other: AddResponse => other
      }
    }
    case 2 => new IntAppendingVector(addr, maxBytes, nbits, signed, dispose) {
      final def addData(v: Int): AddResponse = checkOffset() match {
        case Ack =>
          val origByte = if (bitShift == 0) 0 else nativePtrReader.getByte(writeOffset)
          val newByte = (origByte | (v << bitShift)).toByte
          UnsafeUtils.setByte(writeOffset, newByte)
          bumpBitShift()
          Ack
        case other: AddResponse => other
      }
    }
  }

  /**
   * Quickly create an IntBinaryVector from a sequence of Ints which can be optimized.
   */
  def apply(memFactory: MemFactory, data: Seq[Int]): BinaryAppendableVector[Int] = {
    val vect = appendingVectorNoNA(memFactory, data.length)
    data.foreach(vect.addData)
    vect
  }

  /**
   * Returns an IntVectorDataReader object for a simple (no mask) Int BinaryVector
   */
  def simple(acc: MemoryReader, vector: BinaryVectorPtr): IntVectorDataReader = {
    // get nbits, etc and decide
    if (PrimitiveVectorReader.signed(acc, vector)) {
      PrimitiveVectorReader.nbits(acc, vector) match {
        case 32 => OffheapSignedIntVector32
        case 16 => OffheapSignedIntVector16
        case 8  => OffheapSignedIntVector8
      }
    } else {
      PrimitiveVectorReader.nbits(acc, vector) match {
        case 32 => OffheapSignedIntVector32
        case 16 => OffheapUnsignedIntVector16
        case 8  => OffheapUnsignedIntVector8
        case 4  => OffheapUnsignedIntVector4
        case 2  => OffheapUnsignedIntVector2
      }
    }
  }

  def apply(buffer: ByteBuffer): IntVectorDataReader = apply(MemoryReader.fromByteBuffer(buffer), 0)

  import WireFormat._

  /**
   * Parses the type of vector from the WireFormat word at address+4 and returns the appropriate
   * IntVectorDataReader object for parsing it
   */
  def apply(acc: MemoryReader, vector: BinaryVectorPtr): IntVectorDataReader = {
    BinaryVector.vectorType(acc, vector) match {
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_INT)        => MaskedIntBinaryVector
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_INT_NOMASK) => simple(acc, vector)
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_REPEATED)   => IntConstVector
    }
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
  def optimize(memFactory: MemFactory, vector: OptimizingPrimitiveAppender[Int]): BinaryVectorPtr = {
    // Get nbits and signed
    val (min, max) = vector.minMax
    val (nbits, signed) = minMaxToNbitsSigned(min, max)
    val dispose = () => vector.dispose()
    if (vector.noNAs) {
      if (min == max) {
        ConstVector.make(memFactory, vector.length, 4) { addr => UnsafeUtils.setInt(ZeroPointer, addr, vector(0)) }
      // No NAs?  Use just the PrimitiveAppendableVector
      } else if (nbits == vector.nbits) { vector.dataVect(memFactory) }
      else {
        val newVect = IntBinaryVector.appendingVectorNoNA(memFactory, vector.length, nbits, signed)
        newVect.addVector(vector)
        newVect.freeze(None)  // we're already creating a new copy
      }
    } else {
      // Some NAs and same number of bits?  Just keep NA mask
      if (nbits == vector.nbits) { vector.getVect(memFactory) }
      // Some NAs and different number of bits?  Create new vector and copy data over
      else {
        val newVect = IntBinaryVector.appendingVector(memFactory, vector.length, nbits, signed)
        newVect.addVector(vector)
        newVect.freeze(None)
      }
    }
  }
}

/**
 * An iterator optimized for speed and type-specific to avoid boxing.
 * It has no hasNext() method - because it is guaranteed to visit every element, and this way
 * you can avoid another method call for performance.
 */
trait IntIterator extends TypedIterator {
  def next: Int
}

/**
 * +0000   4-byte length word
 * +0004   2-byte WireFormat
 * +0006   2-byte Bitshift / signed / NBits  (for format see PrimitiveAppendableVector)
 * +0008   start of packed integer data
 */
trait IntVectorDataReader extends VectorDataReader {
  import PrimitiveVector.HeaderLen
  import PrimitiveVectorReader._

  // Iterator to go through bytes.  Put var in constructor for much faster access.
  class GenericIntIterator(acc: MemoryReader, vector: BinaryVectorPtr, var n: Int) extends IntIterator {
    final def next: Int = {
      val data = apply(acc, vector, n)
      n += 1
      data
    }
  }

  /**
   * Retrieves the element at position/row n, where n=0 is the first element of the vector.
   */
  def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Int

  /**
   * Returns the number of elements in this BinaryVector
   */
  def length(acc: MemoryReader, vector: BinaryVectorPtr): Int =
    ((numBytes(acc, vector) - HeaderLen) * 8 +
      (if (bitShift(acc, vector) != 0) bitShift(acc, vector) - 8 else 0)) / nbits(acc, vector)

  /**
   * Sums up the Int values in the vector from position start to position end.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param start the starting element # in the vector to sum, 0 == first element
   * @param end the ending element # in the vector to sum, inclusive
   * @return the Long sum, since Ints might possibly overflow
   */
  def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long

  private[memory] def defaultSum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long = {
    var rowNo = start
    var sum = 0L
    while (rowNo <= end) {
      sum += apply(acc, vector, rowNo)
      rowNo += 1
    }
    sum
  }

  /**
   * Returns an IntIterator to efficiently go through the elements of the vector.  The user is responsible for
   * knowing how many elements to process.  There is no hasNext.
   * All elements are iterated through, even those designated as "not available".
   * Costs an allocation for the iterator but allows potential performance gains too.
   * NOTE: the default one is not very efficient, it just calls apply() again and again.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param startElement the starting element # in the vector, by default 0 (the first one)
   */
  def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): IntIterator =
    new GenericIntIterator(acc, vector, startElement)

  def debugString(acc: MemoryReader, vector: BinaryVectorPtr, sep: String = ","): String = {
    val it = iterate(acc, vector)
    val size = length(acc, vector)
    (0 to size).map(_ => it.next).mkString(sep)
  }

  /**
   * Converts the BinaryVector to an unboxed Buffer.
   * Only returns elements that are "available".
   */
  def toBuffer(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): Buffer[Int] = {
    val newBuf = Buffer.empty[Int]
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

object OffheapSignedIntVector32 extends IntVectorDataReader {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Int =
    acc.getInt(vector + 8 + n * 4)
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is" +
      s"out of bounds, length=${length(acc, vector)}")
    var addr = vector + 8 + start * 4
    val untilAddr = vector + 8 + end * 4 + 4   // one past the end
    var sum: Long = 0L
    while (addr < untilAddr) {
      sum += acc.getInt(addr)
      addr += 4
    }
    sum
  }
}

object OffheapSignedIntVector16 extends IntVectorDataReader {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Int =
    acc.getShort(vector + 8 + n * 2).toInt
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) " +
      s"is out of bounds, length=${length(acc, vector)}")
    var addr = vector + 8 + start * 2
    val untilAddr = vector + 8 + end * 2 + 2   // one past the end
    var sum = 0L
    while (addr < untilAddr) {
      sum += acc.getShort(addr)
      addr += 2
    }
    sum
  }
}

object OffheapSignedIntVector8 extends IntVectorDataReader {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Int =
    acc.getByte(vector + 8 + n).toInt
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is out " +
      s"of bounds, length=${length(acc, vector)}")
    var addr = vector + 8 + start
    val untilAddr = vector + 8 + end + 1     // one past the end
    var sum = 0L
    while (addr < untilAddr) {
      sum += acc.getByte(addr)
      addr += 1
    }
    sum
  }
}

object OffheapUnsignedIntVector16 extends IntVectorDataReader {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Int =
    (acc.getShort(vector + 8 + n * 2) & 0x0ffff).toInt
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is out " +
      s"of bounds, length=${length(acc, vector)}")
    val startRoundedUp = (start + 3) & ~3
    var sum = defaultSum(acc, vector, start, Math.min(end, startRoundedUp - 1))
    if (startRoundedUp <= end) {
      var addr = vector + 8 + startRoundedUp * 2
      var rowNo = startRoundedUp
      while ((rowNo + 3) <= end) {
        val bytes = acc.getLong(addr)
        sum += ((bytes >> 0) & 0x0ffff) + ((bytes >> 16) & 0x0ffff) +
               ((bytes >> 32) & 0x0ffff) + ((bytes >> 48) & 0x0ffff)
        rowNo += 4  // 4 rows at a time
        addr += 8   // 8 bytes at a time
      }
      sum += defaultSum(acc, vector, rowNo, end)
    }
    sum
  }
}

object OffheapUnsignedIntVector8 extends IntVectorDataReader {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Int =
    (acc.getByte(vector + 8 + n) & 0x00ff).toInt
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is out of " +
      s"bounds, length=${length(acc, vector)}")
    val startRoundedUp = (start + 7) & ~7
    var sum = defaultSum(acc, vector, start, Math.min(end, startRoundedUp - 1))
    if (startRoundedUp <= end) {
      var addr = vector + 8 + startRoundedUp
      var rowNo = startRoundedUp
      while ((rowNo + 7) <= end) {
        val bytes = acc.getLong(addr)
        sum += ((bytes >> 0) & 0x0ff) + ((bytes >> 8) & 0x0ff) +
               ((bytes >> 16) & 0x0ff) + ((bytes >> 24) & 0x0ff) +
               ((bytes >> 32) & 0x0ff) + ((bytes >> 40) & 0x0ff) +
               ((bytes >> 48) & 0x0ff) + ((bytes >> 56) & 0x0ff)
        rowNo += 8  // 8 rows at a time
        addr += 8   // 8 bytes at a time
      }
      sum += defaultSum(acc, vector, rowNo, end)
    }
    sum
  }
}

object OffheapUnsignedIntVector4 extends IntVectorDataReader {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Int =
    (acc.getByte(vector + 8 + n/2) >> ((n & 0x01) * 4)).toInt & 0x0f
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) " +
      s"is out of bounds, length=${length(acc, vector)}")
    val startRoundedUp = (start + 7) & ~7
    var sum = defaultSum(acc, vector, start, Math.min(end, startRoundedUp - 1))
    if (startRoundedUp <= end) {
      var addr = vector + 8 + startRoundedUp/2
      var rowNo = startRoundedUp
      while ((rowNo + 7) <= end) {
        val bytes = acc.getInt(addr)
        sum += ((bytes >> 0) & 0x0f) + ((bytes >> 4) & 0x0f) +
               ((bytes >> 8) & 0x0f) + ((bytes >> 12) & 0x0f) +
               ((bytes >> 16) & 0x0f) + ((bytes >> 20) & 0x0f) +
               ((bytes >> 24) & 0x0f) + ((bytes >> 28) & 0x0f)
        rowNo += 8  // 8 rows at a time
        addr += 4   // 4 bytes at a time
      }
      sum += defaultSum(acc, vector, rowNo, end)
    }
    sum
  }
}

object OffheapUnsignedIntVector2 extends IntVectorDataReader {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Int =
    (acc.getByte(vector + 8 + n/4) >> ((n & 0x03) * 2)).toInt & 0x03
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is " +
      s"out of bounds, length=${length(acc, vector)}")
    val startRoundedUp = (start + 7) & ~7
    var sum = defaultSum(acc, vector, start, Math.min(end, startRoundedUp - 1))
    if (startRoundedUp <= end) {
      var addr = vector + 8 + startRoundedUp/4
      var rowNo = startRoundedUp
      while ((rowNo + 7) <= end) {
        val bytes = acc.getShort(addr).toInt
        sum += ((bytes >> 0) & 0x03) + ((bytes >> 2) & 0x03) +
               ((bytes >> 4) & 0x03) + ((bytes >> 6) & 0x03) +
               ((bytes >> 8) & 0x03) + ((bytes >> 10) & 0x03) +
               ((bytes >> 12) & 0x03) + ((bytes >> 14) & 0x03)
        rowNo += 8  // 8 rows at a time
        addr += 2   // 2 bytes at a time
      }
      sum += defaultSum(acc, vector, rowNo, end)
    }
    sum
  }
}


object MaskedIntBinaryVector extends IntVectorDataReader with BitmapMaskVector {
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Int = {
    val subvect = subvectAddr(acc, vector)
    IntBinaryVector.simple(acc, subvect).apply(acc, subvect, n)
  }

  override def length(acc: MemoryReader, vector: BinaryVectorPtr): Int = super.length(acc, subvectAddr(acc, vector))

  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long =
    IntBinaryVector.simple(acc, subvectAddr(acc, vector)).sum(acc, subvectAddr(acc, vector), start, end)

  override def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): IntIterator =
    IntBinaryVector.simple(acc, subvectAddr(acc, vector)).iterate(acc, subvectAddr(acc, vector), startElement)
}

abstract class IntAppendingVector(addr: BinaryRegion.NativePointer,
                                  maxBytes: Int,
                                  nbits: Short,
                                  signed: Boolean,
                                  val dispose: () => Unit)
extends PrimitiveAppendableVector[Int](addr, maxBytes, nbits, signed) {
  override def vectSubType: Int = WireFormat.SUBTYPE_INT_NOMASK

  final def addNA(): AddResponse = addData(0)
  final def apply(index: Int): Int = reader.apply(nativePtrReader, addr, index)
  val reader = IntBinaryVector.simple(nativePtrReader, addr)
  def copyToBuffer: Buffer[Int] = reader.asIntReader.toBuffer(nativePtrReader, addr)

  final def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = addData(reader.getInt(col))

  final def minMax: (Int, Int) = {
    var min = Int.MaxValue
    var max = Int.MinValue
    cforRange { 0 until length } { index =>
      val data = reader.apply(nativePtrReader, addr, index)
      if (data < min) min = data
      if (data > max) max = data
    }
    (min, max)
  }

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr =
    IntBinaryVector.optimize(memFactory, this)
}

class MaskedIntAppendingVector(addr: BinaryRegion.NativePointer,
                               val maxBytes: Int,
                               maxElements: Int,
                               val nbits: Short,
                               signed: Boolean,
                               val dispose: () => Unit) extends
// First four bytes: offset to SimpleIntBinaryVector
BitmapMaskAppendableVector[Int](addr, maxElements) with OptimizingPrimitiveAppender[Int] {
  def vectMajorType: Int = WireFormat.VECTORTYPE_BINSIMPLE
  def vectSubType: Int = WireFormat.SUBTYPE_INT

  val subVect = IntBinaryVector.appendingVectorNoNA(addr + subVectOffset,
                                                    maxBytes - subVectOffset,
                                                    nbits, signed, dispose)

  def dataVect(memFactory: MemFactory): BinaryVectorPtr = subVect.freeze(memFactory)
  def copyToBuffer: Buffer[Int] = MaskedIntBinaryVector.toBuffer(nativePtrReader, addr)

  final def minMax: (Int, Int) = {
    var min = Int.MaxValue
    var max = Int.MinValue
    cforRange { 0 until length } { index =>
      if (isAvailable(index)) {
        val data = subVect.apply(index)
        if (data < min) min = data
        if (data > max) max = data
      }
    }
    (min, max)
  }

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr =
    IntBinaryVector.optimize(memFactory, this)

  override def newInstance(memFactory: MemFactory, growFactor: Int = 2): BinaryAppendableVector[Int] = {
    val addr = memFactory.allocateOffheap(maxBytes * growFactor)
    val dispose = () => memFactory.freeMemory(addr)
    new MaskedIntAppendingVector(addr, maxBytes * growFactor, maxElements * growFactor,
                                 nbits, signed, dispose)
  }
}

object IntConstVector extends ConstVector with IntVectorDataReader {
  override def length(acc: MemoryReader, vector: BinaryVectorPtr): Int = numElements(acc, vector)
  def apply(acc: MemoryReader, vector: BinaryVectorPtr, i: Int): Int =
    acc.getInt(vector + ConstVector.DataOffset)
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Long =
    (end - start + 1) * apply(acc, vector, 0)
}
