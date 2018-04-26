package filodb.memory.format

import java.nio.ByteBuffer

import scalaxy.loops._

import filodb.memory.MemFactory
import filodb.memory.format.Encodings._

/**
 * This is really the same as FiloVector, but supports off-heap easier.
 * An immutable, zero deserialization, minimal/zero allocation, insanely fast binary sequence.
 * TODO: maybe merge this and FiloVector, or just make FiloVector support ZeroCopyBinary.
 */
trait BinaryVector[@specialized(Int, Long, Double, Boolean) A] extends FiloVector[A] with ZeroCopyBinary {

  /** The major and subtype bytes as defined in WireFormat that will go into the FiloVector header */
  def vectMajorType: Int

  def vectSubType: Int

  /**
    * Should return false if this vector definitely has no NAs, and true if it might have some or is
    * designed to support NAs.
    * Returning false allows optimizations for aggregations.
    */
  def maybeNAs: Boolean

  def isOffheap: Boolean = base == UnsafeUtils.ZeroPointer

  /**
    * Called when this BinaryVector is no longer required.
    * Implementations may choose to free memory or any other resources.
    */
  def dispose: () => Unit

  // Write out the header if it is uninitialized
  // Assumes proper allocation with magic header written to four bytes before offset
  def initHeaderBytes(): Unit =
    if (UnsafeUtils.getInt(base, offset - 4) == BinaryVector.HeaderMagic) {
      UnsafeUtils.setInt(base, offset - 4, WireFormat(vectMajorType, vectSubType))
    }

  /**
    * Produce a FiloVector ByteBuffer with the four-byte header.  The resulting buffer can be used for I/O
    * and fed to FiloVector.apply() to be parsed back.  For most BinaryVectors returned by optimize() and
    * freeze(), a copy is not necessary so this is usually a very inexpensive operation.
    */
  def toFiloBuffer: ByteBuffer = base match {
    case UnsafeUtils.ZeroPointer =>
      initHeaderBytes()
      UnsafeUtils.asDirectBuffer(offset - 4, numBytes + 4)

    case a: Array[Byte] =>
      assert(offset == (UnsafeUtils.arayOffset + 4))
      initHeaderBytes()
      val bb = ByteBuffer.wrap(a)
      bb.limit(numBytes + 4)
      bb.order(java.nio.ByteOrder.LITTLE_ENDIAN)
      bb

    case _ =>
      //we only support cases where base is an Array or null for offheap.
      throw new UnsupportedOperationException
  }

}

trait PrimitiveVector[@specialized(Int, Long, Double, Boolean) A] extends BinaryVector[A] {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE_NOMASK
  val maybeNAs = false
}

object BinaryVector {
  /** Used to reserve memory location "4-byte header will go here" */
  val HeaderMagic = 0x87654300  // The lower 8 bits should not be 00

  /**
    * A memory has a base, offset and a length. An off-heap memory usually has a null base.
    * An array backed memory has the array object as the base. Offset is a Long indicating the
    * location in native memory. For an array it is retrieved using Unsafe.arrayBaseOffset
    * Length is the length of memory in bytes
    */
  type Memory = Tuple3[Any, Long, Int]

  /**
    * A dispose method which does nothing.
    */
  val NoOpDispose = () => {}

}

/**
 * A BinaryVector with an NaMask bitmap for NA values (1/on for missing NA values), 64-bit chunks
 */
trait BitmapMaskVector[A] extends BinaryVector[A] {
  def base: Any
  def bitmapOffset: Long   // NOTE: should be offset + n
  val maybeNAs = true

  final def isAvailable(index: Int): Boolean = {
    // NOTE: length of bitMask may be less than (length / 64) longwords.
    val maskIndex = index >> 6
    val maskVal = UnsafeUtils.getLong(base, bitmapOffset + maskIndex * 8)
    (maskVal & (1L << (index & 63))) == 0
  }
}

trait PrimitiveMaskVector[@specialized(Int, Long, Double, Boolean) A] extends BitmapMaskVector[A] {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE
}

object BitmapMask {
  def numBytesRequired(elements: Int): Int = ((elements + 63) / 64) * 8
}

sealed trait AddResponse
case object Ack extends AddResponse
final case class VectorTooSmall(bytesNeeded: Int, bytesHave: Int) extends AddResponse
case object ItemTooLarge extends AddResponse

/**
 * A BinaryVector that you can append to.  Has some notion of a maximum size (max # of items or bytes)
 * and the user is responsible for resizing if necessary (but see GrowableVector).
 *
 * Replaces the current VectorBuilder API, and greatly simplifies overall APIs in Filo.  AppendableVectors
 * are still FiloVectors so they could be read as they are being built -- and the new optimize() APIS
 * take advantage of this for more immutable optimization.  This approach also lets a user select the
 * amount of CPU to spend optimizing.
 *
 * NOTE: AppendableVectors are not designed to be multi-thread safe for writing.  They are designed for
 * high single-thread performance.
 *
 * ## Error handling
 *
 * The add methods return an AddResponse.  We avoid throwing Exceptions as they are exceptionally expensive!!
 *
 * ## Use Cases and LifeCycle
 *
 * The AppendingVector is mutable and can be reset().  THe idea is to call freeze() or optimize() to produce
 * more compact forms for I/O or longer term storage, but all the forms are readable.  This gives the user
 * great flexibility to choose compression tradeoffs.
 *
 * 1. Fill up one of the appendingVectors and query as is.  Use it like a Seq[].  No need to optimize at all.
 * 2.  --> freeze() --> toFiloBuffer ...  compresses pointer/bitmask only
 * 3.  --> optimize() --> toFiloBuffer ... aggressive compression using all techniques available
 */
trait BinaryAppendableVector[@specialized(Int, Long, Double, Boolean) A] extends BinaryVector[A] {

  /** Max size that current buffer can grow to */
  def maxBytes: Int

  /**
   * Add a Not Available (null) element to the builder.
   * @return Ack or another AddResponse if it cannot be added
   */
  def addNA(): AddResponse

  /**
   * Add a value of type T to the builder.  It will be marked as available.
   * @return Ack or another AddResponse if it cannot be added
   */
  def addData(value: A): AddResponse

  final def add(data: Option[A]): AddResponse = if (data.nonEmpty) addData(data.get) else addNA()

  /**
   * Adds to this vector from a RowReader.  Method avoids boxing.  Does not check for data availability.
   */
  def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse

  /**
   * Adds to this vector from a RowReader, checking for availability
   */
  final def addFromReader(reader: RowReader, col: Int): AddResponse =
    if (reader.notNull(col)) { addFromReaderNoNA(reader, col) }
    else                     { addNA() }

  /** Returns true if every element added is NA, or no elements have been added */
  def isAllNA: Boolean

  /** Returns true if no elements are NA at all, ie every element is available. */
  def noNAs: Boolean

  /**
   * A method to add in bulk all the items from another BinaryVector of the same type.
   * This is a separate method to allow for more efficient implementations such as the ability to
   * copy all the bytes if the data layout are exactly the same.
   *
   * The default implementation is generic but inefficient: does not take advantage of data layout
   */
  def addVector(other: BinaryVector[A]): Unit = {
    for { i <- 0 until other.length optimized } {
      if (other.isAvailable(i)) { addData(other(i))}
      else                      { addNA() }
    }
  }

  /**
   * Checks to see if enough bytes or need to allocate more space.
   */
  def checkSize(need: Int, have: Int): AddResponse =
    if (!(need <= have)) VectorTooSmall(need, have) else Ack

  /**
   * Allocates a new instance of itself growing by factor growFactor.
   * Needs to be defined for any vectors that GrowableVector wraps.
   */
  def newInstance(memFactory: MemFactory, growFactor: Int = 2): BinaryAppendableVector[A] = ???

  /**
   * Returns the number of bytes required for a compacted AppendableVector
   * The default implementation must be overridden if freeze() is also overridden.
   */
  def frozenSize: Int =
    if (numBytes >= primaryMaxBytes) numBytes - (primaryMaxBytes - primaryBytes) else numBytes

  /**
   * Compact this vector, removing unused space and return an immutable version of this FiloVector
   * that cannot be appended to, by default in a new space using up minimum space.
   *
   * WARNING: by default this copies the bytes so that the current bytes are not modified.  Do not set
   *          copy to false unless you know what you are doing, this may move bytes in place and cause
   *          heartache and unusable AppendableVectors.
   *
   * The default implementation assumes the following common case:
   *  - AppendableBinaryVectors usually are divided into a fixed primary area and a secondary variable
   *    area that can extend up to maxBytes.  Ex.: the area for bitmap masks or UTF8 offets/lengths.
   *  - primary area can extend up to primaryMaxBytes but is currently at primaryBytes
   *  - compaction works by moving secondary area up (primaryMaxBytes - primaryBytes) bytes, either
   *    in place or to a new location (and the primary area would be copied too).
   *  - finishCompaction is called to instantiate the new BinaryVector and do any other cleanup.
   *
   * @param newBaseOffset optionally, compact not in place but to a new location.  If left as None,
   *                      any compaction will be done "in-place" in the same buffer.
   */
  def freeze(memFactory: MemFactory): BinaryVector[A] = {
    val (base, off, _) = memFactory.allocateWithMagicHeader(frozenSize)
    freeze(Some((base, off)))
  }

  def freeze(newBaseOffset: Option[(Any, Long)]): BinaryVector[A] =
    if (newBaseOffset.isEmpty && numBytes == frozenSize) {
      // It is better to return a simple read-only BinaryVector, for two reasons:
      // 1) It cannot be cast into an AppendingVector and made mutable
      // 2) It is probably higher performance for reads
      finishCompaction(base, offset)
    } else {
      val (newBase, newOffset) = newBaseOffset.getOrElse((base, offset))
      if (newBaseOffset.nonEmpty) copyTo(newBase, newOffset, n = primaryBytes)
      if (numBytes > primaryMaxBytes) {
        copyTo(newBase, newOffset + primaryBytes, primaryMaxBytes, numBytes - primaryMaxBytes)
      }
      finishCompaction(newBase, newOffset)
    }

  // The defaults below only work for vectors with no variable/secondary area.  Override as needed.
  def primaryBytes: Int = numBytes
  def primaryMaxBytes: Int = maxBytes

  // Does any necessary metadata adjustments and instantiates immutable BinaryVector
  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[A]

  /**
   * Run some heuristics over this vector and automatically determine and return a more optimized
   * vector if possible.  The default implementation just does freeze(), but for example
   * an IntVector could return a vector with a smaller bit packed size given a max integer.
   * This always produces a new, frozen copy and takes more CPU than freeze() but can result in dramatically
   * smaller vectors using advanced techniques such as delta-delta or dictionary encoding.
   *
   * By default it copies using the passed memFactory to allocate
   */
  def optimize(memFactory: MemFactory,hint: EncodingHint = AutoDetect): BinaryVector[A] = freeze(memFactory)

  /**
   * Clears the elements so one can start over.
   */
  def reset(): Unit
}

case class GrowableVector[@specialized(Int, Long, Double, Boolean) A](memFactory: MemFactory,
                                                                      var inner: BinaryAppendableVector[A])
extends AppendableVectorWrapper[A, A] {
  def addData(value: A): AddResponse = inner.addData(value) match {
    case e: VectorTooSmall =>
      val newInst = inner.newInstance(memFactory)
      newInst.addVector(inner)
      inner.dispose()   // free memory in case it's off-heap .. just before reference is lost
      inner = newInst
      inner.addData(value)
    case other: AddResponse => other
  }

  override def addNA(): AddResponse = inner.addNA() match {
    case e: VectorTooSmall =>
      val newInst = inner.newInstance(memFactory)
      newInst.addVector(inner)
      inner.dispose()   // free memory in case it's off-heap .. just before reference is lost
      inner = newInst
      inner.addNA()
    case other: AddResponse => other
  }

  def apply(index: Int): A = inner(index)

  override def dispose: () => Unit = inner.dispose
}

trait AppendableVectorWrapper[@specialized(Int, Long, Double, Boolean) A, I] extends BinaryAppendableVector[A] {
  def inner: BinaryAppendableVector[I]

  def addNA(): AddResponse = inner.addNA()
  def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = inner.addFromReaderNoNA(reader, col)

  final def isAvailable(index: Int): Boolean = inner.isAvailable(index)
  final def base: Any = inner.base
  final def numBytes: Int = inner.numBytes
  final def offset: Long = inner.offset
  final override def length: Int = inner.length

  final def maybeNAs: Boolean = inner.maybeNAs
  final def maxBytes: Int = inner.maxBytes
  def isAllNA: Boolean = inner.isAllNA
  def noNAs: Boolean = inner.noNAs
  override def primaryBytes: Int = inner.primaryBytes
  override def primaryMaxBytes: Int = inner.primaryMaxBytes
  def vectMajorType: Int = inner.vectMajorType
  def vectSubType: Int = inner.vectSubType
  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[A] =
    inner.finishCompaction(newBase, newOff).asInstanceOf[BinaryVector[A]]
  override def frozenSize: Int = inner.frozenSize
  final def reset(): Unit = inner.reset()
  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVector[A] =
    inner.optimize(memFactory, hint).asInstanceOf[BinaryVector[A]]

  override def dispose: () => Unit = inner.dispose
}

trait OptimizingPrimitiveAppender[@specialized(Int, Long, Double, Boolean) A] extends BinaryAppendableVector[A] {
  def minMax: (A, A)
  def nbits: Short

  /**
   * Extract just the data vector with no NA mask or overhead
   */
  def dataVect(memFactory: MemFactory): BinaryVector[A]

  /**
   * Extracts the entire vector including NA information
   */
  def getVect(memFactory: MemFactory): BinaryVector[A] = freeze(memFactory)
}

/**
 * A BinaryAppendableVector for simple primitive types, ie where each element has a fixed length
 * and every element is available (there is no bitmap NA mask).
 */
abstract class PrimitiveAppendableVector[@specialized(Int, Long, Double, Boolean) A]
  (val base: Any, val offset: Long, val maxBytes: Int, val nbits: Short, signed: Boolean)
extends OptimizingPrimitiveAppender[A] {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE_NOMASK
  var writeOffset: Long = offset + 4
  var bitShift: Int = 0
  override final def length: Int = ((writeOffset - offset - 4).toInt * 8 + bitShift) / nbits

  UnsafeUtils.setShort(base, offset, nbits.toShort)
  UnsafeUtils.setByte(base, offset + 2, if (signed) 1 else 0)

  def numBytes: Int = (writeOffset - offset).toInt + (if (bitShift != 0) 1 else 0)

  private final val dangerZone = offset + maxBytes
  final def checkOffset(): AddResponse =
    if (writeOffset >= dangerZone) VectorTooSmall((writeOffset - offset).toInt + 1, maxBytes) else Ack

  protected final def bumpBitShift(): Unit = {
    bitShift = (bitShift + nbits) % 8
    if (bitShift == 0) writeOffset += 1
    UnsafeUtils.setByte(base, offset + 3, bitShift.toByte)
  }

  final def isAvailable(index: Int): Boolean = true

  override final def addVector(other: BinaryVector[A]): Unit = other match {
    case v: BitmapMaskAppendableVector[A] =>
      addVector(v.subVect)
    case v: BinaryVector[A] =>
      // Optimization: this vector does not support NAs so just add the data
      require(numBytes + (nbits * v.length / 8) <= maxBytes,
             s"Not enough space to add ${v.length} elems; nbits=$nbits; need ${maxBytes-numBytes} bytes")
      for { i <- 0 until v.length optimized } { addData(v(i)) }
  }

  final def isAllNA: Boolean = (length == 0)
  final def noNAs: Boolean = (length > 0)
  val maybeNAs = false

  final def dataVect(memFactory: MemFactory): BinaryVector[A] = freeze(memFactory)

  def reset(): Unit = {
    writeOffset = offset + 4
    bitShift = 0
  }
}

/**
 * Maintains a fast NA bitmap mask as we append elements
 */
abstract class BitmapMaskAppendableVector[@specialized(Int, Long, Double, Boolean) A]
  (val base: Any, val bitmapOffset: Long, maxElements: Int)
extends BitmapMaskVector[A] with BinaryAppendableVector[A] {
  // The base vector holding the actual values
  def subVect: BinaryAppendableVector[A]

  val bitmapMaskBufferSize = BitmapMask.numBytesRequired(maxElements)
  var curBitmapOffset = 0
  var curMask: Long = 1L

  UnsafeUtils.unsafe.setMemory(base, bitmapOffset, bitmapMaskBufferSize, 0)

  val subVectOffset = 4 + bitmapMaskBufferSize
  UnsafeUtils.setInt(base, offset, subVectOffset)

  // The number of bytes taken up by the bitmap mask right now
  final def bitmapBytes: Int = curBitmapOffset + (if (curMask == 1L) 0 else 8)

  final def nextMaskIndex(): Unit = {
    curMask = curMask << 1
    if (curMask == 0) {
      curMask = 1L
      curBitmapOffset += 8
    }
  }

  final def resetMask(): Unit = {
    UnsafeUtils.unsafe.setMemory(base, bitmapOffset, bitmapMaskBufferSize, 0)
    curBitmapOffset = 0
    curMask = 1L
  }

  final def reset(): Unit = {
    subVect.reset()
    resetMask()
  }

  override final def length: Int = subVect.length
  final def numBytes: Int = 4 + bitmapMaskBufferSize + subVect.numBytes
  final def apply(index: Int): A = subVect.apply(index)

  final def addNA(): AddResponse = checkSize(curBitmapOffset, bitmapMaskBufferSize) match {
    case Ack =>
      val resp = subVect.addNA()
      if (resp == Ack) {
        val maskVal = UnsafeUtils.getLong(base, bitmapOffset + curBitmapOffset)
        UnsafeUtils.setLong(base, bitmapOffset + curBitmapOffset, maskVal | curMask)
        nextMaskIndex()
      }
      resp
    case other: AddResponse => other
  }

  final def addData(value: A): AddResponse = {
    val resp = subVect.addData(value)
    if (resp == Ack) nextMaskIndex()
    resp
  }

  def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = {
    val resp = subVect.addFromReaderNoNA(reader, col)
    if (resp == Ack) nextMaskIndex()
    resp
  }

  final def isAllNA: Boolean = {
    for { word <- 0 until curBitmapOffset/8 optimized } {
      if (UnsafeUtils.getLong(base, bitmapOffset + word * 8) != -1L) return false
    }
    val naMask = curMask - 1
    (UnsafeUtils.getLong(base, bitmapOffset + curBitmapOffset) & naMask) == naMask
  }

  final def noNAs: Boolean = {
    for { word <- 0 until curBitmapOffset/8 optimized } {
      if (UnsafeUtils.getLong(base, bitmapOffset + word * 8) != 0) return false
    }
    val naMask = curMask - 1
    (UnsafeUtils.getLong(base, bitmapOffset + curBitmapOffset) & naMask) == 0
  }

  override def primaryBytes: Int = (bitmapOffset - offset).toInt + bitmapBytes
  override def primaryMaxBytes: Int = (bitmapOffset - offset).toInt + bitmapMaskBufferSize

  override final def addVector(other: BinaryVector[A]): Unit = other match {
    // Optimized case: we are empty, so just copy over entire bitmap from other one
    case v: BitmapMaskAppendableVector[A] if length == 0 =>
      copyMaskFrom(v)
      subVect.addVector(v.subVect)
    // Non-optimized  :(
    case v: BinaryVector[A] =>
      super.addVector(other)
  }

  final def copyMaskFrom(other: BitmapMaskAppendableVector[A]): Unit = {
    checkSize(other.bitmapBytes, this.bitmapMaskBufferSize)
    UnsafeUtils.unsafe.copyMemory(other.base, other.bitmapOffset,
                                  base, bitmapOffset,
                                  other.bitmapBytes)
    curBitmapOffset = other.curBitmapOffset
    curMask = other.curMask
  }
}
