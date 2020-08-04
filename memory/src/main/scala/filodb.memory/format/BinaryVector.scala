package filodb.memory.format

import java.nio.ByteBuffer

import debox.Buffer
import spire.syntax.cfor._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format.Encodings._
import filodb.memory.format.MemoryReader._

/**
 * An offheap, immutable, zero deserialization, minimal/zero allocation, insanely fast binary sequence.
 * NOTE: BinaryVectors are just memory regions.  To minimize heap usage, NO heap objects exist for BinaryVectors
 * at all.  However, VectorDataReader objects can be used to access them, when you pass in the BinaryVectorPtr.
 * Class instances for appendable BinaryVectors do exist. They are intended for reusing and creating BinaryVector
 * regions (via optimize()) elsewhere.
 */
object BinaryVector {
  // TODO: make this more typesafe, using @newsubtype or Long with Pointer etc.
  /**
   * A native pointer to a BinaryVector.
   */
  type BinaryVectorPtr = BinaryRegion.NativePointer

  /**
   * Returns the vector type and subtype from the WireFormat bytes of a BinaryVector
   */
  def vectorType(acc: MemoryReader, addr: BinaryVectorPtr): Int =
    WireFormat.majorAndSubType(acc.getShort(addr + 4))
  def majorVectorType(acc: MemoryReader, addr: BinaryVectorPtr): Int =
    WireFormat.majorVectorType(acc.getShort(addr + 4))

  def writeMajorAndSubType(acc: MemoryAccessor, addr: BinaryVectorPtr, majorType: Int, subType: Int): Unit =
    acc.setShort(addr + 4, WireFormat(majorType, subType).toShort)

  /**
   * Returns the total number of bytes of the BinaryVector at address
   */
  final def totalBytes(acc: MemoryReader, addr: BinaryVectorPtr): Int = acc.getInt(addr) + 4

  final def asBuffer(addr: BinaryVectorPtr): ByteBuffer = {
    UnsafeUtils.asDirectBuffer(addr, totalBytes(nativePtrReader, addr))
  }

  type BufToDataReader = PartialFunction[Class[_], ByteBuffer => VectorDataReader]

  val defaultBufToReader: BufToDataReader = {
    case Classes.Int    => (b => vectors.IntBinaryVector(b))
    case Classes.Long   => (b => vectors.LongBinaryVector(b))
    case Classes.Double => (b => vectors.DoubleVector(b))
    case Classes.UTF8   => (b => vectors.UTF8Vector(b))
    case Classes.Histogram => (b => vectors.HistogramVector(b))
  }

  def reader(clazz: Class[_], acc: MemoryReader, addr: BinaryVectorPtr): VectorDataReader = {
    clazz match {
      case Classes.Int => vectors.IntBinaryVector(acc, addr)
      case Classes.Long => vectors.LongBinaryVector(acc, addr)
      case Classes.Double => vectors.DoubleVector(acc, addr)
      case Classes.UTF8 => vectors.UTF8Vector(acc, addr)
      case Classes.Histogram => vectors.HistogramVector(acc, addr)
    }
  }

  /**
    * A dispose method which does nothing.
    */
  val NoOpDispose = () => {}
}

//scalastyle:off
import BinaryVector.BinaryVectorPtr
import vectors.HistogramVector.HistIterator
//scalastyle:on

trait TypedIterator {
  final def asIntIt: vectors.IntIterator = this.asInstanceOf[vectors.IntIterator]
  final def asLongIt: vectors.LongIterator = this.asInstanceOf[vectors.LongIterator]
  final def asDoubleIt: vectors.DoubleIterator = this.asInstanceOf[vectors.DoubleIterator]
  final def asUTF8It: vectors.UTF8Iterator = this.asInstanceOf[vectors.UTF8Iterator]
  final def asHistIt: HistIterator = this.asInstanceOf[HistIterator]
}

trait BooleanIterator extends TypedIterator {
  def next: Boolean
}

object AlwaysAvailableIterator extends BooleanIterator {
  final def next: Boolean = true
}

trait AvailableReader {
  /**
   * Returns a BooleanIterator returning true or false for each element's availability
   * By default returns one that says every element is available.  Override if this is not true.
   */
  def iterateAvailable(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): BooleanIterator =
    AlwaysAvailableIterator
}

/**
 * Base trait for offheap BinaryVectors, to be mixed into objects.
 * New-style BinaryVectors are BinaryRegionLarge's, having a 4-byte length header.
 * Following the 4-byte length header was the old WireFormat word.
 * Note that it must be completely stateless. The goal is to use NO heap objects or space,
 * simply a Long native pointer to point at where the BinaryVector is.
 */
trait VectorDataReader extends AvailableReader {
  /**
   * Returns the number of bytes of the BinaryVector FOLLOWING the 4-byte length header.
   */
  final def numBytes(acc: MemoryReader, addr: BinaryVectorPtr): Int = acc.getInt(addr)

  /**
   * Returns the number of eleemnts in the vector
   */
  def length(acc: MemoryReader, addr: BinaryVectorPtr): Int

  /**
   * Returns a TypedIterator to efficiently go through the elements of the vector.  The user is responsible for
   * knowing how many elements to process.  There is no hasNext.
   * All elements are iterated through, even those designated as "not available".
   * Costs an allocation for the iterator but allows potential performance gains too.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param startElement the starting element # in the vector, by default 0 (the first one)
   */
  def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): TypedIterator

  /**
    * Used only for testing. When using in production be careful of unnecessary allocation
    * when backing memory is already on heap.
    */
  def toBytes(acc: MemoryReader, vector: BinaryVectorPtr): Array[Byte] = {
    val numByts = numBytes(acc, vector) + 4
    val bytes = new Array[Byte](numByts)
    acc.copy(vector, MemoryAccessor.fromArray(bytes), 0, numByts)
    bytes
  }

  def toHexString(acc: MemoryReader, vector: BinaryVectorPtr): String =
    s"0x${toBytes(acc, vector).map("%02X" format _).mkString}"

  def debugString(acc: MemoryReader, vector: BinaryVectorPtr, sep: String = ","): String

  def asIntReader: vectors.IntVectorDataReader = this.asInstanceOf[vectors.IntVectorDataReader]
  def asLongReader: vectors.LongVectorDataReader = this.asInstanceOf[vectors.LongVectorDataReader]
  def asDoubleReader: vectors.DoubleVectorDataReader = this.asInstanceOf[vectors.DoubleVectorDataReader]
  def asUTF8Reader: vectors.UTF8VectorDataReader = this.asInstanceOf[vectors.UTF8VectorDataReader]
  def asHistReader: vectors.HistogramReader = this.asInstanceOf[vectors.HistogramReader]
}

/**
 * CorrectionMeta stores the type-specific correction amount for counter vectors.
 * It is also used to propagate and accumulate corrections as one iterates through vectors.
 */
trait CorrectionMeta

object NoCorrection extends CorrectionMeta

/**
 * Trait that extends VectorDataReaders with methods assisting counter-like vectors that may reset or need correction
 */
trait CounterVectorReader extends VectorDataReader {
  /**
   * Detects if there is a drop/reset from end of last chunk to the first value of this chunk.
   * Returns a new CorrectionMeta with a new correction value, which should be equal to the old value
   * plus any detected drop.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param meta CorrectionMeta obtained from updateCorrection()
   * @return a new CorrectionMeta with adjusted correction value if needed
   */
  def detectDropAndCorrection(acc: MemoryReader, vector: BinaryVectorPtr, meta: CorrectionMeta): CorrectionMeta

  /**
   * Updates the CorrectionMeta from this vector,
   * namely the total correction over that period plus the last value of the vector.
   * Returns a new CorrectionMeta which has lastValue plus the total running correction including prev correction.
   * IE this method should upward adjust the correction in meta based on any new corrections detected in this chunk.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param meta CorrectionMeta with total running correction info.  lastValue is ignored
   */
  def updateCorrection(acc: MemoryReader, vector: BinaryVectorPtr, meta: CorrectionMeta): CorrectionMeta

  /**
    * Identifies row numbers at which histogram or counter has dropped
    */
  def dropPositions(acc2: MemoryReader, vector: BinaryVectorPtr): debox.Buffer[Int]
}

// An efficient iterator for the bitmap mask, rotating a mask as we go
class BitmapMaskIterator(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int) extends BooleanIterator {
  var bitmapAddr: Long = vector + 12 + (startElement >> 6) * 8
  var bitMask: Long = 1L << (startElement & 0x3f)

  // Rotates mask and returns if next element is available or not
  final def next: Boolean = {
    val avail = (acc.getLong(bitmapAddr) & bitMask) == 0
    bitMask <<= 1
    if (bitMask == 0) {
      bitmapAddr += 8
      bitMask = 1L
    }
    avail
  }
}

/**
 * A BinaryVector with an NaMask bitmap for NA values (1/on for missing NA values), 64-bit chunks
 */
trait BitmapMaskVector extends AvailableReader {
  // Returns the address of the subvector
  final def subvectAddr(acc: MemoryReader, addr: BinaryVectorPtr): BinaryVectorPtr =
    addr + acc.getInt(addr + 8)

  final def isAvailable(acc: MemoryReader, addr: BinaryVectorPtr, index: Int): Boolean = {
    // NOTE: length of bitMask may be less than (length / 64) longwords.
    val maskIndex = index >> 6
    val maskVal = acc.getLong(addr + 12 + maskIndex * 8)
    (maskVal & (1L << (index & 63))) == 0
  }

  override def iterateAvailable(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): BooleanIterator =
    new BitmapMaskIterator(acc, vector, startElement)
}

object BitmapMask extends BitmapMaskVector {
  def numBytesRequired(elements: Int): Int = ((elements + 63) / 64) * 8
}

sealed trait AddResponse
case object Ack extends AddResponse
final case class VectorTooSmall(bytesNeeded: Int, bytesHave: Int) extends AddResponse
case object ItemTooLarge extends AddResponse
case object BucketSchemaMismatch extends AddResponse   // Trying to add a histogram with nonmatching schema
case object InvalidHistogram extends AddResponse

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
trait BinaryAppendableVector[@specialized(Int, Long, Double, Boolean) A] {
  // Native address of the appendable vector
  def addr: BinaryRegion.NativePointer

  /** Max size that current buffer can grow to */
  def maxBytes: Int

  /** Number of current bytes used up */
  def numBytes: Int

  /**
   * Number of elements currently in this AppendableVector
   */
  def length: Int

  def dispose: () => Unit

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

  /** Returns a reader that can be used to read from this vect */
  def reader: VectorDataReader

  /** Copies the data out of this appendable vector to an on-heap Buffer */
  def copyToBuffer: Buffer[A]

  /**
   * Returns the value at index A.  TODO: maybe deprecate this?
   */
  def apply(index: Int): A

  def isAvailable(index: Int): Boolean

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
  def addVector(other: BinaryAppendableVector[A]): Unit = {
    cforRange { 0 until other.length } { i =>
      if (other.isAvailable(i)) { addData(other(i))}
      else                      { addNA() }
    }
  }

  /**
   * Checks to see if enough bytes or need to allocate more space.
   */
  def checkSize(need: Int, have: Int): AddResponse =
    if (!(need <= have)) VectorTooSmall(need, have) else Ack

  @inline final def incNumBytes(inc: Int): Unit = UnsafeUtils.unsafe.getAndAddInt(UnsafeUtils.ZeroPointer, addr, inc)

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
  def freeze(memFactory: MemFactory): BinaryVectorPtr = {
    val newAddr = memFactory.allocateOffheap(frozenSize)
    freeze(Some(newAddr))
  }

  def freeze(newAddrOpt: Option[BinaryRegion.NativePointer]): BinaryVectorPtr =
    if (newAddrOpt.isEmpty && numBytes == frozenSize) {
      // It is better to return a simple read-only BinaryVector, for two reasons:
      // 1) It cannot be cast into an AppendingVector and made mutable
      // 2) It is probably higher performance for reads
      finishCompaction(addr)
    } else {
      val newAddress = newAddrOpt.getOrElse(addr)
      if (newAddrOpt.nonEmpty) UnsafeUtils.copy(addr, newAddress, primaryBytes)
      if (numBytes > primaryMaxBytes) {
        UnsafeUtils.copy(addr + primaryMaxBytes, newAddress + primaryBytes, numBytes - primaryMaxBytes)
        UnsafeUtils.setInt(newAddress, numBytes - 4)
      }
      finishCompaction(newAddress)
    }

  // The defaults below only work for vectors with no variable/secondary area.  Override as needed.
  def primaryBytes: Int = numBytes
  def primaryMaxBytes: Int = maxBytes

  // Does any necessary metadata adjustments and instantiates immutable BinaryVector
  def finishCompaction(newAddr: Long): BinaryVectorPtr

  /**
   * Run some heuristics over this vector and automatically determine and return a more optimized
   * vector if possible.  The default implementation just does freeze(), but for example
   * an IntVector could return a vector with a smaller bit packed size given a max integer.
   * This always produces a new, frozen copy and takes more CPU than freeze() but can result in dramatically
   * smaller vectors using advanced techniques such as delta-delta or dictionary encoding.
   *
   * By default it copies using the passed memFactory to allocate
   */
  def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr = freeze(memFactory)

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
  final def numBytes: Int = inner.numBytes
  final def addr: BinaryRegion.NativePointer = inner.addr
  final override def length: Int = inner.length
  final def reader: VectorDataReader = inner.reader
  final def copyToBuffer: Buffer[A] = inner.copyToBuffer.asInstanceOf[Buffer[A]]

  final def maxBytes: Int = inner.maxBytes
  def isAllNA: Boolean = inner.isAllNA
  def noNAs: Boolean = inner.noNAs
  override def primaryBytes: Int = inner.primaryBytes
  override def primaryMaxBytes: Int = inner.primaryMaxBytes
  override def finishCompaction(newAddr: Long): BinaryVectorPtr = inner.finishCompaction(newAddr)
  override def frozenSize: Int = inner.frozenSize
  final def reset(): Unit = inner.reset()
  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr =
    inner.optimize(memFactory, hint)

  override def dispose: () => Unit = inner.dispose
}

trait OptimizingPrimitiveAppender[@specialized(Int, Long, Double, Boolean) A] extends BinaryAppendableVector[A] {
  def minMax: (A, A)
  def nbits: Short

  /**
   * Extract just the data vector with no NA mask or overhead
   */
  def dataVect(memFactory: MemFactory): BinaryVectorPtr

  /**
   * Extracts the entire vector including NA information
   */
  def getVect(memFactory: MemFactory): BinaryVectorPtr = freeze(memFactory)
}

object PrimitiveVector {
  val HeaderLen = 4
  val OffsetWireFormat = 4
  val OffsetNBits = 6
  val OffsetBitShift = 7
  val OffsetData = 8

  val NBitsMask = 0x07f
  val SignMask  = 0x080
  val DropMask = 0x08000
}

object PrimitiveVectorReader {
  import PrimitiveVector._
  final def nbits(acc: MemoryReader, vector: BinaryVectorPtr): Int =
    acc.getByte(vector + OffsetNBits) & NBitsMask
  final def bitShift(acc: MemoryReader, vector: BinaryVectorPtr): Int =
    acc.getByte(vector + OffsetBitShift) & 0x03f
  final def signed(acc: MemoryReader, vector: BinaryVectorPtr): Boolean =
    (acc.getByte(vector + OffsetNBits) & SignMask) != 0
  final def dropped(acc: MemoryReader, vector: BinaryVectorPtr): Boolean =
    (acc.getShort(vector + OffsetNBits) & DropMask) != 0
  final def markDrop(acc: MemoryAccessor, vector: BinaryVectorPtr): Unit =
    acc.setShort(vector + OffsetNBits,
      (acc.getShort(vector + OffsetNBits) | DropMask).toShort)
}

/**
 * A BinaryAppendableVector for simple primitive types, ie where each element has a fixed length
 * and every element is available (there is no bitmap NA mask).
 * +0000 length word
 * +0004 WireFormat(16 bits)
 * +0006 nbits/signed:
 *    RxBBBBBBSNNNNNNN  bits 0-6:  nbits
 *                      bits 7:    signed 0 or 1
 *                      bits 8-13: bitshift
 *                      bit  15:   1=reset/drop within chunk
 */
abstract class PrimitiveAppendableVector[@specialized(Int, Long, Double, Boolean) A]
  (val addr: BinaryRegion.NativePointer, val maxBytes: Int, val nbits: Short, signed: Boolean)
extends OptimizingPrimitiveAppender[A] {
  import PrimitiveVector._
  def vectMajorType: Int = WireFormat.VECTORTYPE_BINSIMPLE
  def vectSubType: Int = WireFormat.SUBTYPE_PRIMITIVE_NOMASK
  var writeOffset: Long = addr + OffsetData
  var bitShift: Int = 0
  override final def length: Int = ((writeOffset - (addr + 8)).toInt * 8 + bitShift) / nbits

  UnsafeUtils.setInt(addr, HeaderLen)   // 4 bytes after this length word
  BinaryVector.writeMajorAndSubType(MemoryAccessor.nativePtrAccessor, addr, vectMajorType, vectSubType)
  UnsafeUtils.setShort(addr + OffsetNBits, ((nbits & NBitsMask) | (if (signed) SignMask else 0)).toShort)

  def numBytes: Int = nativePtrReader.getInt(addr) + 4

  private final val dangerZone = addr + maxBytes
  final def checkOffset(): AddResponse =
    if (writeOffset >= dangerZone) VectorTooSmall((writeOffset - addr).toInt + 1, maxBytes) else Ack

  @inline final def incWriteOffset(inc: Int): Unit = {
    writeOffset += inc
    incNumBytes(inc)
  }

  @inline protected final def bumpBitShift(): Unit = {
    if (bitShift == 0) incNumBytes(1)   // just before increment, means we are using that byte now
    bitShift = (bitShift + nbits) % 8
    if (bitShift == 0) writeOffset += 1
    UnsafeUtils.setByte(addr + OffsetBitShift, bitShift.toByte)
  }

  final def isAvailable(index: Int): Boolean = true

  override final def addVector(other: BinaryAppendableVector[A]): Unit = other match {
    case v: BitmapMaskAppendableVector[A] =>
      addVector(v.subVect)
    case v: BinaryAppendableVector[A] =>
      // Optimization: this vector does not support NAs so just add the data
      require(numBytes + (nbits * v.length / 8) <= maxBytes,
             s"Not enough space to add ${v.length} elems; nbits=$nbits; need ${maxBytes-numBytes} bytes")
      cforRange { 0 until v.length } { i => addData(v(i)) }
  }

  final def isAllNA: Boolean = (length == 0)
  final def noNAs: Boolean = (length > 0)

  final def dataVect(memFactory: MemFactory): BinaryVectorPtr = freeze(memFactory)

  def finishCompaction(newAddress: BinaryRegion.NativePointer): BinaryVectorPtr = newAddress

  def reset(): Unit = {
    writeOffset = addr + OffsetData
    bitShift = 0
    UnsafeUtils.setByte(addr + OffsetBitShift, bitShift.toByte)
    UnsafeUtils.setInt(addr, HeaderLen)   // 4 bytes after this length word
  }
}

/**
 * Maintains a fast NA bitmap mask as we append elements
 * +0000 length word
 * +0004 WireFormat word
 * +0008 offset to the subvector (after bitmask data)
 */
abstract class BitmapMaskAppendableVector[@specialized(Int, Long, Double, Boolean) A]
  (val addr: BinaryRegion.NativePointer, maxElements: Int)
extends BinaryAppendableVector[A] {
  def vectMajorType: Int
  def vectSubType: Int

  // The base vector holding the actual values
  def subVect: BinaryAppendableVector[A]
  def reader: VectorDataReader = subVect.reader

  def bitmapOffset: BinaryRegion.NativePointer = addr + 12
  val bitmapMaskBufferSize = BitmapMask.numBytesRequired(maxElements)
  var curBitmapOffset = 0
  var curMask: Long = 1L

  UnsafeUtils.unsafe.setMemory(UnsafeUtils.ZeroPointer, bitmapOffset, bitmapMaskBufferSize, 0)

  UnsafeUtils.setInt(addr, 8 + bitmapMaskBufferSize)
  BinaryVector.writeMajorAndSubType(MemoryAccessor.nativePtrAccessor, addr, vectMajorType, vectSubType)
  val subVectOffset = 12 + bitmapMaskBufferSize
  UnsafeUtils.setInt(addr + 8, subVectOffset)

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
    UnsafeUtils.unsafe.setMemory(UnsafeUtils.ZeroPointer, bitmapOffset, bitmapMaskBufferSize, 0)
    curBitmapOffset = 0
    curMask = 1L
  }

  final def reset(): Unit = {
    subVect.reset()
    resetMask()
  }

  override final def length: Int = subVect.length
  final def numBytes: Int = 12 + bitmapMaskBufferSize + subVect.numBytes
  final def isAvailable(index: Int): Boolean = BitmapMask.isAvailable(nativePtrReader, addr, index)
  final def apply(index: Int): A = subVect.apply(index)

  final def addNA(): AddResponse = checkSize(curBitmapOffset, bitmapMaskBufferSize) match {
    case Ack =>
      val resp = subVect.addNA()
      if (resp == Ack) {
        val maskVal = nativePtrReader.getLong(bitmapOffset + curBitmapOffset)
        UnsafeUtils.setLong(bitmapOffset + curBitmapOffset, maskVal | curMask)
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
    cforRange { 0 until curBitmapOffset/8 } { word =>
      if (nativePtrReader.getLong(bitmapOffset + word * 8) != -1L) return false
    }
    val naMask = curMask - 1
    (nativePtrReader.getLong(bitmapOffset + curBitmapOffset) & naMask) == naMask
  }

  final def noNAs: Boolean = {
    cforRange { 0 until curBitmapOffset/8 } { word =>
      if (nativePtrReader.getLong(bitmapOffset + word * 8) != 0) return false
    }
    val naMask = curMask - 1
    (nativePtrReader.getLong(bitmapOffset + curBitmapOffset) & naMask) == 0
  }

  def finishCompaction(newAddr: BinaryRegion.NativePointer): BinaryVectorPtr = {
    // Don't forget to write the new subVectOffset
    UnsafeUtils.setInt(newAddr + 8, (bitmapOffset + bitmapBytes - addr).toInt)
    // and also the new length
    UnsafeUtils.setInt(newAddr, frozenSize - 4)
    newAddr
  }

  override def primaryBytes: Int = (bitmapOffset - addr).toInt + bitmapBytes
  override def primaryMaxBytes: Int = (bitmapOffset - addr).toInt + bitmapMaskBufferSize

  override final def addVector(other: BinaryAppendableVector[A]): Unit = other match {
    // Optimized case: we are empty, so just copy over entire bitmap from other one
    case v: BitmapMaskAppendableVector[A] if length == 0 =>
      copyMaskFrom(v)
      subVect.addVector(v.subVect)
    // Non-optimized  :(
    case v: BinaryAppendableVector[A] =>
      super.addVector(other)
  }

  final def copyMaskFrom(other: BitmapMaskAppendableVector[A]): Unit = {
    checkSize(other.bitmapBytes, this.bitmapMaskBufferSize)
    UnsafeUtils.unsafe.copyMemory(UnsafeUtils.ZeroPointer, other.bitmapOffset,
                                  UnsafeUtils.ZeroPointer, bitmapOffset,
                                  other.bitmapBytes)
    curBitmapOffset = other.curBitmapOffset
    curMask = other.curMask
  }
}
