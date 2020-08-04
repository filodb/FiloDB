package filodb.memory.format.vectors

import java.nio.ByteBuffer

import debox.Buffer
import spire.syntax.cfor._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.Encodings._
import filodb.memory.format.MemoryReader._

/**
  * Constructor methods for UTF8 vector types, as well as UTF8/binary blob utilities
  */
object UTF8Vector {
  /**
    * Creates a UTF8Vector that holds references to original UTF8 strings, but can optimize to final forms.
    * Typical usage:  {{{ UTF8Vector(strings).optimize().toFiloBuffer }}}
    * Or to control dictionary encoding:  use optimizedVector(...)
    */
  def apply(memFactory: MemFactory, strings: Seq[ZeroCopyUTF8String]): BinaryAppendableVector[ZeroCopyUTF8String] = {
    val maxBytes = 12 + (8 * strings.length) + strings.map(_.length).sum
    val vect = appendingVector(memFactory, strings.length, maxBytes)
    strings.foreach { str =>
      if (ZeroCopyUTF8String.isNA(str)) vect.addNA() else vect.addData(str)
    }
    vect
  }

  /**
    * Creates an appendable UTF8 string vector given the max capacity and max elements.
    * This can be written to wire but not as optimized as FixedMax and DictUTF8 vectors.
    * Be conservative.  The amount of space needed is at least 4 + 4 * #strings + the space needed
    * for the strings themselves; add another 4 bytes per string when more than 32KB is needed.
    *
    * @param maxBytes the initial max # of bytes allowed.  Will grow as needed.
    */
  def appendingVector(memFactory: MemFactory,
                      maxElements: Int,
                      maxBytes: Int): BinaryAppendableVector[ZeroCopyUTF8String] = {
    val addr = memFactory.allocateOffheap(maxBytes)
    val dispose = () => memFactory.freeMemory(addr)
    new GrowableVector(memFactory, new UTF8AppendableVector(addr, maxBytes, maxElements, dispose))
  }

  /**
   * Parses the type of vector from a DirectByteBuffer (offheap) and returns a UTF8VectorDataReader
   */
  def apply(buffer: ByteBuffer): UTF8VectorDataReader = apply(MemoryReader.fromByteBuffer(buffer), 0)

  import WireFormat._

  /**
   * Parses the type of vector from the WireFormat word at address+4 and returns the appropriate
   * UTF8VectorDataReader object for parsing it
   */
  def apply(acc: MemoryReader, vector: BinaryVectorPtr): UTF8VectorDataReader = {
    BinaryVector.vectorType(acc, vector) match {
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_UTF8)     => UTF8FlexibleVectorDataReader
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_FIXEDMAXUTF8) => ???
      case x if x == WireFormat(VECTORTYPE_BINDICT, SUBTYPE_UTF8)       => UTF8DictVectorDataReader
      case x if x == WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_REPEATED) => UTF8ConstVector
    }
  }

  val SmallOffsetNBits  = 20
  val SmallLenNBits     = 31 - SmallOffsetNBits
  val MaxSmallOffset    = Math.pow(2, SmallOffsetNBits).toInt - 1
  val MaxSmallLen       = Math.pow(2, SmallLenNBits).toInt - 1
  val MaxStringLen      = 0x0f000
  val SmallOffsetMask   = MaxSmallOffset << SmallLenNBits
  val EmptyBlob         = 0x80000000
  val NABlob            = 0x00000000     // NA blob.  32-bit offset cannot be 0.
  val NAShort           = 0xff00.toShort // Used only for FixedMaxUTF8Vector.  Zero length str.

  // Create the fixed-field int for variable length data blobs.  If the result is negative (bit 31 set),
  // then the offset and length are both packed in; otherwise, the fixed int is just an offset to a
  // 4-byte int containing length, followed by the actual blob
  final def blobFixedInt(offset: Int, blobLength: Int): Int =
  if (offset <= MaxSmallOffset && blobLength <= MaxSmallLen) {
    0x80000000 | (offset << SmallLenNBits) | blobLength
  } else {
    offset
  }

  final def smallOff(fixedData: Int): Int = (fixedData & SmallOffsetMask) >> SmallLenNBits
}

/**
 * An iterator optimized for speed and type-specific to avoid boxing.
 * It has no hasNext() method - because it is guaranteed to visit every element, and this way
 * you can avoid another method call for performance.
 */
trait UTF8Iterator extends TypedIterator {
  def next: ZeroCopyUTF8String   // TODO: really, should return native pointer to UTF8StringMedium
}

/**
 * A VectorDataReader object that supports extraction of UTF8 string/blob data BinaryVectors
 */
trait UTF8VectorDataReader extends VectorDataReader {
  /**
   * Retrieves the element at position/row n, where n=0 is the first element of the vector.
   */
  def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): ZeroCopyUTF8String

  /**
   * Returns a UTF8Iterator to efficiently go through the elements of the vector.  The user is responsible for
   * knowing how many elements to process.  There is no hasNext.
   * All elements are iterated through, even those designated as "not available".
   * Costs an allocation for the iterator but allows potential performance gains too.
   * @param vector the BinaryVectorPtr native address of the BinaryVector
   * @param startElement the starting element # in the vector, by default 0 (the first one)
   */
  def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): UTF8Iterator

  def debugString(acc: MemoryReader, vector: BinaryVectorPtr, sep: String = "\n"): String = {
    val it = iterate(acc, vector)
    val size = length(acc, vector)
    (0 to size).map(_ => it.next).mkString(sep)
  }

  /**
   * Converts the BinaryVector to an unboxed Buffer.
   * Only returns elements that are "available".
   */
  // NOTE: I know this code is repeated but I don't want to have to debug specialization/unboxing/traits right now
  def toBuffer(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): Buffer[ZeroCopyUTF8String] = {
    val newBuf = Buffer.empty[ZeroCopyUTF8String]
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
  * A BinaryVector holding UTF8Strings or blobs.
  * A fixed 4-byte area per string + offset to the actual blob.  Overhead of at least 4 bytes per string.
  * O(1) random access.
  *
  * Layout:
  * +0   numBytes   total number of bytes
  * +4   WireFormat word
  * +8   word       number of elements
  * +12...nElems*4   each string has 32-bit word, which contains both offset+length or just offset
  */
object UTF8FlexibleVectorDataReader extends UTF8VectorDataReader {
  import UTF8Vector._

  final def length(acc: MemoryReader, vector: BinaryVectorPtr): Int = acc.getInt(vector + 8)
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, index: Int): ZeroCopyUTF8String = {
    val fixedData = acc.getInt(vector + 12 + index * 4)
    if (fixedData != NABlob) {
      val utf8addr = vector + (if (fixedData < 0) smallOff(fixedData) else (fixedData))
      val utf8len = if (fixedData < 0) fixedData & MaxSmallLen else acc.getInt(vector + fixedData)
      new ZeroCopyUTF8String(acc.base, acc.baseOffset + utf8addr, utf8len)
    } else {
      ZeroCopyUTF8String.NA
    }
  }

  final def iterate(acc: MemoryReader, vector: BinaryVectorPtr,
                    startElement: Int = 0): UTF8Iterator = new UTF8Iterator {
    private final var n = startElement
    final def next: ZeroCopyUTF8String = {
      val data = apply(acc, vector, n)
      n += 1
      data
    }
  }

  override def iterateAvailable(acc: MemoryReader, vector: BinaryVectorPtr,
                                startElement: Int = 0): BooleanIterator = new BooleanIterator {
    private final var fixedDataPtr = vector + 12 + startElement * 4
    final def next: Boolean = {
      val avail = acc.getInt(fixedDataPtr) != NABlob
      fixedDataPtr += 4
      avail
    }
  }
}

/**
  * The appendable (and readable) version of UTF8Vector.  Copies original strings into new space - so be
  * sure this is what you want.
  */
class UTF8AppendableVector(val addr: BinaryRegion.NativePointer,
                           val maxBytes: Int,
                           maxElements: Int,
                           override val dispose: () => Unit) extends BinaryAppendableVector[ZeroCopyUTF8String] {
  import UTF8Vector._
  import WireFormat._

  private var _len = 0

  override final def length: Int = _len

  override val primaryMaxBytes = 12 + (maxElements * 4)
  private var curFixedOffset = addr + 12
  var numBytes: Int = primaryMaxBytes

  // Write out initial length, WordFormat
  UnsafeUtils.setInt(addr, primaryMaxBytes - 4)
  UnsafeUtils.setInt(addr + 4, WireFormat(VECTORTYPE_BINSIMPLE, SUBTYPE_UTF8))
  UnsafeUtils.setInt(addr + 8, 0)

  final def reset(): Unit = {
    _len = 0
    curFixedOffset = addr + 12
    numBytes = primaryMaxBytes
    UnsafeUtils.setInt(addr + 8, 0)
  }

  override def primaryBytes: Int = (curFixedOffset - addr).toInt

  private def bumpLen(): Unit = {
    _len += 1
    curFixedOffset += 4
    UnsafeUtils.setInt(addr + 8, _len)
  }

  final def addData(data: ZeroCopyUTF8String): AddResponse = checkSize(length + 1, maxElements) match {
    case Ack =>
      assert(data.length < MaxStringLen)
      val fixedData = appendBlob(data)
      UnsafeUtils.setInt(curFixedOffset, fixedData)
      bumpLen()
      Ack
    case other: AddResponse => other
  }

  final def addNA(): AddResponse = checkSize(length + 1, maxElements) match {
    case Ack =>
      UnsafeUtils.setInt(curFixedOffset, NABlob)
      bumpLen()
      Ack
    case other: AddResponse => other
  }

  final def apply(n: Int): ZeroCopyUTF8String =
    UTF8FlexibleVectorDataReader.apply(nativePtrReader, addr, n)
  final def isAvailable(n: Int): Boolean = UnsafeUtils.getInt(addr + 12 + n * 4) != NABlob
  final def reader: VectorDataReader = UTF8FlexibleVectorDataReader
  def copyToBuffer: Buffer[ZeroCopyUTF8String] =
    UTF8FlexibleVectorDataReader.toBuffer(nativePtrReader, addr)

  final def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = addData(reader.filoUTF8String(col))

  final def isAllNA: Boolean = {
    var fixedOffset = addr + 12
    while (fixedOffset < curFixedOffset) {
      if (UnsafeUtils.getInt(fixedOffset) != NABlob) return false
      fixedOffset += 4
    }
    return true
  }

  final def noNAs: Boolean = {
    var fixedOffset = addr + 12
    while (fixedOffset < curFixedOffset) {
      if (UnsafeUtils.getInt(fixedOffset) == NABlob) return false
      fixedOffset += 4
    }
    return true
  }

  override def newInstance(memFactory: MemFactory, growFactor: Int = 2): UTF8AppendableVector = {
    val newAddr = memFactory.allocateOffheap(maxBytes * growFactor)
    val dispose = () => memFactory.freeMemory(newAddr)
    new UTF8AppendableVector(newAddr, maxBytes * growFactor, maxElements * growFactor, dispose)
  }

  /**
    * Returns the minimum and maximum length (# bytes) of all the elements.
    * Useful for calculating which type of UTF8Vector to use.
    *
    * @return (Int, Int) = (minBytes, maxBytes) of all elements
    */
  final def minMaxStrLen: (Int, Int) = {
    var min = Int.MaxValue
    var max = 0
    cforRange { 0 until _len } { index =>
      val fixedData = UnsafeUtils.getInt(addr + 12 + index * 4)
      if (fixedData != NABlob) {
        val utf8len = if (fixedData < 0) fixedData & MaxSmallLen else UnsafeUtils.getInt(addr + fixedData)
        if (utf8len < min) min = utf8len
        if (utf8len > max) max = utf8len
      }
    }
    (min, max)
  }

  override def finishCompaction(newAddr: BinaryRegion.NativePointer): BinaryVectorPtr = {
    val offsetDiff = -((maxElements - _len) * 4)
    adjustOffsets(UnsafeUtils.ZeroPointer, newAddr, offsetDiff)
    UnsafeUtils.setInt(newAddr, frozenSize - 4)
    newAddr
  }

  override def optimize(memFactory: MemFactory,
                        hint: EncodingHint = AutoDetect): BinaryVectorPtr = hint match {
    case AutoDictString(spaceThreshold, samplingRate) => optimizedVector(memFactory, spaceThreshold, samplingRate)
    case Encodings.DictionaryEncoding => optimizedVector(memFactory, spaceThreshold = 1.1)
    case Encodings.SimpleEncoding     => freeze(memFactory)
    case hint: Any                    => optimizedVector(memFactory)
  }

  def optimizedVector(memFactory: MemFactory,
                      spaceThreshold: Double = 0.6,
                      samplingRate: Double = 0.3): BinaryVectorPtr =
    DictUTF8Vector.shouldMakeDict(memFactory, this, spaceThreshold,
      samplingRate, numBytes).map { dictInfo =>
      if (noNAs && dictInfo.codeMap.size == 1) {
        val firstStr = apply(0)
        ConstVector.make(memFactory, this.length, firstStr.length + 2) { addr =>
          firstStr.copyTo(UnsafeUtils.ZeroPointer, addr + 2)
          UnsafeUtils.setShort(UnsafeUtils.ZeroPointer, addr, firstStr.length.toShort)
        }
      } else {
        DictUTF8Vector.makeVector(memFactory, dictInfo)
      }
    }.getOrElse {
      freeze(memFactory)
    }

  // WARNING: no checking for if delta pushes small offsets out.  Intended for compactions only.
  private def adjustOffsets(newBase: Any, newOff: Long, delta: Int): Unit = {
    cforRange { 0 until _len } { i =>
      val fixedData = UnsafeUtils.getInt(newBase, newOff + 12 + i * 4)
      val newData = if (fixedData < 0) {
        if (fixedData == NABlob) {
          NABlob
        } else {
          val newDelta = smallOff(fixedData) + delta
          blobFixedInt(newDelta, fixedData & MaxSmallLen)
        }
      } else {
        fixedData + delta
      }
      UnsafeUtils.setInt(newBase, newOff + 12 + i * 4, newData)
    }
  }

  /**
    * Reserves space from the variable length area at the end.
    * If it succeeds, the numBytes will be moved up at the end of the call.
    *
    * @return the Long offset at which the variable space starts
    */
  private def reserveVarBytes(bytesToReserve: Int): Long = {
    checkSize(numBytes + bytesToReserve, maxBytes)
    val offsetToWrite = addr + numBytes
    numBytes += bytesToReserve
    offsetToWrite
  }

  /**
    * Appends a variable length blob to the end, returning the 32-bit fixed length data field that either
    * contains both offset and length or just the offset, in which case first 4 bytes in var section contains
    * the length.  Bytes will be copied from original blob.
    */
  private def appendBlob(blob: ZeroCopyBinary): Int = {
    // First, get the fixed int which encodes offset and len and see if we need another 4 bytes for offset
    val fixedData = blobFixedInt(numBytes, blob.length)
    val destAddr = reserveVarBytes(blob.length + (if (fixedData < 0) 0 else 4))
    if (fixedData < 0) {
      blob.copyTo(UnsafeUtils.ZeroPointer, destAddr)
    } else {
      UnsafeUtils.setInt(destAddr, blob.length)
      blob.copyTo(UnsafeUtils.ZeroPointer, destAddr + 4)
    }
    fixedData
  }
}

/**
 * Constant (single value) UTF8 vector.  Format:
 * +0000 length bytes
 * +0004 WireFormat
 * +0008 logical length / number of elements
 * +0012 UTF8StringMedium (2 length bytes followed by UTF8 data)
 */
object UTF8ConstVector extends ConstVector with UTF8VectorDataReader {
  override def length(acc: MemoryReader, vector: BinaryVectorPtr): Int = numElements(acc, vector)
  // TODO: return just a pointer (NativePointer) or a UTF8StringMedium value class
  def apply(acc: MemoryReader, vector: BinaryVectorPtr, i: Int): ZeroCopyUTF8String = {
    new ZeroCopyUTF8String(acc.base, acc.baseOffset +  vector + 14,
      UnsafeUtils.getShort(vector + 12) & 0x0ffff)
  }
  def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): UTF8Iterator = new UTF8Iterator {
    def next: ZeroCopyUTF8String = apply(acc, vector, 0)
  }
}
