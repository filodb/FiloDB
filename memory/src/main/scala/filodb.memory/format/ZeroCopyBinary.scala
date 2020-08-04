package filodb.memory.format

import net.jpountz.xxhash.XXHashFactory
import spire.syntax.cfor._

import filodb.memory.{MemFactory, UTF8StringMedium}

/**
 * Essentially like a (void *) pointer to an untyped binary blob which supports very basic operations.
 * Allows us to do zero-copy comparisons and other ops.
 * Intended for small blobs like UTF8-encoded strings.
 * The API is immutable, but the underlying bytes could change- but that is not recommended.
 */
trait ZeroCopyBinary extends Ordered[ZeroCopyBinary] {
  import ZeroCopyBinary._

  def base: Any
  def offset: Long
  def numBytes: Int

  def length: Int = numBytes

  /**
   * Compares byte by byte starting with byte 0
   */
  def compare(other: ZeroCopyBinary): Int = {
    val minLen = Math.min(numBytes, other.numBytes)
    // TODO: compare 4 bytes at a time, or even 8
    val minLenAligned = minLen & -4
    val wordComp = UnsafeUtils.wordCompare(base, offset, other.base, other.offset, minLenAligned)
    if (wordComp == 0) {
      cforRange { minLenAligned until minLen } { i =>
        val res = getByte(i) - other.getByte(i)
        if (res != 0) return res
      }
      return numBytes - other.numBytes
    } else wordComp
  }

  final def getByte(byteNum: Int): Byte = UnsafeUtils.getByte(base, offset + byteNum)

  final def copyTo(dest: Any, destOffset: Long, delta: Int = 0, n: Int = numBytes): Unit =
    UnsafeUtils.unsafe.copyMemory(base, offset + delta, dest, destOffset, n)

  final def asNewByteArray: Array[Byte] = {
    val newArray = new Array[Byte](numBytes)
    copyTo(newArray, UnsafeUtils.arayOffset)
    newArray
  }

  // Temporary: convert to a UTF8StringMedium
  final def toUTF8StringMedium(factory: MemFactory): UTF8StringMedium = {
    require(numBytes <= 65534)
    val (newBase, newOffset, _) = factory.allocate(numBytes + 2)
    require(newBase == UnsafeUtils.ZeroPointer, s"Native memory was not allocated, you used factory $factory")
    UnsafeUtils.unsafe.copyMemory(base, offset, newBase, newOffset + 2, numBytes)
    UnsafeUtils.setShort(newBase, newOffset, numBytes.toShort)
    new UTF8StringMedium(newOffset)
  }

  /**
   * Returns an array of bytes.  If this ZeroCopyBinary is already a byte array
   * with exactly numBytes bytes, then just return that, to avoid another copy.
   * Otherwise, call asNewByteArray to return a copy.
   */
  def bytes: Array[Byte] = {
    //scalastyle:off
    if (base != null && base.isInstanceOf[Array[Byte]] && offset == UnsafeUtils.arayOffset) {
      //scalastyle:on
      base.asInstanceOf[Array[Byte]]
    } else {
      asNewByteArray
    }
  }

  override def equals(other: Any): Boolean = other match {
    case z: ZeroCopyBinary =>
      (numBytes == z.numBytes) && UnsafeUtils.equate(base, offset, z.base, z.offset, numBytes)
    case o: Any => false
    case UnsafeUtils.ZeroPointer => false
  }

  private var hash64: Long = -1L

  // Ideally, hash without copying to another byte array, esp if the base storage is a byte array already
  def cachedHash64: Long = {
    if (hash64 == -1L) {
      val hash = base match {
        case a: Array[Byte] => hasher64.hash(a, offset.toInt - UnsafeUtils.arayOffset, numBytes, Seed)
        case o: Any         => hasher64.hash(asNewByteArray, 0, numBytes, Seed)
        case UnsafeUtils.ZeroPointer => hasher64.hash(asNewByteArray, 0, numBytes, Seed)
      }
      hash64 = hash
    }
    hash64
  }

  override def hashCode: Int = cachedHash64.toInt ^ (cachedHash64 >> 32).toInt
}

object ZeroCopyBinary {
  // NOTE: fastestInstance sometimes returns JNI lib, which seems much slower for shorter strings
  val xxhashFactory = XXHashFactory.fastestJavaInstance
  val hasher32 = xxhashFactory.hash32
  val hasher64 = xxhashFactory.hash64
  val Seed = 0x9747b28c
}

/**
 * A zero-copy UTF8 string class
 * Not intended for general purpose use, mostly for fast comparisons and sorts without the need to
 * deserialize to a regular Java string
 */
// scalastyle:off
// FIXME Needs to use MemoryAccessor
final class ZeroCopyUTF8String(val base: Any, val offset: Long, val numBytes: Int) extends ZeroCopyBinary {
  import ZeroCopyUTF8String._
  import filodb.memory.UTF8String._

  final def asNewString: String = new String(asNewByteArray)
  override def toString: String = asNewString

  final def numChars: Int = {
    var len = 0
    var i = 0
    while (i < numBytes) {
      len += 1
      i += numBytesForFirstByte(getByte(i))
    }
    len
  }

  /**
   * Returns a substring of this.  The returned string does not have bytes copied; simply different
   * pointers to the same area of memory.  This is possible because ZCB's are immutable.
   * @param start the position of first code point
   * @param until the position after last code point, exclusive.
   */
  final def substring(start: Int, until: Int): ZeroCopyUTF8String = {
    if (until <= start || start >= numBytes) {
      empty
    } else {
      var i = 0
      var c = 0
      while (i < numBytes && c < start) {
        i += numBytesForFirstByte(getByte(i))
        c += 1
      }

      val j = i
      while (i < numBytes && c < until) {
        i += numBytesForFirstByte(getByte(i))
        c += 1
      }

      if (i > j) new ZeroCopyUTF8String(base, offset + j, i - j) else empty
    }
  }

  override def equals(other: Any): Boolean = other match {
    case u: UTF8Wrapper => super.equals(u.utf8)
    case o: Any         => super.equals(o)
  }

  private def matchAt(s: ZeroCopyUTF8String, pos: Int): Boolean =
    if (s.numBytes + pos > numBytes || pos < 0) { false }
    else { UnsafeUtils.equate(base, offset + pos, s.base, s.offset, s.numBytes) }

  final def startsWith(prefix: ZeroCopyUTF8String): Boolean = matchAt(prefix, 0)

  final def endsWith(suffix: ZeroCopyUTF8String): Boolean = matchAt(suffix, numBytes - suffix.numBytes)

  final def contains(substring: ZeroCopyUTF8String): Boolean =
    if (substring.numBytes == 0) { true }
    else {
      val firstByte = substring.getByte(0)
      cforRange { 0 to (numBytes - substring.numBytes) } { i =>
        if (getByte(i) == firstByte && matchAt(substring, i)) return true
      }
      false
    }
}

@SerialVersionUID(1012L)
case class UTF8Wrapper(var utf8: ZeroCopyUTF8String) extends java.io.Externalizable {
  //scalastyle:off
  def this() = this(null)
  //scalastyle:on

  override def equals(other: Any): Boolean = other match {
    case u: UTF8Wrapper => u.utf8 == this.utf8
    case z: ZeroCopyUTF8String => this.utf8 == z
    case o: Any         => super.equals(o)
  }

  override def hashCode: Int = utf8.hashCode

  override def toString: String = utf8.toString

  def writeExternal(out: java.io.ObjectOutput): Unit = {
    out.writeInt(utf8.length)
    out.write(utf8.bytes)
  }
  def readExternal(in: java.io.ObjectInput): Unit = {
    val utf8Bytes = new Array[Byte](in.readInt())
    in.readFully(utf8Bytes, 0, utf8Bytes.size)
    utf8 = ZeroCopyUTF8String(utf8Bytes)
  }
}

object ZeroCopyUTF8String {
  def apply(bytes: Array[Byte]): ZeroCopyUTF8String =
    new ZeroCopyUTF8String(bytes, UnsafeUtils.arayOffset, bytes.size)

  def apply(bytes: Array[Byte], offset: Int, len: Int): ZeroCopyUTF8String = {
    require(offset + len <= bytes.size, s"offset + len ($offset + $len) exceeds size ${bytes.size}")
    new ZeroCopyUTF8String(bytes, UnsafeUtils.arayOffset + offset, len)
  }

  def apply(str: String): ZeroCopyUTF8String = apply(str.getBytes("UTF-8"))

  val empty = ZeroCopyUTF8String("")

  // The official ZeroCopyUTF8String instance designated to equal NA / not available
  val NA = empty

  final def isNA(utf8: ZeroCopyUTF8String): Boolean = utf8.base == NA.base

  implicit class StringToUTF8(str: String) {
    def utf8: ZeroCopyUTF8String = ZeroCopyUTF8String(str)
  }

  implicit object ZeroCopyUTF8BinaryOrdering extends Ordering[ZeroCopyUTF8String] {
    def compare(a: ZeroCopyUTF8String, b: ZeroCopyUTF8String): Int = a.compare(b)
  }
}
