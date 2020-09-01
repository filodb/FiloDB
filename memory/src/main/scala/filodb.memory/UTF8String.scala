package filodb.memory

object UTF8String {
  val bytesOfCodePointInUTF8 = Array(2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    5, 5, 5, 5,
    6, 6)

  /**
   * Returns the number of bytes for a code point with the first byte as `b`
   */
  def numBytesForFirstByte(b: Byte): Int = {
    val offset = (b & 0xFF) - 192;
    if (offset >= 0) bytesOfCodePointInUTF8(offset) else 1
  }
}

/**
 * Common traits for binary/UTF8 String types that are also BinaryRegions (have length prefixes)
 */
trait BinaryString extends BinaryRegion {
  import format.UnsafeUtils
  import java.nio.charset.StandardCharsets

  def copyByteArrayTo(bytes: Array[Byte], dest: Any, destOffset: Long): Unit

  def apply(s: String, factory: MemFactory = MemFactory.onHeapFactory): (Any, Long) = {
    apply(s.getBytes(StandardCharsets.UTF_8), factory)
  }

  // Unfortunately we need to prepend the length bytes so need to allocate another byte array :(
  def apply(bytes: Array[Byte], factory: MemFactory): (Any, Long) = {
    require(bytes.size <= 65534, s"Byte array of size ${bytes.size} does not fit into UTF8StringMedium")
    // Add 2 bytes for length
    val (base, offset, nBytes) = factory.allocate(bytes.size + lenBytes)
    copyByteArrayTo(bytes, base, offset)
    (base, offset)
  }

  def matchAt(base: Any, offset: Long, other: Any, otherOffset: Long, pos: Int): Boolean = {
    val numBytesUs = numBytes(base, offset)
    val numBytesOther = numBytes(other, otherOffset)
    if (numBytesOther + pos > numBytesUs || pos < 0) { false }
    else { UnsafeUtils.equate(base, offset + lenBytes + pos, other, otherOffset + lenBytes, numBytesOther) }
  }

  /**
   * Returns true if the UTF8String at (base, offset) starts with the UTF8String at (other, otherOffset)
   */
  final def startsWith(base: Any, offset: Long, other: Any, otherOffset: Long): Boolean =
    matchAt(base, offset, other, otherOffset, 0)

  final def endsWith(base: Any, offset: Long, other: Any, otherOffset: Long): Boolean =
    matchAt(base, offset, other, otherOffset, numBytes(base, offset) - numBytes(other, otherOffset))

  final def toString(base: Any, offset: Long): String = {
    val bytes = asNewByteArray(base, offset)
    new String(bytes, lenBytes, bytes.size - lenBytes, StandardCharsets.UTF_8)
  }
}

/**
 * A "medium length" UTF8 string of < 64KB bytes which can be on or offheap.
 * Since there can be many millions of strings and we want to save heap space, we don't want to create
 * an on-heap object for every string.  Instead, the idea is that we deal with containers of strings, such as
 * UTF8Vectors.  They have two options to avoid object allocation:
 * 1) Be offheap and deal with 64-bit native pointers, which are Long primitives.
 *     The UTF8StringMedium value class can be used to wrap the pointers.
 * 2) Instead of returning a tuple of (base, offset) which results in allocation, design APIs which pass in both
 *     the base and offset, and let the user invoke any methods on the object here as needed.
 */
object UTF8StringMedium extends BinaryString {
  import format.UnsafeUtils
  import UTF8String._

  final def numBytes(base: Any, offset: Long): Int = UnsafeUtils.getShort(base, offset) & 0x0FFFF

  val lenBytes = 2

  def copyByteArrayTo(bytes: Array[Byte], dest: Any, destOffset: Long): Unit = {
    UnsafeUtils.unsafe.copyMemory(bytes, UnsafeUtils.arayOffset, dest, destOffset + lenBytes, bytes.size)
    UnsafeUtils.setShort(dest, destOffset, bytes.size.toShort)
  }

  def copyByteArrayTo(bytes: Array[Byte], byteIndex: Int, len: Int, dest: Any, destOffset: Long): Unit = {
    UnsafeUtils.unsafe.copyMemory(bytes, UnsafeUtils.arayOffset + byteIndex, dest, destOffset + lenBytes, len)
    UnsafeUtils.setShort(dest, destOffset, len.toShort)
  }

  /**
   * Creates a native memory UTF8StringMedium value class.
   * @param factory a MemFactory that uses native memory, NOT the onHeapFactory
   */
  def native(s: String, factory: MemFactory): UTF8StringMedium = {
    val (base, addr) = apply(s, factory)
    require(base == UnsafeUtils.ZeroPointer, s"Native memory was not allocated, you used factory $factory")
    new UTF8StringMedium(addr)
  }

  implicit class StringToUTF8Native(str: String) {
    def utf8(factory: MemFactory): UTF8StringMedium = native(str, factory)
  }

  /**
   * The number of UTF8 characters in the UTF8String
   */
  final def numChars(base: Any, offset: Long): Int = {
    var len = 0
    var curOffset = offset + lenBytes
    val endOffset = curOffset + numBytes(base, offset)
    while (curOffset < endOffset) {
      len += 1
      curOffset += numBytesForFirstByte(UnsafeUtils.getByte(base, curOffset))
    }
    len
  }
}

/**
 * Methods for dealing with strings with a one-byte length prefix
 */
object UTF8StringShort extends BinaryString {
  import format.UnsafeUtils

  final def numBytes(base: Any, offset: Long): Int = UnsafeUtils.getByte(base, offset) & 0x00FF

  val lenBytes = 1

  def copyByteArrayTo(bytes: Array[Byte], dest: Any, destOffset: Long): Unit = {
    require(bytes.size < 256)
    UnsafeUtils.unsafe.copyMemory(bytes, UnsafeUtils.arayOffset, dest, destOffset + lenBytes, bytes.size)
    UnsafeUtils.setByte(dest, destOffset, bytes.size.toByte)
  }

  def copyByteArrayTo(bytes: Array[Byte], byteIndex: Int, len: Int, dest: Any, destOffset: Long): Unit = {
    require(len < 256)
    UnsafeUtils.unsafe.copyMemory(bytes, UnsafeUtils.arayOffset + byteIndex, dest, destOffset + lenBytes, len)
    UnsafeUtils.setByte(dest, destOffset, len.toByte)
  }

  implicit class StringToNative(str: String) {
    def utf8short(factory: MemFactory): BinaryRegion.NativePointer = {
      val (base, addr) = apply(str, factory)
      require(base == UnsafeUtils.ZeroPointer, s"Native memory was not allocated, you used factory $factory")
      addr
    }
  }
}

/**
 * A value class for UTF8StringMedium's.  Note that value classes can get boxed under many circumstances.
 * Also this is only allowed for native memory strings where we have a stable pointer.
 *
 * NOTE: I'm not really sure if having a value class is any use.  You cannot even redefine hashCode and equals!
 */
class UTF8StringMedium(val address: BinaryRegion.NativePointer) extends AnyVal {
  import format.UnsafeUtils._

  def numBytes: Int = UTF8StringMedium.numBytes(address)

  // String length = number of characters
  def length: Int = UTF8StringMedium.numChars(ZeroPointer, address)

  def compare(other: UTF8StringMedium): Int = UTF8StringMedium.compare(address, other.address)

  def startsWith(other: UTF8StringMedium): Boolean =
    UTF8StringMedium.startsWith(ZeroPointer, address, ZeroPointer, other.address)

  def endsWith(other: UTF8StringMedium): Boolean =
    UTF8StringMedium.endsWith(ZeroPointer, address, ZeroPointer, other.address)

  // NOTE: value objects are not allowed to redefine hashCode method
  def hash32: Int = UTF8StringMedium.hashCode(ZeroPointer, address)
  def hash64: Long = UTF8StringMedium.hashCode64(ZeroPointer, address)

  override def toString: String = UTF8StringMedium.toString(ZeroPointer, address)
}
