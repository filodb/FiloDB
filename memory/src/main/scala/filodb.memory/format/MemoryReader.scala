package filodb.memory.format

import java.nio.ByteBuffer

import com.kenai.jffi.MemoryIO
import org.agrona.DirectBuffer

import filodb.memory.format.UnsafeUtils._

/**
  * Factory methods for Memory Readers
  */
object MemoryReader {
  def fromArray(array: Array[Byte]): MemoryReader = MemoryAccessor.fromArray(array)
  def nativePtrReader: MemoryReader = MemoryAccessor.nativePtrAccessor
  def fromByteBuffer(buf: ByteBuffer): MemoryReader = MemoryAccessor.fromByteBuffer(buf)
}

/**
  * Abstraction for reading from memory byte sequence. Clients can used this
  * abstraction without worrying whether underlying memory could be
  * on-heap or off-heap.
  *
  * The `addr` parameter in the methods below indicate the address of the
  * memory position relative to the start of the memory region or byte sequence represented by the
  * implementing class. IMPORTANT to note that the addr parameter is NOT same as the unsafe
  * offset for any given on-heap object.
  *
  * The accessor/address pair is first formed during the time when the bytes are first written into
  * memory. Once the address is formed, clients can read data using the reader methods. Address
  * arithmetic can be done to access bytes relative to the original address.
  *
  * Abstraction to the memory access is formed by pairing both the accessor along with an address.
  * One item alone does not provide the abstraction. We could have combined the two into one java object,
  * but that would only be possible if we paid the cost of performance due to java object allocation.
  */
sealed trait MemoryReader {

  /**
    * The On-Heap java object used to perform unsafe access.
    *
    * TODO in next iteration: make this protected
    */
  def base: Array[Byte]

  /**
    * Offset to the start of the memory region represented by this accessor. Typically,
    * all unsafe access is done by adding this amount to the `addr` method parameter in order
    * to read the bytes stored.
    *
    * TODO in next iteration: make this protected
    */
  def baseOffset: Long
  def wrapInto(buf: DirectBuffer, addr: Long, length: Int): Unit
  final def getByte(addr: Long): Byte = unsafe.getByte(base, baseOffset + addr)
  final def getShort(addr: Long): Short = unsafe.getShort(base, baseOffset + addr)
  final def getInt(addr: Long): Int = unsafe.getInt(base, baseOffset + addr)
  final def getIntVolatile(addr: Long): Int = unsafe.getIntVolatile(base, baseOffset + addr)
  final def getLong(addr: Long): Long = unsafe.getLong(base, baseOffset + addr)
  final def getLongVolatile(addr: Long): Long = unsafe.getLongVolatile(base, baseOffset + addr)
  final def getDouble(addr: Long): Double = unsafe.getDouble(base, baseOffset + addr)
  final def getFloat(addr: Long): Double = unsafe.getFloat(base, baseOffset + addr)
  final def copy(fromAddr: Long, toAcc: MemoryAccessor, toAddr: Long, numBytes: Int): Unit =
    UnsafeUtils.copy(base, baseOffset + fromAddr, toAcc.base, toAcc.baseOffset + toAddr, numBytes)
}

/**
  * Factory methods for Memory Accessors
  */
object MemoryAccessor {
  def fromArray(array: Array[Byte]): MemoryAccessor = new ByteArrayAccessor(array)
  def nativePtrAccessor: MemoryAccessor = NativePointerAccessor
  def fromByteBuffer(buf: ByteBuffer): MemoryAccessor = {
    if (buf.isDirect) DirectBufferAccessor(buf)
    else new OnHeapByteBufferAccessor(buf)
  }
}

/**
  * Abstraction for reading and writing to memory byte sequence. Clients can used this
  * abstraction without worrying whether underlying memory could be
  * on-heap or off-heap.
  *
  * The `addr` parameter in the methods below indicate the address of the
  * memory position relative to the start of the memory region or byte sequence represented by the
  * implementing class. IMPORTANT to note that the addr parameter is NOT same as the unsafe
  * offset for any given on-heap object.
  *
  * The accessor/address pair is first formed during the time when the bytes are first written into
  * memory. Once the address is formed, clients can read data using the reader methods. Address
  * arithmetic can be done to access bytes relative to the original address.
  *
  * Abstraction to the memory access is formed by pairing both the accessor along with an address.
  * One item alone does not provide the abstraction. We could have combined the two into one java object,
  * but that would only be possible if we paid the cost of performance due to java object allocation.
  */
sealed trait MemoryAccessor extends MemoryReader {
  final def setByte(addr: Long, byt: Byte): Unit = unsafe.putByte(base, baseOffset + addr, byt)
  final def setShort(addr: Long, s: Short): Unit = unsafe.putShort(base, baseOffset + addr, s)
  final def setInt(addr: Long, i: Int): Unit = unsafe.putInt(base, baseOffset + addr, i)
  final def setIntVolatile(addr: Long, i: Int): Unit = unsafe.putIntVolatile(base, baseOffset + addr, i)
  final def setLong(addr: Long, l: Long): Unit = unsafe.putLong(base, baseOffset + addr, l)
  final def setDouble(addr: Long, d: Double): Unit = unsafe.putDouble(base, baseOffset + addr, d)
  final def setFloat(addr: Long, f: Float): Unit = unsafe.putFloat(base, baseOffset + addr, f)
  //  def wordCompare(thisOffset: Long, destObj: MemoryBase, destOffset: Long, n: Int): Int
  //  def compareTo(offset1: Long, numBytes1: Int, base2: MemoryBase, offset2: Long, numBytes2: Int): Int
}

/**
  * Implementation where the addressable region is all of native memory. Address parameter
  * in methods are relative to the first byte of RAM, and hence translate to a native pointers.
  *
  * This is a singleton implementation to avoid allocation per pointer.
  *
  * One could envision an alternate implementation which pays the cost of an on-heap
  * allocation but additionally provides bounds checks for safer memory access.
  */
object NativePointerAccessor extends MemoryAccessor {
  // TODO check bounds of array before accessing
  val base = ZeroArray
  val baseOffset: Long = 0

  override def wrapInto(buf: DirectBuffer, addr: Long, length: Int): Unit = {
    buf.wrap(addr, length)
  }
}

/**
  * Implementation where addressable memory is within a byte array. The address parameter
  * in methods refers to memory positions relative to the beginning of the array.
  */
class ByteArrayAccessor(val base: Array[Byte]) extends MemoryAccessor {
  // TODO check bounds of array before accessing
  val baseOffset: Long = UnsafeUtils.arayOffset

  override def wrapInto(buf: DirectBuffer, addr: Long, length: Int): Unit = {
    buf.wrap(base, addr.toInt, length)
  }
}

/**
  * Implementation where addressable memory is within an on-heap byte buffer. The `addr` parameter
  * in methods refers to memory positions relative to the position of the byte
  * buffer at the time of construction
  */
class OnHeapByteBufferAccessor(buf: ByteBuffer) extends MemoryAccessor {

  var base: Array[Byte] = _
  var baseOffset: Long = _
  var length: Int = _
  val origBufPos = buf.position()

  // TODO check bounds of array before accessing
  require (!buf.isDirect, "buf arg cannot be a DirectBuffer")
  if (buf.hasArray) {
    base = buf.array
    baseOffset = arayOffset.toLong + buf.arrayOffset + buf.position()
    length = buf.limit() - buf.position()
  } else {
    assert(buf.isReadOnly)
    base = unsafe.getObject(buf, byteBufArrayField).asInstanceOf[Array[Byte]]
    baseOffset = unsafe.getInt(buf, byteBufOffsetField) + arayOffset.toLong
    length = buf.limit() - buf.position()
  }

  override def wrapInto(dBuf: DirectBuffer, addr: Long, length: Int): Unit = {
    dBuf.wrap(buf, origBufPos + addr.toInt, length)
  }

}

/**
  * Implementation where addressable memory is within an off-heap byte buffer,
  * essentially a direct byte buffer. The `addr` parameter
  * in methods refers to memory positions relative to the position of the byte
  * buffer at the time of construction.
  */
case class DirectBufferAccessor(buf: ByteBuffer) extends MemoryAccessor {

  // TODO check bounds of array before accessing
  require (buf.isDirect, "buf arg needs to be a DirectBuffer")
  val address = MemoryIO.getCheckedInstance.getDirectBufferAddress(buf)
  val base: Array[Byte] = UnsafeUtils.ZeroArray
  val baseOffset: Long = address + buf.position()
  val length: Int = buf.limit() - buf.position()

  override def wrapInto(dBuf: DirectBuffer, addr: Long, length: Int): Unit = {
    dBuf.wrap(buf, addr.toInt, length)
  }

}
