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
  * memory position relative to the start of the byte sequence represented by the
  * implementing class. IMPORTANT to note that the addr parameter is NOT same as the unsafe
  * offset for any given on-heap object.
  */
sealed trait MemoryReader {
  def base: Any
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
  * memory position relative to the start of the byte sequence represented by the
  * implementing class. IMPORTANT to note that the addr parameter is NOT same as the unsafe
  * offset for any given on-heap object.
  */
sealed trait MemoryAccessor extends MemoryReader {
  final def setByte(addr: Long, byt: Byte): Unit = unsafe.putByte(base, baseOffset + addr, byt)
  final def setShort(addr: Long, s: Short): Unit = unsafe.putShort(base, baseOffset + addr, s)
  final def setInt(addr: Long, i: Int): Unit = unsafe.putInt(base, baseOffset + addr, i)
  final def setIntVolatile(addr: Long, i: Int): Unit = unsafe.putIntVolatile(base, baseOffset + addr, i)
  final def setLong(addr: Long, l: Long): Unit = unsafe.putLong(base, baseOffset + addr, l)
  final def setDouble(addr: Long, d: Double): Unit = unsafe.putDouble(base, baseOffset + addr, d)
  final def setFloat(addr: Long, f: Float): Unit = unsafe.putFloat(base, baseOffset + addr, f)
  final def copy(fromAddr: Long, toAcc: MemoryReader, toAddr: Long, numBytes: Int): Unit =
    UnsafeUtils.copy(base, baseOffset + fromAddr, toAcc.base, toAcc.baseOffset + toAddr, numBytes)
  //  def wordCompare(thisOffset: Long, destObj: MemoryBase, destOffset: Long, n: Int): Int
  //  def compareTo(offset1: Long, numBytes1: Int, base2: MemoryBase, offset2: Long, numBytes2: Int): Int
}

/**
  * Implementation used to access a native pointer. The `addr` parameter in methods imply
  * a raw native pointer.
  *
  * This is a static implementation of MemoryReader to avoid allocation
  * per pointer.
  *
  * One could envision an alternate implementation which pays the cost of an on-heap
  * allocation but additionally provides bounds checks for safe memory access.
  */
object NativePointerAccessor extends MemoryAccessor {
  // TODO check bounds of array before accessing
  val base = ZeroPointer
  val baseOffset: Long = 0

  override def wrapInto(buf: DirectBuffer, addr: Long, length: Int): Unit = {
    buf.wrap(addr, length)
  }
}

/**
  * Implementation used to access a bytes within an on-heap byte array. The `addr` parameter
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
  * Implementation used to access a bytes within an on-heap byte buffer. The `addr` parameter
  * in methods refers to memory positions relative to the current position of the byte buffer.
  */
class OnHeapByteBufferAccessor(buf: ByteBuffer) extends MemoryAccessor {

  var base: Any = _
  var baseOffset: Long = _
  var length: Int = _

  // TODO check bounds of array before accessing
  require (!buf.isDirect, "buf arg cannot be a DirectBuffer")
  if (buf.hasArray) {
    base = buf.array
    baseOffset = arayOffset.toLong + buf.arrayOffset + buf.position()
    length = buf.limit() - buf.position()
  } else {
    assert(buf.isReadOnly)
    base = unsafe.getObject(buf, byteBufArrayField)
    baseOffset = unsafe.getInt(buf, byteBufOffsetField) + arayOffset.toLong
    length = buf.limit() - buf.position()
  }

  override def wrapInto(dBuf: DirectBuffer, addr: Long, length: Int): Unit = {
    dBuf.wrap(buf, addr.toInt, length)
  }

}

/**
  * Implementation used to access bytes within an off-heap, direct byte buffer. The `addr` parameter
  * in methods refers to memory positions relative to the current position of the byte buffer.
  */
case class DirectBufferAccessor(buf: ByteBuffer) extends MemoryAccessor {

  var base: Any = _
  var baseOffset: Long = _
  var length: Int = _

  // TODO check bounds of array before accessing
  require (buf.isDirect, "buf arg needs to be a DirectBuffer")
  val address = MemoryIO.getCheckedInstance.getDirectBufferAddress(buf)
  base = UnsafeUtils.ZeroPointer
  baseOffset = address + buf.position()
  length = buf.limit() - buf.position()

  override def wrapInto(dBuf: DirectBuffer, addr: Long, length: Int): Unit = {
    dBuf.wrap(buf, addr.toInt, length)
  }

}

