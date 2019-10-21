package filodb.memory.format

import java.nio.ByteBuffer

import com.kenai.jffi.MemoryIO

import filodb.memory.format.UnsafeUtils._

object MemoryAccessor {

  def fromArray(array: Array[Byte]): MemoryAccessor = ByteArrayAccessor(array)
  def nativePointer: MemoryAccessor = NativePointerAccessor
  def fromByteBuffer(buf: ByteBuffer): MemoryAccessor = {
    if (buf.isDirect) DirectBufferAccessor(buf)
    else OnHeapByteBufferAccessor(buf)
  }
}

trait MemoryAccessor {

  def base: Any
  def baseOffset: Long

  final def getByte(offset: Long): Byte = unsafe.getByte(base, baseOffset + offset)
  final def getShort(offset: Long): Short = unsafe.getShort(base, baseOffset + offset)
  final def getInt(offset: Long): Int = unsafe.getInt(base, baseOffset + offset)
  final def getIntVolatile(offset: Long): Int = unsafe.getIntVolatile(base, baseOffset + offset)
  final def getLong(offset: Long): Long = unsafe.getLong(base, baseOffset + offset)
  final def getLongVolatile(offset: Long): Long = unsafe.getLongVolatile(base, baseOffset + offset)
  final def getDouble(offset: Long): Double = unsafe.getDouble(base, baseOffset + offset)
  final def getFloat(offset: Long): Double = unsafe.getFloat(base, baseOffset + offset)

  final def setByte(offset: Long, byt: Byte): Unit = unsafe.putByte(base, baseOffset + offset, byt)
  final def setShort(offset: Long, s: Short): Unit = unsafe.putShort(base, baseOffset + offset, s)
  final def setInt(offset: Long, i: Int): Unit = unsafe.putInt(base, baseOffset + offset, i)
  final def setIntVolatile(offset: Long, i: Int): Unit = unsafe.putIntVolatile(base, baseOffset + offset, i)
  final def setLong(offset: Long, l: Long): Unit = unsafe.putLong(base, baseOffset + offset, l)
  final def setDouble(offset: Long, d: Double): Unit = unsafe.putDouble(base, baseOffset + offset, d)
  final def setFloat(offset: Long, f: Float): Unit = unsafe.putFloat(base, baseOffset + offset, f)

  //  def copyTo(offset: Long, dest: MemoryBase, destOffset: Long, numBytes: Long): Unit
  //
  //  def copyFrom(src: MemoryBase, srcOffset: Long, offset: Long, numBytes: Long): Unit
  //
  //  def wordCompare(thisOffset: Long, destObj: MemoryBase, destOffset: Long, n: Int): Int
  //  def compareTo(offset1: Long, numBytes1: Int, base2: MemoryBase, offset2: Long, numBytes2: Int): Int

}

object NativePointerAccessor extends MemoryAccessor {
  val base = ZeroPointer
  val baseOffset: Long = 0
}

case class ByteArrayAccessor(base: Array[Byte]) extends MemoryAccessor {
  // TODO check bounds of array before accessing
  val baseOffset: Long = UnsafeUtils.arayOffset
}

case class OnHeapByteBufferAccessor(buf: ByteBuffer) extends MemoryAccessor {

  var base: Any = _
  var baseOffset: Long = _
  var length: Int = _

  // TODO check bounds of array before accessing
  if (buf.hasArray) {
    base = buf.array
    baseOffset = arayOffset.toLong + buf.arrayOffset + buf.position()
    length = buf.limit() - buf.position()
  } else if (buf.isDirect) {
    throw new IllegalArgumentException("This is a DirectBuffer. Use DirectBufferAccessor.")
  } else {
    assert(buf.isReadOnly)
    base = unsafe.getObject(buf, byteBufArrayField).asInstanceOf[Array[Byte]]
    baseOffset = unsafe.getInt(buf, byteBufOffsetField) + arayOffset.toLong
    length = buf.limit() - buf.position()
  }
}

case class DirectBufferAccessor(buf: ByteBuffer) extends MemoryAccessor {

  var base: Any = _
  var baseOffset: Long = _
  var length: Int = _

  // TODO check bounds of array before accessing
  require (!buf.isDirect, "buf arg needs to be a DirectBuffer")
  val address = MemoryIO.getCheckedInstance.getDirectBufferAddress(buf)
  base = UnsafeUtils.ZeroPointer
  baseOffset = address + buf.position()
  length = buf.limit() - buf.position()

}

