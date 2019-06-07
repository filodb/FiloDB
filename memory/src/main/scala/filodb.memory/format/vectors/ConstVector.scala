package filodb.memory.format.vectors

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr

/**
 * Layout of a ConstVector:
 * +0000 length bytes
 * +0004 WireFormat
 * +0008 logical length / number of elements
 * +0012 the constant data
 */
object ConstVector {
  import BinaryRegion.NativePointer
  import UnsafeUtils.ZeroPointer

  val DataOffset = 12

  /**
   * Allocates, fills, and returns pointer for a ConstVector.
   * @param len the logical length or # of repeats
   * @param neededBytes the bytes needed for one element
   * @param fillBytes a function to fill out bytes at given native pointer address
   * @return the BinaryVectorPtr for the ConstVector
   */
  def make(memFactory: MemFactory, len: Int, neededBytes: Int)(fillBytes: NativePointer => Unit): BinaryVectorPtr = {
    val (_, addr, _) = memFactory.allocate(12 + neededBytes)
    UnsafeUtils.setInt(ZeroPointer, addr, 8 + neededBytes)
    UnsafeUtils.setInt(ZeroPointer, addr + 4, WireFormat(WireFormat.VECTORTYPE_BINSIMPLE, WireFormat.SUBTYPE_REPEATED))
    UnsafeUtils.setInt(ZeroPointer, addr + 8, len)
    fillBytes(addr + DataOffset)
    addr
  }
}

trait ConstVector {
  def numElements(addr: BinaryVectorPtr): Int = UnsafeUtils.getInt(addr + 8)
}
