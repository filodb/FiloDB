package filodb.memory.data

import debox.Buffer

import filodb.memory.BinaryRegion.NativePointer

/**
 * An unboxed iterator over SortedIDMap elements which are native 64-bit Long pointers
 */
trait ElementIterator {
  def hasNext: Boolean
  def next: NativePointer

  def toBuffer: Buffer[NativePointer] = {
    val buf = Buffer.empty[NativePointer]
    while (hasNext) buf += next
    buf
  }

  def count: Int = {
    var _count = 0
    while (hasNext) {
      _count += 1
      next
    }
    _count
  }
}