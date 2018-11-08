package filodb.memory.data

import debox.Buffer

import filodb.memory.BinaryRegion.NativePointer

/**
 * An unboxed iterator over SortedIDMap elements which are native 64-bit Long pointers.
 * When constructed, the iterator holds a shared lock over the backing collection, to protect
 * the contents of the native pointers. The close method must be called when the native pointers
 * don't need to be accessed anymore, and then the lock is released.
 */
trait ElementIterator {
  def close(): Unit
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

/**
 * Lazily instantiates a wrapped iterator until hasNext or next is called.
 */
//scalastyle:off
class LazyElementIterator(source: () => ElementIterator) extends ElementIterator {
  private var it: ElementIterator = _

  // Note: If close is called before the iterator is assigned, then there's seemingly no
  // reason to go to the source and create an iterator just to close it. Doing so anyhow
  // ensures that any side effects from constructing the iterator are observed, and it
  // also ensures that a closed iterator stays closed.
  override def close(): Unit = sourceIt().close()

  override def hasNext: Boolean = sourceIt().hasNext

  override def next: NativePointer = sourceIt().next

  private def sourceIt(): ElementIterator = {
    if (it == null) it = source()
    it
  }
}
//scalastyle:on
