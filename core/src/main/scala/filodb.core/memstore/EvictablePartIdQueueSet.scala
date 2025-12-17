package filodb.core.memstore

import java.util.NoSuchElementException

import filodb.memory.format.UnsafeUtils

/**
 * Specialized ordered set for integers to maintain list of integer
 * partIds without boxing them. Duplicates are not stored multiple times.
 * One could use TreeSet like data structures, but this optimization is intended to
 * lower memory usage.
 *
 * Collection threadsafe due to synchronized methods. Not expected to produce high throughput.
 */
class EvictablePartIdQueueSet(initSize: Int) {

  // package private for unit test validation. Dont access
  private[memstore] val arr = new IntArrayQueue(initSize)
  private[memstore] val set = new scala.collection.mutable.HashSet[Int]()

  def put(partId: Int): Unit = synchronized {
    if (!set(partId)) {
      set += partId
      arr.add(partId)
    }
  }

  /**
   * Remove `numItemsToRemove` items from queue set and place into sink.
   * The sink may not have requested number of items if collection did not have
   * that many in the first place.
   */
  def removeInto(numItemsToRemove: Int, sink: scala.collection.mutable.ArrayBuffer[Int]): Unit = synchronized {
    var numRemoved = 0
    while (numRemoved < numItemsToRemove && !set.isEmpty) {
      val partId = arr.remove()
      set.remove(partId)
      sink += partId
      numRemoved += 1
    }
  }

  def size: Int = synchronized {
    set.size
  }
}

/**
 * A simple array based circular queue that allows indexed access to its elements.
 * Array doubles itself when it reaches capacity.
 */
class IntArrayQueue(initialSize: Int = 8) {

  private val minSize = 8
  // package private for unit test validation. Dont access
  private[memstore] var items: Array[Int] = UnsafeUtils.ZeroPointer.asInstanceOf[Array[Int]]
  private var hd: Int = 0 // Points to next item to be removed if non-empty
  private var tl: Int = 0 // Next empty position to add if position is available

  items = allocateArray(initialSize)

  private def allocateArray(n: Int): Array[Int] = {
    var size = Math.max(n, minSize)
    if (n >= size) {
      size |= (size >>> 1)
      size |= (size >>> 2)
      size |= (size >>> 4)
      size |= (size >>> 8)
      size |= (size >>> 16)
      size += 1
      if (size < 0) {
        size >>>= 1
      }
    }
    new Array[Int](size)
  }

  private def doubleCapacity(): Unit = {
    require(hd == tl)
    val initialSize = items.length
    val numLeftOfHead = hd
    val numRightOfHead = items.length - numLeftOfHead
    val newCapacity = items.length << 1
    if (newCapacity < 0) throw new IllegalStateException("Overflow. Cannot allocate.")
    val newItems = new Array[Int](newCapacity)
    Array.copy(items, numLeftOfHead, newItems, 0, numRightOfHead)
    Array.copy(items, 0, newItems, numRightOfHead, numLeftOfHead)
    items = newItems
    hd = 0
    tl = initialSize
  }

  def add(e: Int): Unit = {
    items(tl) = e
    tl = (tl + 1) & (items.length - 1) // can do & instead of % since power of 2
    if (tl == hd) doubleCapacity()
  }

  def size: Int = (tl - hd) & (items.length - 1)

  def isEmpty: Boolean = size == 0

  def remove(): Int = {
    if (size == 0) throw new NoSuchElementException
    val result = items(hd)
    hd = (hd + 1) & (items.length - 1)
    result
  }
}
