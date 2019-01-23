package filodb.query.util

import java.util.NoSuchElementException

/**
  * A simple array based circular queue that allows indexed access to its elements.
  * Array doubles itself when it reaches capacity.
  */
class IndexedArrayQueue[T](initialSize: Int = 8) {

  private val minSize = 8

  private[util] var items: Array[Any] = allocateArray(initialSize)
  /**
    * Points to next item to be removed if non-empty
    */
  private[util] var hd: Int = 0
  /**
    * Next empty position to add if position is available
    */
  private[util] var tl: Int = 0

  private def allocateArray(n: Int): Array[Any] = {
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
    new Array[Any](size)
  }

  private def doubleCapacity(): Unit = {
    require(hd == tl)
    val initialSize = items.length
    val numLeftOfHead = hd
    val numRightOfHead = items.length - numLeftOfHead
    val newCapacity = items.length << 1
    if (newCapacity < 0) throw new IllegalStateException("Overflow. Cannot allocate.")
    val newItems = new Array[Any](newCapacity)
    Array.copy(items, numLeftOfHead, newItems, 0, numRightOfHead)
    Array.copy(items, 0, newItems, numRightOfHead, numLeftOfHead)
    items = newItems
    hd = 0
    tl = initialSize
  }

  def add(e: T): Unit = {
    items(tl) = e
    tl = (tl + 1) & (items.length - 1) // can do & instead of % since power of 2
    if (tl == hd) doubleCapacity()
  }

  def size: Int = (tl - hd) & (items.length - 1)

  def isEmpty: Boolean = size == 0

  def remove(): T = {
    if (size == 0) throw new NoSuchElementException
    val result = items(hd)
    // scalastyle:off
    items(hd) = null  // remove reference
    // scalastyle:on
    hd = (hd + 1) & (items.length - 1)
    result.asInstanceOf[T]
  }

  def head: T = if (size == 0) throw new NoSuchElementException() else items(hd).asInstanceOf[T]

  def last: T = {
    if (size == 0) throw new NoSuchElementException()
    val idx = if (tl == 0) items.length - 1 else tl -1
    items(idx).asInstanceOf[T]
  }

  def apply(i: Int): T = {
    if (i < 0 || i >= size) throw new ArrayIndexOutOfBoundsException(i)
    val idx = (hd + i) & (items.length - 1)
    items(idx).asInstanceOf[T]
  }

  override def toString(): String = {
    val elements = for { i <- 0 until size } yield this(i)
    elements.mkString("IndexedArrayQueue(", ", ", ")")
  }
}

