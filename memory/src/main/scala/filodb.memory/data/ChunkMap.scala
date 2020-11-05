package filodb.memory.data

import scala.collection.mutable.{HashMap, Map}
import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.memory.{NativeMemoryManager, OutOfOffheapMemoryException}
import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.format.UnsafeUtils

/**
 * Specialized map/set implementation which maps long keys to native pointers. The key must be
 * derived from the object referenced by the native pointer, hence this map behaves somewhat
 * like a set. The bulk of the map contents is maintained in an off-heap (native) array,
 * managed like a circular buffer. The elements are sorted, and so lookup operations perform a
 * binary search.
 *
 * To safely use this class from multiple threads, a lock must be acquired and released. Some
 * of the methods do this already, and only those that require explicit locks are documented as
 * such. The lock protects the map and it ensures that referenced memory isn't reclaimed too
 * soon by the block manager. Hold the shared lock while reading elements, and release it when
 * the memory can be reclaimed. To be effective, all writes into the map must acquire an
 * exclusive lock. The lock implementation spins if necessary, but it yields the current thread
 * to be fair with other threads. To help reduce the likelihood of deadlocks, a thread which is
 * waiting to acquire the exclusive lock times out and retries while waiting, to help advance
 * threads which are stuck behind the exclusive lock request. A warning is logged by the
 * exclusive waiter when it's timeout has reached one second. This indicates that a deadlock
 * likely exists and cannot be auto resolved.
 *
 * NOTE: By convention, methods which require that the caller obtain the lock are denoted with
 * a "Do" in the name, such as `chunkmapDoGet`. All other methods acquire the lock
 * automatically. Care must be take with respect to reentrancy. An exclusive lock cannot be
 * acquired again once held, and the current thread will deadlock with itself.
 *
 * The implementation stores elements in a sorted circular buffer, assuming that most inserts
 * are higher than all other keys, and that most deletes are against the lowest key. These
 * operations can perform in constant time as a result. For keys not at the extremities, the
 * the contents of the circular buffer must shift around, leading to a very high cost when the
 * map is very large.
 */
object ChunkMap extends StrictLogging {
  private val _logger = logger

  private val lockStateOffset = UnsafeUtils.unsafe.objectFieldOffset(
    classOf[ChunkMap].getDeclaredField("lockState"))

  private val InitialExclusiveRetryTimeoutNanos = 1.millisecond.toNanos
  private val MaxExclusiveRetryTimeoutNanos = 1.minute.toNanos

  private val exclusiveLockWait = Kamon.counter("memory-exclusive-lock-waits").withoutTags
  private val sharedLockLingering = Kamon.counter("memory-shared-lock-lingering").withoutTags
  private val chunkEvictions = Kamon.counter("memory-chunk-evictions").withoutTags

  // Tracks all the shared locks held, by each thread.
  private val sharedLockCounts = new ThreadLocal[Map[ChunkMap, Int]] {
    override def initialValue() = new HashMap[ChunkMap, Int]
  }

  // Returns true if the current thread has acquired the shared lock at least once.
  private def hasSharedLock(inst: ChunkMap): Boolean = sharedLockCounts.get.contains(inst)

  // Updates the shared lock count, for the current thread.
  private def adjustSharedLockCount(inst: ChunkMap, amt: Int): Unit = {
    val countMap = sharedLockCounts.get
    if (!countMap.contains(inst)) {
      if (amt > 0) {
        countMap.put(inst, amt)
      }
    } else {
      val newCount = countMap(inst) + amt
      if (newCount <= 0) {
        countMap.remove(inst)
      } else {
        countMap.put(inst, newCount)
      }
    }
  }

  /**
   * Releases all shared locks, against all ChunkMap instances, for the current thread.
   */
  //scalastyle:off null
  def releaseAllSharedLocks(): Int = {
    var total = 0
    val countMap = sharedLockCounts.get
    if (countMap != null) {
      for ((inst, amt) <- countMap) {
        if (amt > 0) {
          total += amt
          sharedLockLingering.increment(amt)
          _logger.warn(s"Releasing all shared locks for: $inst, amount: $amt")
          var lockState = 0
          do {
            lockState = UnsafeUtils.getIntVolatile(inst, lockStateOffset)
          } while (!UnsafeUtils.unsafe.compareAndSwapInt(inst, lockStateOffset, lockState, lockState - amt))
        }
      }

      countMap.clear
    }
    total
  }
  //scalastyle:on null

  /**
    * Validate no locks are held by the thread. Typically invoked prior to
    * consumption from a query iterator. If there are lingering locks,
    * it is quite possible a lock acquire or release bug exists
    */
  def validateNoSharedLocks(unitTest: Boolean = false): Unit = {
    // Count up the number of held locks.
    var total = 0
    val countMap = sharedLockCounts.get
    if (countMap != null) {
      for ((inst, amt) <- countMap) {
        if (amt > 0) {
          total += amt
        }
      }
    }

    if (total > 10) { // lenient check for now
      val ex = new RuntimeException(s"Number of locks lingering: $total. " +
        s"This is indicative of a possible lock acquisition/release bug.")
      Shutdown.haltAndCatchFire(ex)
    }
  }

}

/**
 * @param memFactory a THREAD-SAFE factory for allocating offheap space
 * @param capacity initial capacity of the map; must be more than 0
 */
class ChunkMap(val memFactory: NativeMemoryManager, var capacity: Int) {
  require(capacity > 0)

  private var lockState: Int = 0
  private var size: Int = 0
  private var first: Int = 0
  private var arrayPtr = memFactory.allocateOffheap(capacity << 3, zero = true)

  import ChunkMap._

  /**
   * Returns the number of total elements in the map.
   */
  final def chunkmapSize(): Int = {
    chunkmapWithShared(size)
  }

  /**
   * Returns the element at the given key, or NULL (0) if the key isn't found. Takes O(log n)
   * time. Caller must hold any lock.
   */
  final def chunkmapDoGet(key: Long): NativePointer = {
    val index = doBinarySearch(key)
    if (index >= 0) arrayGet(realIndex(index)) else 0
  }

  /**
   * Returns true if the given key exists in this map. Takes O(log n) time.
   */
  final def chunkmapContains(key: Long): Boolean = {
    chunkmapWithShared(doBinarySearch(key) >= 0)
  }

  /**
   * Returns the first element, the one with the lowest key. Caller must hold any lock.
   * Throws IndexOutOfBoundsException if there are no elements.
   */
  final def chunkmapDoGetFirst(): NativePointer = {
    if (size <= 0) {
      throw new IndexOutOfBoundsException
    }
    arrayGet(first)
  }

  /**
   * Returns the last element, the one with the highest key. Caller must hold any lock.
   * Throws IndexOutOfBoundsException if there are no elements.
   */
  final def chunkmapDoGetLast(): NativePointer = {
    if (size <= 0) {
      throw new IndexOutOfBoundsException
    }
    arrayGet(realIndex(first + size - 1))
  }

  /**
   * Produces an ElementIterator for going through every element of the map in increasing key order.
   */
  final def chunkmapIterate(): ElementIterator = {
    new LazyElementIterator(() => {
      chunkmapAcquireShared()
      try {
        new MapIterator(first, first + size)
      } catch {
        case e: Throwable => chunkmapReleaseShared(); throw e;
      }
    })
  }

  /**
   * Produces an ElementIterator for iterating elements in increasing key order from startKey
   * to endKey.
   * @param startKey start at element whose key is equal or immediately greater than startKey
   * @param endKey end iteration when element is greater than endKey.  endKey is inclusive.
   */
  final def chunkmapSlice(startKey: Long, endKey: Long): ElementIterator = {
    new LazyElementIterator(() => {
      chunkmapAcquireShared()
      try {
        new MapIterator(doBinarySearch(startKey) & 0x7fffffff, first + size) {
          override def isPastEnd: Boolean = chunkmapKeyRetrieve(getNextElem) > endKey
        }
      } catch {
        case e: Throwable => chunkmapReleaseShared(); throw e;
      }
    })
  }

  /**
   * Produces an ElementIterator for iterating elements in increasing key order starting
   * with startKey.
   * @param startKey start at element whose key is equal or immediately greater than startKey
   */
  final def chunkmapSliceToEnd(startKey: Long): ElementIterator = {
    new LazyElementIterator(() => {
      chunkmapAcquireShared()
      try {
        new MapIterator(doBinarySearch(startKey) & 0x7fffffff, first + size)
      } catch {
        case e: Throwable => chunkmapReleaseShared(); throw e;
      }
    })
  }

  /**
   * Acquire exclusive access to this map, spinning if necessary. Exclusive lock isn't re-entrant.
   */
  final def chunkmapAcquireExclusive(): Unit = {
    // Spin-lock implementation. Because the owner of the shared lock might be blocked by this
    // thread as it waits for an exclusive lock, deadlock is possible. To mitigate this problem,
    // timeout and retry, allowing shared lock waiters to make progress. The timeout doubles
    // for each retry, up to a limit, but the retries continue indefinitely.

    var timeoutNanos = InitialExclusiveRetryTimeoutNanos
    var warned = false

    // scalastyle:off null
    while (true) {
      if (tryAcquireExclusive(timeoutNanos)) {
        if (arrayPtr == 0) {
          chunkmapReleaseExclusive()
          throw new IllegalStateException("ChunkMap is freed");
        }
        return
      }

      timeoutNanos = Math.min(timeoutNanos << 1, MaxExclusiveRetryTimeoutNanos)

      if (!warned && timeoutNanos >= MaxExclusiveRetryTimeoutNanos) {
        if (hasSharedLock(this)) {
          // Self deadlock. Upgrading the shared lock to an exclusive lock is possible if the
          // current thread is the only shared lock owner, but this isn't that common. Instead,
          // this is a bug which needs to be fixed.
          throw new IllegalStateException("Cannot acquire exclusive lock because thread already owns a shared lock")
        }
        exclusiveLockWait.increment()
        _logger.warn(s"Waiting for exclusive lock: $this")
        warned = true
      } else if (warned && timeoutNanos >= MaxExclusiveRetryTimeoutNanos) {
        val lockState = UnsafeUtils.getIntVolatile(this, lockStateOffset)
        Shutdown.haltAndCatchFire(new RuntimeException(s"Unable to acquire exclusive lock: $lockState"))
      }
    }
  }

  /**
   * Acquire exclusive access to this map, spinning if necessary. Exclusive lock isn't re-entrant.
   *
   * @return false if timed out
   */
  private def tryAcquireExclusive(timeoutNanos: Long): Boolean = {
    // Spin-lock implementation.

    var lockState = 0

    // First set the high bit, to signal an exclusive lock request.

    var done = false
    do {
      lockState = UnsafeUtils.getIntVolatile(this, lockStateOffset)
      if (lockState < 0) {
        // Wait for exclusive lock to be released.
        Thread.`yield`
      } else if (UnsafeUtils.unsafe.compareAndSwapInt(this, lockStateOffset, lockState, lockState | 0x80000000)) {
        if (lockState == 0) {
          return true
        }
        done = true
      }
    } while (!done)

    // Wait for shared lock owners to release the lock.

    val endNanos = System.nanoTime + timeoutNanos

    do {
      Thread.`yield`
      lockState = UnsafeUtils.getIntVolatile(this, lockStateOffset)
      if ((lockState & 0x7fffffff) == 0) {
        return true
      }
    } while (System.nanoTime() < endNanos)

    // Timed out. Release the exclusive lock request signal and yield (to permit shared access again).

    while(!UnsafeUtils.unsafe.compareAndSwapInt(this, lockStateOffset, lockState, lockState & 0x7fffffff)) {
      lockState = UnsafeUtils.getIntVolatile(this, lockStateOffset)
    }

    Thread.`yield`
    return false
  }

  /**
   * Release an acquired exclusive lock.
   */
  final def chunkmapReleaseExclusive(): Unit = {
    UnsafeUtils.setIntVolatile(this, lockStateOffset, 0)
  }

  /**
   * Run the given function body with the exclusive lock held, which isn't re-entrant.
   */
  final def chunkmapWithExclusive[T](body: => T): T = {
    chunkmapAcquireExclusive()
    try {
      body
    } finally {
      chunkmapReleaseExclusive()
    }
  }

  /**
   * Acquire shared access to this map, spinning if necessary.
   */
  final def chunkmapAcquireShared(): Unit = {
    // Spin-lock implementation.

    var lockState = 0

    while (true) {
      lockState = UnsafeUtils.getIntVolatile(this, lockStateOffset)
      if (lockState < 0 && !hasSharedLock(this)) {
        // Wait for exclusive lock to be released.
        Thread.`yield`
      } else if (UnsafeUtils.unsafe.compareAndSwapInt(this, lockStateOffset, lockState, lockState + 1)) {
        adjustSharedLockCount(this, +1)
        return
      }
    }
  }

  /**
   * Release an acquired shared lock.
   */
  final def chunkmapReleaseShared(): Unit = {
    var lockState = 0
    do {
      lockState = UnsafeUtils.getIntVolatile(this, lockStateOffset)
    } while (!UnsafeUtils.unsafe.compareAndSwapInt(this, lockStateOffset, lockState, lockState - 1))
    adjustSharedLockCount(this, -1)
  }

  /**
   * Run the given function body with the shared lock held.
   */
  final def chunkmapWithShared[T](body: => T): T = {
    chunkmapAcquireShared()
    try {
      body
    } finally {
      chunkmapReleaseShared()
    }
  }

  /**
   * Inserts/replaces the element into the map using the key computed from the element.
   * In case of replacing existing value for same key - then the last write wins.
   * Takes O(1) time if key is the highest in the map, or O(n) otherwise. Caller must hold
   * exclusive lock.
   * @param element the native pointer to the offheap element; must be able to apply
   * chunkmapKeyRetrieve to it to get the key
   * @param evictKey The highest key which can be evicted (removed) if necessary to make room
   * if no additional native memory can be allocated. The memory for the evicted chunks isn't
   * freed here, under the assumption that the chunk is a part of a Block, which gets evicted
   * later. This in turn calls chunkmapDoRemove, which does nothing because the chunk reference
   * was already removed.
   */
  final def chunkmapDoPut(element: NativePointer, evictKey: Long = Long.MinValue): Unit = {
    require(element != 0)
    chunkmapDoPut(chunkmapKeyRetrieve(element), element, evictKey)
  }

  /**
   * Atomically inserts the element it returns IF AND ONLY IF the element isn't
   * already in the map. Caller must hold exclusive lock.
   * @return true if the element was inserted, false otherwise
   */
  final def chunkmapDoPutIfAbsent(element: NativePointer, evictKey: Long = Long.MinValue): Boolean = {
    require(element != 0)
    val key = chunkmapKeyRetrieve(element)
    if (doBinarySearch(key) >= 0) {
      return false
    }
    chunkmapDoPut(key, element, evictKey)
    true
  }

  //scalastyle:off
  private def chunkmapDoPut(key: Long, element: NativePointer, evictKey: Long): Unit = {
    if (size == 0) {
      arraySet(0, element)
      first = 0
      size = 1
      return
    }

    // Ensure enough capacity, under the assumption that in most cases the element is
    // inserted and not simply replaced.
    if (size >= capacity) {
      try {
        val newArrayPtr = memFactory.allocateOffheap(capacity << 4, zero=true)
        if (first == 0) {
          // No wraparound.
          UnsafeUtils.unsafe.copyMemory(arrayPtr, newArrayPtr, size << 3)
        } else {
          // Wraparound correction.
          val len = (capacity - first) << 3
          UnsafeUtils.unsafe.copyMemory(arrayPtr + (first << 3), newArrayPtr, len)
          UnsafeUtils.unsafe.copyMemory(arrayPtr, newArrayPtr + len, first << 3)
          first = 0
        }
        memFactory.freeMemory(arrayPtr)
        arrayPtr = newArrayPtr
        capacity <<= 1
      } catch {
        case e: OutOfOffheapMemoryException => {
          // Try to evict the first entry instead of expanding the array.
          if (evictKey == Long.MinValue || chunkmapKeyRetrieve(arrayGet(first)) > evictKey) {
            throw e
          }
          chunkEvictions.increment()
          first += 1
          if (first >= capacity) {
            // Wraparound.
            first = 0
          }
          size -= 1
        }
      }
    }

    {
      val last = first + size - 1
      val rlast = realIndex(last)
      val lastKey = chunkmapKeyRetrieve(arrayGet(rlast))

      if (key > lastKey) {
        // New highest key; this is the expected common case.
        arraySet(realIndex(last + 1), element)
        size += 1
        return
      }

      if (key == lastKey) {
        // Replacing the last element.
        arraySet(rlast, element)
        return
      }
    }

    var index = doBinarySearch(key)

    if (index >= 0) {
      // Replacing an existing element.
      arraySet(realIndex(index), element)
      return
    }

    // Convert to insertion index.
    index &= 0x7fffffff

    val ri = realIndex(index)
    val rlast = realIndex(first + size) // rlast is the new index after insertion
    val amt = rlast - ri
    if (amt >= 0) {
      // Shift the elements; no wraparound correction is required.
      arrayCopy(ri, ri + 1, amt) // src, dst, len
    } else {
      // Shift the elements with wraparound correction.
      arrayCopy(0, 1, rlast)
      arraySet(0, arrayGet(capacity - 1))
      arrayCopy(ri, ri + 1, capacity - index - 1)
    }

    arraySet(ri, element)
    size += 1
  }
  //scalastyle:on

  /**
   * Removes the element at key from the map. Takes O(1) time if the key is the first,
   * otherwise O(n) time on average. Caller must hold exclusive lock.
   * @param key the key to remove. If key isn't present, then nothing is changed.
   */
  final def chunkmapDoRemove(key: Long): Unit = {
    if (size <= 0) {
      return
    }

    // Check if matches the first key.
    if (key == chunkmapKeyRetrieve(arrayGet(first))) {
      first += 1
      if (first >= capacity) {
        // Wraparound.
        first = 0
      }
    } else {
      val index = doBinarySearch(key)
      if (index < 0) {
        // Not found.
        return
      }
      val ri = realIndex(index)
      val rlast = realIndex(first + size - 1)
      val amt = rlast - ri
      if (amt >= 0) {
        // Shift the elements; no wraparound correction is required.
        arrayCopy(ri + 1, ri, amt) // src, dst, len
      } else {
        // Shift the elements with wraparound correction.
        arrayCopy(ri + 1, ri, capacity - index - 1)
        arraySet(capacity - 1, arrayGet(0))
        arrayCopy(1, 0, rlast)
      }
    }

    size -= 1
  }

  /**
   * Removes all elements for the given key and lower. Caller must hold exclusive lock.
   * @param key the highest key to remove
   * @return amount removed
   */
  final def chunkmapDoRemoveFloor(key: Long): Int = {
    if (size <= 0) {
      return 0
    }

    val newFirst = {
      // Check if matches the first key, or else search for it.
      if (key == chunkmapKeyRetrieve(arrayGet(first))) {
        first + 1
      } else {
        val ix = doBinarySearch(key)
        if (ix < 0) {
          ix & 0x7fffffff
        } else {
          ix + 1
        }
      }
    }

    val amt = newFirst - first
    first = realIndex(newFirst)
    size -= amt;
    amt
  }

  final def chunkmapFree(): Unit = {
    try {
      chunkmapAcquireExclusive()
    } catch {
      // Already freed.
      case e: IllegalStateException => return
    }

    try {
      if (arrayPtr != 0) {
        memFactory.freeMemory(arrayPtr)
        capacity = 0
        size = 0
        first = 0
        arrayPtr = 0
      }
    } finally {
      chunkmapReleaseExclusive()
    }
  }

  /**
   * Method which retrieves a pointer to the key/ID within the element. It just reads the first
   * eight bytes from the element as the ID. Please override to implement custom functionality.
   */
  private def chunkmapKeyRetrieve(elementPtr: NativePointer): Long = {
    if (elementPtr == 0) {
      throw new NullPointerException()
    }
    UnsafeUtils.getLong(elementPtr)
  }

  /**
   * Does a binary search for the element with the given key. Caller must hold any lock.
   * @param key the key to search for
   * @return found index, or index with bit 31 set if not found
   */
  private def doBinarySearch(key: Long): Int = {
    var low = first
    var high = first + size - 1

    while (low <= high) {
      var mid = (low + high) >>> 1
      var midKey = chunkmapKeyRetrieve(arrayGet(realIndex(mid)))
      if (midKey < key) {
        low = mid + 1
      } else if (midKey > key) {
        high = mid - 1
      } else {
        return mid
      }
    }

    return low | 0x80000000
  }

  /**
   * Returns the real index in the array, correcting for circular buffer wraparound.
   */
  private def realIndex(index: Int): Int = {
    var ix = index
    if (ix >= capacity) {
      ix -= capacity
    }
    ix
  }

  private def arrayGet(index: Int): NativePointer = {
    UnsafeUtils.getLong(arrayPtr + (index << 3))
  }

  private def arraySet(index: Int, value: NativePointer): Unit = {
    UnsafeUtils.setLong(arrayPtr + (index << 3), value)
  }

  private def arrayCopy(srcIndex: Int, dstIndex: Int, len: Int): Unit = {
    UnsafeUtils.unsafe.copyMemory(arrayPtr + (srcIndex << 3), arrayPtr + (dstIndex << 3), len << 3)
  }

  /**
   * @param index initialized to first index to read from
   * @param lastIndex last index to read from (exclusive)
   */
  private class MapIterator(var index: Int, val lastIndex: Int) extends ElementIterator {
    private var closed: Boolean = false
    private var nextElem: NativePointer = 0

    final def close(): Unit = {
      if (!closed) doClose()
    }

    private def doClose(): Unit = {
      closed = true
      nextElem = 0
      chunkmapReleaseShared()
    }

    final def hasNext: Boolean = {
      if (nextElem == 0) {
        if (closed) return false
        if (index >= lastIndex) {
          doClose()
          return false
        }
        nextElem = arrayGet(realIndex(index))
        if (isPastEnd) {
          doClose()
          return false
        }
      }
      return true
    }

    final def next: NativePointer = {
      var next = nextElem
      if (next == 0) {
        if (hasNext) {
          next = nextElem
        } else {
          throw new NoSuchElementException()
        }
      }
      nextElem = 0
      index += 1
      next
    }

    final def lock(): Unit = chunkmapAcquireShared()

    final def unlock(): Unit = chunkmapReleaseShared()

    final def getNextElem: NativePointer = nextElem

    /**
     * Check if the current element is just past the end, and iteration should stop.
     * Override this method to actually do something.
     */
    def isPastEnd: Boolean = false
  }
}
