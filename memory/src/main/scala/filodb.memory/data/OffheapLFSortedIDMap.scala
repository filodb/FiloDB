package filodb.memory.data

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.MemFactory
import filodb.memory.format.UnsafeUtils

/**
 * Offheap (mostly) Lock-Free Sorted ID Map.
 * The OffheapLFSortedIDMap was written to replace the ConcurrentSkipListMap and use 1/10th of CSLM overhead,
 * be completely offheap, and fit our use cases much better.
 *
 * Note: As currently used within FiloDB, locks must be acquired on the map to ensure that
 * referenced memory isn't reclaimed too soon by the block manager. Hold the shared lock
 * while reading elements, and release it when the memory can be reclaimed. To be effective,
 * all writes into the map must acquire an exclusive lock. The lock implementation spins if
 * necessary, but it yields the current thread to be fair with other threads. To help reduce
 * the likelihood of deadlocks, a thread which is waiting to acquire the exclusive lock times
 * out and retries while waiting, to help advance threads which are stuck behind the exclusive
 * lock request. A warning is logged by the exclusive waiter when it's timeout has reached one
 * second. This indicates that a deadlock likely exists and cannot be auto resolved.
 *
 * The OffheapLFSortedIDMap is a data structure with the following properties:
 * - Everything (except for the Long pointer reference) both data and metadata is completely offheap
 * - Memory efficiency is (much) more important than CPU or safety
 * - Each value's key must be derivable from the value using a function
 * - Data is kept sorted by key
 * - Fast lookups/contains in <= O(lg n)
 * - Is optimized for small n (say n usually < 100)
 * - Must be multi-thread safe for modifications, except OK for replacements of existing elements to not be MTsafe
 *    - but optimized for low concurrency/conflicts
 * - Most insertions are at head (newest keys are usually highest) but a few are near the tail
 * - Most deletions are near the tail
 *
 * Design:
 * AtomicLong reference to array memory location
 * Ring-structured array with head and tail
 * Head and tail modifications involve lockfree pointer updates mostly
 * Modifications in middle involve copying array and atomic reference swap
 * Lookups involve binary search
 *
 * Memory layout:
 * +0000  (uint16) head: long array position of head (highest ordered) element
 * +0002  (uint16) tail: long array position of tail (lowest ordered) element
 * +0004  (bool/uint8)  flag, if nonzero, means copying of data structure is happening.  Retry later please!
 * +0006  (uint16) maximum number of elements this can hold
 * +0008  64-bit/Long array of pointers to the actual offheap "value" for each element
 *
 * The metadata must fit in 64-bits so that it can be atomically swapped (esp head, tail, flag)
 * The key must be a Long, so the function to derive a Key from a Value is a Long => Long.
 * The maximum number of elements that can be accommodated is 65535, but it's really designed for much smaller n.
 * This is because if the head is one less than the tail, that is interpreted as an empty Map.
 */
object OffheapLFSortedIDMap extends StrictLogging {
  val OffsetHead = 0    // Head is the highest keyed index/element number in the ring, -1 initially
  val OffsetTail = 2
  val OffsetCopyFlag = 4
  val OffsetMaxElements = 6
  val OffsetElementPtrs = 8

  val CopyFlagMask = 0xff00000000L
  val MinMaxElements = 4       // Must be more than a few
  val MaxMaxElements = 65535   // Top/absolute limit on # of elements

  val IllegalStateResult = -1  // Returned from binarySearch when state changed underneath

  val InitialExclusiveRetryTimeoutNanos = 1.millisecond.toNanos
  val MaxExclusiveRetryTimeoutNanos = 1.second.toNanos

  val _logger = logger

  val sharedLockLingering = Kamon.counter("memory-shared-lock-lingering")

  // Tracks all the shared locks held, by each thread.
  val sharedLockCounts = new ThreadLocal[Map[MapHolder, Int]]

  // Lock state memory offsets for all known MapHolder classes.
  val lockStateOffsets = new ConcurrentHashMap[Class[_ <: MapHolder], Long]

  // Updates the shared lock count, for the current thread.
  //scalastyle:off
  def adjustSharedLockCount(inst: MapHolder, amt: Int): Unit = {
    var countMap = sharedLockCounts.get

    if (countMap == null) {
      if (amt <= 0) {
        return
      }
      countMap = new HashMap[MapHolder, Int]
      sharedLockCounts.set(countMap)
    }

    var newCount = amt

    countMap.get(inst) match {
      case None => if (newCount <= 0) return
      case Some(count) => {
        newCount += count
        if (newCount <= 0) {
          countMap.remove(inst)
          return
        }
      }
    }

    countMap.put(inst, newCount)
  }

  /**
   * Releases all shared locks, against all OffheapLFSortedIDMap instances, for the current thread.
   */
  def releaseAllSharedLocks(): Int = {
    var total = 0
    val countMap = sharedLockCounts.get
    if (countMap != null) {
      var lastKlass: Class[_ <: MapHolder] = null
      var lockStateOffset = 0L

      for ((inst, amt) <- countMap) {
        if (amt > 0) {
          val holderKlass = inst.getClass

          if (holderKlass != lastKlass) {
            lockStateOffset = lockStateOffsets.get(holderKlass)
            lastKlass = holderKlass
          }

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
  //scalastyle:on

  /**
    * Validate no locks are held by the thread. Typically invoked prior to
    * consumption from a query iterator. If there are lingering locks,
    * it is quite possible a lock acquire or release bug exists
    */
  def validateNoSharedLocks(): Unit = {
    val numLocksReleased = OffheapLFSortedIDMap.releaseAllSharedLocks()
    if (numLocksReleased > 0) {
      logger.warn(s"Number of locks was non-zero: $numLocksReleased. " +
        s"This is indicative of a possible lock acquisition/release bug.")
    }
  }

  def bytesNeeded(maxElements: Int): Int = {
    require(maxElements <= MaxMaxElements)
    OffsetElementPtrs + 8 * maxElements
  }

  /**
   * Initializes a new OffheapLFSortedIDMap in offheap memory with initial capacity maxElements
   * @param memFactory an offheap MemFactory for allocating memory
   * @param maxElements the maximum initial capacity + 1
   * @return pointer to the initialized memory region
   */
  def allocNew(memFactory: MemFactory, maxElements: Int): NativePointer = {
    require(maxElements >= MinMaxElements)
    val mapPtr = memFactory.allocateOffheap(bytesNeeded(maxElements), zero=true)
    UnsafeUtils.setInt(mapPtr, maxElements - 1)  // head = maxElemnts - 1, one logical less than tail
    UnsafeUtils.setInt(mapPtr + OffsetCopyFlag, maxElements << 16)
    // TODO: do we need to initialize rest of memory?  Should be initialized already
    mapPtr
  }
}

// A MapHolder is the class instance that contains the pointer to the location of the offheap map.  It is a var
// as the OffheapLFSortedIDMap code will mutate it atomically as the map grows/changes.
trait MapHolder {
  var mapPtr: NativePointer
  var lockState: Int
}

/**
 * This is a reader class because we don't want to impose the restriction that each instance of this
 * offheap data structure requires an onheap class instance.  That would in the case of core FiloDB use up too
 * much heap memory.  Instead, what we rely on is that some other class instance (for example, each instance
 * of TSPartition) implements MapHolder.  Thus all the methods of this class are passed a Mapholder
 * and we can thus reuse one instance of this class across many many "instances" of the offheap map where
 * each offheap map pointer resides in the mapPtr field of the MapHolder.
 *
 * @param memFactory a THREAD-SAFE factory for allocating offheap space
 * @param holderKlass the Class of the MapHolder used to hold the mapPtr pointer
 */
class OffheapLFSortedIDMapReader(memFactory: MemFactory, holderKlass: Class[_ <: MapHolder]) {
  import OffheapLFSortedIDMap._

  val exclusiveLockWait = Kamon.counter("memory-exclusive-lock-waits")

  /**
   * Default keyFunc which maps pointer to element to the Long keyID.  It just reads the first eight bytes
   * from the element as the ID.  Please override to implement custom functionality.
   */
  def keyFunc(elementPtr: NativePointer): Long = UnsafeUtils.getLongVolatile(elementPtr)

  protected val mapPtrOffset = UnsafeUtils.unsafe.objectFieldOffset(holderKlass.getDeclaredField("mapPtr"))
  protected val lockStateOffset = UnsafeUtils.unsafe.objectFieldOffset(holderKlass.getDeclaredField("lockState"))

  lockStateOffsets.putIfAbsent(holderKlass, lockStateOffset)

  // basic accessor classes; caller must hold a lock.
  @inline final def head(inst: MapHolder): Int = state(inst).head
  @inline final def tail(inst: MapHolder): Int = state(inst).tail
  @inline final def maxElements(inst: MapHolder): Int = state(inst).maxElem

  /**
   * Number of total elements in the map
   */
  final def length(inst: MapHolder): Int = {
    withShared(inst, doLength(inst))
  }

  // Caller must hold a lock.
  @inline private final def doLength(inst: MapHolder): Int = state(inst).length

  /**
   * Accesses the element at index index, where 0 = tail or lowest element and (length - 1) is the head or highest
   * Returns the pointer to the value. Caller must hold a lock.
   */
  @inline final def at(inst: MapHolder, index: Int): NativePointer = {
    val _mapPtr = mapPtr(inst)
    val _state = state(_mapPtr)
    if (_state == MapState.empty) 0 else getElem(_mapPtr, realIndex(_state, index))
  }

  /**
   * Returns the element at the given key, or NULL (0) if the key is not found.  Takes O(log n) time.
   * Caller must hold a lock.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param key the key to search for
   */
  final def apply(inst: MapHolder, key: Long): NativePointer = {
    val res = binarySearch(inst, key)
    if (res >= 0) at(inst, res) else 0
  }

  /**
   * Returns true if the given key exists in this map.  Takes O(log n) time.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param key the key to search for
   */
  final def contains(inst: MapHolder, key: Long): Boolean = {
    withShared(inst, if (doLength(inst) > 0) binarySearch(inst, key) >= 0 else false)
  }

  /**
   * Returns the first element, the one with the lowest key. Caller must hold a lock.
   * Throws IndexOutOfBoundsException if there are no elements.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   */
  final def first(inst: MapHolder): NativePointer = {
    if (doLength(inst) > 0) { getElem(mapPtr(inst), tail(inst)) }
    else                    { throw new IndexOutOfBoundsException }
  }

  /**
   * Returns the last element, the one with the highest key. Caller must hold a lock.
   * Throws IndexOutOfBoundsException if there are no elements.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   */
  final def last(inst: MapHolder): NativePointer = {
    if (doLength(inst) > 0) { getElem(mapPtr(inst), head(inst)) }
    else                    { throw new IndexOutOfBoundsException }
  }

  /**
   * Produces an ElementIterator for going through every element of the map in increasing key order.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   */
  final def iterate(inst: MapHolder): ElementIterator = {
    new LazyElementIterator(() => {
      acquireShared(inst)
      try {
        makeElemIterator(inst, 0)(alwaysContinue)
      } catch {
        case e: Throwable => releaseShared(inst); throw e;
      }
    })
  }

  /**
   * Produces an ElementIterator for iterating elements in increasing key order from startKey to endKey
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param startKey start at element whose key is equal or immediately greater than startKey
   * @param endKey end iteration when element is greater than endKey.  endKey is inclusive.
   */
  final def slice(inst: MapHolder, startKey: Long, endKey: Long): ElementIterator = {
    new LazyElementIterator(() => {
      acquireShared(inst)
      try {
        val _mapPtr = mapPtr(inst)
        val _state = state(_mapPtr)
        val logicalStart = binarySearch(_mapPtr, _state, startKey) & 0x7fffffff
        makeElemIterator(inst, logicalStart) { elem: NativePointer => keyFunc(elem) <= endKey }
      } catch {
        case e: Throwable => releaseShared(inst); throw e;
      }
     })
  }

  /**
   * Produces an ElementIterator for iterating elements in increasing key order starting with startKey.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param startKey start at element whose key is equal or immediately greater than startKey
   */
  final def sliceToEnd(inst: MapHolder, startKey: Long): ElementIterator = {
    new LazyElementIterator(() => {
      acquireShared(inst)
      try {
        val _mapPtr = mapPtr(inst)
        val _state = state(_mapPtr)
        val logicalStart = binarySearch(_mapPtr, _state, startKey) & 0x7fffffff
        makeElemIterator(inst, logicalStart)(alwaysContinue)
      } catch {
        case e: Throwable => releaseShared(inst); throw e;
      }
    })
  }

  /**
   * Does a binary search for the element with the given key. Caller must hold a lock.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param key the key to search for
   * @return the element number (that can be passed to at) if exact match found, or
   *         element number BEFORE the element to insert, with bit 31 set, if not exact match.
   *         0 if key is lower than tail/first element, and length if key is higher than last element
   *         IllegalStateResult if state changed underneath
   */
  def binarySearch(inst: MapHolder, key: Long): Int = {
    var result = IllegalStateResult
    do {
      val _mapPtr = mapPtr(inst)
      val _state = state(_mapPtr)
      if (_state == MapState.empty) return IllegalStateResult
      require(_state.length > 0, "Cannot binarySearch inside an empty map")
      result = binarySearch(_mapPtr, _state, key)
    } while (result == IllegalStateResult)
    result
  }

  def binarySearch(_mapPtr: NativePointer, _state: MapState, key: Long): Int = {
    val mapLen = _state.length
    if (!check(_mapPtr, _state)) return IllegalStateResult

    @annotation.tailrec def innerBinSearch(first: Int, len: Int): Int =
      if (first >= mapLen) {
        // Past the last element.  Return mapLen with not found bit set
        mapLen | 0x80000000
      } else if (len == 0) {
        val elem = getElem(_mapPtr, realIndex(_state, first))
        if (keyFunc(elem) == key) first else first | 0x80000000
      } else {
        val half = len >>> 1
        val middle = first + half
        val elem = getElem(_mapPtr, realIndex(_state, middle))
        if (!check(_mapPtr, _state)) { IllegalStateResult }
        else {
          val elementKey = keyFunc(elem)
          if (elementKey == key) {
            middle
          } else if (elementKey < key) {
            innerBinSearch(middle + 1, len - half - 1)
          } else {
            innerBinSearch(first, half)
          }
        }
      }

    innerBinSearch(0, mapLen)
  }

  /**
   * Acquire exclusive access to this map, spinning if necessary. Exclusive lock isn't re-entrant.
   */
  def acquireExclusive(inst: MapHolder): Unit = {
    // Spin-lock implementation. Because the owner of the shared lock might be blocked by this
    // thread as it waits for an exclusive lock, deadlock is possible. To mitigate this problem,
    // timeout and retry, allowing shared lock waiters to make progress. The timeout doubles
    // for each retry, up to a limit, but the retries continue indefinitely.

    var timeoutNanos = InitialExclusiveRetryTimeoutNanos
    var warned = false

    while (true) {
      if (tryAcquireExclusive(inst, timeoutNanos)) {
        return
      }

      timeoutNanos = Math.min(timeoutNanos << 1, MaxExclusiveRetryTimeoutNanos)

      if (!warned && timeoutNanos >= MaxExclusiveRetryTimeoutNanos) {
        exclusiveLockWait.increment()
        _logger.warn(s"Waiting for exclusive lock: $inst")
        warned = true
      }
    }
  }

  /**
   * Acquire exclusive access to this map, spinning if necessary. Exclusive lock isn't re-entrant.
   *
   * @return false if timed out
   */
  private def tryAcquireExclusive(inst: MapHolder, timeoutNanos: Long): Boolean = {
    // Spin-lock implementation.

    var lockState = 0

    // First set the high bit, to signal an exclusive lock request.

    var done = false
    do {
      lockState = UnsafeUtils.getIntVolatile(inst, lockStateOffset)
      if (lockState < 0) {
        // Wait for exclusive lock to be released.
        Thread.`yield`
      } else if (UnsafeUtils.unsafe.compareAndSwapInt(inst, lockStateOffset, lockState, lockState | 0x80000000)) {
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
      lockState = UnsafeUtils.getIntVolatile(inst, lockStateOffset)
      if ((lockState & 0x7fffffff) == 0) {
        return true
      }
    } while (System.nanoTime() < endNanos)

    // Timed out. Release the exclusive lock request signal and yield (to permit shared access again).

    while(!UnsafeUtils.unsafe.compareAndSwapInt(inst, lockStateOffset, lockState, lockState & 0x7fffffff)) {
      lockState = UnsafeUtils.getIntVolatile(inst, lockStateOffset)
    }

    Thread.`yield`
    return false
  }

  /**
   * Release an acquired exclusive lock.
   */
  def releaseExclusive(inst: MapHolder): Unit = {
    UnsafeUtils.setIntVolatile(inst, lockStateOffset, 0)
  }

  /**
   * Run the given function body with the exclusive lock held, which isn't re-entrant.
   */
  def withExclusive[T](inst: MapHolder, body: => T): T = {
    acquireExclusive(inst)
    try {
      body
    } finally {
      releaseExclusive(inst)
    }
  }

  /**
   * Acquire shared access to this map, spinning if necessary.
   */
  def acquireShared(inst: MapHolder): Unit = {
    // Spin-lock implementation.

    var lockState = 0

    while (true) {
      lockState = UnsafeUtils.getIntVolatile(inst, lockStateOffset)
      if (lockState < 0) {
        // Wait for exclusive lock to be released.
        Thread.`yield`
      } else if (UnsafeUtils.unsafe.compareAndSwapInt(inst, lockStateOffset, lockState, lockState + 1)) {
        adjustSharedLockCount(inst, +1)
        return
      }
    }
  }

  /**
   * Release an acquired shared lock.
   */
  def releaseShared(inst: MapHolder): Unit = {
    var lockState = 0
    do {
      lockState = UnsafeUtils.getIntVolatile(inst, lockStateOffset)
    } while (!UnsafeUtils.unsafe.compareAndSwapInt(inst, lockStateOffset, lockState, lockState - 1))
    adjustSharedLockCount(inst, -1)
  }

  /**
   * Run the given function body with the shared lock held.
   */
  def withShared[T](inst: MapHolder, body: => T): T = {
    acquireShared(inst)
    try {
      body
    } finally {
      releaseShared(inst)
    }
  }

  // curIdx has to be initialized to one less than the starting logical index
  // NOTE: This always fetches items using the official API.  This is slower, but guarantees that no matter
  // how slowly the iterator user pulls, it will always be pulling the right thing even if state/mapPtr changes.
  private class SortedIDMapElemIterator(inst: MapHolder,
                                        var curIdx: Int,
                                        continue: NativePointer => Boolean,
                                        var closed: Boolean = false,
                                        var nextElem: NativePointer = 0L)
  extends ElementIterator {
    final def close(): Unit = {
      if (!closed) doClose()
    }

    private def doClose(): Unit = {
       closed = true;
       releaseShared(inst)
    }

    final def hasNext: Boolean = {
      if (closed) return false
      nextElem = at(inst, curIdx + 1)
      val result = curIdx < (doLength(inst) - 1) && continue(nextElem)
      if (!result) doClose()
      result
    }

    final def next: NativePointer = {
      if (closed) throw new NoSuchElementException()
      curIdx += 1
      nextElem
    }
  }

  // Note: Caller must have acquired shared lock. It's released when iterator is closed.
  private def makeElemIterator(inst: MapHolder, logicalStart: Int)
                              (continue: NativePointer => Boolean): ElementIterator =
    new SortedIDMapElemIterator(inst, logicalStart - 1, continue)

  private def alwaysContinue(p: NativePointer): Boolean = true

  // "real" index adjusting for position of head/tail
  @inline protected def mapPtr(inst: MapHolder): NativePointer = UnsafeUtils.getLongVolatile(inst, mapPtrOffset)
  private[memory] def state(inst: MapHolder): MapState = state(mapPtr(inst))
  protected def state(mapPtr: NativePointer): MapState =
    if (isPtrNull(mapPtr)) MapState.empty else MapState(UnsafeUtils.getLongVolatile(mapPtr))
  @inline protected final def realIndex(state: MapState, index: Int): Int =
    if (state.maxElem == 0) 0 else (index + state.tail) % state.maxElem
  @inline protected final def elemPtr(mapPtr: NativePointer, realIndex: Int): NativePointer =
    mapPtr + OffsetElementPtrs + 8 * realIndex
  @inline protected final def getElem(mapPtr: NativePointer, realIndex: Int): NativePointer =
    UnsafeUtils.getLongVolatile(elemPtr(mapPtr, realIndex))

  // For some reason, occasionally the upper 4 bits of a read pointer can be nonzero even though the rest of it is.
  // So this is a "safer" null check
  @inline final def isPtrNull(ptr: NativePointer): Boolean = (ptr & 0xffffffffffffffL) == 0

  @inline protected def check(mapPtr: NativePointer, state: MapState): Boolean =
    !isPtrNull(mapPtr) && UnsafeUtils.getLongVolatile(mapPtr) == state.state && state.state != 0
}

class OffheapLFSortedIDMapMutator(memFactory: MemFactory, holderKlass: Class[_ <: MapHolder])
extends OffheapLFSortedIDMapReader(memFactory, holderKlass) {
  import OffheapLFSortedIDMap._

  /**
   * Inserts/replaces the element into the Map using the key computed from the element,
   * atomically changing the state, and retrying until compare and swap succeeds.
   * In case of replacing existing value for same key - then the last write wins.
   * Takes O(1) time if key is the highest in the map, or O(n) otherwise.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param element the native pointer to the offheap element; must be able to apply keyFunc to it to get the key
   */
  //scalastyle:off
  final def put(inst: MapHolder, element: NativePointer): Unit = {
    // Inner method to retry for optimistic locking/lockfree loop
    def innerPut(key: Long, newElem: NativePointer): Boolean = {
      // Get current state in one shot and make decisions.  All decisions are based on the initial state and
      // subsequent operations always use CAS techniques to ensure that there is no possibility of corruption/change
      // of state from time of initial entry until the CAS.  If CAS fails then something changed underneath and
      // we return false and retry.
      val _mapPtr = mapPtr(inst)
      val initState = state(_mapPtr)
      // All of these should be using registers for super fast calculations
      val _head = initState.head
      val _maxElem = initState.maxElem
      // Below should never happen with exclusive locking during writes
      require(initState != MapState.empty && !initState.copyFlag)
      val _len = initState.length
      // If empty, add to head
      if (_len == 0) { atomicHeadAdd(_mapPtr, initState, newElem) }
      else {
        // Problem with checking head is that new element might not have been written just after CAS succeeds
        val headElem = getElem(_mapPtr, _head)
        val headKey = keyFunc(headElem)
        // If key == head element key, directly replace w/o binary search
        if (key == headKey) { atomicReplace(_mapPtr, _head, headElem, newElem) }
        else if (key > headKey) {
          // If higher than head (common case), and room, atomic switch and add to head
          if (_len < (_maxElem - 1)) { atomicHeadAdd(_mapPtr, initState, newElem) }
          //   ...  or copy, insert, atomic switch ref.  Remember to release current memory block.
          else { copyInsertAtomicSwitch(inst, initState, _head + 1, newElem, _maxElem * 2) }
        } else {
          // key < headKey, Binary search.  If match, atomic compareAndSwap
          val res = binarySearch(_mapPtr, initState, key)  // TODO: make this based on original state
          if (res >= 0) {
            // NOTE: replaces do not need to be atomic, but we can try anyways
            // We need to "adjust" the binarySearch result, which is a logical index, into one adjusted for the tail
            val insertIndex = (res + initState.tail) % _maxElem
            val origElem = getElem(_mapPtr, insertIndex)
            atomicReplace(_mapPtr, insertIndex, origElem, newElem)
          } else if (res == IllegalStateResult) {
            false
          // No match.  Copy, insert, atomic switch ref.  Release cur mem block
          } else {
            val insertIndex = ((res & 0x7fffffff) + initState.tail) % _maxElem
            copyInsertAtomicSwitch(inst, initState, insertIndex, newElem,
                                   if (_len < (_maxElem - 1)) _maxElem else _maxElem * 2)
          }
        }
      }
    }

    require(element != 0, s"Cannot insert/put NULL elements")
    val newKey = keyFunc(element)
    while (!innerPut(newKey, element)) {
      if (state(inst) == MapState.empty) return   // maxElems cannot be zero
    }
  }
  //scalastyle:on

  /**
   * Atomically inserts the element it returns IF AND ONLY IF
   * the item with the given key is not already in the map.
   * To achieve the above goals, a new copy of the map is always made and the copyFlag CAS is used to
   * guarantee only one party can do the insertion at a time.
   * Thus using this function has some side effects:
   * - No O(1) head optimization
   * @return true if the item was inserted, false otherwise
   */
  final def putIfAbsent(inst: MapHolder, key: Long, element: NativePointer): Boolean = {
    var inserted = false
    while (!inserted) {
      val _mapPtr = mapPtr(inst)
      val initState = state(_mapPtr)
      if (initState == MapState.empty) return false
      val _maxElem = initState.maxElem
      if (initState.length == 0) { inserted = atomicHeadAdd(_mapPtr, initState, element) }
      else {
        val res = binarySearch(_mapPtr, initState, key)  // TODO: make this based on original state
        if (res >= 0) {
          // key already present in map, just return false
          return false
        } else if (res != IllegalStateResult) {
          val insertIndex = ((res & 0x7fffffff) + initState.tail) % _maxElem
          inserted = copyInsertAtomicSwitch(inst, initState, insertIndex, element,
                       if (initState.length < (_maxElem - 1)) _maxElem else _maxElem * 2)
        }
      }
    }
    inserted
  }

  /**
   * Removes the element at key atomically from the map, retrying in case of conflict.
   * Takes O(1) time if the key is at the tail, otherwise O(n) time on average.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param key the key to remove.  If key is not present then nothing is changed.
   */
  final def remove(inst: MapHolder, key: Long): Unit = {
    def innerRemove(key: Long): Boolean = {
      val _mapPtr = mapPtr(inst)
      val _state  = state(_mapPtr)
      if (_state == MapState.empty || _state.copyFlag || _state.maxElem == 0) return false
      if (_state.length <= 0) {
        true
      } else if (check(_mapPtr, _state) && _mapPtr == mapPtr(inst)) {
        // Is the tail the element to be removed?  Then remove it, this is O(1)
        val tailElem = getElem(_mapPtr, _state.tail)
        if (key == keyFunc(tailElem)) {
          atomicTailRemove(_mapPtr, _state)
        } else {
          // Not the tail, do a binary search, O(log n)
          val res = binarySearch(_mapPtr, _state, key)
          if (res == IllegalStateResult) { false }
          // if key not found, just return not found
          else if (res < 0)              { true }
          else {
            copyRemoveAtomicSwitch(inst, _state, realIndex(_state, res))
          }
        }
      } else { false }
    }

    while (!innerRemove(key)) {
      if (state(inst) == MapState.empty) return   // don't modify a null/absent map
    }
  }

  /**
   * Frees the memory used by the map pointed to by inst.mapPtr, using CAS such that it will wait for concurrent
   * modifications occurring to finish first.
   * First the state is reset to 0, then the mapPtr itself is reset to 0, then the memory is finally freed.
   * After this is called, concurrent modifications and reads of the map in inst will fail gracefully.
   */
  final def free(inst: MapHolder): Unit = {
    withExclusive(inst, {
      var curState = state(inst)
      while (curState != MapState.empty) {
        val mapPtr = inst.mapPtr
        if (casState(mapPtr, curState, MapState.empty))
          if (UnsafeUtils.unsafe.compareAndSwapLong(inst, mapPtrOffset, mapPtr, 0))
            memFactory.freeMemory(mapPtr)
        curState = state(inst)
      }
    })
  }

  private def casLong(mapPtr: NativePointer, mapOffset: Int, oldLong: Long, newLong: Long): Boolean =
    UnsafeUtils.unsafe.compareAndSwapLong(UnsafeUtils.ZeroPointer, mapPtr + mapOffset, oldLong, newLong)

  private def casLong(pointer: NativePointer, oldLong: Long, newLong: Long): Boolean =
    UnsafeUtils.unsafe.compareAndSwapLong(UnsafeUtils.ZeroPointer, pointer, oldLong, newLong)

  private def casState(mapPtr: NativePointer, oldState: MapState, newState: MapState): Boolean =
    casLong(mapPtr, 0, oldState.state, newState.state)

  private def atomicHeadAdd(mapPtr: NativePointer, initState: MapState, newElem: NativePointer): Boolean =
    !initState.copyFlag && {
      // compute new head
      val newHead = (initState.head + 1) % initState.maxElem

      // Check the new spot is uninitialized, then use CAS to protect state while we write new element
      // After CAS we can directly write stuff as we are essentially protected
      getElem(mapPtr, newHead) == 0L &&
      atomicEnableCopyFlag(mapPtr, initState) && {
        UnsafeUtils.setLong(elemPtr(mapPtr, newHead), newElem)
        UnsafeUtils.setLong(mapPtr, initState.withHead(newHead).state)
        true
      }
    }

  private def atomicTailRemove(mapPtr: NativePointer, state: MapState): Boolean =
    !state.copyFlag && {
      // compute new tail
      val oldTail = state.tail
      val newTail = (oldTail + 1) % state.maxElem
      val swapped = casState(mapPtr, state, state.withTail(newTail))
      // If CAS succeeds, clear out element at old tail.  This helps prevent problems during inserts.
      if (swapped) UnsafeUtils.setLong(elemPtr(mapPtr, oldTail), 0)
      swapped
    }

  private def atomicReplace(mapPtr: NativePointer,
                            index: Int,
                            oldElem: NativePointer,
                            newElem: NativePointer): Boolean =
    casLong(mapPtr, OffsetElementPtrs + 8 * index, oldElem, newElem)

  private def copyInsertAtomicSwitch(inst: MapHolder,
                                     initState: MapState,
                                     insertIndex: Int,
                                     newElem: NativePointer,
                                     newMaxElems: Int): Boolean =
    copyMutateSwitchMap(inst, initState, newMaxElems) { case (startIndex, endIndex, movePtr) =>
      val _mapPtr = mapPtr(inst)
      // If insertIndex = endIndex + 1, that == inserting at end
      if (insertIndex >= startIndex && insertIndex <= (endIndex + 1)) {
        val insertOffset = 8 * (insertIndex - startIndex)  // # of bytes to copy before insertion point
        UnsafeUtils.copy(elemPtr(_mapPtr, startIndex), movePtr, insertOffset)
        UnsafeUtils.setLong(movePtr + insertOffset, newElem)
        UnsafeUtils.copy(elemPtr(_mapPtr, insertIndex), movePtr + insertOffset + 8, 8 * (endIndex - insertIndex + 1))
        endIndex - startIndex + 2   // include endIndex and also include new element
      } else {
        // no need to insert within this range, just copy everything and return same # of elements
        val numElemsToCopy = endIndex - startIndex + 1
        UnsafeUtils.copy(elemPtr(_mapPtr, startIndex), movePtr, numElemsToCopy * 8)
        numElemsToCopy
      }
    }

  private def copyRemoveAtomicSwitch(inst: MapHolder, initState: MapState, removeIndex: Int): Boolean = {
    // Shrink if the new length will be less than half of current maxElems
    val _maxElem = initState.maxElem
    val newMaxElems = if (_maxElem > 8 && initState.length <= (_maxElem / 2)) _maxElem/2 else _maxElem
    copyMutateSwitchMap(inst, initState, newMaxElems) { case (startIndex, endIndex, movePtr) =>
      val _mapPtr = mapPtr(inst)
      if (removeIndex >= startIndex && removeIndex <= endIndex) {
        val removeOffset = 8 * (removeIndex - startIndex)  // # of bytes to copy before insertion point
        UnsafeUtils.copy(elemPtr(_mapPtr, startIndex), movePtr, removeOffset)
        UnsafeUtils.copy(elemPtr(_mapPtr, removeIndex + 1), movePtr + removeOffset, 8 * (endIndex - removeIndex))
        endIndex - startIndex
      } else {
        val numElemsToCopy = endIndex - startIndex + 1
        UnsafeUtils.copy(elemPtr(_mapPtr, startIndex), movePtr, numElemsToCopy * 8)
        numElemsToCopy
      }
    }
  }

  private def atomicEnableCopyFlag(mapPtr: NativePointer, initState: MapState): Boolean =
    !initState.copyFlag && casState(mapPtr, initState, initState.withCopyFlagOn)

  /**
   * This takes care of atomically copying/transforming current array to a new location and swapping out the
   * map pointers in the map address field of the instance.
   * In theory the copy flag is not necessary but helps minimize # of concurrent modifications attempted.
   * @param xformFunc function for copying to new address is passed (startIndex, endIndex, newElemPtr)
   *          where newElemPtr is a pointer to the address where the function is supposed to start copying/transforming
   *          original elements starting at startIndex (0-based, not tail based).
   *          The # of elements written is returned.
   */
  private def copyMutateSwitchMap(inst: MapHolder, initState: MapState, newMaxElems: Int)
                                 (xformFunc: (Int, Int, NativePointer) => Int): Boolean = {
    val oldMapPtr = mapPtr(inst)  // Should we pass this in to avoid this changing under us too?
    // 1. must not be already copying and must atomically enable copying
    atomicEnableCopyFlag(oldMapPtr, initState) && {
      // 2. allocate new space
      val newMapPtr = memFactory.allocateOffheap(bytesNeeded(newMaxElems), zero=true)

      // 3. first part: from tail to head/end
      val _head = initState.head
      val endIndex = if (_head < initState.tail) initState.maxElem - 1 else _head
      val firstPartElems = xformFunc(initState.tail, endIndex, newMapPtr + OffsetElementPtrs)

      // 4. second part, optional: from 0 to head, if head was orig < tail
      val secondPartElems = if (_head < initState.tail) {
        xformFunc(0, _head, elemPtr(newMapPtr, firstPartElems))
      } else {
        0
      }

      // 5. write new state at new loc, atomic switch mapPtr, free old loc
      // New tail = 0, new head = total # elements - 1
      val newState = (firstPartElems + secondPartElems - 1).toLong | (newMaxElems.toLong << 48)
      UnsafeUtils.setLong(newMapPtr, newState)
      // NOTE: state has to be valid before the CAS, once the CAS is done another thread will try to read it
      // It is safe to write the state since we are only ones who know about the new mem region
      if (UnsafeUtils.unsafe.compareAndSwapLong(inst, mapPtrOffset, oldMapPtr, newMapPtr)) {
        UnsafeUtils.setLong(oldMapPtr, 0)   // zero old state so those still reading will stop
        memFactory.freeMemory(oldMapPtr)
        true
      } else {
        // CAS of map pointer failed, free new map memory and try again.  Also unset copy flag?
        // Though we should really never reach this state, since we had the original lock on copy flag
        memFactory.freeMemory(newMapPtr)
        UnsafeUtils.setLong(oldMapPtr, initState.state)
        false
      }
    }
  }
}

/**
 * This is a value class, it should not occupy any heap objects.  It is used to encapsulate and make the Map state
 * bitfield access easier, and make the state access more typesafe.
 */
final case class MapState(state: Long) extends AnyVal {
  def head: Int = (state & 0x0ffff).toInt
  def tail: Int = ((state >> 16) & 0x0ffff).toInt
  def copyFlag: Boolean = (state & OffheapLFSortedIDMap.CopyFlagMask) != 0
  def maxElem: Int = ((state >> 48) & 0x0ffff).toInt
  def length: Int = if (maxElem > 0) (head - tail + 1 + maxElem) % maxElem else 0

  def details: String = s"MapState(head=$head tail=$tail copyFlag=$copyFlag maxElem=$maxElem len=$length)"

  def withHead(newHead: Int): MapState = MapState(state & ~0x0ffffL | newHead)
  def withTail(newTail: Int): MapState = MapState(state & ~0x0ffff0000L | (newTail.toLong << 16))
  def withCopyFlagOn: MapState = MapState(state | OffheapLFSortedIDMap.CopyFlagMask)
}

object MapState {
  val empty = MapState(0)
}

/**
 * A convenient class which uses one onheap object for each offheap SortedIDMap, probably around 50 bytes.
 * If you want to save more space, it's better to share an implementation of OffheapLFSortedIDMap amongst multiple
 * actual maps.  The API in here is pretty convenient though.
 */
class OffheapLFSortedIDMap(memFactory: MemFactory, var mapPtr: NativePointer, var lockState: Int = 0)
extends OffheapLFSortedIDMapMutator(memFactory, classOf[OffheapLFSortedIDMap]) with MapHolder {
  final def length: Int = length(this)
  final def apply(key: Long): NativePointer = apply(this, key)
  final def contains(key: Long): Boolean = contains(this, key)
  final def first: NativePointer = first(this)
  final def last: NativePointer = last(this)
  final def iterate: ElementIterator = iterate(this)
  final def slice(startKey: Long, endKey: Long): ElementIterator = slice(this, startKey, endKey)
  final def sliceToEnd(startKey: Long): ElementIterator = sliceToEnd(this, startKey)
  final def put(elem: NativePointer): Unit = put(this, elem)
  final def remove(key: Long): Unit = remove(this, key)

  // Locking methods.
  final def acquireExclusive(): Unit = acquireExclusive(this)
  final def releaseExclusive(): Unit = releaseExclusive(this)
  final def withExclusive[T](body: => T): T = withExclusive(this, body)
  final def acquireShared(): Unit = acquireShared(this)
  final def releaseShared(): Unit = releaseShared(this)
  final def withShared[T](body: => T): T = withShared(this, body)
}

object SingleOffheapLFSortedIDMap {
  def apply(memFactory: MemFactory, maxElements: Int): OffheapLFSortedIDMap =
    new OffheapLFSortedIDMap(memFactory, OffheapLFSortedIDMap.allocNew(memFactory, maxElements))
}