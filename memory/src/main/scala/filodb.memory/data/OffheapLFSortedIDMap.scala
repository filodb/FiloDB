package filodb.memory.data

import com.typesafe.scalalogging.StrictLogging

import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.MemFactory
import filodb.memory.format.UnsafeUtils

/**
 * Offheap Lock-Free Sorted ID Map
 * The OffheapLFSortedIDMap was written to replace the ConcurrentSkipListMap and use 1/10th of CSLM overhead,
 * be completely offheap, and fit our use cases much better.
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
  val MinMaxElements = 8       // Must be more than a few
  val MaxMaxElements = 65535   // Top/absolute limit on # of elements

  val IllegalStateResult = -1  // Returned from binarySearch when state changed underneath

  val _logger = logger

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
    val mapPtr = memFactory.allocateOffheap(bytesNeeded(maxElements))
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
}

/**
 * This is an abstract class because we don't want to impose the restriction that each instance of this
 * offheap data structure requires an onheap class instance.  That would in the case of core FiloDB use up too
 * much heap memory.  Instead, what we rely on is that some other class instance (for example, each instance
 * of TSPartition) implements MapHolder.  Thus all the methods of this class are passed a Mapholder
 * and we can thus reuse one instance of this class across many many "instances" of the offheap map where
 * each offheap map pointer resides in the mapPtr field of the MapHolder.
 *
 * @param holderKlass the Class of the MapHolder used to hold the mapPtr pointer
 */
// scalastyle:off number.of.methods
class OffheapLFSortedIDMap(memFactory: MemFactory, holderKlass: Class[_ <: MapHolder]) {
  import OffheapLFSortedIDMap._

  /**
   * Default keyFunc which maps pointer to element to the Long keyID.  It just reads the first eight bytes
   * from the element as the ID.  Please override to implement custom functionality.
   */
  def keyFunc(elementPtr: NativePointer): Long = UnsafeUtils.getLong(elementPtr)

  private val mapPtrOffset = UnsafeUtils.unsafe.objectFieldOffset(holderKlass.getDeclaredField("mapPtr"))

  // basic accessor classes
  @inline final def head(inst: MapHolder): Int = state(inst).head
  @inline final def tail(inst: MapHolder): Int = state(inst).tail
  @inline final def maxElements(inst: MapHolder): Int = state(inst).maxElem

  /**
   * Number of total elements in the map
   */
  @inline final def length(inst: MapHolder): Int = state(inst).length

  /**
   * Accesses the element at index index, where 0 = tail or lowest element and (length - 1) is the head or highest
   * Returns the pointer to the value.
   */
  @inline final def at(inst: MapHolder, index: Int): NativePointer = {
    val _mapPtr = mapPtr(inst)
    getElem(_mapPtr, realIndex(state(_mapPtr), index))
  }

  /**
   * Returns the element at the given key, or NULL (0) if the key is not found.  Takes O(log n) time.
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
  final def contains(inst: MapHolder, key: Long): Boolean =
    if (length(inst) > 0) binarySearch(inst, key) >= 0 else false

  /**
   * Returns the first element, the one with the lowest key.
   * Throws IndexOutOfBoundsException if there are no elements.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   */
  final def first(inst: MapHolder): NativePointer =
    if (length(inst) > 0) { getElem(mapPtr(inst), tail(inst)) }
    else                  { throw new IndexOutOfBoundsException }

  /**
   * Returns the last element, the one with the highest key.
   * Throws IndexOutOfBoundsException if there are no elements.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   */
  final def last(inst: MapHolder): NativePointer =
    if (length(inst) > 0) { getElem(mapPtr(inst), head(inst)) }
    else                  { throw new IndexOutOfBoundsException }

  /**
   * Produces an ElementIterator for going through every element of the map in increasing key order.
   * State is captured when the method is first called.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   */
  final def iterate(inst: MapHolder): ElementIterator = makeElemIterator(inst, 0)(alwaysContinue)

  /**
   * Produces an ElementIterator for iterating elements in increasing key order from startKey to endKey
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param startKey start at element whose key is equal or immediately greater than startKey
   * @param endKey end iteration when element is greater than endKey.  endKey is inclusive.
   */
  final def slice(inst: MapHolder, startKey: Long, endKey: Long): ElementIterator = {
    val _mapPtr = mapPtr(inst)
    val _state = state(_mapPtr)
    val logicalStart = binarySearch(_mapPtr, _state, startKey) & 0x7fffffff
    makeElemIterator(inst, logicalStart) { elem: NativePointer => keyFunc(elem) <= endKey }
  }

  /**
   * Produces an ElementIterator for iterating elements in increasing key order starting with startKey.
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param startKey start at element whose key is equal or immediately greater than startKey
   */
  final def sliceToEnd(inst: MapHolder, startKey: Long): ElementIterator = {
    val _mapPtr = mapPtr(inst)
    val _state = state(_mapPtr)
    val logicalStart = binarySearch(_mapPtr, _state, startKey) & 0x7fffffff
    makeElemIterator(inst, logicalStart)(alwaysContinue)
  }

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
      if (initState.copyFlag || _maxElem == 0) return false   // trouble in paradise/bad state; reset
      val _len = initState.length
      // If empty, add to head
      if (_len == 0) { atomicHeadAdd(_mapPtr, initState, newElem) }
      else {
        // Problem with checking head is that new element might not have been written just after CAS succeeds
        val headElem = getElem(_mapPtr, _head)
        if (isNullPtr(headElem)  || !check(_mapPtr, initState)) return false
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

    val newKey = keyFunc(element)
    while (!innerPut(newKey, element)) {}
  }
  //scalastyle:on

  /**
   * Atomically calls the elementFunc and inserts the element it returns IF AND ONLY IF
   * the item with the given key is not already in the map.  elementFunc is not called if key already exists.
   * To achieve the above goals, a new copy of the map is always made and the copyFlag CAS is used to
   * guarantee only one party can do the insertion and call elementFunc at a time.
   * Thus using this function has some side effects:
   * - heap allocation of the elementFunc
   * - No O(1) head optimization
   * @return true if the item was inserted, false otherwise
   */
  final def putIfAbsent(inst: MapHolder, key: Long, elementFunc: => NativePointer): Boolean = {
    var inserted = false
    while (!inserted) {
      val _mapPtr = mapPtr(inst)
      val initState = state(_mapPtr)
      val _maxElem = initState.maxElem
      if (initState.length == 0) { inserted = atomicHeadAddFunc(_mapPtr, initState, elementFunc) }
      else {
        val res = binarySearch(_mapPtr, initState, key)  // TODO: make this based on original state
        if (res >= 0) {
          // key already present in map, just return false
          return false
        } else if (res != IllegalStateResult) {
          val insertIndex = ((res & 0x7fffffff) + initState.tail) % _maxElem
          inserted = copyInsertAtomicSwitchFunc(inst, initState, insertIndex, elementFunc,
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
   * @return the removed Element, or NULL if key was not found or empty map
   */
  final def remove(inst: MapHolder, key: Long): NativePointer = {
    var prevElem: NativePointer = 0L

    def innerRemove(key: Long): Boolean = {
      val _mapPtr = mapPtr(inst)
      val _state  = state(_mapPtr)
      if (_state.copyFlag || _state.maxElem == 0) return false   // trouble in paradise; reset
      prevElem = 0L
      if (_state.length <= 0) {
        true
      } else if (check(_mapPtr, _state) && _mapPtr == mapPtr(inst)) {
        // Is the tail the element to be removed?  Then remove it, this is O(1)
        val tailElem = getElem(_mapPtr, _state.tail)
        if (isNullPtr(tailElem)) return false               // state must be corrupted; retry
        if (key == keyFunc(tailElem)) {
          prevElem = tailElem
          atomicTailRemove(_mapPtr, _state)
        } else {
          // Not the tail, do a binary search, O(log n)
          val res = binarySearch(_mapPtr, _state, key)
          if (res == IllegalStateResult) { false }
          // if key not found, just return not found
          else if (res < 0)              { true }
          else {
            prevElem = getElem(_mapPtr, realIndex(_state, res))
            copyRemoveAtomicSwitch(inst, _state, realIndex(_state, res))
          }
        }
      } else { false }
    }

    while (!innerRemove(key)) {}
    prevElem
  }

  /**
   * Does a binary search for the element with the given key
   * @param inst the instance (eg TSPartition) with the mapPtr field containing the map address
   * @param key the key to search for
   * @return the element number (that can be passed to at) if exact match found, or
   *         element number BEFORE the element to insert, with bit 31 set, if not exact match.
   *         0 if key is lower than tail/first element, and length if key is higher than last element
   *         IllegalStateResult if state changed underneath
   */
  private def binarySearch(inst: MapHolder, key: Long): Int = {
    var result = IllegalStateResult
    do {
      val _mapPtr = mapPtr(inst)
      val _state = state(_mapPtr)
      require(_state.length > 0, "Cannot binarySearch inside an empty map")
      result = binarySearch(_mapPtr, _state, key)
    } while (result == IllegalStateResult)
    result
  }

  private def binarySearch(_mapPtr: NativePointer, _state: MapState, key: Long): Int = {
    val mapLen = _state.length

    @annotation.tailrec def innerBinSearch(first: Int, len: Int): Int =
      if (first >= mapLen) {
        // Past the last element.  Return mapLen with not found bit set
        mapLen | 0x80000000
      } else if (len == 0) {
        val elem = getElem(_mapPtr, realIndex(_state, first))
        if (isNullPtr(elem)) { IllegalStateResult }
        else if (keyFunc(elem) == key) first else first | 0x80000000
      } else {
        val half = len >>> 1
        val middle = first + half
        val elem = getElem(_mapPtr, realIndex(_state, middle))
        if (isNullPtr(elem) || !check(_mapPtr, _state)) { IllegalStateResult }
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

  // curIdx has to be initialized to one less than the starting "realIndex"
  private class SortedIDMapElemIterator(mapPtr: NativePointer, var curIdx: Int, continue: NativePointer => Boolean)
  extends ElementIterator {
    val _state = state(mapPtr)
    final def nextIdx: Int = (curIdx + 1) % _state.maxElem
    final def hasNext: Boolean = {
      val nextElem = getElem(mapPtr, nextIdx)
      curIdx != _state.head && !isNullPtr(nextElem) && continue(nextElem)
    }
    final def next: NativePointer = {
      curIdx = nextIdx
      getElem(mapPtr, curIdx)
    }
  }

  private def makeElemIterator(inst: MapHolder, logicalStart: Int)
                              (continue: NativePointer => Boolean): ElementIterator = {
    val _mapPtr = mapPtr(inst)
    val _state = state(_mapPtr)
    // Note that if the map is empty, then head is at tail - 1, so this should work for empty maps too
    val startIndex = (_state.tail + logicalStart - 1 + _state.maxElem) % _state.maxElem  // one before tail
    new SortedIDMapElemIterator(_mapPtr, startIndex, continue)
  }

  private def alwaysContinue(p: NativePointer): Boolean = true

  // "real" index adjusting for position of head/tail
  @inline private def mapPtr(inst: MapHolder): NativePointer = UnsafeUtils.getLong(inst, mapPtrOffset)
  private def state(inst: MapHolder): MapState = state(mapPtr(inst))
  private def state(mapPtr: NativePointer): MapState = MapState(UnsafeUtils.getLong(mapPtr))
  @inline private final def realIndex(state: MapState, index: Int): Int = (index + state.tail) % state.maxElem
  @inline private final def elemPtr(mapPtr: NativePointer, realIndex: Int): NativePointer =
    mapPtr + OffsetElementPtrs + 8 * realIndex
  @inline private final def getElem(mapPtr: NativePointer, realIndex: Int): NativePointer =
    UnsafeUtils.getLong(elemPtr(mapPtr, realIndex))

  @inline private def check(mapPtr: NativePointer, state: MapState): Boolean =
    UnsafeUtils.getLong(mapPtr) == state.state
  // Enough to check lower 48 bits.  For some odd reason once in a while upper 4 bits are nonzero
  @inline private def isNullPtr(ptr: NativePointer): Boolean = (ptr & 0xffffffffffffL) == 0L

  private def casLong(mapPtr: NativePointer, mapOffset: Int, oldLong: Long, newLong: Long): Boolean =
    UnsafeUtils.unsafe.compareAndSwapLong(UnsafeUtils.ZeroPointer, mapPtr + mapOffset, oldLong, newLong)

  private def casState(mapPtr: NativePointer, oldState: MapState, newState: MapState): Boolean =
    casLong(mapPtr, 0, oldState.state, newState.state)

  private def atomicHeadAdd(mapPtr: NativePointer, initState: MapState, newElem: NativePointer): Boolean =
    !initState.copyFlag && {
      // compute new head
      val newHead = (initState.head + 1) % initState.maxElem

      // compare and swap 64-bit state. If succeed, write out newElem at new head
      // NOTE: we cannot write the new element before CAS as we're not sure we have right to write it yet
      val swapped = casState(mapPtr, initState, initState.withHead(newHead))
      if (swapped) UnsafeUtils.setLong(elemPtr(mapPtr, newHead), newElem)
      swapped
    }

  private def atomicHeadAddFunc(mapPtr: NativePointer, initState: MapState, elemFunc: => NativePointer): Boolean =
    !initState.copyFlag && {
      // compute new head
      val newHead = (initState.head + 1) % initState.maxElem

      // compare and swap 64-bit state. If succeed, write out newElem at new head
      // NOTE: we cannot write the new element before CAS as we're not sure we have right to write it yet
      val swapped = casState(mapPtr, initState, initState.withHead(newHead))
      if (swapped) UnsafeUtils.setLong(elemPtr(mapPtr, newHead), elemFunc)
      swapped
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
    copyInsertAtomicSwitchFunc(inst, initState, insertIndex, newElem, newMaxElems)

  private def copyInsertAtomicSwitchFunc(inst: MapHolder,
                                         initState: MapState,
                                         insertIndex: Int,
                                         elemFunc: => NativePointer,
                                         newMaxElems: Int): Boolean =
    copyMutateSwitchMap(inst, initState, newMaxElems) { case (startIndex, endIndex, movePtr) =>
      val _mapPtr = mapPtr(inst)
      // If insertIndex = endIndex + 1, that == inserting at end
      if (insertIndex >= startIndex && insertIndex <= (endIndex + 1)) {
        val insertOffset = 8 * (insertIndex - startIndex)  // # of bytes to copy before insertion point
        UnsafeUtils.copy(elemPtr(_mapPtr, startIndex), movePtr, insertOffset)
        UnsafeUtils.setLong(movePtr + insertOffset, elemFunc)
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
    !initState.copyFlag && {
      val newState = MapState(initState.state | CopyFlagMask)
      casState(mapPtr, initState, newState)
    }

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
      val newMapPtr = memFactory.allocateOffheap(bytesNeeded(newMaxElems))

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
        try {
          memFactory.freeMemory(oldMapPtr)
        } catch {
          case e: Exception => _logger.error(s"Could not free old map pointer $oldMapPtr, probably harmless", e)
        }
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
  def length: Int = (head - tail + 1 + maxElem) % maxElem

  def withHead(newHead: Int): MapState = MapState(state & ~0x0ffffL | newHead)
  def withTail(newTail: Int): MapState = MapState(state & ~0x0ffff0000L | (newTail.toLong << 16))
}

/**
 * A convenient class which uses one onheap object for each offheap SortedIDMap, probably around 50 bytes.
 * If you want to save more space, it's better to share an implementation of OffheapLFSortedIDMap amongst multiple
 * actual maps.  The API in here is pretty convenient though.
 */
class SingleOffheapLFSortedIDMap(memFactory: MemFactory, var mapPtr: NativePointer)
extends OffheapLFSortedIDMap(memFactory, classOf[SingleOffheapLFSortedIDMap]) with MapHolder {
  final def length: Int = length(this)
  final def apply(key: Long): NativePointer = apply(this, key)
  final def contains(key: Long): Boolean = contains(this, key)
  final def first: NativePointer = first(this)
  final def last: NativePointer = last(this)
  final def iterate: ElementIterator = iterate(this)
  final def slice(startKey: Long, endKey: Long): ElementIterator = slice(this, startKey, endKey)
  final def sliceToEnd(startKey: Long): ElementIterator = sliceToEnd(this, startKey)
  final def put(elem: NativePointer): Unit = put(this, elem)
  final def remove(key: Long): NativePointer = remove(this, key)
}

object SingleOffheapLFSortedIDMap {
  def apply(memFactory: MemFactory, maxElements: Int): SingleOffheapLFSortedIDMap =
    new SingleOffheapLFSortedIDMap(memFactory, OffheapLFSortedIDMap.allocNew(memFactory, maxElements))
}