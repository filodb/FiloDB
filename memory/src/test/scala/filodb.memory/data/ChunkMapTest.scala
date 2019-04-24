package filodb.memory.data

import scala.collection.mutable.HashSet
import scala.concurrent.Future
import scala.util.Random

import debox.Buffer
import org.scalatest.concurrent.ScalaFutures

import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.NativeMemoryManager
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.vectors.NativeVectorTest

class ChunkMapTest extends NativeVectorTest with ScalaFutures {
  def makeElementWithID(id: Long): NativePointer = {
    val newElem = memFactory.allocateOffheap(16)
    UnsafeUtils.setLong(newElem, id)
    // Ignore the second eight bytes
    newElem
  }

  def makeElems(ids: Seq[Long]): Array[NativePointer] = ids.toArray.map(makeElementWithID)

  def checkElems(ids: Seq[Long], elems: Buffer[Long]): Unit = {
    elems.map(UnsafeUtils.getLong).toVector shouldEqual ids
  }

  it("should be empty when first starting") {
    val map = new ChunkMap(memFactory, 8)
    map.chunkmapSize shouldEqual 0
    map.chunkmapContains(5L) shouldEqual false
    intercept[IndexOutOfBoundsException] { map.chunkmapDoGetFirst }
    intercept[IndexOutOfBoundsException] { map.chunkmapDoGetLast }
    map.chunkmapDoGet(5L) shouldEqual 0
    map.chunkmapIterate.toBuffer shouldEqual Buffer.empty[Long]

    map.chunkmapFree()
  }

  it("should insert and read back properly in various places") {
    val map = new ChunkMap(memFactory, 8)
    val elems = makeElems((0 to 11).map(_.toLong))

    // when empty
    map.chunkmapDoPut(elems(5))
    map.chunkmapSize shouldEqual 1
    map.chunkmapContains(5L) shouldEqual true
    map.chunkmapDoGet(5L) shouldEqual elems(5)
    map.chunkmapDoGetLast shouldEqual elems(5)
    map.chunkmapDoGetFirst shouldEqual elems(5)
    checkElems(Seq(5L), map.chunkmapIterate.toBuffer)

    // last, not empty and not full
    map.chunkmapDoPut(elems(8))
    map.chunkmapSize shouldEqual 2
    map.chunkmapContains(8L) shouldEqual true
    map.chunkmapDoGetLast shouldEqual elems(8)
    map.chunkmapDoGetFirst shouldEqual elems(5)
    checkElems(Seq(5L, 8L), map.chunkmapIterate.toBuffer)

    // middle, not empty and not full (no resize)
    map.chunkmapDoPut(elems(6))
    map.chunkmapSize shouldEqual 3
    map.chunkmapContains(6L) shouldEqual true
    map.chunkmapDoGetLast shouldEqual elems(8)
    map.chunkmapDoGetFirst shouldEqual elems(5)
    checkElems(Seq(5L, 6L, 8L), map.chunkmapIterate.toBuffer)

    // Should be no resizing as long as length/# elements < 7
    Seq(2, 3, 9, 7).foreach { n =>
      map.chunkmapDoPut(elems(n))
      map.chunkmapContains(n.toLong) shouldEqual true
    }
    map.chunkmapSize shouldEqual 7
    map.chunkmapDoGetLast shouldEqual elems(9)
    checkElems(Seq(2L, 3L, 5L, 6L, 7L, 8L, 9L), map.chunkmapIterate.toBuffer)

    // last, full (should resize)
    map.chunkmapDoPut(elems(10))
    map.chunkmapSize shouldEqual 8
    checkElems(Seq(2L, 3L, 5L, 6L, 7L, 8L, 9L, 10L), map.chunkmapIterate.toBuffer)

    // middle, full (should resize)
    // should not resize until # elements = 15
    val elems2 = makeElems((21 to 27).map(_.toLong))
    elems2.foreach { elem =>
      map.chunkmapDoPut(elem)
      map.chunkmapContains(UnsafeUtils.getLong(elem)) shouldEqual true
    }
    map.chunkmapSize shouldEqual 15
    map.chunkmapDoGetLast shouldEqual elems2.last

    map.chunkmapDoPut(elems(4))
    map.chunkmapSize shouldEqual 16
    checkElems(((2 to 10) ++ (21 to 27)).map(_.toLong), map.chunkmapIterate.toBuffer)

    map.chunkmapFree()
  }

  it("should replace existing elements in various places") {
    // pre-populate with elements 2 to 10
    val map = new ChunkMap(memFactory, 8)
    val elems = makeElems((2 to 10).map(_.toLong))
    elems.foreach { elem =>
      map.chunkmapDoPut(elem)
      map.chunkmapContains(UnsafeUtils.getLong(elem)) shouldEqual true
    }
    map.chunkmapSize shouldEqual 9
    map.chunkmapDoGetLast shouldEqual elems.last
    map.chunkmapDoGet(4L) shouldEqual elems(2)

    // replace in middle
    val newElem4 = makeElementWithID(4L)
    map.chunkmapDoPut(newElem4)
    map.chunkmapSize shouldEqual 9
    map.chunkmapDoGet(4L) shouldEqual newElem4
    map.chunkmapDoGet(4L) should not equal (elems(2))
    checkElems((2 to 10).map(_.toLong), map.chunkmapIterate.toBuffer)

    // replace at head
    val newElem10 = makeElementWithID(10L)
    map.chunkmapDoPut(newElem10)
    map.chunkmapSize shouldEqual 9
    map.chunkmapDoGet(10L) shouldEqual newElem10
    map.chunkmapDoGetLast shouldEqual newElem10
    map.chunkmapDoGet(10L) should not equal (elems.last)
    checkElems((2 to 10).map(_.toLong), map.chunkmapIterate.toBuffer)

    map.chunkmapFree()
  }

  it("should putIfAbsent only if item doesn't already exist") {
    // pre-populate with elements 2 to 10
    val map = new ChunkMap(memFactory, 8)
    val elems = makeElems((2 to 10).map(_.toLong))
    map.chunkmapSize shouldEqual 0

    map.chunkmapDoPutIfAbsent(elems(0)) shouldEqual true
    map.chunkmapSize shouldEqual 1

    val twoElem = makeElementWithID(2)
    map.chunkmapDoPutIfAbsent(twoElem) shouldEqual false
    map.chunkmapSize shouldEqual 1

    map.chunkmapDoPutIfAbsent(elems(3)) shouldEqual true
    map.chunkmapSize shouldEqual 2

    map.chunkmapDoPutIfAbsent(elems(3)) shouldEqual false
    map.chunkmapSize shouldEqual 2

    val elemIt = map.chunkmapIterate
    try {
      elemIt.hasNext shouldEqual true
      elemIt.next shouldEqual elems(0)
      elemIt.hasNext shouldEqual true
      elemIt.next shouldEqual elems(3)
    } finally {
      elemIt.close()
    }

    map.chunkmapFree()
  }

  it("should not be able to put NULL elements") {
    val map = new ChunkMap(memFactory, 8)
    intercept[IllegalArgumentException] {
      map.chunkmapDoPut(0)
    }
    map.chunkmapFree()
  }

  it("should insert, delete, and reinsert") {
    // insert 1 item, then delete it, test map is truly empty
    val map = new ChunkMap(memFactory, 8)
    map.chunkmapDoPut(makeElementWithID(1))
    map.chunkmapSize shouldEqual 1
    map.chunkmapDoRemove(1L)
    map.chunkmapSize shouldEqual 0
    checkElems(Nil, map.chunkmapIterate.toBuffer)

    // pre-populate with various elements
    val elems = makeElems((2 to 10).map(_.toLong))
    elems.foreach { elem =>
      map.chunkmapDoPut(elem)
      map.chunkmapContains(UnsafeUtils.getLong(elem)) shouldEqual true
    }
    map.chunkmapSize shouldEqual 9
    map.chunkmapDoGetLast shouldEqual elems.last
    map.chunkmapDoGetFirst shouldEqual elems.head
    map.chunkmapDoGet(4L) shouldEqual elems(2)

    // remove at tail.  No resizing should occur.
    map.chunkmapDoRemove(2L)
    map.chunkmapDoGetFirst shouldEqual elems(1)
    map.chunkmapSize shouldEqual 8
    checkElems((3 to 10).map(_.toLong), map.chunkmapIterate.toBuffer)

    // remove in middle.  Resizing because 8 -> 7?
    map.chunkmapDoRemove(6L)
    map.chunkmapSize shouldEqual 7
    checkElems(Seq(3L, 4L, 5L, 7L, 8L, 9L, 10L), map.chunkmapIterate.toBuffer)

    // re-insert removed element
    map.chunkmapDoPut(elems(4))
    map.chunkmapSize shouldEqual 8
    checkElems((3 to 10).map(_.toLong), map.chunkmapIterate.toBuffer)

    map.chunkmapFree()
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  it("should handle concurrent inserts in various places") {
    // Let's have 1 thread inserting at head, and another one inserting in middle
    val map = new ChunkMap(memFactory, 32)
    val headElems = makeElems((100 to 199).map(_.toLong))
    val midElems = makeElems((0 to 99).map(_.toLong))

    val headThread = Future {
      headElems.foreach { elem =>
        map.chunkmapWithExclusive(map.chunkmapDoPut(elem))
        map.chunkmapContains(UnsafeUtils.getLong(elem)) shouldEqual true
      }
    }
    val midThread = Future {
      midElems.foreach { elem =>
        map.chunkmapWithExclusive(map.chunkmapDoPut(elem))
        map.chunkmapContains(UnsafeUtils.getLong(elem)) shouldEqual true
      }
    }
    Future.sequence(Seq(headThread, midThread)).futureValue

    map.chunkmapSize shouldEqual (headElems.length + midElems.length)
    checkElems((0 to 199).map(_.toLong), map.chunkmapIterate.toBuffer)

    map.chunkmapFree()
  }

  it("should handle concurrent inserts and ensure slice/iterations return sane data") {
    // 1 thread inserts random elem.  Another allocates random strings in the buffer, just to
    // increase chances of reading random crap
    val map = new ChunkMap(memFactory, 32)
    val elems = makeElems((0 to 99).map(_.toLong)).toSeq

    val insertThread = Future {
      Random.shuffle(elems).foreach { elem =>
        map.chunkmapWithExclusive(map.chunkmapDoPut(elem))
        map.chunkmapContains(UnsafeUtils.getLong(elem)) shouldEqual true
      }
    }
    val stringThread = Future {
      (0 to 199).foreach { n =>
        val addr = memFactory.allocateOffheap(12)
        UnsafeUtils.setInt(addr, Random.nextInt(1000000))
        UnsafeUtils.setInt(addr + 4, Random.nextInt(1000000))
        UnsafeUtils.setInt(addr + 8, Random.nextInt(1000000))
      }
    }
    val readThread = Future {
      (0 to 30).foreach { n =>
        map.chunkmapSlice(25, 75).toBuffer.map(UnsafeUtils.getLong).foreach { key =>
          // This cannot always be guaranteed, esp if inserts change things underneath
          //key should be >= 25L
          key should be <= 75L
        }
      }
    }
    Future.sequence(Seq(insertThread, stringThread, readThread)).futureValue

    map.chunkmapSize shouldEqual elems.length
    checkElems((0 to 99).map(_.toLong), map.chunkmapIterate.toBuffer)

    map.chunkmapFree()
  }

  it("should handle concurrent inserts and deletes in various places") {
    // First insert 0 to 99 single threaded
    val map = new ChunkMap(memFactory, 32)
    val elems = makeElems((0 to 99).map(_.toLong))
    elems.foreach { elem =>
      map.chunkmapWithExclusive(map.chunkmapDoPut(elem))
    }
    map.chunkmapSize shouldEqual elems.length

    val moreElems = makeElems((100 to 199).map(_.toLong))
    val toDelete = util.Random.shuffle(0 to 99)

    // Now, have one thread deleting 0-99, while second one inserts 100-199
    val deleteThread = Future {
      toDelete.foreach { n =>
        map.chunkmapWithExclusive(map.chunkmapDoRemove(n))
        map.chunkmapContains(n) shouldEqual false
      }
    }

    val insertThread = Future {
      moreElems.foreach { elem =>
        map.chunkmapWithExclusive(map.chunkmapDoPut(elem))
        // once in a while this could fail
        //map.chunkmapContains(UnsafeUtils.getLong(elem)) shouldEqual true
      }
    }

    Future.sequence(Seq(deleteThread, insertThread)).futureValue

    // Final map should have ONLY 100-199
    map.chunkmapSize shouldEqual moreElems.length
    checkElems((100 to 199).map(_.toLong), map.chunkmapIterate.toBuffer)

    map.chunkmapFree()
  }

  it("should slice correctly") {
    val map = new ChunkMap(memFactory, 32)
    val elems = makeElems((0 to 30 by 3).map(_.toLong))
    elems.foreach { elem =>
      map.chunkmapDoPut(elem)
    }
    map.chunkmapSize shouldEqual elems.length

    // slice: match startKey, but not endKey
    checkElems(Seq(9L, 12L, 15L), map.chunkmapSlice(9L, 16L).toBuffer)
    checkElems((0 to 15 by 3).map(_.toLong), map.chunkmapSlice(0L, 16L).toBuffer)
    checkElems((18 to 30 by 3).map(_.toLong), map.chunkmapSlice(18L, 31L).toBuffer)
    checkElems(Seq(30L), map.chunkmapSlice(30L, 30L).toBuffer)

    // slice: not match startKey, match endKey
    checkElems((0 to 12 by 3).map(_.toLong), map.chunkmapSlice(-1L, 12L).toBuffer)
    checkElems((12 to 18 by 3).map(_.toLong), map.chunkmapSlice(10L, 18L).toBuffer)
    checkElems(Nil, map.chunkmapSlice(19L, 18L).toBuffer)

    // slice: no match for either
    checkElems((12 to 18 by 3).map(_.toLong), map.chunkmapSlice(10L, 19L).toBuffer)
    checkElems((0 to 15 by 3).map(_.toLong), map.chunkmapSlice(-2L, 17L).toBuffer)
    checkElems((21 to 30 by 3).map(_.toLong), map.chunkmapSlice(20L, 33L).toBuffer)
    checkElems(Nil, map.chunkmapSlice(16L, 17L).toBuffer)

    map.chunkmapFree()
  }

  it("should sliceToEnd correctly") {
    val map = new ChunkMap(memFactory, 32)
    val elems = makeElems((0 to 30 by 3).map(_.toLong))
    elems.foreach { elem =>
      map.chunkmapDoPut(elem)
    }
    map.chunkmapSize shouldEqual elems.length

    checkElems((18 to 30 by 3).map(_.toLong), map.chunkmapSliceToEnd(18L).toBuffer)
    checkElems((0 to 30 by 3).map(_.toLong), map.chunkmapSliceToEnd(0L).toBuffer)

    checkElems((18 to 30 by 3).map(_.toLong), map.chunkmapSliceToEnd(17L).toBuffer)
    checkElems(Nil, map.chunkmapSliceToEnd(31L).toBuffer)
    checkElems(Seq(30L), map.chunkmapSliceToEnd(30L).toBuffer)

    map.chunkmapFree()
  }

  it("should behave gracefully once map is freed") {
    val map = new ChunkMap(memFactory, 32)
    val elems = makeElems((0 to 30 by 3).map(_.toLong))
    elems.foreach { elem =>
      map.chunkmapDoPut(elem)
    }
    map.chunkmapSize shouldEqual elems.length

    map.chunkmapFree()
    map.chunkmapSize shouldEqual 0
    map.chunkmapDoGet(2L) shouldEqual 0
    map.chunkmapContains(3L) shouldEqual false
    intercept[IndexOutOfBoundsException] { map.chunkmapDoGetFirst }
    intercept[IndexOutOfBoundsException] { map.chunkmapDoGetLast }
    map.chunkmapIterate.toBuffer shouldEqual Buffer.empty[Long]
    map.chunkmapSliceToEnd(18L).toBuffer shouldEqual Buffer.empty[Long]
    map.chunkmapSize shouldEqual 0
    map.chunkmapDoRemove(6L)

    // Double free does nothing.
    map.chunkmapFree()
  }

  it("should handle random access") {
    // This test exercises more circular buffer cases. This can be verified with simple code
    // coverage examination. The important cases deal with wraparound when adding and removing
    // elements, which do end up getting tested with these parameters.

    val rnd = new java.util.Random(8675309)
    val map = new ChunkMap(memFactory, 4)
    val set = new HashSet[Long]()

    var size = 0

    for (i <- 1 to 1000) {
      val id = rnd.nextInt(50)
      if (rnd.nextBoolean()) {
        set.add(id)
        if (map.chunkmapDoPutIfAbsent(makeElementWithID(id))) {
          size += 1
        }
        map.chunkmapContains(id) shouldEqual true
      } else {
        if (map.chunkmapContains(id)) {
          set.remove(id) shouldEqual true
          map.chunkmapDoRemove(id)
          map.chunkmapContains(id) shouldEqual false
          size -= 1
        } else {
          set.remove(id) shouldEqual false
        }
      }
      map.chunkmapSize shouldEqual size
      set.size shouldEqual size
    }

    map.chunkmapFree()
  }

  it("should support uncontended locking behavior") {
    val map = new ChunkMap(memFactory, 32)

    map.chunkmapAcquireExclusive()
    map.chunkmapReleaseExclusive()

    map.chunkmapAcquireShared()
    map.chunkmapReleaseShared()

    // Shouldn't stall.
    map.chunkmapAcquireExclusive()
    map.chunkmapReleaseExclusive()

    // Re-entrant shared lock.
    map.chunkmapAcquireShared()
    map.chunkmapAcquireShared()
    map.chunkmapReleaseShared()
    map.chunkmapReleaseShared()

    // Shouldn't stall.
    map.chunkmapAcquireExclusive()
    map.chunkmapReleaseExclusive()

    map.chunkmapFree()
  }

  it("should support exclusive lock") {
    val map = new ChunkMap(memFactory, 32)

    map.chunkmapAcquireExclusive()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.chunkmapAcquireExclusive()
        acquired = true
        map.chunkmapReleaseExclusive()
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    val durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Now let the second lock request complete.
    map.chunkmapReleaseExclusive()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.chunkmapFree()
  }

  it("should block exclusive lock when shared lock is held") {
    val map = new ChunkMap(memFactory, 32)

    map.chunkmapAcquireShared()
    map.chunkmapAcquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.chunkmapAcquireExclusive()
        acquired = true
        map.chunkmapReleaseExclusive()
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    map.chunkmapReleaseShared()

    stuck.join(500)
    durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L * 2

    acquired shouldBe false

    // Now let the exclusive lock request complete.
    map.chunkmapReleaseShared()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.chunkmapFree()
  }

  it("should block shared lock when exclusive lock is held") {
    val map = new ChunkMap(memFactory, 32)

    map.chunkmapAcquireExclusive()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.chunkmapAcquireShared()
        acquired = true
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Now let the shared lock request complete.
    map.chunkmapReleaseExclusive()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    // Can acquire more shared locks.
    map.chunkmapAcquireShared()
    map.chunkmapAcquireShared()

    // Release all shared locks.
    for (i <- 1 to 3) map.chunkmapReleaseShared

    // Exclusive can be acquired again.
    map.chunkmapAcquireExclusive()
    map.chunkmapReleaseExclusive()

    map.chunkmapFree()
  }

  it("should delay shared lock when exclusive lock is waiting") {
    val map = new ChunkMap(memFactory, 32)

    map.chunkmapAcquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.chunkmapAcquireExclusive()
        acquired = true
        map.chunkmapReleaseExclusive()
      }
    }

    stuck.start()

    var startNanos = System.nanoTime()
    stuck.join(1000)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 1000000000L

    acquired shouldBe false

    // Need to start in another thread due to reentrancy check.
    val delayed = new Thread {
      override def run(): Unit = {
        for (i <- 1 to 2) {
          map.chunkmapAcquireShared()
          map.chunkmapReleaseShared()
          Thread.sleep(100)
        }
      }
    }

    startNanos = System.nanoTime()
    delayed.start()
    delayed.join()
    durationNanos = System.nanoTime() - startNanos

    durationNanos should be > 1000000000L
    acquired shouldBe false

    // Now let the exclusive lock request complete.
    map.chunkmapReleaseShared()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.chunkmapFree()
  }

  it("should release all shared locks held by the current thread") {
    val map = new ChunkMap(memFactory, 32)

    map.chunkmapAcquireShared()
    map.chunkmapAcquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.chunkmapAcquireExclusive()
        acquired = true
        map.chunkmapReleaseExclusive()
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Releasing all shared locks allows the exclusive lock request to complete.
    ChunkMap.releaseAllSharedLocks()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.chunkmapFree()
  }

  it("should release all shared locks held for only the current thread") {
    val map = new ChunkMap(memFactory, 32)

    map.chunkmapAcquireShared()
    map.chunkmapAcquireShared()

    // Acquire another share, in another thread.
    val shareThread = new Thread {
      override def run(): Unit = map.chunkmapAcquireShared()
    }

    shareThread.start()
    shareThread.join()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.chunkmapAcquireExclusive()
        acquired = true
        map.chunkmapReleaseExclusive()
      }
    }

    stuck.start()

    var startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Releasing one thread shared locks isn't sufficient.
    ChunkMap.releaseAllSharedLocks()
    Thread.`yield`

    startNanos = System.nanoTime()
    stuck.join(500)
    durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    // Now let the exclusive lock request complete.
    map.chunkmapReleaseShared()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.chunkmapFree()
  }

  it("should support reentrant shared lock when exclusive lock is requested") {
    val map = new ChunkMap(memFactory, 32)

    map.chunkmapAcquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.chunkmapAcquireExclusive()
        acquired = true
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Shared lock is held by current thread, so it can easily acquire more.
    for (i <- 1 to 100) map.chunkmapAcquireShared()

    acquired shouldBe false

    // Release all shared locks.
    for (i <- 1 to 101) map.chunkmapReleaseShared()

    stuck.join()
    acquired shouldBe true
    map.chunkmapReleaseExclusive()

    map.chunkmapFree()
  }

  it("should evict when out of memory") {
    // Very little memory is available.
    val mm = new NativeMemoryManager(200)

    // Tiny initial capacity.
    val map = new ChunkMap(mm, 1)

    val elems = makeElems((0 to 19).map(_.toLong))
    elems.foreach { elem =>
      map.chunkmapDoPut(elem, 9) // Evict anything less than or equal to 9.
    }

    // Not 20 due to evictions.
    map.chunkmapSize shouldEqual 16

    for (i <- 4 to 19) {
      map.chunkmapContains(i) shouldEqual true
    }

    mm.freeAll()
  }
}
