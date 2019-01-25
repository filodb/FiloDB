package filodb.memory.data

import scala.collection.mutable.HashSet
import scala.concurrent.Future
import scala.util.Random

import debox.Buffer
import org.scalatest.concurrent.ScalaFutures

import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.vectors.NativeVectorTest

class OffheapSortedIDMapTest extends NativeVectorTest with ScalaFutures {
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
    val map = new OffheapSortedIDMap(memFactory, 8)
    map.mapSize shouldEqual 0
    map.mapContains(5L) shouldEqual false
    intercept[IndexOutOfBoundsException] { map.mapGetFirst }
    intercept[IndexOutOfBoundsException] { map.mapGetLast }
    map.mapGet(5L) shouldEqual 0
    map.mapIterate.toBuffer shouldEqual Buffer.empty[Long]

    map.mapFree()
  }

  it("should insert and read back properly in various places") {
    val map = new OffheapSortedIDMap(memFactory, 8)
    val elems = makeElems((0 to 11).map(_.toLong))

    // when empty
    map.mapPut(elems(5))
    map.mapSize shouldEqual 1
    map.mapContains(5L) shouldEqual true
    map.mapGet(5L) shouldEqual elems(5)
    map.mapGetLast shouldEqual elems(5)
    map.mapGetFirst shouldEqual elems(5)
    checkElems(Seq(5L), map.mapIterate.toBuffer)

    // last, not empty and not full
    map.mapPut(elems(8))
    map.mapSize shouldEqual 2
    map.mapContains(8L) shouldEqual true
    map.mapGetLast shouldEqual elems(8)
    map.mapGetFirst shouldEqual elems(5)
    checkElems(Seq(5L, 8L), map.mapIterate.toBuffer)

    // middle, not empty and not full (no resize)
    map.mapPut(elems(6))
    map.mapSize shouldEqual 3
    map.mapContains(6L) shouldEqual true
    map.mapGetLast shouldEqual elems(8)
    map.mapGetFirst shouldEqual elems(5)
    checkElems(Seq(5L, 6L, 8L), map.mapIterate.toBuffer)

    // Should be no resizing as long as length/# elements < 7
    Seq(2, 3, 9, 7).foreach { n =>
      map.mapPut(elems(n))
      map.mapContains(n.toLong) shouldEqual true
    }
    map.mapSize shouldEqual 7
    map.mapGetLast shouldEqual elems(9)
    checkElems(Seq(2L, 3L, 5L, 6L, 7L, 8L, 9L), map.mapIterate.toBuffer)

    // last, full (should resize)
    map.mapPut(elems(10))
    map.mapSize shouldEqual 8
    checkElems(Seq(2L, 3L, 5L, 6L, 7L, 8L, 9L, 10L), map.mapIterate.toBuffer)

    // middle, full (should resize)
    // should not resize until # elements = 15
    val elems2 = makeElems((21 to 27).map(_.toLong))
    elems2.foreach { elem =>
      map.mapPut(elem)
      map.mapContains(UnsafeUtils.getLong(elem)) shouldEqual true
    }
    map.mapSize shouldEqual 15
    map.mapGetLast shouldEqual elems2.last

    map.mapPut(elems(4))
    map.mapSize shouldEqual 16
    checkElems(((2 to 10) ++ (21 to 27)).map(_.toLong), map.mapIterate.toBuffer)

    map.mapFree()
  }

  it("should replace existing elements in various places") {
    // pre-populate with elements 2 to 10
    val map = new OffheapSortedIDMap(memFactory, 8)
    val elems = makeElems((2 to 10).map(_.toLong))
    elems.foreach { elem =>
      map.mapPut(elem)
      map.mapContains(UnsafeUtils.getLong(elem)) shouldEqual true
    }
    map.mapSize shouldEqual 9
    map.mapGetLast shouldEqual elems.last
    map.mapGet(4L) shouldEqual elems(2)

    // replace in middle
    val newElem4 = makeElementWithID(4L)
    map.mapPut(newElem4)
    map.mapSize shouldEqual 9
    map.mapGet(4L) shouldEqual newElem4
    map.mapGet(4L) should not equal (elems(2))
    checkElems((2 to 10).map(_.toLong), map.mapIterate.toBuffer)

    // replace at head
    val newElem10 = makeElementWithID(10L)
    map.mapPut(newElem10)
    map.mapSize shouldEqual 9
    map.mapGet(10L) shouldEqual newElem10
    map.mapGetLast shouldEqual newElem10
    map.mapGet(10L) should not equal (elems.last)
    checkElems((2 to 10).map(_.toLong), map.mapIterate.toBuffer)

    map.mapFree()
  }

  it("should putIfAbsent only if item doesn't already exist") {
    // pre-populate with elements 2 to 10
    val map = new OffheapSortedIDMap(memFactory, 8)
    val elems = makeElems((2 to 10).map(_.toLong))
    map.mapSize shouldEqual 0

    map.mapPutIfAbsent(elems(0)) shouldEqual true
    map.mapSize shouldEqual 1

    val twoElem = makeElementWithID(2)
    map.mapPutIfAbsent(twoElem) shouldEqual false
    map.mapSize shouldEqual 1

    map.mapPutIfAbsent(elems(3)) shouldEqual true
    map.mapSize shouldEqual 2

    map.mapPutIfAbsent(elems(3)) shouldEqual false
    map.mapSize shouldEqual 2

    val elemIt = map.mapIterate
    try {
      elemIt.hasNext shouldEqual true
      elemIt.next shouldEqual elems(0)
      elemIt.hasNext shouldEqual true
      elemIt.next shouldEqual elems(3)
    } finally {
      elemIt.close()
    }

    map.mapFree()
  }

  it("should not be able to put NULL elements") {
    val map = new OffheapSortedIDMap(memFactory, 8)
    intercept[IllegalArgumentException] {
      map.mapPut(0)
    }
    map.mapFree()
  }

  it("should insert, delete, and reinsert") {
    // insert 1 item, then delete it, test map is truly empty
    val map = new OffheapSortedIDMap(memFactory, 8)
    map.mapPut(makeElementWithID(1))
    map.mapSize shouldEqual 1
    map.mapRemove(1L)
    map.mapSize shouldEqual 0
    checkElems(Nil, map.mapIterate.toBuffer)

    // pre-populate with various elements
    val elems = makeElems((2 to 10).map(_.toLong))
    elems.foreach { elem =>
      map.mapPut(elem)
      map.mapContains(UnsafeUtils.getLong(elem)) shouldEqual true
    }
    map.mapSize shouldEqual 9
    map.mapGetLast shouldEqual elems.last
    map.mapGetFirst shouldEqual elems.head
    map.mapGet(4L) shouldEqual elems(2)

    // remove at tail.  No resizing should occur.
    map.mapRemove(2L)
    map.mapGetFirst shouldEqual elems(1)
    map.mapSize shouldEqual 8
    checkElems((3 to 10).map(_.toLong), map.mapIterate.toBuffer)

    // remove in middle.  Resizing because 8 -> 7?
    map.mapRemove(6L)
    map.mapSize shouldEqual 7
    checkElems(Seq(3L, 4L, 5L, 7L, 8L, 9L, 10L), map.mapIterate.toBuffer)

    // re-insert removed element
    map.mapPut(elems(4))
    map.mapSize shouldEqual 8
    checkElems((3 to 10).map(_.toLong), map.mapIterate.toBuffer)

    map.mapFree()
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  it("should handle concurrent inserts in various places") {
    // Let's have 1 thread inserting at head, and another one inserting in middle
    val map = new OffheapSortedIDMap(memFactory, 32)
    val headElems = makeElems((100 to 199).map(_.toLong))
    val midElems = makeElems((0 to 99).map(_.toLong))

    val headThread = Future {
      headElems.foreach { elem =>
        map.mapWithExclusive(map.mapPut(elem))
        map.mapContains(UnsafeUtils.getLong(elem)) shouldEqual true
      }
    }
    val midThread = Future {
      midElems.foreach { elem =>
        map.mapWithExclusive(map.mapPut(elem))
        map.mapContains(UnsafeUtils.getLong(elem)) shouldEqual true
      }
    }
    Future.sequence(Seq(headThread, midThread)).futureValue

    map.mapSize shouldEqual (headElems.length + midElems.length)
    checkElems((0 to 199).map(_.toLong), map.mapIterate.toBuffer)

    map.mapFree()
  }

  it("should handle concurrent inserts and ensure slice/iterations return sane data") {
    // 1 thread inserts random elem.  Another allocates random strings in the buffer, just to
    // increase chances of reading random crap
    val map = new OffheapSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 99).map(_.toLong)).toSeq

    val insertThread = Future {
      Random.shuffle(elems).foreach { elem =>
        map.mapWithExclusive(map.mapPut(elem))
        map.mapContains(UnsafeUtils.getLong(elem)) shouldEqual true
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
        map.mapSlice(25, 75).toBuffer.map(UnsafeUtils.getLong).foreach { key =>
          // This cannot always be guaranteed, esp if inserts change things underneath
          //key should be >= 25L
          key should be <= 75L
        }
      }
    }
    Future.sequence(Seq(insertThread, stringThread, readThread)).futureValue

    map.mapSize shouldEqual elems.length
    checkElems((0 to 99).map(_.toLong), map.mapIterate.toBuffer)

    map.mapFree()
  }

  it("should handle concurrent inserts and deletes in various places") {
    // First insert 0 to 99 single threaded
    val map = new OffheapSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 99).map(_.toLong))
    elems.foreach { elem =>
      map.mapWithExclusive(map.mapPut(elem))
    }
    map.mapSize shouldEqual elems.length

    val moreElems = makeElems((100 to 199).map(_.toLong))
    val toDelete = util.Random.shuffle(0 to 99)

    // Now, have one thread deleting 0-99, while second one inserts 100-199
    val deleteThread = Future {
      toDelete.foreach { n =>
        map.mapWithExclusive(map.mapRemove(n))
        map.mapContains(n) shouldEqual false
      }
    }

    val insertThread = Future {
      moreElems.foreach { elem =>
        map.mapWithExclusive(map.mapPut(elem))
        // once in a while this could fail
        //map.mapContains(UnsafeUtils.getLong(elem)) shouldEqual true
      }
    }

    Future.sequence(Seq(deleteThread, insertThread)).futureValue

    // Final map should have ONLY 100-199
    map.mapSize shouldEqual moreElems.length
    checkElems((100 to 199).map(_.toLong), map.mapIterate.toBuffer)

    map.mapFree()
  }

  it("should slice correctly") {
    val map = new OffheapSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 30 by 3).map(_.toLong))
    elems.foreach { elem =>
      map.mapPut(elem)
    }
    map.mapSize shouldEqual elems.length

    // slice: match startKey, but not endKey
    checkElems(Seq(9L, 12L, 15L), map.mapSlice(9L, 16L).toBuffer)
    checkElems((0 to 15 by 3).map(_.toLong), map.mapSlice(0L, 16L).toBuffer)
    checkElems((18 to 30 by 3).map(_.toLong), map.mapSlice(18L, 31L).toBuffer)
    checkElems(Seq(30L), map.mapSlice(30L, 30L).toBuffer)

    // slice: not match startKey, match endKey
    checkElems((0 to 12 by 3).map(_.toLong), map.mapSlice(-1L, 12L).toBuffer)
    checkElems((12 to 18 by 3).map(_.toLong), map.mapSlice(10L, 18L).toBuffer)
    checkElems(Nil, map.mapSlice(19L, 18L).toBuffer)

    // slice: no match for either
    checkElems((12 to 18 by 3).map(_.toLong), map.mapSlice(10L, 19L).toBuffer)
    checkElems((0 to 15 by 3).map(_.toLong), map.mapSlice(-2L, 17L).toBuffer)
    checkElems((21 to 30 by 3).map(_.toLong), map.mapSlice(20L, 33L).toBuffer)
    checkElems(Nil, map.mapSlice(16L, 17L).toBuffer)

    map.mapFree()
  }

  it("should sliceToEnd correctly") {
    val map = new OffheapSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 30 by 3).map(_.toLong))
    elems.foreach { elem =>
      map.mapPut(elem)
    }
    map.mapSize shouldEqual elems.length

    checkElems((18 to 30 by 3).map(_.toLong), map.mapSliceToEnd(18L).toBuffer)
    checkElems((0 to 30 by 3).map(_.toLong), map.mapSliceToEnd(0L).toBuffer)

    checkElems((18 to 30 by 3).map(_.toLong), map.mapSliceToEnd(17L).toBuffer)
    checkElems(Nil, map.mapSliceToEnd(31L).toBuffer)
    checkElems(Seq(30L), map.mapSliceToEnd(30L).toBuffer)

    map.mapFree()
  }

  it("should behave gracefully once map is freed") {
    val map = new OffheapSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 30 by 3).map(_.toLong))
    elems.foreach { elem =>
      map.mapPut(elem)
    }
    map.mapSize shouldEqual elems.length

    map.mapFree()
    map.mapSize shouldEqual 0
    map.mapGet(2L) shouldEqual 0
    map.mapContains(3L) shouldEqual false
    intercept[IndexOutOfBoundsException] { map.mapGetFirst }
    intercept[IndexOutOfBoundsException] { map.mapGetLast }
    map.mapIterate.toBuffer shouldEqual Buffer.empty[Long]
    map.mapSliceToEnd(18L).toBuffer shouldEqual Buffer.empty[Long]
    map.mapSize shouldEqual 0
    map.mapRemove(6L)

    // Double free does nothing.
    map.mapFree()
  }

  it("should handle random access") {
    // This test exercises more circular buffer cases. This can be verified with simple code
    // coverage examination. The important cases deal with wraparound when adding and removing
    // elements, which do end up getting tested with these parameters.

    val rnd = new java.util.Random(8675309)
    val map = new OffheapSortedIDMap(memFactory, 4)
    val set = new HashSet[Long]()

    var size = 0

    for (i <- 1 to 1000) {
      val id = rnd.nextInt(50)
      if (rnd.nextBoolean()) {
        set.add(id)
        if (map.mapPutIfAbsent(makeElementWithID(id))) {
          size += 1
        }
        map.mapContains(id) shouldEqual true
      } else {
        if (map.mapContains(id)) {
          set.remove(id) shouldEqual true
          map.mapRemove(id)
          map.mapContains(id) shouldEqual false
          size -= 1
        } else {
          set.remove(id) shouldEqual false
        }
      }
      map.mapSize shouldEqual size
      set.size shouldEqual size
    }

    map.mapFree()
  }

  it("should support uncontended locking behavior") {
    val map = new OffheapSortedIDMap(memFactory, 32)

    map.mapAcquireExclusive()
    map.mapReleaseExclusive()

    map.mapAcquireShared()
    map.mapReleaseShared()

    // Shouldn't stall.
    map.mapAcquireExclusive()
    map.mapReleaseExclusive()

    // Re-entrant shared lock.
    map.mapAcquireShared()
    map.mapAcquireShared()
    map.mapReleaseShared()
    map.mapReleaseShared()

    // Shouldn't stall.
    map.mapAcquireExclusive()
    map.mapReleaseExclusive()

    map.mapFree()
  }

  it("should support exclusive lock") {
    val map = new OffheapSortedIDMap(memFactory, 32)

    map.mapAcquireExclusive()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.mapAcquireExclusive()
        acquired = true
        map.mapReleaseExclusive()
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    val durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Now let the second lock request complete.
    map.mapReleaseExclusive()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.mapFree()
  }

  it("should block exclusive lock when shared lock is held") {
    val map = new OffheapSortedIDMap(memFactory, 32)

    map.mapAcquireShared()
    map.mapAcquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.mapAcquireExclusive()
        acquired = true
        map.mapReleaseExclusive()
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    map.mapReleaseShared()

    stuck.join(500)
    durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L * 2

    acquired shouldBe false

    // Now let the exclusive lock request complete.
    map.mapReleaseShared()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.mapFree()
  }

  it("should block shared lock when exclusive lock is held") {
    val map = new OffheapSortedIDMap(memFactory, 32)

    map.mapAcquireExclusive()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.mapAcquireShared()
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
    map.mapReleaseExclusive()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    // Can acquire more shared locks.
    map.mapAcquireShared()
    map.mapAcquireShared()

    // Release all shared locks.
    for (i <- 1 to 3) map.mapReleaseShared

    // Exclusive can be acquired again.
    map.mapAcquireExclusive()
    map.mapReleaseExclusive()

    map.mapFree()
  }

  it("should delay shared lock when exclusive lock is waiting") {
    val map = new OffheapSortedIDMap(memFactory, 32)

    map.mapAcquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.mapAcquireExclusive()
        acquired = true
        map.mapReleaseExclusive()
      }
    }

    stuck.start()

    var startNanos = System.nanoTime()
    stuck.join(1000)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 1000000000L

    acquired shouldBe false

    startNanos = System.nanoTime()
    for (i <- 1 to 2) {
      map.mapAcquireShared()
      map.mapReleaseShared()
      Thread.sleep(100)
    }
    durationNanos = System.nanoTime() - startNanos

    durationNanos should be > 1000000000L
    acquired shouldBe false

    // Now let the exclusive lock request complete.
    map.mapReleaseShared()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.mapFree()
  }

  it("should release all shared locks held by the current thread") {
    val map = new OffheapSortedIDMap(memFactory, 32)

    map.mapAcquireShared()
    map.mapAcquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.mapAcquireExclusive()
        acquired = true
        map.mapReleaseExclusive()
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Releasing all shared locks allows the exclusive lock request to complete.
    OffheapSortedIDMap.releaseAllSharedLocks()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.mapFree()
  }

  it("should release all shared locks held for only the current thread") {
    val map = new OffheapSortedIDMap(memFactory, 32)

    map.mapAcquireShared()
    map.mapAcquireShared()

    // Acquire another share, in another thread.
    val shareThread = new Thread {
      override def run(): Unit = map.mapAcquireShared()
    }

    shareThread.start()
    shareThread.join()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.mapAcquireExclusive()
        acquired = true
        map.mapReleaseExclusive()
      }
    }

    stuck.start()

    var startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Releasing one thread shared locks isn't sufficient.
    OffheapSortedIDMap.releaseAllSharedLocks()
    Thread.`yield`

    startNanos = System.nanoTime()
    stuck.join(500)
    durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    // Now let the exclusive lock request complete.
    map.mapReleaseShared()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    map.mapFree()
  }
}
