package filodb.memory.data

import scala.concurrent.Future
import scala.util.Random

import debox.Buffer
import org.scalatest.concurrent.ScalaFutures

import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.vectors.NativeVectorTest

class OffheapLFSortedIDMapTest extends NativeVectorTest with ScalaFutures {
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
    val map = SingleOffheapLFSortedIDMap(memFactory, 8)
    map.length shouldEqual 0
    map.maxElements(map) shouldEqual 8
    map.tail(map) shouldEqual 0
    map.contains(5L) shouldEqual false
    intercept[IndexOutOfBoundsException] { map.first }
    intercept[IndexOutOfBoundsException] { map.last }
    intercept[IllegalArgumentException] { map(5L) }
    map.iterate.toBuffer shouldEqual Buffer.empty[Long]
  }

  it("should insert and read back properly in various places") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 8)
    val elems = makeElems((0 to 11).map(_.toLong))

    // when empty
    map.put(elems(5))
    map.length shouldEqual 1
    map.contains(5L) shouldEqual true
    map(5L) shouldEqual elems(5)
    map.last shouldEqual elems(5)
    map.first shouldEqual elems(5)
    checkElems(Seq(5L), map.iterate.toBuffer)

    // at head, not empty and not full
    map.put(elems(8))
    map.length shouldEqual 2
    map.contains(8L) shouldEqual true
    map.last shouldEqual elems(8)
    map.first shouldEqual elems(5)
    checkElems(Seq(5L, 8L), map.iterate.toBuffer)

    // in middle, not empty and not full (no resize)
    map.put(elems(6))
    map.length shouldEqual 3
    map.contains(6L) shouldEqual true
    map.last shouldEqual elems(8)
    map.first shouldEqual elems(5)
    map.maxElements(map) shouldEqual 8
    checkElems(Seq(5L, 6L, 8L), map.iterate.toBuffer)

    // Should be no resizing as long as length/# elements < 7
    Seq(2, 3, 9, 7).foreach { n =>
      map.put(elems(n))
      map.contains(n.toLong) shouldEqual true
    }
    map.length shouldEqual 7
    map.maxElements(map) shouldEqual 8  // still not resized hopefully
    map.last shouldEqual elems(9)
    checkElems(Seq(2L, 3L, 5L, 6L, 7L, 8L, 9L), map.iterate.toBuffer)

    // at head, full (should resize)
    val origPtr = map.mapPtr
    map.put(elems(10))
    map.length shouldEqual 8
    map.maxElements(map) shouldEqual 16
    map.mapPtr should not equal (origPtr)
    checkElems(Seq(2L, 3L, 5L, 6L, 7L, 8L, 9L, 10L), map.iterate.toBuffer)

    // in middle, full (should resize)
    // should not resize until # elements = 15
    val elems2 = makeElems((21 to 27).map(_.toLong))
    elems2.foreach { elem =>
      map.put(elem)
      map.contains(UnsafeUtils.getLong(elem)) shouldEqual true
    }
    map.length shouldEqual 15
    map.maxElements(map) shouldEqual 16  // still not resized hopefully
    map.last shouldEqual elems2.last

    map.put(elems(4))
    map.length shouldEqual 16
    map.maxElements(map) shouldEqual 32
    checkElems(((2 to 10) ++ (21 to 27)).map(_.toLong), map.iterate.toBuffer)
  }

  it("should replace existing elements in various places") {
    // pre-populate with elements 2 to 10
    val map = SingleOffheapLFSortedIDMap(memFactory, 8)
    val elems = makeElems((2 to 10).map(_.toLong))
    elems.foreach { elem =>
      map.put(elem)
      map.contains(UnsafeUtils.getLong(elem)) shouldEqual true
    }
    map.length shouldEqual 9
    map.last shouldEqual elems.last
    map(4L) shouldEqual elems(2)

    // replace in middle
    val newElem4 = makeElementWithID(4L)
    map.put(newElem4)
    map.length shouldEqual 9
    map(4L) shouldEqual newElem4
    map(4L) should not equal (elems(2))
    checkElems((2 to 10).map(_.toLong), map.iterate.toBuffer)

    // replace at head
    val newElem10 = makeElementWithID(10L)
    map.put(newElem10)
    map.length shouldEqual 9
    map(10L) shouldEqual newElem10
    map.last shouldEqual newElem10
    map(10L) should not equal (elems.last)
    checkElems((2 to 10).map(_.toLong), map.iterate.toBuffer)
  }

  it("should putIfAbsent only if item doesn't already exist") {
    // pre-populate with elements 2 to 10
    val map = SingleOffheapLFSortedIDMap(memFactory, 8)
    val elems = makeElems((2 to 10).map(_.toLong))
    map.length shouldEqual 0

    map.putIfAbsent(map, 2, elems(0)) shouldEqual true
    map.length shouldEqual 1

    val twoElem = makeElementWithID(2)
    map.putIfAbsent(map, 2, twoElem) shouldEqual false
    map.length shouldEqual 1

    map.putIfAbsent(map, 5, elems(3)) shouldEqual true
    map.length shouldEqual 2

    map.putIfAbsent(map, 5, elems(3)) shouldEqual false
    map.length shouldEqual 2

    val elemIt = map.iterate
    try {
      elemIt.hasNext shouldEqual true
      elemIt.next shouldEqual elems(0)
      elemIt.hasNext shouldEqual true
      elemIt.next shouldEqual elems(3)
    } finally {
      elemIt.close()
    }

    // TODO: add concurrency tests...
  }

  it("should not be able to put NULL elements") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 8)
    intercept[IllegalArgumentException] {
      map.put(0)
    }
  }

  it("should insert, delete, and reinsert") {
    // insert 1 item, then delete it, test map is truly empty
    val map = SingleOffheapLFSortedIDMap(memFactory, 8)
    map.put(makeElementWithID(1))
    map.length shouldEqual 1
    map.remove(1L)
    map.length shouldEqual 0
    checkElems(Nil, map.iterate.toBuffer)

    // pre-populate with various elements
    val elems = makeElems((2 to 10).map(_.toLong))
    elems.foreach { elem =>
      map.put(elem)
      map.contains(UnsafeUtils.getLong(elem)) shouldEqual true
    }
    map.length shouldEqual 9
    map.maxElements(map) shouldEqual 16
    map.last shouldEqual elems.last
    map.first shouldEqual elems.head
    map(4L) shouldEqual elems(2)

    // remove at tail.  No resizing should occur.
    map.remove(2L)
    map.first shouldEqual elems(1)
    map.length shouldEqual 8
    map.maxElements(map) shouldEqual 16
    checkElems((3 to 10).map(_.toLong), map.iterate.toBuffer)

    // remove in middle.  Resizing because 8 -> 7?
    map.remove(6L)
    map.length shouldEqual 7
    map.maxElements(map) shouldEqual 8
    checkElems(Seq(3L, 4L, 5L, 7L, 8L, 9L, 10L), map.iterate.toBuffer)

    // re-insert removed element
    map.put(elems(4))
    map.length shouldEqual 8
    checkElems((3 to 10).map(_.toLong), map.iterate.toBuffer)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  it("should handle concurrent inserts in various places") {
    // Let's have 1 thread inserting at head, and another one inserting in middle
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)
    val headElems = makeElems((100 to 199).map(_.toLong))
    val midElems = makeElems((0 to 99).map(_.toLong))

    val headThread = Future {
      headElems.foreach { elem =>
        map.withExclusive(map.put(elem))
        map.contains(UnsafeUtils.getLong(elem)) shouldEqual true
      }
    }
    val midThread = Future {
      midElems.foreach { elem =>
        map.withExclusive(map.put(elem))
        map.contains(UnsafeUtils.getLong(elem)) shouldEqual true
      }
    }
    Future.sequence(Seq(headThread, midThread)).futureValue

    map.length shouldEqual (headElems.length + midElems.length)
    checkElems((0 to 199).map(_.toLong), map.iterate.toBuffer)
  }

  it("should handle concurrent inserts and ensure slice/iterations return sane data") {
    // 1 thread inserts random elem.  Another allocates random strings in the buffer, just to increase
    // chances of reading random crap
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 99).map(_.toLong)).toSeq

    val insertThread = Future {
      Random.shuffle(elems).foreach { elem =>
        map.withExclusive(map.put(elem))
        map.contains(UnsafeUtils.getLong(elem)) shouldEqual true
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
        map.slice(25, 75).toBuffer.map(UnsafeUtils.getLong).foreach { key =>
          // key should be >= 25L   // This cannot always be guaranteed, esp if inserts change things underneath
          key should be <= 75L
        }
      }
    }
    Future.sequence(Seq(insertThread, stringThread, readThread)).futureValue

    map.length shouldEqual elems.length
    checkElems((0 to 99).map(_.toLong), map.iterate.toBuffer)
  }

  it("should handle concurrent inserts and deletes in various places") {
    // First insert 0 to 99 single threaded
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 99).map(_.toLong))
    elems.foreach { elem =>
      map.withExclusive(map.put(elem))
    }
    map.length shouldEqual elems.length

    val moreElems = makeElems((100 to 199).map(_.toLong))
    val toDelete = util.Random.shuffle(0 to 99)

    // Now, have one thread deleting 0-99, while second one inserts 100-199
    val deleteThread = Future {
      toDelete.foreach { n =>
        map.withExclusive(map.remove(n))
        map.contains(n) shouldEqual false
      }
    }

    val insertThread = Future {
      moreElems.foreach { elem =>
        map.withExclusive(map.put(elem))
        // map.contains(UnsafeUtils.getLong(elem)) shouldEqual true   // once in a while this could fail
      }
    }

    Future.sequence(Seq(deleteThread, insertThread)).futureValue

    // Final map should have ONLY 100-199
    map.length shouldEqual moreElems.length
    checkElems((100 to 199).map(_.toLong), map.iterate.toBuffer)
  }

  it("should slice correctly") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 30 by 3).map(_.toLong))
    elems.foreach { elem =>
      map.put(elem)
    }
    map.length shouldEqual elems.length

    // slice: match startKey, but not endKey
    checkElems(Seq(9L, 12L, 15L), map.slice(9L, 16L).toBuffer)
    checkElems((0 to 15 by 3).map(_.toLong), map.slice(0L, 16L).toBuffer)
    checkElems((18 to 30 by 3).map(_.toLong), map.slice(18L, 31L).toBuffer)
    checkElems(Seq(30L), map.slice(30L, 30L).toBuffer)

    // slice: not match startKey, match endKey
    checkElems((0 to 12 by 3).map(_.toLong), map.slice(-1L, 12L).toBuffer)
    checkElems((12 to 18 by 3).map(_.toLong), map.slice(10L, 18L).toBuffer)
    checkElems(Nil, map.slice(19L, 18L).toBuffer)

    // slice: no match for either
    checkElems((12 to 18 by 3).map(_.toLong), map.slice(10L, 19L).toBuffer)
    checkElems((0 to 15 by 3).map(_.toLong), map.slice(-2L, 17L).toBuffer)
    checkElems((21 to 30 by 3).map(_.toLong), map.slice(20L, 33L).toBuffer)
    checkElems(Nil, map.slice(16L, 17L).toBuffer)
  }

  it("should sliceToEnd correctly") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 30 by 3).map(_.toLong))
    elems.foreach { elem =>
      map.put(elem)
    }
    map.length shouldEqual elems.length

    checkElems((18 to 30 by 3).map(_.toLong), map.sliceToEnd(18L).toBuffer)
    checkElems((0 to 30 by 3).map(_.toLong), map.sliceToEnd(0L).toBuffer)

    checkElems((18 to 30 by 3).map(_.toLong), map.sliceToEnd(17L).toBuffer)
    checkElems(Nil, map.sliceToEnd(31L).toBuffer)
    checkElems(Seq(30L), map.sliceToEnd(30L).toBuffer)
  }

  it("should behave gracefully once map is freed") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)
    val elems = makeElems((0 to 30 by 3).map(_.toLong))
    elems.foreach { elem =>
      map.put(elem)
    }
    map.length shouldEqual elems.length

    map.free(map)
    map.length shouldEqual 0
    map(2L) shouldEqual 0
    map.contains(3L) shouldEqual false
    intercept[IndexOutOfBoundsException] { map.first }
    intercept[IndexOutOfBoundsException] { map.last }
    map.iterate.toBuffer shouldEqual Buffer.empty[Long]
    map.sliceToEnd(18L).toBuffer shouldEqual Buffer.empty[Long]
    map.length shouldEqual 0
    map.remove(6L)
  }

  it("should support uncontended locking behavior") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)

    map.acquireExclusive()
    map.releaseExclusive()

    map.acquireShared()
    map.releaseShared()

    // Shouldn't stall.
    map.acquireExclusive()
    map.releaseExclusive()

    // Re-entrant shared lock.
    map.acquireShared()
    map.acquireShared()
    map.releaseShared()
    map.releaseShared()

    // Shouldn't stall.
    map.acquireExclusive()
    map.releaseExclusive()
  }

  it("should support exclusive lock") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)

    map.acquireExclusive()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.acquireExclusive()
        acquired = true
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    val durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Now let the second lock request complete.
    map.releaseExclusive()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true
  }

  it("should block exclusive lock when shared lock is held") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)

    map.acquireShared()
    map.acquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.acquireExclusive()
        acquired = true
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    map.releaseShared()

    stuck.join(500)
    durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L * 2

    acquired shouldBe false

    // Now let the exclusive lock request complete.
    map.releaseShared()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true
  }

  it("should block shared lock when exclusive lock is held") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)

    map.acquireExclusive()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.acquireShared()
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
    map.releaseExclusive()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true

    // Can acquire more shared locks.
    map.acquireShared()
    map.acquireShared()

    // Release all shared locks.
    for (i <- 1 to 3) map.releaseShared

    // Exclusive can be acquired again.
    map.acquireExclusive()
  }

  it("should delay shared lock when exclusive lock is waiting") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)

    map.acquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.acquireExclusive()
        acquired = true
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
      map.acquireShared()
      map.releaseShared()
      Thread.sleep(100)
    }
    durationNanos = System.nanoTime() - startNanos

    durationNanos should be > 1000000000L
    acquired shouldBe false

    // Now let the exclusive lock request complete.
    map.releaseShared()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true
  }

  it("should release all shared locks held by the current thread") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)

    map.acquireShared()
    map.acquireShared()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.acquireExclusive()
        acquired = true
      }
    }

    stuck.start()

    val startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Releasing all shared locks allows the exclusive lock request to complete.
    OffheapLFSortedIDMap.releaseAllSharedLocks()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true
  }

  it("should release all shared locks held for only the current thread") {
    val map = SingleOffheapLFSortedIDMap(memFactory, 32)

    map.acquireShared()
    map.acquireShared()

    // Acquire another share, in another thread.
    val shareThread = new Thread {
      override def run(): Unit = map.acquireShared()
    }

    shareThread.start()
    shareThread.join()

    @volatile var acquired = false

    val stuck = new Thread {
      override def run(): Unit = {
        map.acquireExclusive()
        acquired = true
      }
    }

    stuck.start()

    var startNanos = System.nanoTime()
    stuck.join(500)
    var durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    acquired shouldBe false

    // Releasing one thread shared locks isn't sufficient.
    OffheapLFSortedIDMap.releaseAllSharedLocks()
    Thread.`yield`

    startNanos = System.nanoTime()
    stuck.join(500)
    durationNanos = System.nanoTime() - startNanos

    durationNanos should be >= 500000000L

    // Now let the exclusive lock request complete.
    map.releaseShared()
    Thread.`yield`

    stuck.join(10000)

    acquired shouldBe true
  }
}