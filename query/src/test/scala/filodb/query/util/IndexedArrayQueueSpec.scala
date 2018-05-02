package filodb.query.util

import org.scalatest.{FunSpec, Matchers}

class IndexedArrayQueueSpec extends FunSpec with Matchers {

  it ("should be able to perform add and remove") {
    val q = new IndexedArrayQueue[String](8)
    q.items.size shouldEqual 16

    // add elements
    for { i <- 0 until 8 } {
      q.add(i.toString)
    }
    for { i <- 0 until 8 } {
      q(i) shouldEqual i.toString
    }
    q.size shouldEqual 8

    // remove elements
    q.remove shouldEqual "0"
    q.remove shouldEqual "1"
    q.remove shouldEqual "2"
    q.size shouldEqual 5
    for { i <- 0 until 5 } {
      q(i) shouldEqual (i + 3).toString  // 3 elements removed so far
    }
    q(0) shouldEqual q.head
    q(q.size - 1) shouldEqual q.last

    // add more elements so tl wraps to beginning of array
    for { i <- 8 until 16 } {
      q.add(i.toString)
    }
    // now head > tail
    q.hd should be > q.tl
    q.size shouldEqual 13
    for { i <- 0 until 13 } {
      q(i) shouldEqual (i + 3).toString // 3 elements removed so far
    }
    intercept[ArrayIndexOutOfBoundsException] {
      q(13)
    }
    intercept[ArrayIndexOutOfBoundsException] {
      q(-1)
    }
    q(0) shouldEqual q.head
    q(q.size - 1) shouldEqual q.last

    // add more elements so array size doubles
    for { i <- 16 until 20 } {
      q.add(i.toString)
    }
    q.size shouldEqual 17
    q.items.size shouldEqual 32
    q.hd should be < q.tl
    intercept[ArrayIndexOutOfBoundsException] {
      q(17)
    }
    q(0) shouldEqual q.head
    q(q.size - 1) shouldEqual q.last
  }
}
