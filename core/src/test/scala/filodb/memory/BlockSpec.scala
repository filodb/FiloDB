package filodb.memory

import java.util.ConcurrentModificationException

import filodb.memory.BlockManager._
import filodb.memory.format.vectors.{IntBinaryVector, LongBinaryVector, UTF8Vector}
import filodb.memory.format.{FiloVector, ZeroCopyUTF8String}
import filodb.memory.impl.PageAlignedBlockManager

import com.kenai.jffi.PageManager
import org.scalatest.Matchers
import org.scalatest.concurrent.ConductorFixture
import org.scalatest.fixture.FunSuite

/**
  * Concurrency tests on a block
  */
//noinspection ScalaStyle
class BlockSpec extends FunSuite with ConductorFixture with Matchers {

  val pageSize = PageManager.getInstance().pageSize()

  val blockManager = new PageAlignedBlockManager(10 * pageSize)

  val intVector = IntBinaryVector.appendingVector(10)
  val longVector = LongBinaryVector.appendingVector(10)

  (0 until 10).foreach {
    i =>
      intVector.add(Some(i))
      longVector.add(Some(i))
  }

  protected def toUTF8(str: String) = {
    UTF8Vector(Array(ZeroCopyUTF8String(str)))
  }

  test("Allow the owning thread to write to the buffer before it is sealed") {
    (conductor: Conductor) =>
      import conductor._
      var block: Block = null
      thread("Owning") {
        //1 page
        block = blockManager.requestBlock(noReclaimPolicy).get
        block.own()
        val utf8Vector = toUTF8("I am the owning thread. I can write what I please")
        block.write(utf8Vector)
      }
  }

  test("Not allow a non-owning thread to write") {
    (conductor: Conductor) =>
      import conductor._
      var block: Block = null
      thread("Owning") {
        //1 page
        block = blockManager.requestBlock(noReclaimPolicy).get
        block.own()
        val utf8Vector = toUTF8("I am the owning thread. I can write what I please")
        block.write(utf8Vector)
        waitForBeat(1)
      }
      thread("Non owning") {
        waitForBeat(1)
        intercept[ConcurrentModificationException] {
          val utf8Vector = toUTF8("I am not the owning thread. That is why this throws an exception")
          block.write(utf8Vector)
        }
      }
  }

  test("Allow transfer of ownership to another thread") {
    (conductor: Conductor) =>
      import conductor._
      var block: Block = null
      thread("Creator") {
        //1 page
        block = blockManager.requestBlock(noReclaimPolicy).get
        block.own()
        val utf8Vector = toUTF8("I own this thread now")
        block.write(utf8Vector)
        waitForBeat(1)
        waitForBeat(2)
        intercept[ConcurrentModificationException] {
          val utf8Vector = toUTF8("I don't own this block anymore. So I can't write")
          block.write(utf8Vector)
        }
      }
      thread("Just another guy") {
        waitForBeat(1)
        intercept[ConcurrentModificationException] {
          val utf8Vector = toUTF8("I don't own this block yet. So I can't write")
          block.write(utf8Vector)
        }
        block.own
        val utf8Vector = toUTF8("I own this now. So this write will succeed")
        block.write(utf8Vector)
        waitForBeat(2)
      }
  }

  test("Allow reading by one thread") {
    (conductor: Conductor) =>
      var block: Block = null
      //1 page
      block = blockManager.requestBlock(noReclaimPolicy).get
      block.own()
      val (offset1, length1) = block.write(intVector)
      val (offset2, length2) = block.write(longVector)
      val (offset3, length3) = block.write(intVector)
      val (offset4, length4) = block.write(longVector)
      //now reads should succeed
      val intBuffer1 = FiloVector.make(block.read(offset1, length1), classOf[Int])
      val longBuffer1 = FiloVector.make(block.read(offset2, length2), classOf[Long])
      val intBuffer2 = FiloVector.make(block.read(offset3, length3), classOf[Int])
      val longBuffer2 = FiloVector.make(block.read(offset4, length4), classOf[Long])

      intBuffer1.size should be(10)
      intBuffer2.size should be(10)
      intBuffer1.get(0) should be(Some(0))
      intBuffer2.get(9) should be(Some(9))
      longBuffer1.size should be(10)
      longBuffer2.size should be(10)
      longBuffer1.get(0) should be(Some(0L))
      longBuffer2.get(1) should be(Some(1L))
      longBuffer2.get(9) should be(Some(9L))

  }

  test("Allow multiple threads to read from different offsets correctly") {
    (conductor: Conductor) =>
      import conductor._
      var block: Block = null
      var (off1, len1) = (0, 0)
      var (off2, len2) = (0, 0)
      thread("Owning") {
        block = blockManager.requestBlock(noReclaimPolicy).get
        block.own()
        val (offset1, length1) = block.write(intVector)
        val (offset2, length2) = block.write(longVector)
        val (offset3, length3) = block.write(intVector)
        val (offset4, length4) = block.write(longVector)
        off1 = offset1
        len1 = length1
        off2 = offset2
        len2 = length2
        waitForBeat(1)
        //now reads should succeed
        val intBuffer1 = FiloVector.make(block.read(offset1, length1), classOf[Int])
        val longBuffer1 = FiloVector.make(block.read(offset2, length2), classOf[Long])
        val intBuffer2 = FiloVector.make(block.read(offset3, length3), classOf[Int])
        val longBuffer2 = FiloVector.make(block.read(offset4, length4), classOf[Long])

        intBuffer1.get(0) should be(Some(0))
        intBuffer2.get(1) should be(Some(1))
        longBuffer1.get(0) should be(Some(0L))
        longBuffer2.get(1) should be(Some(1L))
      }
      thread("Non owning") {
        waitForBeat(1)
        //i can read once sealed even though I don't own the block
        val intBuffer1 = FiloVector.make(block.read(off1, len1), classOf[Int])
        val longBuffer1 = FiloVector.make(block.read(off2, len2), classOf[Long])
        intBuffer1.get(3) should be(Some(3))
        longBuffer1.get(2) should be(Some(2L))
      }

  }

}
