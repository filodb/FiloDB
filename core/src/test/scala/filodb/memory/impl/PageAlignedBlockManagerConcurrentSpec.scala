package filodb.memory.impl

import filodb.memory.BlockManager._
import filodb.memory.format.ZeroCopyUTF8String
import filodb.memory.format.vectors.UTF8Vector

import org.scalatest.Matchers
import org.scalatest.concurrent.ConductorFixture
import org.scalatest.fixture.FunSuite

class PageAlignedBlockManagerConcurrentSpec extends FunSuite with ConductorFixture with Matchers {

  val blockManager = new PageAlignedBlockManager(2048 * 1024)
  val pageSize = blockManager.blockSizeInBytes
  val ut8Str = toUTF8("Best way to manage memory is to not manage memory")

  protected def toUTF8(str: String) = {
    UTF8Vector(Array(ZeroCopyUTF8String(str)))
  }

  test("Should allow multiple thread to request blocks safely") {
    (conductor: Conductor) =>
      import conductor._

      thread("Random guy") {
        //1 page
        val blocks = blockManager.requestBlocks(pageSize, noReclaimPolicy)
        blocks.size should be(1)
        val block = blocks.head
        block.own()
        block.write(ut8Str)
        waitForBeat(1)
      }
      thread("Another dude") {
        //2 page
        val blocks = blockManager.requestBlocks(2 * pageSize, noReclaimPolicy)
        blocks.size should be(2)
        val block = blocks.head
        block.own()
        block.write(ut8Str)
        waitForBeat(1)
      }
      thread("Yet another dude") {
        //3 page
        val blocks = blockManager.requestBlocks(3 * pageSize, noReclaimPolicy)
        blocks.size should be(3)
        val block = blocks.head
        block.own()
        block.write(ut8Str)
        waitForBeat(1)
      }

  }
}
