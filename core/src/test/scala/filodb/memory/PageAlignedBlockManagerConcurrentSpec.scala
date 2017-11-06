package filodb.memory

import org.scalatest.Matchers
import org.scalatest.concurrent.ConductorFixture
import org.scalatest.fixture.FunSuite

class PageAlignedBlockManagerConcurrentSpec extends FunSuite with ConductorFixture with Matchers {

  val blockManager = new PageAlignedBlockManager(2048 * 1024)
  val pageSize = blockManager.blockSizeInBytes

  test("Should allow multiple thread to request blocks safely") {
    (conductor: Conductor) =>
      import conductor._

      thread("Random guy") {
        //1 page
        val blocks = blockManager.requestBlocks(pageSize)
        blocks.size should be(1)
        val block = blocks.head
        block.own()
        block.position(block.position() + 1)
        waitForBeat(1)
      }
      thread("Another dude") {
        //2 page
        val blocks = blockManager.requestBlocks(2 * pageSize)
        blocks.size should be(2)
        val block = blocks.head
        block.own()
        block.position(block.position() + 1)
        waitForBeat(1)
      }
      thread("Yet another dude") {
        //3 page
        val blocks = blockManager.requestBlocks(3 * pageSize)
        blocks.size should be(3)
        val block = blocks.head
        block.own()
        block.position(block.position() + 1)
        waitForBeat(1)
      }

  }
}
