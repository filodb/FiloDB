package filodb.memory

import org.scalatest.{Matchers, BeforeAndAfterAll}
import org.scalatest.concurrent.ConductorFixture
import org.scalatest.fixture.FunSuite

class PageAlignedBlockManagerConcurrentSpec extends FunSuite
with ConductorFixture with Matchers with BeforeAndAfterAll {
  import PageAlignedBlockManagerSpec._

  val memoryStats = new MemoryStats(Map("test"-> "test"))
  val blockManager = new PageAlignedBlockManager(2048 * 1024, memoryStats, testReclaimer, 1)
  val pageSize = blockManager.blockSizeInBytes

  override def afterAll(): Unit = {
    blockManager.releaseBlocks()
  }

  test("Should allow multiple thread to request blocks safely") {
    (conductor: Conductor) =>
      import conductor._

      thread("Random guy") {
        //1 page
        val blocks = blockManager.requestNonReclaimableBlocks(pageSize)
        blocks.size should be(1)
        val block = blocks.head
        block.own()
        block.position(block.position() + 1)
        waitForBeat(1)
      }
      thread("Another dude") {
        //2 page
        val blocks = blockManager.requestNonReclaimableBlocks(2 * pageSize)
        blocks.size should be(2)
        val block = blocks.head
        block.own()
        block.position(block.position() + 1)
        waitForBeat(1)
      }
      thread("Yet another dude") {
        //3 page
        val blocks = blockManager.requestNonReclaimableBlocks(3 * pageSize)
        blocks.size should be(3)
        val block = blocks.head
        block.own()
        block.position(block.position() + 1)
        waitForBeat(1)
      }

  }
}
