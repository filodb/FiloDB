package filodb.memory

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ConductorFixture
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.scalatest.matchers.should.Matchers

class PageAlignedBlockManagerConcurrentSpec extends FixtureAnyFunSuite
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

      threadNamed("Random guy") {
        //1 page
        val blocks = blockManager.requestBlocks(pageSize, false)
        blocks.size should be(1)
        val block = blocks.head
        block.position(block.position() + 1)
        waitForBeat(1)
      }
      threadNamed("Another dude") {
        //2 page
        val blocks = blockManager.requestBlocks(2 * pageSize, false)
        blocks.size should be(2)
        val block = blocks.head
        block.position(block.position() + 1)
        waitForBeat(1)
      }
      threadNamed("Yet another dude") {
        //3 page
        val blocks = blockManager.requestBlocks(3 * pageSize, false)
        blocks.size should be(3)
        val block = blocks.head
        block.position(block.position() + 1)
        waitForBeat(1)
      }

  }
}
