package filodb.memory

import scala.language.reflectiveCalls

import org.scalatest.{FlatSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}

class BlockSpec extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  import PageAlignedBlockManagerSpec._

  val stats = new MemoryStats(Map("test1" -> "test1"))
  val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, testReclaimer, 1, 72)

  before {
    testReclaimer.reclaimedBytes = 0
    testReclaimer.addresses.clear()
  }

  override def afterAll(): Unit = {
    blockManager.releaseBlocks()
  }

  it should "allocate metadata and report remaining bytes accurately" in {
    val block = blockManager.requestBlock(None).get
    block.own()
    block.capacity shouldEqual 4096
    block.remaining shouldEqual 4096

    // Now change the position
    block.position(200)
    block.remaining shouldEqual 3896

    // Now allocate some metadata and watch remaining bytes shrink
    block.allocMetadata(40)
    block.remaining shouldEqual 3854
  }

  it should "return null when allocate metadata if not enough space" in {
    val block = blockManager.requestBlock(None).get
    block.own()
    block.capacity shouldEqual 4096
    block.remaining shouldEqual 4096

    block.position(3800)
    block.remaining shouldEqual (4096-3800)
    block.allocMetadata(300) shouldEqual 0
    block.remaining shouldEqual (4096-3800)   // still same space remaining
  }

  it should "not reclaim when block has not been marked reclaimable" in {
    val block = blockManager.requestBlock(None).get
    block.own()

    intercept[IllegalStateException] { block.reclaim() }
  }

  it should "call reclaimListener with address of all allocated metadatas" in {
    val block = blockManager.requestBlock(None).get
    block.own()
    block.capacity shouldEqual 4096
    block.remaining shouldEqual 4096

    block.position(200)
    val addr1 = block.allocMetadata(30)
    val addr2 = block.allocMetadata(40)

    block.remaining shouldEqual 3822

    block.markReclaimable()
    block.reclaim()
    testReclaimer.reclaimedBytes shouldEqual 70
    testReclaimer.addresses shouldEqual Seq(addr2, addr1)
  }
}