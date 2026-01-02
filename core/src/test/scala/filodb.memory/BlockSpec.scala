package filodb.memory

import com.kenai.jffi.PageManager

import scala.language.reflectiveCalls
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BlockSpec extends AnyFlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  import PageAlignedBlockManagerSpec._

  val evictionLock = new EvictionLock
  val stats = new MemoryStats(Map("test1" -> "test1"))
  val numPagesPerBlock = 1
  val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, testReclaimer, numPagesPerBlock, evictionLock)
  val expectedEmptyBlockSize = numPagesPerBlock * PageManager.getInstance().pageSize()

  before {
    testReclaimer.reclaimedBytes = 0
    testReclaimer.addresses.clear()
  }

  override def afterAll(): Unit = {
    blockManager.releaseBlocks()
  }

  it should "allocate metadata and report remaining bytes accurately" in {
    val block = blockManager.requestBlock(false).get
    block.capacity shouldEqual expectedEmptyBlockSize
    block.remaining() shouldEqual expectedEmptyBlockSize

    // Now change the position
    block.position(200)
    block.remaining() shouldEqual (expectedEmptyBlockSize - 200)

    // Now allocate some metadata and watch remaining bytes shrink
    block.allocMetadata(40)
    block.remaining() shouldEqual (expectedEmptyBlockSize - 200 - 40 - 2)
  }

  it should "return null when allocate metadata if not enough space" in {
    val block = blockManager.requestBlock(false).get
    block.capacity shouldEqual expectedEmptyBlockSize
    block.remaining() shouldEqual expectedEmptyBlockSize

    val newPosition  = PageManager.getInstance().pageSize().toInt - 299
    block.position(newPosition)
    block.remaining() shouldEqual (expectedEmptyBlockSize-newPosition)
    intercept[OutOfOffheapMemoryException] { block.allocMetadata(300) }
    block.remaining() shouldEqual (expectedEmptyBlockSize-newPosition)   // still same space remaining
  }

  it should "not reclaim when block has not been marked reclaimable" in {
    val block = blockManager.requestBlock(false).get

    intercept[IllegalStateException] { block.reclaim() }
  }

  it should "call reclaimListener with address of all allocated metadatas" in {
    val block = blockManager.requestBlock(false).get
    block.capacity shouldEqual expectedEmptyBlockSize
    block.remaining() shouldEqual expectedEmptyBlockSize

    block.position(200)
    val addr1 = block.allocMetadata(30)
    val addr2 = block.allocMetadata(40)

    block.remaining() shouldEqual (expectedEmptyBlockSize - 200 - 30 - 2 - 40 - 2)

    //XXX for debugging
    println(block.detailedDebugString)

    block.markReclaimable()
    block.reclaim()
    testReclaimer.reclaimedBytes shouldEqual 70
    testReclaimer.addresses shouldEqual Seq(addr2, addr1)
  }
}
