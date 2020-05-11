package filodb.memory

import scala.language.reflectiveCalls

import com.kenai.jffi.PageManager
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

object PageAlignedBlockManagerSpec {
  val testReclaimer = new ReclaimListener {
    var reclaimedBytes = 0
    val addresses = new collection.mutable.ArrayBuffer[Long]
    def onReclaim(metadata: Long, numBytes: Int): Unit = {
      reclaimedBytes += numBytes
      addresses += metadata
    }
  }
}

class PageAlignedBlockManagerSpec extends FlatSpec with Matchers with BeforeAndAfter {
  import PageAlignedBlockManagerSpec._

  val pageSize = PageManager.getInstance().pageSize()

  before {
    testReclaimer.reclaimedBytes = 0
  }

  it should "Allocate blocks as requested for size" in {
    //2MB
    val stats = new MemoryStats(Map("test1" -> "test1"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, testReclaimer, 1)

//    val fbm = freeBlocksMetric(stats)
//    fbm.max should be(512)
    val blockSize = blockManager.blockSizeInBytes
    val blocks = blockManager.requestNonReclaimableBlocks(blockSize * 10)
    blocks.size should be(10)
//    val ubm = usedBlocksMetric(stats)
//    ubm.max should be(10)
//    val fbm2 = freeBlocksMetric(stats)
//    fbm2.min should be(502)

    blockManager.releaseBlocks()
  }

  it should "Align block size to page size" in {
    val stats = new MemoryStats(Map("test2" -> "test2"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, testReclaimer, 1)
    val blockSize = blockManager.blockSizeInBytes
    blockSize should be(pageSize)

    blockManager.releaseBlocks()
  }

  it should "Not allow a request of blocks over the upper bound if no blocks are reclaimable" in {
    //2 pages
    val stats = new MemoryStats(Map("test3" -> "test3"))
    val blockManager = new PageAlignedBlockManager(2 * pageSize, stats, testReclaimer, 1)
    val blockSize = blockManager.blockSizeInBytes
//    val fbm = freeBlocksMetric(stats)
//    fbm.max should be(2)
    val firstRequest = blockManager.requestNonReclaimableBlocks(blockSize * 2)
    //used 2 out of 2
    firstRequest.size should be(2)
//    val ubm = usedBlocksMetric(stats)
//    ubm.max should be(2)
    //cannot fulfill
//    val fbm2 = freeBlocksMetric(stats)
//    fbm2.min should be(0)
    val secondRequest = blockManager.requestNonReclaimableBlocks(blockSize * 2)
    secondRequest should be(Seq.empty)

    blockManager.releaseBlocks()
  }

  it should "Allocate blocks as requested even when upper bound is reached if blocks can be reclaimed" in {
    //2 pages
    val stats = new MemoryStats(Map("test4" -> "test4"))
    val blockManager = new PageAlignedBlockManager(2 * pageSize, stats, testReclaimer, 1)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestNonReclaimableBlocks(blockSize * 2)
    //used 2 out of 2
    firstRequest.size should be(2)
    //simulate writing to the block
    firstRequest.head.own()
    firstRequest.head.position(blockSize.toInt - 1)
    //mark them as reclaimable
    firstRequest.foreach(_.markReclaimable())
    val secondRequest = blockManager.requestNonReclaimableBlocks(blockSize * 2)
//    val brm = reclaimedBlocksMetric(stats)
//    brm.count should be(2)
    //this request will fulfill
    secondRequest.size should be(2)
    secondRequest.head.hasCapacity(10) should be(true)

    blockManager.releaseBlocks()
  }

  it should "Fail to Allocate blocks when enough blocks cannot be reclaimed" in {
    //4 pages
    val stats = new MemoryStats(Map("test5" -> "test5"))
    val blockManager = new PageAlignedBlockManager(4 * pageSize, stats, testReclaimer, 1)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestNonReclaimableBlocks(blockSize * 2)
    //used 2 out of 4
    firstRequest.size should be(2)
    //only 2 left - cannot fulfill request
    val secondRequest = blockManager.requestNonReclaimableBlocks(blockSize * 3)
//    val brm = reclaimedBlocksMetric(stats)
//    brm.count should be(0)
    secondRequest should be(Seq.empty)

    blockManager.releaseBlocks()
  }

  it should "allocate and reclaim blocks" in {
    val stats = new MemoryStats(Map("test5" -> "test5"))
    // This block manager has 5 blocks capacity
    val blockManager = new PageAlignedBlockManager(5 * pageSize, stats, testReclaimer, 1)

    blockManager.usedBlocks.size() shouldEqual 0

    // first allocate non-reclaimable block
    blockManager.requestNonReclaimableBlock().map(_.markReclaimable).isDefined shouldEqual true
    blockManager.usedBlocks.size shouldEqual 1

    blockManager.requestReclaimableBlock().map(_.markReclaimable).isDefined shouldEqual true
    blockManager.requestReclaimableBlock().map(_.markReclaimable).isDefined shouldEqual true
    blockManager.requestReclaimableBlock().isDefined shouldEqual true
    blockManager.usedBlocks.size() shouldEqual 4

    blockManager.requestReclaimableBlock().map(_.markReclaimable).isDefined shouldEqual true
    blockManager.usedBlocks.size() shouldEqual 5

    // requesting more blocks should force reclamation of the older ones
    blockManager.requestReclaimableBlock().isDefined shouldEqual true
    blockManager.requestReclaimableBlock().isDefined shouldEqual true
    blockManager.requestReclaimableBlock().isDefined shouldEqual true

    blockManager.usedBlocks.size() shouldEqual 5

    blockManager.releaseBlocks()
  }

  it should ("allocate blocks using BlockMemFactory with ownership and reclaims") in {
    val stats = new MemoryStats(Map("test5" -> "test5"))
    // This block manager has 5 blocks capacity
    val blockManager = new PageAlignedBlockManager(5 * pageSize, stats, testReclaimer, 1)

    blockManager.usedBlocks.size() shouldEqual 0

    val factory = new BlockMemFactory(blockManager, true, 24, Map("foo" -> "bar"), false)

    // There should be one non-reclaimable block allocated, owned by factory
    blockManager.usedBlocks.size shouldEqual 1

    factory.currentBlock.owner shouldEqual Some(factory)

    // Now allocate 4 more non-reclaimable blocks, that will use up all blocks
    blockManager.requestNonReclaimableBlock().isDefined shouldEqual true
    blockManager.requestNonReclaimableBlock().isDefined shouldEqual true
    blockManager.requestNonReclaimableBlock().isDefined shouldEqual true
    blockManager.requestNonReclaimableBlock().isDefined shouldEqual true
    blockManager.usedBlocks.size shouldEqual 5

    // Mark as reclaimable the blockMemFactory's block.  Then request more blocks, that one will be reclaimed.
    // Check ownership is now cleared.
    factory.currentBlock.markReclaimable
    blockManager.requestReclaimableBlock().isDefined shouldEqual true

    factory.currentBlock.owner shouldEqual None  // new requestor did not have owner
  }

  it should "ensure free space" in {
    val stats = new MemoryStats(Map("test5" -> "test5"))
    // This block manager has 5 blocks capacity
    val blockManager = new PageAlignedBlockManager(5 * pageSize, stats, testReclaimer, 1)

    blockManager.numFreeBlocks shouldEqual 5
    blockManager.ensureFreePercent(50)
    blockManager.numFreeBlocks shouldEqual 5

    blockManager.requestNonReclaimableBlock().map(_.markReclaimable).isDefined shouldEqual true
    blockManager.numFreeBlocks shouldEqual 4
    blockManager.ensureFreePercent(50)
    blockManager.numFreeBlocks shouldEqual 4

    blockManager.requestNonReclaimableBlock().map(_.markReclaimable).isDefined shouldEqual true
    blockManager.numFreeBlocks shouldEqual 3
    blockManager.ensureFreePercent(50)
    blockManager.numFreeBlocks shouldEqual 3

    blockManager.requestNonReclaimableBlock().map(_.markReclaimable).isDefined shouldEqual true
    blockManager.numFreeBlocks shouldEqual 2
    blockManager.ensureFreePercent(50)
    // Should actually have done something this time.
    blockManager.numFreeBlocks shouldEqual 3

    blockManager.requestNonReclaimableBlock().map(_.markReclaimable).isDefined shouldEqual true
    blockManager.numFreeBlocks shouldEqual 2
    blockManager.ensureFreePercent(90)
    // Should reclaim multiple blocks.
    blockManager.numFreeBlocks shouldEqual 5

    blockManager.releaseBlocks()
  }
}
