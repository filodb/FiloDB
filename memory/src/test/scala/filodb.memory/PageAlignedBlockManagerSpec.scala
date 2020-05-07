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
  import collection.JavaConverters._

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
    val blocks = blockManager.requestBlocks(blockSize * 10, None)
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
    val firstRequest = blockManager.requestBlocks(blockSize * 2, None)
    //used 2 out of 2
    firstRequest.size should be(2)
//    val ubm = usedBlocksMetric(stats)
//    ubm.max should be(2)
    //cannot fulfill
//    val fbm2 = freeBlocksMetric(stats)
//    fbm2.min should be(0)
    val secondRequest = blockManager.requestBlocks(blockSize * 2, None)
    secondRequest should be(Seq.empty)

    blockManager.releaseBlocks()
  }

  it should "Allocate blocks as requested even when upper bound is reached if blocks can be reclaimed" in {
    //2 pages
    val stats = new MemoryStats(Map("test4" -> "test4"))
    val blockManager = new PageAlignedBlockManager(2 * pageSize, stats, testReclaimer, 1)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestBlocks(blockSize * 2, None)
    //used 2 out of 2
    firstRequest.size should be(2)
    //simulate writing to the block
    firstRequest.head.own()
    firstRequest.head.position(blockSize.toInt - 1)
    //mark them as reclaimable
    firstRequest.foreach(_.markReclaimable())
    val secondRequest = blockManager.requestBlocks(blockSize * 2, None)
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
    val firstRequest = blockManager.requestBlocks(blockSize * 2, None)
    //used 2 out of 4
    firstRequest.size should be(2)
    //only 2 left - cannot fulfill request
    val secondRequest = blockManager.requestBlocks(blockSize * 3, None)
//    val brm = reclaimedBlocksMetric(stats)
//    brm.count should be(0)
    secondRequest should be(Seq.empty)

    blockManager.releaseBlocks()
  }

  it should "allocate and reclaim blocks with time order" in {
    val stats = new MemoryStats(Map("test5" -> "test5"))
    // This block manager has 5 blocks capacity
    val blockManager = new PageAlignedBlockManager(5 * pageSize, stats, testReclaimer, 1)

    blockManager.usedBlocks.size() shouldEqual 0
    blockManager.numTimeOrderedBlocks shouldEqual 0
    blockManager.usedBlocksTimeOrdered.size shouldEqual 0

    // first allocate non-time ordered block
    blockManager.requestBlock(None).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.usedBlocks.size shouldEqual 1

    blockManager.requestBlock(Some(1000L)).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.requestBlock(Some(1000L)).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.requestBlock(Some(1000L)).isDefined shouldEqual true
    blockManager.usedBlocksTimeOrdered.get(1000L).size() shouldEqual 3

    blockManager.requestBlock(Some(9000L)).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.usedBlocksTimeOrdered.get(9000L).size() shouldEqual 1

    blockManager.numTimeOrderedBlocks shouldEqual 4
    blockManager.usedBlocksTimeOrdered.size shouldEqual 2

    // reclaim from time ordered blocks should kick in for next 3 requests
    blockManager.requestBlock(Some(10000L)).isDefined shouldEqual true
    blockManager.requestBlock(Some(10000L)).isDefined shouldEqual true
    blockManager.requestBlock(Some(10000L)).isDefined shouldEqual true
    blockManager.usedBlocksTimeOrdered.get(10000L).size() shouldEqual 3
    blockManager.usedBlocksTimeOrdered.get(1000L).size() shouldEqual 1 // should have reduced to 2
    // 9000L list should be gone since last block reclaimed
    blockManager.hasTimeBucket(9000L) shouldEqual false

    blockManager.numTimeOrderedBlocks shouldEqual 4
    blockManager.usedBlocksTimeOrdered.keySet.asScala shouldEqual Set(1000L, 10000L)

    // reclaim should first happen to time ordered blocks, not the regular ones first. Should still be 1
    blockManager.usedBlocks.size shouldEqual 1

    blockManager.requestBlock(Some(12)).isDefined shouldEqual true
    // now, reclaim should have happened from regular list
    blockManager.usedBlocks.size shouldEqual 0

    // since only 4 blocks were marked reclaimable, next request should fail to allocate
    // there are no blocks reclaimable in any list
    blockManager.requestBlock(Some(12)).isDefined shouldEqual false

    // Mark everything up to 5000L time as reclaimable even if not full.
    // Then request another block, and this time it should succeed.
    blockManager.markBucketedBlocksReclaimable(5000L)
    blockManager.requestBlock(Some(12)).isDefined shouldEqual true

    blockManager.releaseBlocks()
  }

  it should ("allocate blocks using BlockMemFactory with ownership and reclaims") in {
    val stats = new MemoryStats(Map("test5" -> "test5"))
    // This block manager has 5 blocks capacity
    val blockManager = new PageAlignedBlockManager(5 * pageSize, stats, testReclaimer, 1)

    blockManager.usedBlocks.size() shouldEqual 0
    blockManager.numTimeOrderedBlocks shouldEqual 0
    blockManager.usedBlocksTimeOrdered.size shouldEqual 0

    val factory = new BlockMemFactory(blockManager, Some(10000L), 24, Map("foo" -> "bar"), false)

    // There should be one time ordered block allocated, owned by factory
    blockManager.usedBlocks.size shouldEqual 0
    blockManager.numTimeOrderedBlocks shouldEqual 1
    blockManager.hasTimeBucket(10000L) shouldEqual true

    factory.currentBlock.owner shouldEqual Some(factory)

    // Now allocate 4 more regular blocks, that will use up all blocks
    blockManager.requestBlock(None).isDefined shouldEqual true
    blockManager.requestBlock(None).isDefined shouldEqual true
    blockManager.requestBlock(None).isDefined shouldEqual true
    blockManager.requestBlock(None).isDefined shouldEqual true
    blockManager.usedBlocks.size shouldEqual 4
    blockManager.numTimeOrderedBlocks shouldEqual 1

    // Mark as reclaimable the blockMemFactory's block.  Then request more blocks, that one will be reclaimed.
    // Check ownership is now cleared.
    factory.currentBlock.markReclaimable
    blockManager.requestBlock(Some(9000L)).isDefined shouldEqual true
    blockManager.hasTimeBucket(10000L) shouldEqual false
    blockManager.hasTimeBucket(9000L) shouldEqual true

    factory.currentBlock.owner shouldEqual None  // new requestor did not have owner
  }

  it should "ensure free space" in {
    val stats = new MemoryStats(Map("test5" -> "test5"))
    // This block manager has 5 blocks capacity
    val blockManager = new PageAlignedBlockManager(5 * pageSize, stats, testReclaimer, 1)

    blockManager.numFreeBlocks shouldEqual 5
    blockManager.ensureFreePercent(50)
    blockManager.numFreeBlocks shouldEqual 5

    blockManager.requestBlock(None).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.numFreeBlocks shouldEqual 4
    blockManager.ensureFreePercent(50)
    blockManager.numFreeBlocks shouldEqual 4

    blockManager.requestBlock(None).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.numFreeBlocks shouldEqual 3
    blockManager.ensureFreePercent(50)
    blockManager.numFreeBlocks shouldEqual 3

    blockManager.requestBlock(None).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.numFreeBlocks shouldEqual 2
    blockManager.ensureFreePercent(50)
    // Should actually have done something this time.
    blockManager.numFreeBlocks shouldEqual 3

    blockManager.requestBlock(None).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.numFreeBlocks shouldEqual 2
    blockManager.ensureFreePercent(90)
    // Should reclaim multiple blocks.
    blockManager.numFreeBlocks shouldEqual 5

    blockManager.releaseBlocks()
  }
}
