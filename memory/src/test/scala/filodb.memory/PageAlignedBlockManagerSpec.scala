package filodb.memory

import com.kenai.jffi.PageManager
import org.scalatest.{FlatSpec, Matchers}

class PageAlignedBlockManagerSpec extends FlatSpec with Matchers {

  val pageSize = PageManager.getInstance().pageSize()

  it should "Allocate blocks as requested for size" in {
    //2MB
    val stats = new MemoryStats(Map("test1" -> "test1"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, 1, 72)

//    val fbm = freeBlocksMetric(stats)
//    fbm.max should be(512)
    val blockSize = blockManager.blockSizeInBytes
    val blocks = blockManager.requestBlocks(blockSize * 10, None)
    blocks.size should be(10)
//    val ubm = usedBlocksMetric(stats)
//    ubm.max should be(10)
//    val fbm2 = freeBlocksMetric(stats)
//    fbm2.min should be(502)
  }

  it should "Align block size to page size" in {
    val stats = new MemoryStats(Map("test2" -> "test2"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, 1, 72)
    val blockSize = blockManager.blockSizeInBytes
    blockSize should be(pageSize)
  }

  it should "Not allow a request of blocks over the upper bound if no blocks are reclaimable" in {
    //2 pages
    val stats = new MemoryStats(Map("test3" -> "test3"))
    val blockManager = new PageAlignedBlockManager(2 * pageSize, stats, 1, 72)
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
  }

  it should "Allocate blocks as requested even when upper bound is reached if blocks can be reclaimed" in {
    //2 pages
    val stats = new MemoryStats(Map("test4" -> "test4"))
    val blockManager = new PageAlignedBlockManager(2 * pageSize, stats, 1, 72)
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
  }

  it should "Fail to Allocate blocks when enough blocks cannot be reclaimed" in {
    //4 pages
    val stats = new MemoryStats(Map("test5" -> "test5"))
    val blockManager = new PageAlignedBlockManager(4 * pageSize, stats, 1, 72)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestBlocks(blockSize * 2, None)
    //used 2 out of 4
    firstRequest.size should be(2)
    //only 2 left - cannot fulfill request
    val secondRequest = blockManager.requestBlocks(blockSize * 3, None)
//    val brm = reclaimedBlocksMetric(stats)
//    brm.count should be(0)
    secondRequest should be(Seq.empty)
  }

  it should "allocate and reclaim blocks with time order" in {
    val stats = new MemoryStats(Map("test5" -> "test5"))
    // This block manager has 5 blocks capacity
    val blockManager = new PageAlignedBlockManager(5 * pageSize, stats, 1, 72)

    blockManager.usedBlocks.size() shouldEqual 0
    for { i <- 0 until 72} {
      blockManager.usedBlocksTimeOrdered(i).size() shouldEqual 0
    }
    blockManager.timeOrderedBlocksEnabled shouldEqual true

    // first allocate non-time ordered block
    blockManager.requestBlock(None).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.usedBlocks.size shouldEqual 1

    blockManager.requestBlock(Some(1)).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.requestBlock(Some(1)).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.requestBlock(Some(1)).isDefined shouldEqual true
    blockManager.usedBlocksTimeOrdered(1).size() shouldEqual 3

    blockManager.requestBlock(Some(9)).map(_.markReclaimable).isDefined shouldEqual true
    blockManager.usedBlocksTimeOrdered(9).size() shouldEqual 1

    // reclaim from time ordered blocks should kick in for next 3 requests
    blockManager.requestBlock(Some(10)).isDefined shouldEqual true
    blockManager.requestBlock(Some(10)).isDefined shouldEqual true
    blockManager.requestBlock(Some(10)).isDefined shouldEqual true
    blockManager.usedBlocksTimeOrdered(10).size() shouldEqual 3
    blockManager.usedBlocksTimeOrdered(1).size() shouldEqual 1 // should have reduced to 2
    blockManager.usedBlocksTimeOrdered(9).size() shouldEqual 0 // should have reduced to 0

    // reclaim should first happen to time ordered blocks, not the regular ones first. Should still be 1
    blockManager.usedBlocks.size shouldEqual 1

    blockManager.requestBlock(Some(12)).isDefined shouldEqual true
    // now, reclaim should have happened from regular list
    blockManager.usedBlocks.size shouldEqual 0

    // since only 4 blocks were marked reclaimable, next request should fail to allocate
    // there are no blocks reclaimable in any list
    blockManager.requestBlock(Some(12)).isDefined shouldEqual false

    blockManager.reclaimTimeOrderedBlocks()

    for { i <- 0 until 72} {
      blockManager.usedBlocksTimeOrdered(i).size() shouldEqual 0
    }

    blockManager.timeOrderedBlocksEnabled shouldEqual false

  }

}
