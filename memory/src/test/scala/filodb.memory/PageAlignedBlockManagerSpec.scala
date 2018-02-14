package filodb.memory

import com.kenai.jffi.PageManager
import kamon.metric.instrument.CollectionContext
import org.scalatest.{FlatSpec, Matchers}

class PageAlignedBlockManagerSpec extends FlatSpec with Matchers {

  val pageSize = PageManager.getInstance().pageSize()


  it should "Allocate blocks as requested for size" in {
    //2MB
    val stats = new MemoryStats(Map("test1" -> "test1"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats)

    val fbm = freeBlocksMetric(stats)
    fbm.max should be(512)
    val blockSize = blockManager.blockSizeInBytes
    val blocks = blockManager.requestBlocks(blockSize * 10)
    blocks.size should be(10)
    val ubm = usedBlocksMetric(stats)
    ubm.max should be(10)
    val fbm2 = freeBlocksMetric(stats)
    fbm2.min should be(502)
  }

  it should "Align block size to page size" in {
    val stats = new MemoryStats(Map("test2" -> "test2"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats)
    val blockSize = blockManager.blockSizeInBytes
    blockSize should be(pageSize)
  }

  it should "Not allow a request of blocks over the upper bound if no blocks are reclaimable" in {
    //2 pages
    val stats = new MemoryStats(Map("test3" -> "test3"))
    val blockManager = new PageAlignedBlockManager(2 * pageSize, stats)
    val blockSize = blockManager.blockSizeInBytes
    val fbm = freeBlocksMetric(stats)
    fbm.max should be(2)
    val firstRequest = blockManager.requestBlocks(blockSize * 2)
    //used 2 out of 2
    firstRequest.size should be(2)
    val ubm = usedBlocksMetric(stats)
    ubm.max should be(2)
    //cannot fulfill
    val fbm2 = freeBlocksMetric(stats)
    fbm2.min should be(0)
    val secondRequest = blockManager.requestBlocks(blockSize * 2)
    secondRequest should be(Seq.empty)

  }

  it should "Allocate blocks as requested even when upper bound is reached if blocks can be reclaimed" in {
    //2 pages
    val stats = new MemoryStats(Map("test4" -> "test4"))
    val blockManager = new PageAlignedBlockManager(2 * pageSize, stats)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestBlocks(blockSize * 2)
    //used 2 out of 2
    firstRequest.size should be(2)
    //simulate writing to the block
    firstRequest.head.own()
    firstRequest.head.position(blockSize.toInt - 1)
    //mark them as reclaimable
    firstRequest.foreach(_.markReclaimable())
    val secondRequest = blockManager.requestBlocks(blockSize * 2)
    val brm = reclaimedBlocksMetric(stats)
    brm.count should be(2)
    //this request will fulfill
    secondRequest.size should be(2)
    secondRequest.head.hasCapacity(10) should be(true)
  }

  it should "Fail to Allocate blocks when enough blocks cannot be reclaimed" in {
    //4 pages
    val stats = new MemoryStats(Map("test5" -> "test5"))
    val blockManager = new PageAlignedBlockManager(4 * pageSize, stats)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestBlocks(blockSize * 2)
    //used 2 out of 4
    firstRequest.size should be(2)
    //only 2 left - cannot fulfill request
    val secondRequest = blockManager.requestBlocks(blockSize * 3)
    val brm = reclaimedBlocksMetric(stats)
    brm.count should be(0)
    secondRequest should be(Seq.empty)
  }

  private def usedBlocksMetric(stats: MemoryStats) = {
    stats.usedBlocksMetric.collect(CollectionContext(100))
  }

  private def freeBlocksMetric(stats: MemoryStats) = {
    stats.freeBlocksMetric.collect(CollectionContext(100))
  }

  private def reclaimedBlocksMetric(stats: MemoryStats) = {
    stats.blocksReclaimedMetric.collect(CollectionContext(100))
  }


}
