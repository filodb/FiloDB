package filodb.memory

import com.kenai.jffi.PageManager
import org.scalatest.{FlatSpec, Matchers}

class PageAlignedBlockManagerSpec extends FlatSpec with Matchers {

  val pageSize = PageManager.getInstance().pageSize()
  val metricTags = new MemoryStats(Map("test"-> "test"))

  it should "Allocate blocks as requested for size" in {
    //2MB
    val blockManager = new PageAlignedBlockManager(2048 * 1024, metricTags)
    val blockSize = blockManager.blockSizeInBytes
    val blocks = blockManager.requestBlocks(blockSize * 10)
    blocks.size should be(10)
  }

  it should "Align block size to page size" in {
    val blockManager = new PageAlignedBlockManager(2048 * 1024, metricTags)
    val blockSize = blockManager.blockSizeInBytes
    blockSize should be(pageSize)
  }

  it should "Not allow a request of blocks over the upper bound with a no reclaim policy" in {
    //2 pages
    val blockManager = new PageAlignedBlockManager(2 * pageSize, metricTags)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestBlocks(blockSize * 2)
    //used 2 out of 2
    firstRequest.size should be(2)
    //cannot fulfill
    val secondRequest = blockManager.requestBlocks(blockSize * 2)
    secondRequest should be(Seq.empty)

  }

  it should "Allocate blocks as requested even when upper bound is reached if blocks can be reclaimed" in {
    //2 pages
    val blockManager = new PageAlignedBlockManager(2 * pageSize, metricTags)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestBlocks(blockSize * 2)
    //used 2 out of 2
    firstRequest.size should be(2)
    //simulate writing to the block
    firstRequest(0).own()
    firstRequest(0).position(blockSize.toInt -1)
    //mark them as reclaimable
    firstRequest.foreach(_.markReclaimable())
    val secondRequest = blockManager.requestBlocks(blockSize * 2)
    //this request will fulfill
    secondRequest.size should be(2)
    secondRequest(0).hasCapacity(10) should be(true)
  }

  it should "Fail to Allocate blocks when enough blocks cannot be reclaimed" in {
    //4 pages
    val blockManager = new PageAlignedBlockManager(4 * pageSize, metricTags)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestBlocks(blockSize * 2)
    //used 2 out of 4
    firstRequest.size should be(2)
    //only 2 left - cannot fulfill request
    val secondRequest = blockManager.requestBlocks(blockSize * 3)
    secondRequest should be(Seq.empty)
  }


}
