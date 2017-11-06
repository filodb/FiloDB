package filodb.memory

import com.kenai.jffi.PageManager
import org.scalatest.{FlatSpec, Matchers}

class PageAlignedBlockManagerSpec extends FlatSpec with Matchers {

  val pageSize = PageManager.getInstance().pageSize()

  it should "Allocate blocks as requested for size" in {
    //2MB
    val blockManager = new PageAlignedBlockManager(2048 * 1024)
    val blockSize = blockManager.blockSizeInBytes
    val blocks = blockManager.requestBlocks(blockSize * 10)
    blocks.size should be(10)
  }

  it should "Align block size to page size" in {
    val blockManager = new PageAlignedBlockManager(2048 * 1024)
    val blockSize = blockManager.blockSizeInBytes
    blockSize should be(pageSize)
  }

  it should "Not allow a request of blocks over the upper bound with a no reclaim policy" in {
    //2 pages
    val blockManager = new PageAlignedBlockManager(2 * pageSize)
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
    val blockManager = new PageAlignedBlockManager(2 * pageSize)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestBlocks(blockSize * 2)
    //used 2 out of 2
    firstRequest.size should be(2)
    //mark them as reclaimable
    firstRequest.foreach(_.markReclaimable())
    val secondRequest = blockManager.requestBlocks(blockSize * 2)
    //this request will fulfill
    secondRequest.size should be(2)
  }

  it should "Fail to Allocate blocks when enough blocks cannot be reclaimed" in {
    //4 pages
    val blockManager = new PageAlignedBlockManager(4 * pageSize)
    val blockSize = blockManager.blockSizeInBytes
    val firstRequest = blockManager.requestBlocks(blockSize * 2)
    //used 2 out of 4
    firstRequest.size should be(2)
    //only 2 left - cannot fulfill request
    val secondRequest = blockManager.requestBlocks(blockSize * 3)
    secondRequest should be(Seq.empty)
  }


}
