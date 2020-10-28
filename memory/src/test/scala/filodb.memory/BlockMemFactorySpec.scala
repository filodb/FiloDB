package filodb.memory

import scala.collection.JavaConverters._

import com.kenai.jffi.PageManager
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import filodb.memory.PageAlignedBlockManagerSpec.testReclaimer

class BlockMemFactorySpec extends AnyFlatSpec with Matchers {

  val pageSize = PageManager.getInstance().pageSize()

  it should "Mark all blocks of BlockMemFactory as reclaimable when used as done in ingestion pipeline" in {
    val stats = new MemoryStats(Map("test1" -> "test1"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, testReclaimer, 1)
    val bmf = new BlockMemFactory(blockManager, None, 50, Map("test" -> "val"), false)

    // simulate encoding of multiple ts partitions in flush group

    for { flushGroup <- 0 to 1 } {
      for {tsParts <- 0 to 5} {
        bmf.startMetaSpan()
        for {chunks <- 0 to 3} {
          bmf.allocateOffheap(1000)
        }
        bmf.endMetaSpan(d => {}, 45)
      }
      // full blocks are tracked as they are allocated
      flushGroup match {
        case 0 => bmf.fullBlocksToBeMarkedAsReclaimable.size shouldEqual 5
        case 1 => bmf.fullBlocksToBeMarkedAsReclaimable.size shouldEqual 6
      }
      // full blocks are marked as reclaimable
      bmf.markFullBlocksReclaimable()
    }

    // only the current block is not reclaimable
    blockManager.usedBlocks.asScala.count(!_.canReclaim) shouldEqual 1

    blockManager.usedBlocks.size shouldEqual 12
    blockManager.tryReclaim(3) shouldEqual 3
    blockManager.usedBlocks.size shouldEqual 9 // 3 are reclaimed

    blockManager.releaseBlocks()
  }

}
