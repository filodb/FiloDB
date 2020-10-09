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
    val bmf = new BlockMemFactory(blockManager, 50, Map("test" -> "val"), false)

    // simulate encoding of multiple ts partitions in flush group
    for { tsParts <- 0 to 10 } {
      bmf.startMetaSpan()
      for { chunks <- 0 to 3 } {
        bmf.allocateOffheap(1000)
      }
      bmf.endMetaSpan(d => {}, 45)
    }

    // full blocks are tracked as they are allocated
    bmf.fullBlocksToBeMarkedAsReclaimable.size shouldEqual 10

    // none of the blocks are marked as reclaimable
    blockManager.usedIngestionBlocks.asScala.count(!_.canReclaim) shouldEqual 11 // currentBlock is also not reclaimable

    // tryReclaim should not yield anything
    blockManager.tryReclaim(3) shouldEqual 0

    // after flush task is done, simulate marking as reclaimable
    bmf.markAllBlocksReclaimable()
    // now all the blocks, including currentBlock should be reclaimable
    blockManager.usedIngestionBlocks.asScala.forall(_.canReclaim) shouldEqual true

    // reclaim call should now yield blocks
    blockManager.usedIngestionBlocks.size shouldEqual 11
    blockManager.tryReclaim(3) shouldEqual 3
    blockManager.usedIngestionBlocks.size shouldEqual 8 // 3 are reclaimed

    blockManager.releaseBlocks()
  }

  it should "Mark all blocks of BlockMemFactory as reclaimable when used in ODP by DemandPagedChunkStore" in {
    val stats = new MemoryStats(Map("test1" -> "test1"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, testReclaimer, 1)

    // create block mem factories for different time buckets
    val bmf = new BlockMemFactory(blockManager, 50, Map("test" -> "val"), true)

    // simulate paging in chunks from cassandra
      for {tsParts <- 0 to 10} {
        bmf.startMetaSpan()
        for {chunks <- 0 to 3} {
          bmf.allocateOffheap(1000)
        }
        bmf.endMetaSpan(d => {}, 45)
      }

    // we dont track full blocks in ODP mode
    bmf.fullBlocksToBeMarkedAsReclaimable.isEmpty shouldEqual true

    // usedBlocks is not used for ODP mode
    blockManager.usedIngestionBlocks.isEmpty shouldEqual true

    // time ordered blocks is used in ODP mode
    // 11 blocks are used, out of which 10 are reclaimable, except the "current block"
    blockManager.usedOdpBlocks.asScala.count(_.canReclaim) shouldEqual 10
    bmf.currentBlock.canReclaim shouldEqual false // current blocks cannot be reclaimed

    // There should be 11 time ordered blocks used before reclaim
    blockManager.usedOdpBlocks.asScala.size shouldEqual 11
    blockManager.tryReclaim(5) shouldEqual 5
    // after reclaiming 5 blocks, only 11-5 == 6 time ordered blocks should be used
    blockManager.usedOdpBlocks.asScala.size shouldEqual 6

    // even if you try to reclaim all of them, you cannot reclaim current block
    blockManager.tryReclaim(7) shouldEqual 5
    blockManager.usedOdpBlocks.asScala.size shouldEqual 1

    blockManager.releaseBlocks()
  }


  it should "Reclaim Ingestion and ODP blocks in right order when used together" in {
    val stats = new MemoryStats(Map("test1" -> "test1"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, testReclaimer, 1)

    val ingestionFactory = new BlockMemFactory(blockManager, 50, Map("test" -> "val"), false)

    // create block mem factories for different time buckets
    val odpFactory = new BlockMemFactory(blockManager, 50, Map("test" -> "val"), true)

    // simulate encoding of multiple ts partitions in flush group
    for {tsParts <- 0 to 10} {
      ingestionFactory.startMetaSpan()
      for {chunks <- 0 to 3} {
        ingestionFactory.allocateOffheap(1000)
      }
      ingestionFactory.endMetaSpan(d => {}, 45)
    }

    // simulate paging in chunks from cassandra
      for {tsParts <- 0 to 10} {
        odpFactory.startMetaSpan()
        for {chunks <- 0 to 3} {
          odpFactory.allocateOffheap(1000)
        }
        odpFactory.endMetaSpan(d => {}, 45)
      }

    // we mark all ingestion blocks as reclaimable
    ingestionFactory.markAllBlocksReclaimable()

    // here are the use block counts before reclaim call
    blockManager.usedOdpBlocks.size shouldEqual 11
    blockManager.usedIngestionBlocks.size shouldEqual 11
    blockManager.tryReclaim(15) shouldEqual 15

    // after reclaim, only 1 odp block
    blockManager.usedOdpBlocks.asScala.size shouldEqual 1

    // ingestion blocks should be reclaimed only if we cannot get reclaim ODP blocks.
    blockManager.usedIngestionBlocks.asScala.size shouldEqual 6

  }
}
