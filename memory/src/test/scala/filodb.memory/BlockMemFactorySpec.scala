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
    blockManager.usedBlocks.asScala.count(!_.canReclaim) shouldEqual 11 // currentBlock is also not reclaimable

    // tryReclaim should not yield anything
    blockManager.tryReclaim(3) shouldEqual 0

    // after flush task is done, simulate marking as reclaimable
    bmf.markAllBlocksReclaimable()
    // now all the blocks, including currentBlock should be reclaimable
    blockManager.usedBlocks.asScala.forall(_.canReclaim) shouldEqual true

    // reclaim call should now yield blocks
    blockManager.usedBlocks.size shouldEqual 11
    blockManager.tryReclaim(3) shouldEqual 3
    blockManager.usedBlocks.size shouldEqual 8 // 3 are reclaimed

    blockManager.releaseBlocks()
  }

  it should "Mark all blocks of BlockMemFactory as reclaimable when used in ODP by DemandPagedChunkStore" in {
    val stats = new MemoryStats(Map("test1" -> "test1"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, testReclaimer, 1)

    // create block mem factories for different time buckets
    val bmf = Seq(
      new BlockMemFactory(blockManager, Some(10), 50, Map("test" -> "val"), true),
      new BlockMemFactory(blockManager, Some(20), 50, Map("test" -> "val"), true),
      new BlockMemFactory(blockManager, Some(30), 50, Map("test" -> "val"), true))

    // simulate paging in chunks from cassandra
    for { b <- bmf } {
      for {tsParts <- 0 to 10} {
        b.startMetaSpan()
        for {chunks <- 0 to 3} {
          b.allocateOffheap(1000)
        }
        b.endMetaSpan(d => {}, 45)
      }
    }

    // we dont track full blocks in ODP mode
    for { b <- bmf } {
      b.fullBlocksToBeMarkedAsReclaimable.isEmpty shouldEqual true
    }

    // usedBlocks is not used for ODP mode
    blockManager.usedBlocks.isEmpty shouldEqual true

    // time ordered blocks is used in ODP mode
    // 11 blocks are used, out of which 10 are reclaimable, except the "current block"
    Seq(10L, 20L, 30L).foreach { t =>
      blockManager.usedBlocksTimeOrdered.get(t).asScala.count(_.canReclaim) shouldEqual 10
    }
    bmf.exists(_.currentBlock.canReclaim) shouldEqual false // current blocks cannot be reclaimed

    // there should be three items in the tree map, one for each time bucket
    blockManager.usedBlocksTimeOrdered.size shouldEqual 3 // three time buckets

    // try simulating reclamation of oldest time bucket since retention is over
    blockManager.markBucketedBlocksReclaimable(15)
    // one extra block from bucket 10L should be marked reclaimable. Others should not be touched
    blockManager.usedBlocksTimeOrdered.get(10L).asScala.count(_.canReclaim) shouldEqual 11 // current block reclaimable
    blockManager.usedBlocksTimeOrdered.get(10L).asScala.forall(_.canReclaim) shouldEqual true
    blockManager.usedBlocksTimeOrdered.get(20L).asScala.count(_.canReclaim) shouldEqual 10
    blockManager.usedBlocksTimeOrdered.get(30L).asScala.count(_.canReclaim) shouldEqual 10

    // There should be 11+11+11==33 time ordered blocks used before reclaim
    blockManager.usedBlocksTimeOrdered.asScala.values.map(_.size).sum shouldEqual 33
    blockManager.tryReclaim(14) shouldEqual 14
    // after reclaiming 14 blocks, only 33-14 == 19 time ordered blocks should be used
    blockManager.usedBlocksTimeOrdered.asScala.values.map(_.size).sum shouldEqual 19
    // after reclaiming, the oldest bucket should go away, and we should have reclaimed some more from the next oldest
    blockManager.usedBlocksTimeOrdered.containsKey(10L) shouldEqual false
    blockManager.usedBlocksTimeOrdered.get(20L).size shouldEqual 8 // 3 out of the 11 bocks were reclaimed
    // current block still not reclaimable from the 20L bucket
    blockManager.usedBlocksTimeOrdered.get(20L).asScala.count(!_.canReclaim) shouldEqual 1

    blockManager.releaseBlocks()
  }


  it should "Reclaim Ingestion and ODP blocks in right order when used together" in {
    val stats = new MemoryStats(Map("test1" -> "test1"))
    val blockManager = new PageAlignedBlockManager(2048 * 1024, stats, testReclaimer, 1)

    val ingestionFactory = new BlockMemFactory(blockManager, None, 50, Map("test" -> "val"), false)

    // create block mem factories for different time buckets
    val odpFactories = Seq(
      new BlockMemFactory(blockManager, Some(10), 50, Map("test" -> "val"), true),
      new BlockMemFactory(blockManager, Some(20), 50, Map("test" -> "val"), true),
      new BlockMemFactory(blockManager, Some(30), 50, Map("test" -> "val"), true))

    // simulate encoding of multiple ts partitions in flush group
    for {tsParts <- 0 to 10} {
      ingestionFactory.startMetaSpan()
      for {chunks <- 0 to 3} {
        ingestionFactory.allocateOffheap(1000)
      }
      ingestionFactory.endMetaSpan(d => {}, 45)
    }

    // simulate paging in chunks from cassandra
    for {b <- odpFactories} {
      for {tsParts <- 0 to 10} {
        b.startMetaSpan()
        for {chunks <- 0 to 3} {
          b.allocateOffheap(1000)
        }
        b.endMetaSpan(d => {}, 45)
      }
    }

    // we mark one time ordered bucket as reclaimable, and all ingestion blocks as reclaimable
    ingestionFactory.markAllBlocksReclaimable()
    blockManager.markBucketedBlocksReclaimable(10L)

    // here are the use block counts before reclaim call
    blockManager.usedBlocksTimeOrdered.asScala.values.map(_.size).sum shouldEqual 33
    blockManager.usedBlocks.size shouldEqual 11
    blockManager.tryReclaim(35) shouldEqual 35

    // after reclaim, only 2 time ordered blocks which are not reclaimable should remain (since they are current blocks)
    blockManager.usedBlocksTimeOrdered.asScala.values.map(_.size).sum shouldEqual 2

    // ingestion blocks should be reclaimed only if we cannot get reclaim ODP blocks.
    blockManager.usedBlocks.size shouldEqual 7

  }
}
