package filodb.core.memstore

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.MachineMetricsData._
import filodb.core.TestData
import filodb.core.query.ChunkSetReader
import filodb.core.store.{ColumnStore, NullColumnStore}
import filodb.memory.{MemoryStats, NativeMemoryManager, PageAlignedBlockManager, ReclaimListener}
import filodb.memory.format.{TupleRowReader, UnsafeUtils}

class DemandPagedChunkStoreSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import TimeSeriesShard.BlockMetaAllocSize

  var tsPartition: TimeSeriesPartition = null

  val reclaimer = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {
      assert(numBytes == BlockMetaAllocSize)
      val partID = UnsafeUtils.getInt(metaAddr)
      val chunkID = UnsafeUtils.getLong(metaAddr + 4)
      tsPartition.removeChunksAt(chunkID)
    }
  }

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  private val chunkRetentionHours = 72
  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), reclaimer, 1, chunkRetentionHours)
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)
  protected val bufferPool = new WriteBufferPool(memFactory, dataset1, 10, 50)
  protected val pagedChunkStore = new DemandPagedChunkStore(dataset1, blockStore, BlockMetaAllocSize,
                                                            chunkRetentionHours, 1)
  val colStore: ColumnStore = new NullColumnStore()

  it ("should queue and store optimized chunks into demand paged chunk store") {
    for { times <- 0 to 20 } {
      tsPartition = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, colStore, bufferPool, config, false,
        pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))

      // create chunk set readers with chunks from various buckets
      val start = System.currentTimeMillis() - chunkRetentionHours.hours.toMillis
      val data = singleSeriesData(start, 100000).map(TupleRowReader).take(3000)
      val chunks = TestData.toChunkSetStream(dataset1, defaultPartKey, data, 10)
        // 10 rows per chunk, 4 chunks will be created
        .map(ChunkSetReader(_, dataset1, dataset1.dataColumns.map(_.id).toArray))
        .toListL.runAsync.futureValue

      // store them using storeAsync
      pagedChunkStore.storeAsync(chunks, tsPartition).futureValue
    }
    // validate that usedBlocksTimeOrdered sizes are correct
    for { i <- 0 until chunkRetentionHours } {
      blockStore.usedBlocksSize(Some(i)) should be > 1
    }
  }

  it ("should reclaim all blocks when cleanupAndDisableOnDemandPaging is called from scheduled task") {

    pagedChunkStore.cleanupAndDisableOnDemandPaging()

    // validate that usedBlocksTimeOrdered sizes are zero
    for { i <- 0 until chunkRetentionHours } {
      blockStore.usedBlocksSize(Some(i)) shouldEqual 0
    }

    pagedChunkStore.onDemandPagingEnabled shouldEqual false
  }

}
