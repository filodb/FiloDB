package filodb.core.memstore

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.TestData
import filodb.core.store.{ColumnStore, InMemoryMetaStore, NullColumnStore}
import filodb.memory.PageAlignedBlockManager

class DemandPagedChunkStoreSpec extends FunSpec with Matchers with BeforeAndAfter
                                                with BeforeAndAfterAll with ScalaFutures {
  import filodb.core.MachineMetricsData._
  import monix.execution.Scheduler.Implicits.global

  val colStore: ColumnStore = new NullColumnStore()
  val chunkRetentionHours = 10

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, colStore, new InMemoryMetaStore(), Some(policy))
  // implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  memStore.setup(dataset1, 0, TestData.storeConf)
  val onDemandPartMaker = memStore.getShardE(dataset1.ref, 0).partitionMaker

  after {
    memStore.reset()
    memStore.metastore.clearAllData()
  }

  it ("should queue and store optimized chunks into demand paged chunk store") {
    val start = System.currentTimeMillis() - chunkRetentionHours.hours.toMillis

    // 2 records per series x 10 series
    val initData = records(dataset1, linearMultiSeries(start).take(20))
    memStore.ingest(dataset1.ref, 0, initData)

    memStore.asInstanceOf[TimeSeriesMemStore].commitIndexBlocking(dataset1.ref)
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10

    val rawData = linearMultiSeries(start, timeStep=100000).drop(100).take(900)  // makes 9 chunks per partition?

    // Now try paging RawPartDatas in, and see that DemandPaging can load blocks into the right partitions
    for { partNo <- 0 to 9 } {
      val chunkStream = filterByPartAndMakeStream(rawData, partNo)
      val rawPartition = TestData.toRawPartData(chunkStream).runAsync.futureValue
      val tsPartition = onDemandPartMaker.populateRawChunks(rawPartition).runAsync.futureValue

      tsPartition.appendingChunkLen shouldEqual 2   // 2 samples ingested into write buffers
      tsPartition.numChunks shouldEqual 10          // write buffers + 9 chunks above
    }

    // validate that usedBlocksTimeOrdered sizes are correct
    val pageManager = onDemandPartMaker.blockManager.asInstanceOf[PageAlignedBlockManager]
    // TODO: make this work.  But the test above verifies that re-populating TSPartition works
    // for { i <- 0 until chunkRetentionHours } {
    //   pageManager.usedBlocksSize(Some(i)) should be > 1
    // }
  }

  it ("should reclaim all blocks when cleanupAndDisableOnDemandPaging is called from scheduled task") {
    onDemandPartMaker.cleanupAndDisableOnDemandPaging()

    // validate that usedBlocksTimeOrdered sizes are zero
    for { i <- 0 until chunkRetentionHours } {
      val pageManager = onDemandPartMaker.blockManager.asInstanceOf[PageAlignedBlockManager]
      pageManager.usedBlocksSize(Some(i)) shouldEqual 0
    }
    onDemandPartMaker.onDemandPagingEnabled shouldEqual false
  }

}
