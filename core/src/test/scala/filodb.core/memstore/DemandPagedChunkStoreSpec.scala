package filodb.core.memstore

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

import filodb.core.{AsyncTest, TestData}
import filodb.core.store.{ColumnStore, InMemoryMetaStore, NullColumnStore, StoreConfig}
import filodb.memory.PageAlignedBlockManager

class DemandPagedChunkStoreSpec extends FunSpec with AsyncTest {
  import filodb.core.MachineMetricsData._
  import monix.execution.Scheduler.Implicits.global

  val colStore: ColumnStore = new NullColumnStore()
  val chunkRetentionHours = 10

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, colStore, new InMemoryMetaStore(), Some(policy))
  // implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  val sourceConf = ConfigFactory.parseString("""flush-interval = 1h
                                               |shard-mem-size = 200MB""".stripMargin)
                                .withFallback(TestData.sourceConf.getConfig("store"))

  memStore.setup(dataset1, 0, StoreConfig(sourceConf))
  val onDemandPartMaker = memStore.getShardE(dataset1.ref, 0).partitionMaker

  after {
    memStore.reset()
    memStore.metastore.clearAllData()
  }

  it ("should queue and store optimized chunks into demand paged chunk store") {
    val start = System.currentTimeMillis() - chunkRetentionHours.hours.toMillis
    val pageManager = onDemandPartMaker.blockManager.asInstanceOf[PageAlignedBlockManager]
    val initFreeBlocks = pageManager.numFreeBlocks

    // 2 records per series x 10 series
    val initData = records(dataset1, linearMultiSeries(start).take(20))
    memStore.ingest(dataset1.ref, 0, initData)

    memStore.commitIndexForTesting(dataset1.ref)
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

    pageManager.numTimeOrderedBlocks should be > 1
    pageManager.numFreeBlocks should be >= (initFreeBlocks - 12)
    val buckets = pageManager.timeBuckets
    buckets.foreach { b => pageManager.hasTimeBucket(b) shouldEqual true }

    // Now, reclaim four time buckets, even if they are not full
    pageManager.markBucketedBlocksReclaimable(buckets(4))

    // try and ODP more data.  Load older data than chunk retention, should still be able to load
    val data2 = linearMultiSeries(start - 2.hours.toMillis, timeStep=100000).take(20)
    val stream2 = filterByPartAndMakeStream(data2, 0)
    val rawPart2 = TestData.toRawPartData(stream2).runAsync.futureValue
    val tsPart2 = onDemandPartMaker.populateRawChunks(rawPart2).runAsync.futureValue
  }
}
