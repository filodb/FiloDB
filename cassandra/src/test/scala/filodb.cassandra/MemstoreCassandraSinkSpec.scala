package filodb.cassandra

import monix.reactive.Observable

import filodb.core._
import filodb.core.memstore.{FlushStream, TimeSeriesMemStore}
import filodb.core.store.{FilteredPartitionScan, InMemoryChunkScan}
import filodb.memory.format.UnsafeUtils

/**
 * Tests a MemStore configured with a Cassandra ChunkSink.
 * Tests the ingestion (via streams) and then chunking and flushing of data to Cassandra, and reading it back
 * (on-demand/in-memory only).
 */
class MemstoreCassandraSinkSpec extends AllTablesTest {
  import MachineMetricsData._

  val memStore = new TimeSeriesMemStore(config, columnStore, metaStore)

  // First create the tables in C*
  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore.initialize().futureValue
    columnStore.initialize(dataset1.ref).futureValue
  }

  before {
    columnStore.truncate(dataset1.ref).futureValue
    metaStore.clearAllData().futureValue
  }


  it("should flush MemStore data to C*, and be able to read back data from C* directly") {
    memStore.setup(dataset1, 0, TestData.storeConf)
    memStore.store.sinkStats.chunksetsWritten shouldEqual 0

    // Flush every 50 records
    // NOTE: Observable.merge(...) cannot interleave records from observables created using fromIterable...
    // thus this is not really interleaved flushing
    val start = System.currentTimeMillis
    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries(startTs=start)))
    val flushStream = FlushStream.everyN(4, 50, stream.share)
    memStore.ingestStream(dataset1.ref, 0, stream, scheduler, flushStream).futureValue

    // Two flushes and 3 chunksets have been flushed
    memStore.store.sinkStats.chunksetsWritten should be >= 3
    memStore.store.sinkStats.chunksetsWritten should be <= 4

    // Verify data still in MemStore... all of it
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val agg1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.head))
                       .map(_.getDouble(0)).sum
    agg1 shouldEqual (1 to 100).map(_.toDouble).sum

    // Verify data is in Cassandra ... but only groups 0, 1 which has following partitions:
    // Series 3, Series 4, Series 8, Series 9
    val splits2 = columnStore.getScanSplits(dataset1.ref, 1)
    val rawParts = columnStore.readRawPartitions(dataset1, Seq(1), FilteredPartitionScan(splits2.head))
                              .toListL.runAsync.futureValue
    val writtenNums = (5 to 95 by 10) ++ (6 to 96 by 10) ++ (8 to 98 by 10)
    // Cannot check the result, because FilteredPartitionScan() will be broken until indices are implemented
    // agg2.result should equal (Array(writtenNums.map(_.toDouble).sum))

    rawParts.foreach { rawPart =>
      println(s"  ${dataset1.partKeySchema.stringify(rawPart.partitionKey, UnsafeUtils.arayOffset)}  ")
    }
    rawParts should have length (4)

    // Reclaim all blocks.  Then verify flushed partitions are not there anymore
    memStore.getShardE(dataset1.ref, 0).reclaimAllBlocksTestOnly()
    val data1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.head), InMemoryChunkScan)
                       .map(_.getDouble(0)).toSeq
    data1 should have length (60)  // 4 partitions were flushed and not in memory anymore

    // Re-read data in memstore.  Verify that on-demand paging will bring data back
    val agg2 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.head))
                       .map(_.getDouble(0)).sum
    agg2 shouldEqual (1 to 100).map(_.toDouble).sum
  }
}