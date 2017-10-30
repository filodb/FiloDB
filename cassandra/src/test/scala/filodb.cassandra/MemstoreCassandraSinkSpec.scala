package filodb.cassandra

import monix.reactive.Observable

import filodb.core._
import filodb.core.memstore.{FlushStream, TimeSeriesMemStore}
import filodb.core.query.AggregationFunction
import filodb.core.store.{FilteredPartitionScan, QuerySpec}

/**
 * Tests a MemStore configured with a Cassandra ChunkSink.
 * Tests the ingestion (via streams) and then chunking and flushing of data to Cassandra, and reading it back.
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
    memStore.setup(dataset1, 0)
    memStore.sink.sinkStats.chunksetsWritten shouldEqual 0

    // Flush every 50 records
    // NOTE: Observable.merge(...) cannot interleave records from observables created using fromIterable...
    // thus this is not really interleaved flushing
    val stream = Observable.fromIterable(records(linearMultiSeries()).take(100).grouped(5).toSeq)
    val flushStream = FlushStream.everyN(4, 50, stream.share)
    memStore.ingestStream(dataset1.ref, 0, stream, flushStream)(ex => throw ex).futureValue

    // Two flushes and 3 chunksets have been flushed
    memStore.sink.sinkStats.chunksetsWritten shouldEqual 3

    // Verify data still in MemStore... all of it
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val query = QuerySpec("min", AggregationFunction.Sum)
    val agg1 = memStore.aggregate(dataset1, query, FilteredPartitionScan(splits.head))
                       .get.runAsync.futureValue
    agg1.result should equal (Array((1 to 100).map(_.toDouble).sum))

    // Verify data is in Cassandra ... but only groups 0, 1 which has following partitions:
    // Series 5, Series 4, Series 7.
    // eg Series 4 = 5, 15, 25, 35, .... 95
    val splits2 = columnStore.getScanSplits(dataset1.ref, 1)
    val agg2 = columnStore.aggregate(dataset1, query, FilteredPartitionScan(splits2.head))
                       .get.runAsync.futureValue
    val writtenNums = (5 to 95 by 10) ++ (6 to 96 by 10) ++ (8 to 98 by 10)
    agg2.result should equal (Array(writtenNums.map(_.toDouble).sum))

  }
}