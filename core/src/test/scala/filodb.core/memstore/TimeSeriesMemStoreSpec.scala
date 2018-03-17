package filodb.core.memstore

import com.typesafe.config.ConfigFactory
import monix.execution.ExecutionModel.BatchedExecution
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.query.{AggregationFunction, ColumnFilter, Filter}
import filodb.core.store.{FilteredPartitionScan, InMemoryMetaStore, NullColumnStore, QuerySpec, SinglePartitionScan}
import filodb.memory.format.ZeroCopyUTF8String

class TimeSeriesMemStoreSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global

  import MachineMetricsData._
  import ZeroCopyUTF8String._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))
  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  after {
    memStore.reset()
    memStore.sink.reset()
    memStore.metastore.clearAllData()
  }

  // Look mama!  Real-time time series ingestion and querying across multiple partitions!
  it("should ingest into multiple series and be able to query across all partitions in real time") {
    memStore.setup(dataset1, 0)
    val data = records(multiSeriesData()).take(20)   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexNames(dataset1.ref).toSeq should equal (Seq(("series", 0)))
    memStore.latestOffset(dataset1.ref, 0) shouldEqual 19

    val minSet = data.map(_.data.getDouble(1)).toSet
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val q = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(split))
    q.map(_.getDouble(0)).toSet should equal (minSet)

    // query the series name string column as well
    val q2 = memStore.scanRows(dataset1, Seq(dataset1.partitionColumns.head.id), FilteredPartitionScan(split))
    q2.map(_.filoUTF8String(0)).toSet should equal (data.map(_.partition.filoUTF8String(0)).toSet)
  }

  it("should ingest into multiple series and aggregate across partitions") {
    memStore.setup(dataset1, 1)
    val data = records(linearMultiSeries()).take(20)   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 1, data)

    // NOTE: ingesting into wrong shard should give an error
    intercept[IllegalArgumentException] {
      memStore.ingest(dataset1.ref, 0, data)
    }

    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val query = QuerySpec("min", AggregationFunction.Sum)
    val agg1 = memStore.aggregate(dataset1, query, FilteredPartitionScan(split))
                       .get.runAsync.futureValue
    agg1.result should equal (Array((1 to 20).map(_.toDouble).sum))
  }

  it("should record partitions in persistent store during ingestion; should be able to load partitions from there") {
    memStore.setup(dataset1, 0)
    chunksetsWritten shouldEqual 0

    // Flush every 25 records. We take 100 records, and do 4 groups.
    // NOTE: due to the batched ExecutionModel of fromIterable, it is hard to determine exactly when the FlushCommand
    // gets mixed in.  The custom BatchedExecution reduces batch size and causes better interleaving though.
    val stream = Observable.fromIterable(records(linearMultiSeries()).take(100).grouped(5).toSeq)
      .executeWithModel(BatchedExecution(5))
    val flushStream = FlushStream.everyN(4, 25, stream)
    memStore.ingestStream(dataset1.ref, 0, stream, flushStream)(ex => throw ex).futureValue

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10

    // all partitions should be written to persistent store
    memStore.sink.scanPartitionKeys(dataset1, 0).toListL.runAsync.futureValue.size shouldEqual 10

    // setup new memStore2, and point it to previously used colStore
    val memStore2 = new TimeSeriesMemStore(config, memStore.sink, new InMemoryMetaStore())
    memStore2.setup(dataset1, 0)
    memStore2.numPartitions(dataset1.ref, 0) shouldEqual 0

    // restore partitions from persistent store and validate that they are loaded
    memStore2.restorePartitions(dataset1, 0).futureValue shouldEqual 10
    memStore2.numPartitions(dataset1.ref, 0) shouldEqual 10

  }

  it("should ingest map/tags column as partition key and aggregate") {
    memStore.setup(dataset2, 0)
    val data = records(withMap(linearMultiSeries())).take(20)   // 2 records per series x 10 series
    memStore.ingest(dataset2.ref, 0, data)

    val split = memStore.getScanSplits(dataset2.ref, 1).head
    val query = QuerySpec("min", AggregationFunction.Sum)
    val filter = ColumnFilter("n", Filter.Equals("2".utf8))
    val agg1 = memStore.aggregate(dataset2, query, FilteredPartitionScan(split, Seq(filter)))
                       .get.runAsync.futureValue
    agg1.result should equal (Array(3 + 8 + 13 + 18))
  }

  it("should be able to handle nonexistent partition keys") {
    memStore.setup(dataset1, 0)

    val q = memStore.scanRows(dataset1, Seq(1), SinglePartitionScan(BinaryRecord.empty))
    q.toBuffer.length should equal (0)
  }

  it("should ingest into multiple series and be able to query on one partition in real time") {
    memStore.setup(dataset1, 0)
    val data = records(multiSeriesData()).take(20)   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    val minSeries0 = data(0).data.getDouble(1)
    val partKey0 = dataset1.partKey(data(0).partition)
    val q = memStore.scanRows(dataset1, Seq(1), SinglePartitionScan(partKey0))
    q.map(_.getDouble(0)).toSeq.head should equal (minSeries0)

    val minSeries1 = data(1).data.getDouble(1)
    val partKey1 = dataset1.partKey("Series 1")
    val q2 = memStore.scanRows(dataset1, Seq(1), SinglePartitionScan(partKey1))
    q2.map(_.getDouble(0)).toSeq.head should equal (minSeries1)
  }

  it("should query on multiple partitions using filters") {
    memStore.setup(dataset1, 0)
    val data = records(linearMultiSeries()).take(20)   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    val filter = ColumnFilter("series", Filter.In(Set("Series 1".utf8, "Series 2".utf8)))
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val q2 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(split, Seq(filter)))
    q2.map(_.getDouble(0)).toSeq should equal (Seq(2.0, 12.0, 3.0, 13.0))
  }

  it("should ingest into multiple shards, getScanSplits, query, get index info from shards") {
    memStore.setup(dataset2, 0)
    memStore.setup(dataset2, 1)
    val data = records(withMap(linearMultiSeries())).take(20)   // 2 records per series x 10 series
    memStore.ingest(dataset2.ref, 0, data)
    val data2 = records(withMap(linearMultiSeries(200000L, 6), 6)).take(20)   // 5 series only
    memStore.ingest(dataset2.ref, 1, data2)

    memStore.activeShards(dataset1.ref) should equal (Seq(0, 1))
    memStore.numRowsIngested(dataset1.ref, 0) should equal (20L)

    val splits = memStore.getScanSplits(dataset2.ref, 1)
    splits should have length (2)

    memStore.indexNames(dataset2.ref).toList should equal (
      Seq(("n", 0), ("series", 0), ("n", 1), ("series", 1)))

    val query = QuerySpec("min", AggregationFunction.Sum)
    val filter = ColumnFilter("n", Filter.Equals("2".utf8))
    val agg1 = memStore.aggregate(dataset2, query, FilteredPartitionScan(splits.head, Seq(filter)))
                       .get.runAsync.futureValue
    agg1.result should equal (Array(3 + 8 + 13 + 18))

    val agg2 = memStore.aggregate(dataset2, query, FilteredPartitionScan(splits.last, Seq(filter)))
                       .get.runAsync.futureValue
    agg2.result should equal (Array(3 + 9 + 15))
  }

  it("should ingestStream into multiple shards and not flush with empty flush stream") {
    memStore.setup(dataset1, 0)
    memStore.setup(dataset1, 1)
    memStore.activeShards(dataset1.ref) should equal (Seq(0, 1))

    val initChunksWritten = chunksetsWritten

    val stream = Observable.fromIterable(records(linearMultiSeries()).take(100).grouped(5).toSeq)
    val fut1 = memStore.ingestStream(dataset1.ref, 0, stream, FlushStream.empty)(ex => throw ex)
    val fut2 = memStore.ingestStream(dataset1.ref, 1, stream, FlushStream.empty)(ex => throw ex)
    // Allow both futures to run first before waiting for completion
    fut1.futureValue
    fut2.futureValue

    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val query = QuerySpec("min", AggregationFunction.Sum)
    val agg1 = memStore.aggregate(dataset1, query, FilteredPartitionScan(splits.head))
                       .get.runAsync.futureValue
    agg1.result should equal (Array((1 to 100).map(_.toDouble).sum))

    val agg2 = memStore.aggregate(dataset1, query, FilteredPartitionScan(splits.last))
                       .get.runAsync.futureValue
    agg2.result should equal (Array((1 to 100).map(_.toDouble).sum))

    // should not have increased
    chunksetsWritten shouldEqual initChunksWritten
  }

  it("should handle errors from ingestStream") {
    memStore.setup(dataset1, 0)
    var err: Throwable = null

    val errStream = Observable.fromIterable(records(linearMultiSeries()).take(100).grouped(5).toSeq)
                              .endWithError(new NumberFormatException)
    val fut = memStore.ingestStream(dataset1.ref, 0, errStream) { ex => err = ex }
    fut.futureValue

    err shouldBe a[NumberFormatException]
  }

  it("should ingestStream and flush on interval") {
    memStore.setup(dataset1, 0)
    val initChunksWritten = chunksetsWritten

    // Flush every 50 records.
    // NOTE: due to the batched ExecutionModel of fromIterable, it is hard to determine exactly when the FlushCommand
    // gets mixed in.  The custom BatchedExecution reduces batch size and causes better interleaving though.
    val stream = Observable.fromIterable(records(linearMultiSeries()).take(100).grouped(5).toSeq)
                           .executeWithModel(BatchedExecution(5))
    val flushStream = FlushStream.everyN(4, 50, stream)
    memStore.ingestStream(dataset1.ref, 0, stream, flushStream)(ex => throw ex).futureValue

    // Two flushes and 3 chunksets have been flushed
    chunksetsWritten shouldEqual initChunksWritten + 3

    // Try reading - should be able to read optimized chunks too
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val query = QuerySpec("min", AggregationFunction.Sum)

    val agg1 = memStore.aggregate(dataset1, query, FilteredPartitionScan(splits.head))
                       .get.runAsync.futureValue
    agg1.result should equal (Array((1 to 100).map(_.toDouble).sum))
  }

  import Iterators._

  it("should recoveryStream, skip some records, and receive a stream of offset updates") {
    // There are 4 subgroups within the shard.  Set up the watermarks like this:
    // 0 -> 5, 1 -> 10, 2 -> 15, 3 -> 20
    // A whole bunch of records should be skipped.  Cut off the stream at record 50.

    memStore.setup(dataset1, 0)
    val initChunksWritten = chunksetsWritten
    val checkpoints = Map(0 -> 5L, 1 -> 10L, 2 -> 15L, 3 -> 20L)

    val stream = Observable.fromIterable(records(linearMultiSeries()).take(100).grouped(5).toSeq)
    val offsets = memStore.recoverStream(dataset1.ref, 0, stream, checkpoints, 10L)
                          .until(_ >= 50L).toListL.runAsync.futureValue

    // Last element exceeding condition should be included
    offsets.drop(1) shouldEqual Seq(29L, 39L, 49L, 59L)
    // no flushes
    chunksetsWritten shouldEqual initChunksWritten

    // Should have less than 50 records ingested
    // Try reading - should be able to read optimized chunks too
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val query = QuerySpec("min", AggregationFunction.Count)
    val agg1 = memStore.aggregate(dataset1, query, FilteredPartitionScan(splits.head))
                       .get.runAsync.futureValue
    agg1.result should equal (Array(46))
  }

  it("should truncate shards properly") {
    memStore.setup(dataset1, 0)
    val data = records(multiSeriesData()).take(20)   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexNames(dataset1.ref).toSeq should equal (Seq(("series", 0)))

    memStore.truncate(dataset1.ref)

    memStore.numPartitions(dataset1.ref, 0) should equal (0)
  }

  private def chunksetsWritten = memStore.sink.sinkStats.chunksetsWritten

  it("should be able to evict partitions properly, flush, and still query") {
    memStore.setup(dataset1, 0)

    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(linearMultiSeries()).take(10)
    memStore.ingest(dataset1.ref, 0, data)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexValues(dataset1.ref, 0, "series").toSeq should have length (10)

    // Now, ingest 22 partitions.  First two partitions ingested should be evicted. Check numpartitions, stats, index
    val data2 = records(linearMultiSeries(numSeries = 22)).take(22)
    memStore.ingest(dataset1.ref, 0, data2)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 20
    memStore.indexValues(dataset1.ref, 0, "series").toSeq should have length (20)
  }
}