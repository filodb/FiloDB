package filodb.core.memstore

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.execution.ExecutionModel.BatchedExecution
import monix.reactive.Observable
import org.apache.lucene.util.BytesRef
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.exceptions.TestFailedException
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, QuerySession, ServiceUnavailableException}
import filodb.core.store._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.vectors.LongHistogram
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TimeSeriesMemStoreSpec extends AnyFunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  implicit val s = monix.execution.Scheduler.Implicits.global

  import MachineMetricsData._
  import ZeroCopyUTF8String._

  val config = ConfigFactory.parseString("""
                                            |filodb.memstore.max-partitions-on-heap-per-shard = 1100
                                            |filodb.memstore.ensure-block-memory-headroom-percent = 10
                                            |filodb.memstore.ensure-tsp-headroom-percent = 10
                                            |  """.stripMargin)
                            .withFallback(ConfigFactory.load("application_test.conf"))
                            .getConfig("filodb")
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore())
  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  after {
    memStore.reset()
    memStore.metastore.clearAllData()
  }

  val schemas1 = Schemas(schema1)

  it("should detect duplicate setup") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    try {
      memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
      fail()
    } catch {
      case e: ShardAlreadySetup => { } // expected
    }
  }


  // Look mama!  Real-time time series ingestion and querying across multiple partitions!
  it("should ingest into multiple series and be able to query across all partitions in real time") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val rawData = multiSeriesData().take(20)
    val data = records(dataset1, rawData)   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    memStore.asInstanceOf[TimeSeriesMemStore].refreshIndexForTesting(dataset1.ref)
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexNames(dataset1.ref, 10).toSeq should equal (Seq(("series", 0)))
    memStore.latestOffset(dataset1.ref, 0) shouldEqual 0

    val minSet = rawData.map(_(1).asInstanceOf[Double]).toSet
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val q = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(split))
    q.map(_.getDouble(0)).toSet should equal (minSet)

    // query the series name string column as well
    val q2 = memStore.scanRows(dataset1, Seq(dataset1.partitionColumns.head.id), FilteredPartitionScan(split))
    val partStrings = q2.map(_.filoUTF8String(0).toString).toSet
    val expectedStrings = rawData.map(_(5).asInstanceOf[String]).toSet
    partStrings shouldEqual expectedStrings
  }

  it("should ingest into multiple series and query across partitions") {
    memStore.setup(dataset1.ref, schemas1, 1, TestData.storeConf)
    val data = records(dataset1, linearMultiSeries().take(20))   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 1, data)

    // NOTE: ingesting into wrong shard should give an error
    intercept[IllegalArgumentException] {
      memStore.ingest(dataset1.ref, 0, data)
    }

    memStore.refreshIndexForTesting(dataset1.ref)
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val agg1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(split)).map(_.getDouble(0)).sum
    agg1 shouldEqual (1 to 20).map(_.toDouble).sum
  }

  it("should ingest map/tags column as partition key and aggregate") {
    memStore.setup(dataset2.ref, schemas2h, 0, TestData.storeConf)
    val data = records(dataset2, withMap(linearMultiSeries().take(20)))   // 2 records per series x 10 series
    memStore.ingest(dataset2.ref, 0, data)

    memStore.asInstanceOf[TimeSeriesMemStore].refreshIndexForTesting(dataset2.ref)
    val split = memStore.getScanSplits(dataset2.ref, 1).head
    val filter = ColumnFilter("n", Filter.Equals("2".utf8))
    val agg1 = memStore.scanRows(dataset2, Seq(1), FilteredPartitionScan(split, Seq(filter))).map(_.getDouble(0)).sum
    agg1 shouldEqual (3 + 8 + 13 + 18)
  }

  it("should ingest histograms and read them back properly") {
    memStore.setup(histDataset.ref, schemas2h, 0, TestData.storeConf)
    val data = linearHistSeries().take(40)
    memStore.ingest(histDataset.ref, 0, records(histDataset, data))
    memStore.refreshIndexForTesting(histDataset.ref)

    memStore.numRowsIngested(histDataset.ref, 0) shouldEqual 40L
    // Below will catch any partition match errors.  Should only be 10 tsParts.
    memStore.numPartitions(histDataset.ref, 0) shouldEqual 10

    val split = memStore.getScanSplits(histDataset.ref, 1).head
    val filter = ColumnFilter("dc", Filter.Equals("1".utf8))
    // check sums
    val sums = memStore.scanRows(histDataset, Seq(2), FilteredPartitionScan(split, Seq(filter)))
                       .map(_.getLong(0)).toList
    sums shouldEqual Seq(data(1)(2).asInstanceOf[Long],
                         data(11)(2).asInstanceOf[Long],
                         data(21)(2).asInstanceOf[Long],
                         data(31)(2).asInstanceOf[Long])

    val hists = memStore.scanRows(histDataset, Seq(3), FilteredPartitionScan(split, Seq(filter)))
                        .map(_.getHistogram(0))
    hists.zipWithIndex.foreach { case (h, i) =>
      h shouldEqual data(1 + 10*i)(3).asInstanceOf[LongHistogram]
    }
  }

  it("should ingest multiple schemas simultaneously into one shard") {
    val ref = dataset2.ref
    memStore.setup(ref, schemas2h, 0, TestData.storeConf)
    val data = linearHistSeries().take(40)
    memStore.ingest(ref, 0, records(histDataset, data))
    val data2 = records(dataset2, withMap(linearMultiSeries()).take(30))   // 3 records per series x 10 series
    memStore.ingest(ref, 0, data2)

    memStore.refreshIndexForTesting(ref)

    memStore.numRowsIngested(ref, 0) shouldEqual 70L
    // Below will catch any partition match errors.  Should be 20 (10 hist + 10 dataset2)
    memStore.numPartitions(ref, 0) shouldEqual 20
  }

  it("should be able to handle nonexistent partition keys") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)

    val q = memStore.scanRows(dataset1, Seq(1), SinglePartitionScan(Array[Byte]()))
    q.toBuffer.length should equal (0)
  }

  it("should ingest into multiple series and be able to query on one partition in real time") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val data = multiSeriesData().take(20)     // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, records(dataset1, data))

    val minSeries0 = data(0)(1).asInstanceOf[Double]
    val partKey0 = partKeyBuilder.partKeyFromObjects(schema1, data(0)(5))
    val q = memStore.scanRows(dataset1, Seq(1), SinglePartitionScan(partKey0, 0))
    q.map(_.getDouble(0)).toSeq.head shouldEqual minSeries0

    val minSeries1 = data(1)(1).asInstanceOf[Double]
    val partKey1 = partKeyBuilder.partKeyFromObjects(schema1, "Series 1")
    val q2 = memStore.scanRows(dataset1, Seq(1), SinglePartitionScan(partKey1, 0))
    q2.map(_.getDouble(0)).toSeq.head shouldEqual minSeries1
  }

  it("should query on multiple partitions using filters") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val data = records(dataset1, linearMultiSeries().take(20))   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)
    memStore.refreshIndexForTesting(dataset1.ref)

    val filter = ColumnFilter("series", Filter.Equals("Series 1".utf8))
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val q2 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(split, Seq(filter)))
    q2.map(_.getDouble(0)).toSeq should equal (Seq(2.0, 12.0))
  }

  it("should ingest into multiple shards, getScanSplits, query, get index info from shards") {
    memStore.setup(dataset2.ref, schemas2h, 0, TestData.storeConf)
    memStore.setup(dataset2.ref, schemas2h, 1, TestData.storeConf)
    val data = records(dataset2, withMap(linearMultiSeries()).take(20))   // 2 records per series x 10 series
    memStore.ingest(dataset2.ref, 0, data)
    val data2 = records(dataset2, withMap(linearMultiSeries(200000L, 6), 6).take(20))   // 5 series only
    memStore.ingest(dataset2.ref, 1, data2)
    memStore.refreshIndexForTesting(dataset2.ref)

    memStore.activeShards(dataset2.ref) should equal (Seq(0, 1))
    memStore.numRowsIngested(dataset2.ref, 0) should equal (20L)

    val splits = memStore.getScanSplits(dataset2.ref, 1)
    splits should have length (2)

    memStore.indexNames(dataset2.ref, 10).toSet should equal (
      Set(("n", 0), ("series", 0), ("n", 1), ("series", 1)))

    val filter = ColumnFilter("n", Filter.Equals("2".utf8))
    val agg1 = memStore.scanRows(dataset2, Seq(1), FilteredPartitionScan(splits.head, Seq(filter)))
                       .map(_.getDouble(0)).sum
    agg1 shouldEqual (3 + 8 + 13 + 18)

    val agg2 = memStore.scanRows(dataset2, Seq(1), FilteredPartitionScan(splits.last, Seq(filter)))
                       .map(_.getDouble(0)).sum
    agg2 shouldEqual (3 + 9 + 15)
  }

  it("should handle errors from ingestStream") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val errStream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries()))
                              .endWithError(new NumberFormatException)
    val fut = memStore.ingestStream(dataset1.ref, 0, errStream, s)
    whenReady(fut.failed) { e =>
      e shouldBe a[NumberFormatException]
    }
  }

  it("should ingestStream and flush on interval") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val initChunksWritten = chunksetsWritten

    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries()))
                           .executeWithModel(BatchedExecution(5))
    memStore.ingestStream(dataset1.ref, 0, stream, s).futureValue

    // Two flushes and 3 chunksets have been flushed
    chunksetsWritten shouldEqual initChunksWritten + 4

    memStore.refreshIndexForTesting(dataset1.ref)
    // Try reading - should be able to read optimized chunks too
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val agg1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.head))
                       .map(_.getDouble(0)).sum
    agg1 shouldEqual ((1 to 100).map(_.toDouble).sum)
  }

  it("should flush dirty part keys during start-ingestion, end-ingestion and re-ingestion") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf.copy(groupsPerShard = 2,
                                                        diskTTLSeconds = 1.hour.toSeconds.toInt,
                                                        flushInterval = 10.minutes))
    Thread sleep 1000
    val numPartKeysWritten = partKeysWritten
    val tsShard = memStore.asInstanceOf[TimeSeriesMemStore].getShard(dataset1.ref, 0).get
    tsShard.dirtyPartitionsForIndexFlush.isEmpty shouldEqual true

    val startTime1 = 10000
    val numSamples = 500

    val stream = Observable.fromIterable(groupedRecords(dataset1,
      linearMultiSeries(startTs= startTime1, seriesPrefix = "Set1"),
      n = numSamples, groupSize = 10, ingestionTimeStep = 310000, ingestionTimeStart = 0))
      .executeWithModel(BatchedExecution(5)) // results in 200 records
    memStore.ingestStream(dataset1.ref, 0, stream, s).futureValue

    partKeysWritten shouldEqual numPartKeysWritten + 10 // 10 set1 series started
    0.until(10).foreach{i => tsShard.partitions.get(i).ingesting shouldEqual true}
    0.until(10).foreach{i => tsShard.activelyIngesting(i) shouldEqual true}

    val startTime2 = startTime1 + 1000 * numSamples

    // ingest more time series so a flush cycle passes without above series ingesting
    // this should result in time series end of ingestion being detected
    val stream2 = Observable.fromIterable(groupedRecords(dataset1,
      linearMultiSeries(startTs= startTime2, seriesPrefix = "Set2"),
      n = numSamples, groupSize = 10, ingestionTimeStep = 310000, ingestionTimeStart = tsShard.lastIngestionTime + 1,
      offset = tsShard.latestOffset.toInt + 1))
      .executeWithModel(BatchedExecution(5)) // results in 200 records
    memStore.ingestStream(dataset1.ref, 0, stream2, s).futureValue

    // 10 Set1 series started + 10 Set1 series ended + 10 Set2 series started
    partKeysWritten shouldEqual numPartKeysWritten + 30
    0.until(10).foreach {i => tsShard.partitions.get(i).ingesting shouldEqual false}
    0.until(10).foreach {i => tsShard.activelyIngesting(i) shouldEqual false}
    10.until(20).foreach {i => tsShard.partitions.get(i).ingesting shouldEqual true}
    10.until(20).foreach {i => tsShard.activelyIngesting(i) shouldEqual true}

    val startTime3 = startTime2 + 1000 * numSamples

    // now reingest the stopped time series set1. This should reset endTime again for the Set1 series
    // it should also end 10 Set2 series
    val stream3 = Observable.fromIterable(groupedRecords(dataset1,
      linearMultiSeries(startTs= startTime3, seriesPrefix = "Set1"),
      n = 500, groupSize = 10, ingestionTimeStep = 310000, ingestionTimeStart = tsShard.lastIngestionTime + 1,
      offset = tsShard.latestOffset.toInt + 1))
      .executeWithModel(BatchedExecution(5)) // results in 200 records
    memStore.ingestStream(dataset1.ref, 0, stream3, s).futureValue

    // 10 Set1 series started + 10 Set1 series ended + 10 Set2 series started + 10 set2 series ended +
    // 10 set1 series restarted
    partKeysWritten shouldEqual numPartKeysWritten + 50
    0.until(10).foreach {i => tsShard.partitions.get(i).ingesting shouldEqual true}
    0.until(10).foreach {i => tsShard.activelyIngesting(i) shouldEqual true}
    10.until(20).foreach {i => tsShard.partitions.get(i).ingesting shouldEqual false}
    10.until(20).foreach {i => tsShard.activelyIngesting(i) shouldEqual false}
  }

  it("should recover index data from col store correctly") {

    val partBuilder = new RecordBuilder(TestData.nativeMem)

    val pkPtrs = GdeltTestData.partKeyFromRecords(dataset1,
      records(dataset1, linearMultiSeries().take(2)), Some(partBuilder))
    val pks = pkPtrs.map(dataset1.partKeySchema.asByteArray(_))

    val colStore = new NullColumnStore() {
      override def scanPartKeys(ref: DatasetRef, shard: Int): Observable[PartKeyRecord] = {
        val keys = Seq(
          PartKeyRecord(pks(0), 50, 100, None), // series that has ended ingestion
          PartKeyRecord(pks(1), 250, Long.MaxValue, None) // series that is currently ingesting
        )
        Observable.fromIterable(keys)
      }
    }

    val memStore = new TimeSeriesMemStore(config, colStore, new InMemoryMetaStore())
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf.copy(groupsPerShard = 2,
      diskTTLSeconds = 1.hour.toSeconds.toInt,
      flushInterval = 10.minutes))
    Thread sleep 1000

    val tsShard = memStore.asInstanceOf[TimeSeriesMemStore].getShard(dataset1.ref, 0).get
    tsShard.recoverIndex().futureValue

    tsShard.partitions.size shouldEqual 1 // only ingesting partitions should be loaded into heap
    tsShard.partKeyIndex.indexNumEntries shouldEqual 2 // all partitions should be added to index
    tsShard.partitions.get(0) shouldEqual UnsafeUtils.ZeroPointer
    tsShard.partitions.get(1).partKeyBytes shouldEqual pks(1)
    tsShard.partitions.get(1).ingesting shouldEqual true

    // Entries should be in part key index
    tsShard.partKeyIndex.startTimeFromPartId(0) shouldEqual 50
    tsShard.partKeyIndex.startTimeFromPartId(1) shouldEqual 250
    tsShard.partKeyIndex.endTimeFromPartId(0) shouldEqual 100
    tsShard.partKeyIndex.endTimeFromPartId(1) shouldEqual Long.MaxValue
    tsShard.partKeyIndex.partKeyFromPartId(0).get shouldEqual new BytesRef(pks(0))
    tsShard.partKeyIndex.partKeyFromPartId(1).get shouldEqual new BytesRef(pks(1))

  }

  it("should lookupPartitions and return correct PartLookupResult") {
    memStore.setup(dataset2.ref, schemas2h, 0, TestData.storeConf)
    val data = records(dataset2, withMap(linearMultiSeries().take(20)))   // 2 records per series x 10 series
    memStore.ingest(dataset2.ref, 0, data)

    memStore.asInstanceOf[TimeSeriesMemStore].refreshIndexForTesting(dataset2.ref)
    val split = memStore.getScanSplits(dataset2.ref, 1).head
    val filter = ColumnFilter("n", Filter.Equals("2".utf8))

    val range = TimeRangeChunkScan(105000L, 2000000L)
    val res = memStore.lookupPartitions(dataset2.ref, FilteredPartitionScan(split, Seq(filter)), range,
      QuerySession.makeForTestingOnly)
    res.firstSchemaId shouldEqual Some(schema2.schemaHash)
    res.partsInMemory.length shouldEqual 2   // two partitions should match
    res.shard shouldEqual 0
    res.chunkMethod shouldEqual range
    res.partIdsMemTimeGap shouldEqual debox.Map(7 -> 107000L)
    res.partIdsNotInMemory.isEmpty shouldEqual true
  }

  import Iterators._

  it("should recoveryStream, skip some records, and receive a stream of offset updates") {
    // There are 4 subgroups within the shard.  Set up the watermarks like this:
    // 0 -> 2L, 1 -> 4L, 2 -> 6L, 3 -> 8L
    // A whole bunch of records should be skipped.  However, remember that each group of 5 records gets one offset.

    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val initChunksWritten = chunksetsWritten
    val checkpoints = Map(0 -> 2L, 1 -> 21L, 2 -> 6L, 3 -> 8L)

    // val stream = Observable.fromIterable(linearMultiSeries().take(100).grouped(5).toSeq.map(records(dataset1, _)))
    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries(), 200))
    // recover from checkpoints.min to checkpoints.max
    val offsets = memStore.recoverStream(dataset1.ref, 0, stream, 2, 21, checkpoints, 4L)
                          .until(_ >= 21L).toListL.runAsync.futureValue

    offsets shouldEqual Seq(7L, 11L, 15L, 19L, 21L) // last offset is always reported
    // no flushes
    chunksetsWritten shouldEqual initChunksWritten

    memStore.refreshIndexForTesting(dataset1.ref)
    // Should have less than 50 records ingested
    // Try reading - should be able to read optimized chunks too
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val data1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.head))
                        .map(_.getDouble(0)).toSeq
    data1.length shouldEqual 47
  }

  it("should recoverStream after timeout, returns endOffset to start normal ingestion") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val checkpoints = Map(0 -> 2L, 1 -> 21L, 2 -> 6L, 3 -> 8L)

    val stream = Observable.never // "never" to mimic no data in stream source
    val startOffset = checkpoints.values.min
    val endOffset = checkpoints.values.max
    // recover from checkpoints.min to checkpoints.max - timeout after 1sec
    val offsets = memStore.recoverStream(dataset1.ref, 0, stream, startOffset, endOffset, checkpoints, 4L)(1.second)
      .until(_ >= 21L).toListL.runAsync.futureValue

    offsets shouldEqual Seq(checkpoints.values.max) // should equal end offset
  }

  it("should truncate shards properly") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val data = records(dataset1, multiSeriesData().take(20))   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    memStore.refreshIndexForTesting(dataset1.ref)
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexNames(dataset1.ref, 10).toSeq should equal (Seq(("series", 0)))

    memStore.truncate(dataset1.ref, numShards = 4)

    memStore.numPartitions(dataset1.ref, 0) should equal (0)
  }

  private def chunksetsWritten = memStore.store.sinkStats.chunksetsWritten.get()
  private def partKeysWritten = memStore.store.sinkStats.partKeysWritten.get()

  // returns the "endTime" or last sample time of evicted partitions
  // used for testing only
  def markPartitionsForEviction(partIDs: Seq[Int]): Unit = {
    val shard = memStore.getShardE(dataset1.ref, 0)
    for { n <- partIDs } {
      val part = shard.partitions.get(n)
      shard.markPartAsNotIngesting(part, false)
    }
  }

  it("should be able to evict partitions properly on ingestion and on ODP") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)

    val shard = memStore.getShardE(dataset1.ref, 0)
    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(dataset1, linearMultiSeries(numSeries = 1000).take(1000))
    memStore.ingest(dataset1.ref, 0, data)

    memStore.refreshIndexForTesting(dataset1.ref)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 1000
    memStore.labelValues(dataset1.ref, 0, "series", 2000).size shouldEqual 1000

    // Purposely mark 100 partitions endTime as occurring a while ago to mark them eligible for eviction
    // We also need to switch buffers so that internally ingestionEndTime() is accurate
    markPartitionsForEviction(0 until 10)

    // Note that we didn't explicitly evict. The code below will test force-eviction. It will also
    // ensure TimeSeriesShard maintains the invariant of not more than maxPartitions (1100 here) limit

    // Now, ingest 120 partitions.  First 110 partitions should be allowed after force-eviction,
    // 10 partitions should be dropped
    val data2 = records(dataset1, linearMultiSeries(numSeries = 1120).drop(1000).take(120))
    memStore.ingest(dataset1.ref, 0, data2)
    Thread sleep 1000    // see if this will make things pass sooner

    shard.partitions.size() shouldEqual 1100
    shard.addPartitionsDisabled() shouldEqual true

    markPartitionsForEviction(10 until 20)
    shard.evictForHeadroom() shouldEqual true // this should evict only 10 partitions, but cannot reach headroom
    // but add of new partitions is enabled since we were able to evict some
    shard.addPartitionsDisabled() shouldEqual false
    shard.partitions.size() shouldEqual 1090

    // fill the 10 slots remaining
    val data3 = records(dataset1, linearMultiSeries(numSeries = 1130).drop(1120).take(10))
    memStore.ingest(dataset1.ref, 0, data3)
    shard.partitions.size() shouldEqual 1100

    // now there is no room for any more partitions. Try ODP
    memStore.refreshIndexForTesting(dataset1.ref)
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val filter = ColumnFilter("series", Filter.Equals("Series 0".utf8))

    val session = QuerySession.makeForTestingOnly()
    // ODP query should fail since there is no room
    val ex = intercept[TestFailedException] {
      memStore.scanPartitions(dataset1.ref, Seq(0, 1), FilteredPartitionScan(split, Seq(filter)),
        querySession = session)
        .toListL.runAsync.futureValue
    }
    session.close() // release lock
    ex.getCause.isInstanceOf[ServiceUnavailableException] shouldEqual true

    // mark 1 partition for eviction
    markPartitionsForEviction(20 until 21)
    // Not we are not explicitly evicting. ODP should not evict
    // ODP query should fail even though there is room for eviction
    // Reason is we dont want ODP cannibalism of partitions
    val ex2 = intercept[TestFailedException] {
      memStore.scanPartitions(dataset1.ref, Seq(0, 1), FilteredPartitionScan(split, Seq(filter)),
        querySession = session)
        .toListL.runAsync.futureValue
    }
    session.close() // release lock
    ex2.getCause.isInstanceOf[ServiceUnavailableException] shouldEqual true

    // after next headroom task run....
    shard.evictForHeadroom() shouldEqual true

    // now query should succeed
    val parts = memStore.scanPartitions(dataset1.ref, Seq(0, 1), FilteredPartitionScan(split, Seq(filter)),
        querySession = session)
        .toListL.runAsync.futureValue
    parts.size shouldEqual 1
    parts.head.partID shouldEqual 0 // same partId as before for ODPed partitions
    session.close() // release lock
    shard.evictableOdpPartIds.size() shouldEqual 1

    // mark some parts as evictable
    markPartitionsForEviction(21 until 25)
    shard.evictForHeadroom() shouldEqual true
    // odp partitions should be evicted first before regular partitions
    shard.evictableOdpPartIds.size() shouldEqual 0

  }

  it("should assign same previously assigned partId using bloom filter when evicted series starts re-ingesting") {

    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)

    val shard0 = memStore.getShardE(dataset1.ref, 0)
    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(dataset1, linearMultiSeries(numSeries = 1000).take(1000))
    memStore.ingest(dataset1.ref, 0, data)
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 1000

    val shard0Partitions = shard0.partitions
    var part0 = shard0Partitions.get(0)
    dataset1.partKeySchema.asJavaString(part0.partKeyBase, part0.partKeyOffset, 0) shouldEqual "Series 0"
    val pkBytes = dataset1.partKeySchema.asByteArray(part0.partKeyBase, part0.partKeyOffset)
    val pk = PartKey(pkBytes, UnsafeUtils.arayOffset)
    shard0.evictedPartKeys.mightContain(pk) shouldEqual false

    // Purposely mark 100 partitions endTime as occurring a while ago to mark them eligible for eviction
    // We also need to switch buffers so that internally ingestionEndTime() is accurate
    markPartitionsForEviction(0 until 10)
    shard0.evictForHeadroom() shouldEqual true // this should evict 10 partitions since maxAllowed is 1100, and headroom is 110

    shard0Partitions.get(0) shouldEqual null

    // bloom filter should contain the pk
    shard0.evictedPartKeys.mightContain(pk) shouldEqual true
    memStore.refreshIndexForTesting(dataset1.ref)

    // re-ingest partId 0
    val data2 = records(dataset1, linearMultiSeries(numSeries = 1).take(1))
    memStore.ingest(dataset1.ref, 0, data2)
    Thread.sleep(1000)

    part0 = shard0Partitions.get(0)
    part0.partID shouldEqual 0
  }

  it("should return extra WriteBuffers to memoryManager properly") {
    val numSeries = 300
    val policy2 = new FixedMaxPartitionsEvictionPolicy(numSeries * 2)
    val store2 = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy2))

    try {
      // Ingest >250 partitions.  Note how much memory is left after all the allocations
      store2.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
      val shard = store2.getShardE(dataset1.ref, 0)

      // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
      val data = records(dataset1, linearMultiSeries(numSeries = numSeries).take(numSeries))
      store2.ingest(dataset1.ref, 0, data)
      store2.refreshIndexForTesting(dataset1.ref)

      store2.numPartitions(dataset1.ref, 0) shouldEqual numSeries
      shard.bufferPools.size shouldEqual 1
      shard.bufferPools.valuesArray.head.poolSize shouldEqual 100    // Two allocations of 200 each = 400; used up 300; 400-300=100
      val afterIngestFree = shard.bufferMemoryManager.numFreeBytes

      // Switch buffers, encode and release/return buffers for all partitions
      val blockFactory = shard.blockFactoryPool.checkoutForOverflow(0)
      for { n <- 0 until numSeries } {
        val part = shard.partitions.get(n)
        part.switchBuffers(blockFactory, encode = true)
      }

      // Ensure queue length does not get beyond 250, and some memory was freed (free bytes increases)
      shard.bufferPools.valuesArray.head.poolSize shouldEqual 250
      val nowFree = shard.bufferMemoryManager.numFreeBytes
      nowFree should be > (afterIngestFree)
    } finally {
      store2.shutdown()    // release snd free the memory
    }
  }
}
