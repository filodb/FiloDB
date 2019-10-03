package filodb.core.memstore

//import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
//import monix.execution.ExecutionModel.BatchedExecution
import monix.reactive.Observable
import org.apache.lucene.util.BytesRef
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.binaryrecord2.{RecordBuilder, RecordContainer}
import filodb.core.memstore.TimeSeriesShard.{indexTimeBucketSchema, indexTimeBucketSegmentSize, PartKey}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{FilteredPartitionScan, InMemoryMetaStore, NullColumnStore, SinglePartitionScan}
import filodb.memory.{BinaryRegionLarge, MemFactory}
import filodb.memory.format.{ArrayStringRowReader, UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.vectors.MutableHistogram

class TimeSeriesMemStoreSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  implicit val s = monix.execution.Scheduler.Implicits.global

  import MachineMetricsData._
  import ZeroCopyUTF8String._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))
  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  after {
    // clear the highest time bucket from test runs
    memStore.metastore.writeHighestIndexTimeBucket(dataset1.ref, 0, -1).futureValue
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
      h shouldEqual data(1 + 10*i)(3).asInstanceOf[MutableHistogram]
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

    val filter =  ColumnFilter("series", Filter.Equals("Series 1".utf8))
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

  it("should ingestStream into multiple shards and not flush with empty flush stream") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    memStore.setup(dataset1.ref, schemas1, 1, TestData.storeConf)
    memStore.activeShards(dataset1.ref) should equal (Seq(0, 1))

    val initChunksWritten = chunksetsWritten

    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries()))
    val fut1 = memStore.ingestStream(dataset1.ref, 0, stream, s)
    val fut2 = memStore.ingestStream(dataset1.ref, 1, stream, s)
    // Allow both futures to run first before waiting for completion
    fut1.futureValue
    fut2.futureValue

    memStore.refreshIndexForTesting(dataset1.ref)
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val agg1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.head))
                       .map(_.getDouble(0)).sum
    agg1 shouldEqual ((1 to 100).map(_.toDouble).sum)

    val agg2 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.last))
                       .map(_.getDouble(0)).sum
    agg2 shouldEqual ((1 to 100).map(_.toDouble).sum)

    // should not have increased
    chunksetsWritten shouldEqual initChunksWritten
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

  /*
  it("should ingestStream and flush on interval") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val initChunksWritten = chunksetsWritten

    // Flush every 50 records.
    // NOTE: due to the batched ExecutionModel of fromIterable, it is hard to determine exactly when the FlushCommand
    // gets mixed in.  The custom BatchedExecution reduces batch size and causes better interleaving though.
    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries()))
                           .executeWithModel(BatchedExecution(5))
    memStore.ingestStream(dataset1.ref, 0, stream, s).futureValue

    // Two flushes and 3 chunksets have been flushed
    // FIXME: fails now
    chunksetsWritten shouldEqual initChunksWritten + 4

    memStore.refreshIndexForTesting(dataset1.ref)
    // Try reading - should be able to read optimized chunks too
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val agg1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.head))
                       .map(_.getDouble(0)).sum
    agg1 shouldEqual ((1 to 100).map(_.toDouble).sum)
  }
   */

  /* FIXME
  it("should flush index time buckets during one group of a flush interval") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf.copy(groupsPerShard = 2,
                                                        demandPagedRetentionPeriod = 1.hour,
                                                        flushInterval = 10.minutes))
    val initTimeBuckets = timebucketsWritten
    val tsShard = memStore.asInstanceOf[TimeSeriesMemStore].getShard(dataset1.ref, 0).get
    tsShard.timeBucketBitmaps.keySet.asScala shouldEqual Set(0)

    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries(), n = 500, groupSize = 10))
      .executeWithModel(BatchedExecution(5)) // results in 200 records
    memStore.ingestStream(dataset1.ref, 0, stream, s).futureValue

    // 500 records / 2 flushGroups per flush interval / 10 records per flush = 25 time buckets
    timebucketsWritten shouldEqual initTimeBuckets + 25

    // 1 hour retention period / 10 minutes flush interval = 6 time buckets to be retained
    tsShard.timeBucketBitmaps.keySet.asScala.toSeq.sorted shouldEqual 19.to(25) // 6 buckets retained + one for current
  }
   */

  /**
    * Tries to write partKeys into time bucket record container and extracts them back into the shard
    */
  def indexRecoveryTest(dataset: Dataset, partKeys: Seq[Long]): Unit = {
    memStore.metastore.writeHighestIndexTimeBucket(dataset.ref, 0, 0)
    val schemas = Schemas(dataset.schema)
    memStore.setup(dataset.ref, schemas, 0,
      TestData.storeConf.copy(groupsPerShard = 2, demandPagedRetentionPeriod = 1.hour,
        flushInterval = 10.minutes))
    val tsShard = memStore.asInstanceOf[TimeSeriesMemStore].getShard(dataset.ref, 0).get
    val timeBucketRb = new RecordBuilder(MemFactory.onHeapFactory, indexTimeBucketSegmentSize)

    partKeys.zipWithIndex.foreach { case (off, i) =>
      timeBucketRb.startNewRecord(indexTimeBucketSchema, 0)
      timeBucketRb.addLong(i + 10) // startTime
      timeBucketRb.addLong(if (i%2 == 0) Long.MaxValue else i + 20) // endTime
      val numBytes = BinaryRegionLarge.numBytes(UnsafeUtils.ZeroPointer, off)
      timeBucketRb.addBlob(UnsafeUtils.ZeroPointer, off, numBytes + 4) // partKey
      timeBucketRb.endRecord(false)
    }
    tsShard.initTimeBuckets()

    val partIdMap = debox.Map.empty[BytesRef, Int]

    timeBucketRb.optimalContainerBytes(true).foreach { bytes =>
      tsShard.extractTimeBucket(new IndexData(1, 0, RecordContainer(bytes)), partIdMap)
    }
    tsShard.refreshPartKeyIndexBlocking()
    partIdMap.size shouldEqual partKeys.size
    partKeys.zipWithIndex.foreach { case (off, i) =>
      val readPartKey = tsShard.partKeyIndex.partKeyFromPartId(i).get
      val expectedPartKey = dataset1.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, off)
      readPartKey.bytes.slice(readPartKey.offset, readPartKey.offset + readPartKey.length) shouldEqual expectedPartKey
      if (i%2 == 0) {
        tsShard.partSet.getWithPartKeyBR(UnsafeUtils.ZeroPointer, off, schemas.part).get.partID shouldEqual i
        tsShard.partitions.containsKey(i) shouldEqual true // since partition is ingesting
      }
      else {
        tsShard.partSet.getWithPartKeyBR(UnsafeUtils.ZeroPointer, off, schemas.part) shouldEqual None
        tsShard.partitions.containsKey(i) shouldEqual false // since partition is not ingesting
      }
    }
  }

  it("should recover index from time buckets") {
    import GdeltTestData._
    // NOTE: gdeltLines3 are actually data samples, not part keys. Only take lines which give unique part keys!!
    val readers = gdeltLines3.take(8).map { line => ArrayStringRowReader(line.split(",")) }
    val partKeys = partKeyFromRecords(dataset1, records(dataset1, readers.take(10)))
    indexRecoveryTest(dataset1, partKeys)
  }

  it("should recover index from time buckets - with two partition columns of type string") {
    indexRecoveryTest(CustomMetricsData.metricdataset, Seq(CustomMetricsData.defaultPartKey))
  }

  it("should recover index from time buckets - with single partition column of type map") {
    indexRecoveryTest(CustomMetricsData.metricdataset2, Seq(CustomMetricsData.defaultPartKey2))
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

  it("should truncate shards properly") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)
    val data = records(dataset1, multiSeriesData().take(20))   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    memStore.refreshIndexForTesting(dataset1.ref)
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexNames(dataset1.ref, 10).toSeq should equal (Seq(("series", 0)))

    memStore.truncate(dataset1.ref)

    memStore.numPartitions(dataset1.ref, 0) should equal (0)
  }

  private def chunksetsWritten = memStore.store.sinkStats.chunksetsWritten
  private def timebucketsWritten = memStore.store.sinkStats.timeBucketsWritten

  // returns the "endTime" or last sample time of evicted partitions
  def markPartitionsForEviction(partIDs: Seq[Int]): Long = {
    val shard = memStore.getShardE(dataset1.ref, 0)
    val blockFactory = shard.overflowBlockFactory
    var endTime = 0L
    for { n <- partIDs } {
      val part = shard.partitions.get(n)
      part.switchBuffers(blockFactory, encode=true)
      shard.updatePartEndTimeInIndex(part, part.timestampOfLatestSample)
      endTime = part.timestampOfLatestSample
    }
    memStore.refreshIndexForTesting(dataset1.ref)
    endTime
  }

  it("should be able to evict partitions properly, flush, and still query") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)

    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(dataset1, linearMultiSeries().take(10))
    memStore.ingest(dataset1.ref, 0, data)

    memStore.refreshIndexForTesting(dataset1.ref)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.labelValues(dataset1.ref, 0, "series").toSeq should have length (10)

    // Purposely mark two partitions endTime as occurring a while ago to mark them eligible for eviction
    // We also need to switch buffers so that internally ingestionEndTime() is accurate
    val endTime = markPartitionsForEviction(0 to 1)

    // Now, ingest 22 partitions.  First two partitions ingested should be evicted. Check numpartitions, stats, index
    val data2 = records(dataset1, linearMultiSeries(numSeries = 22).drop(2).take(20))
    memStore.ingest(dataset1.ref, 0, data2)
    Thread sleep 1000    // see if this will make things pass sooner

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 20
    memStore.getShardE(dataset1.ref, 0).evictionWatermark shouldEqual endTime + 1
    memStore.getShardE(dataset1.ref, 0).addPartitionsDisabled() shouldEqual false

    memStore.refreshIndexForTesting(dataset1.ref)
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val parts = memStore.scanPartitions(dataset1, Seq(0, 1), FilteredPartitionScan(split))
                        .toListL.runAsync
                        .futureValue
                        .asInstanceOf[Seq[TimeSeriesPartition]]
    parts.map(_.partID).toSet shouldEqual (2 to 21).toSet  ++ Set(0)
    // Above query will ODP evicted partition 0 back in, but there is no space for evicted part 1,
    // so it will not be returned as part of query :(
  }

  it("should be able to ODP/query partitions evicted from memory structures when doing index/tag query") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)

    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(dataset1, linearMultiSeries().take(10))
    memStore.ingest(dataset1.ref, 0, data)

    memStore.refreshIndexForTesting(dataset1.ref)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.labelValues(dataset1.ref, 0, "series").toSeq should have length (10)

    // Purposely mark two partitions endTime as occurring a while ago to mark them eligible for eviction
    // We also need to switch buffers so that internally ingestionEndTime() is accurate
    val endTime = markPartitionsForEviction(0 to 1)

    // Now, ingest 20 partitions.  First two partitions ingested should be evicted. Check numpartitions, stats, index
    val data2 = records(dataset1, linearMultiSeries(numSeries = 22).drop(2).take(20))
    memStore.ingest(dataset1.ref, 0, data2)
    Thread sleep 1000    // see if this will make things pass sooner

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 20

    // Try to query "Series 0" which got evicted.  It should create a new partition, and now there should be
    // one more part
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val filter = ColumnFilter("series", Filter.Equals("Series 0".utf8))
    val parts = memStore.scanPartitions(dataset1, Seq(0, 1), FilteredPartitionScan(split, Seq(filter)))
                        .toListL.runAsync
                        .futureValue
                        .asInstanceOf[Seq[TimeSeriesPartition]]
    parts.map(_.partID) shouldEqual Seq(0)    // newly created ODP partitions get earlier partId
    dataset1.partKeySchema.asJavaString(parts.head.partKeyBase, parts.head.partKeyOffset, 0) shouldEqual "Series 0"
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 21
  }

  it("should assign same previously assigned partId using bloom filter when evicted series starts re-ingesting") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)

    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(dataset1, linearMultiSeries().take(10))
    memStore.ingest(dataset1.ref, 0, data)

    memStore.refreshIndexForTesting(dataset1.ref)

    val shard0 = memStore.getShard(dataset1.ref, 0).get
    val shard0Partitions = shard0.partitions

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.labelValues(dataset1.ref, 0, "series").toSeq should have length (10)
    var part0 = shard0Partitions.get(0)
    dataset1.partKeySchema.asJavaString(part0.partKeyBase, part0.partKeyOffset, 0) shouldEqual "Series 0"
    val pkBytes = dataset1.partKeySchema.asByteArray(part0.partKeyBase, part0.partKeyOffset)
    val pk = PartKey(pkBytes, UnsafeUtils.arayOffset)
    shard0.evictedPartKeys.mightContain(pk) shouldEqual false

    // Purposely mark two partitions endTime as occurring a while ago to mark them eligible for eviction
    // We also need to switch buffers so that internally ingestionEndTime() is accurate
    markPartitionsForEviction(0 to 1)

    // Now, ingest 20 partitions.  First two partitions ingested should be evicted.
    val data2 = records(dataset1, linearMultiSeries(numSeries = 22).drop(2).take(20))
    memStore.ingest(dataset1.ref, 0, data2)
    Thread sleep 1000    // see if this will make things pass sooner

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 20

    // scalastyle:off null
    shard0Partitions.get(0) shouldEqual null // since partId 0 has been evicted
    shard0.evictedPartKeys.mightContain(pk) shouldEqual true

    // now re-ingest data for evicted partition with partKey "Series 0"
    val data3 = records(dataset1, linearMultiSeries().take(1))
    memStore.ingest(dataset1.ref, 0, data3)

    // the partId assigned should still be 0
    part0 = shard0Partitions.get(0)
    dataset1.partKeySchema.asJavaString(part0.partKeyBase, part0.partKeyOffset, 0) shouldEqual "Series 0"
  }

  it("should be able to skip ingestion/add partitions if there is no more space left") {
    memStore.setup(dataset1.ref, schemas1, 0, TestData.storeConf)

    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(dataset1, linearMultiSeries().take(10))
    memStore.ingest(dataset1.ref, 0, data)

    memStore.refreshIndexForTesting(dataset1.ref)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.labelValues(dataset1.ref, 0, "series").toSeq should have length (10)

    // Don't mark any partitions for eviction
    // Now, ingest 23 partitions.  Last two partitions cannot be added.  Check numpartitions, stats, index
    val data2 = records(dataset1, linearMultiSeries(numSeries = 23).take(23))
    memStore.ingest(dataset1.ref, 0, data2)

    memStore.getShardE(dataset1.ref, 0).addPartitionsDisabled() shouldEqual true
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 21   // due to the way the eviction policy works
    memStore.getShardE(dataset1.ref, 0).evictionWatermark shouldEqual 0

    memStore.refreshIndexForTesting(dataset1.ref)
    // Check partitions are now 0 to 20, 21/22 did not get added
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val parts = memStore.scanPartitions(dataset1, Seq(0, 1), FilteredPartitionScan(split))
                        .toListL.runAsync
                        .futureValue
                        .asInstanceOf[Seq[TimeSeriesPartition]]
    parts.map(_.partID).toSet shouldEqual (0 to 20).toSet
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
      val blockFactory = shard.overflowBlockFactory
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
