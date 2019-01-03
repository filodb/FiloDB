package filodb.core.memstore

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.execution.ExecutionModel.BatchedExecution
import monix.reactive.Observable
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.binaryrecord2.{RecordBuilder, RecordContainer}
import filodb.core.memstore.TimeSeriesShard.{indexTimeBucketSchema, indexTimeBucketSegmentSize}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{FilteredPartitionScan, InMemoryMetaStore, NullColumnStore, SinglePartitionScan}
import filodb.memory.{BinaryRegionLarge, MemFactory}
import filodb.memory.format.{ArrayStringRowReader, UnsafeUtils, ZeroCopyUTF8String}

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

  // Look mama!  Real-time time series ingestion and querying across multiple partitions!
  it("should ingest into multiple series and be able to query across all partitions in real time") {
    memStore.setup(dataset1, 0, TestData.storeConf)
    val rawData = multiSeriesData().take(20)
    val data = records(dataset1, rawData)   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    memStore.asInstanceOf[TimeSeriesMemStore].commitIndexForTesting(dataset1.ref)
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexNames(dataset1.ref).toSeq should equal (Seq(("series", 0)))
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
    memStore.setup(dataset1, 1, TestData.storeConf)
    val data = records(dataset1, linearMultiSeries().take(20))   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 1, data)

    // NOTE: ingesting into wrong shard should give an error
    intercept[IllegalArgumentException] {
      memStore.ingest(dataset1.ref, 0, data)
    }

    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val agg1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(split)).map(_.getDouble(0)).sum
    agg1 shouldEqual ((1 to 20).map(_.toDouble).sum)
  }

  it("should ingest map/tags column as partition key and aggregate") {
    memStore.setup(dataset2, 0, TestData.storeConf)
    val data = records(dataset2, withMap(linearMultiSeries().take(20)))   // 2 records per series x 10 series
    memStore.ingest(dataset2.ref, 0, data)

    memStore.asInstanceOf[TimeSeriesMemStore].commitIndexForTesting(dataset2.ref)
    val split = memStore.getScanSplits(dataset2.ref, 1).head
    val filter = ColumnFilter("n", Filter.Equals("2".utf8))
    val agg1 = memStore.scanRows(dataset2, Seq(1), FilteredPartitionScan(split, Seq(filter))).map(_.getDouble(0)).sum
    agg1 shouldEqual (3 + 8 + 13 + 18)
  }

  it("should be able to handle nonexistent partition keys") {
    memStore.setup(dataset1, 0, TestData.storeConf)

    val q = memStore.scanRows(dataset1, Seq(1), SinglePartitionScan(Array[Byte]()))
    q.toBuffer.length should equal (0)
  }

  it("should ingest into multiple series and be able to query on one partition in real time") {
    memStore.setup(dataset1, 0, TestData.storeConf)
    val data = multiSeriesData().take(20)     // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, records(dataset1, data))

    val minSeries0 = data(0)(1).asInstanceOf[Double]
    val partKey0 = partKeyBuilder.addFromObjects(data(0)(5))
    val q = memStore.scanRows(dataset1, Seq(1), SinglePartitionScan(partKey0, 0))
    q.map(_.getDouble(0)).toSeq.head shouldEqual minSeries0

    val minSeries1 = data(1)(1).asInstanceOf[Double]
    val partKey1 = partKeyBuilder.addFromObjects("Series 1")
    val q2 = memStore.scanRows(dataset1, Seq(1), SinglePartitionScan(partKey1, 0))
    q2.map(_.getDouble(0)).toSeq.head shouldEqual minSeries1
  }

  it("should query on multiple partitions using filters") {
    memStore.setup(dataset1, 0, TestData.storeConf)
    val data = records(dataset1, linearMultiSeries().take(20))   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)
    memStore.commitIndexForTesting(dataset1.ref)

    val filter =  ColumnFilter("series", Filter.Equals("Series 1".utf8))
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val q2 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(split, Seq(filter)))
    q2.map(_.getDouble(0)).toSeq should equal (Seq(2.0, 12.0))
  }

  it("should ingest into multiple shards, getScanSplits, query, get index info from shards") {
    memStore.setup(dataset2, 0, TestData.storeConf)
    memStore.setup(dataset2, 1, TestData.storeConf)
    val data = records(dataset2, withMap(linearMultiSeries()).take(20))   // 2 records per series x 10 series
    memStore.ingest(dataset2.ref, 0, data)
    val data2 = records(dataset2, withMap(linearMultiSeries(200000L, 6), 6).take(20))   // 5 series only
    memStore.ingest(dataset2.ref, 1, data2)
    memStore.commitIndexForTesting(dataset2.ref)

    memStore.activeShards(dataset1.ref) should equal (Seq(0, 1))
    memStore.numRowsIngested(dataset1.ref, 0) should equal (20L)

    val splits = memStore.getScanSplits(dataset2.ref, 1)
    splits should have length (2)

    memStore.indexNames(dataset2.ref).toSet should equal (
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
    memStore.setup(dataset1, 0, TestData.storeConf)
    memStore.setup(dataset1, 1, TestData.storeConf)
    memStore.activeShards(dataset1.ref) should equal (Seq(0, 1))

    val initChunksWritten = chunksetsWritten

    // val stream = Observable.fromIterable(linearMultiSeries().take(100).grouped(5).toSeq.map(records(dataset1, _)))
    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries()))
    val fut1 = memStore.ingestStream(dataset1.ref, 0, stream, s, FlushStream.empty)
    val fut2 = memStore.ingestStream(dataset1.ref, 1, stream, s, FlushStream.empty)
    // Allow both futures to run first before waiting for completion
    fut1.futureValue
    fut2.futureValue

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
    memStore.setup(dataset1, 0, TestData.storeConf)
    val errStream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries()))
                              .endWithError(new NumberFormatException)
    val fut = memStore.ingestStream(dataset1.ref, 0, errStream, s)
    whenReady(fut.failed) { e =>
      e shouldBe a[NumberFormatException]
    }
  }

  it("should ingestStream and flush on interval") {
    memStore.setup(dataset1, 0, TestData.storeConf)
    val initChunksWritten = chunksetsWritten

    // Flush every 50 records.
    // NOTE: due to the batched ExecutionModel of fromIterable, it is hard to determine exactly when the FlushCommand
    // gets mixed in.  The custom BatchedExecution reduces batch size and causes better interleaving though.
    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries()))
                           .executeWithModel(BatchedExecution(5))
    val flushStream = FlushStream.everyN(4, 50, stream)
    memStore.ingestStream(dataset1.ref, 0, stream, s, flushStream).futureValue

    // Two flushes and 3 chunksets have been flushed
    chunksetsWritten shouldEqual initChunksWritten + 4

    // Try reading - should be able to read optimized chunks too
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val agg1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.head))
                       .map(_.getDouble(0)).sum
    agg1 shouldEqual ((1 to 100).map(_.toDouble).sum)
  }

  it("should flush index time buckets during one group of a flush interval") {
    memStore.setup(dataset1, 0, TestData.storeConf.copy(groupsPerShard = 2,
                                                        demandPagedRetentionPeriod = 1.hour,
                                                        flushInterval = 10.minutes))
    val initTimeBuckets = timebucketsWritten
    val tsShard = memStore.asInstanceOf[TimeSeriesMemStore].getShard(dataset1.ref, 0).get
    tsShard.timeBucketBitmaps.keySet.asScala shouldEqual Set(0)

    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries(), n = 500, groupSize = 10))
      .executeWithModel(BatchedExecution(5)) // results in 200 records
    val flushStream = FlushStream.everyN(2, 10, stream)
    memStore.ingestStream(dataset1.ref, 0, stream, s, flushStream).futureValue

    // 500 records / 2 flushGroups per flush interval / 10 records per flush = 25 time buckets
    timebucketsWritten shouldEqual initTimeBuckets + 25

    // 1 hour retention period / 10 minutes flush interval = 6 time buckets to be retained
    tsShard.timeBucketBitmaps.keySet.asScala.toSeq.sorted shouldEqual 19.to(25) // 6 buckets retained + one for current
  }

  it("should recover index from time buckets") {
    import GdeltTestData._
    memStore.metastore.writeHighestIndexTimeBucket(dataset1.ref, 0, 0)
    memStore.setup(dataset1, 0, TestData.storeConf.copy(groupsPerShard = 2, demandPagedRetentionPeriod = 1.hour,
      flushInterval = 10.minutes))
    val tsShard = memStore.asInstanceOf[TimeSeriesMemStore].getShard(dataset1.ref, 0).get
    val timeBucketRb = new RecordBuilder(MemFactory.onHeapFactory, indexTimeBucketSchema, indexTimeBucketSegmentSize)

    val readers = gdeltLines3.map { line => ArrayStringRowReader(line.split(",")) }
    val partKeys = partKeyFromRecords(dataset1, records(dataset1, readers.take(10)))

    partKeys.zipWithIndex.foreach { case (off, i) =>
      timeBucketRb.startNewRecord()
      timeBucketRb.addLong(i + 10)
      timeBucketRb.addLong(i + 20)
      val numBytes = BinaryRegionLarge.numBytes(UnsafeUtils.ZeroPointer, off)
      timeBucketRb.addBlob(UnsafeUtils.ZeroPointer, off, numBytes + 4)
      timeBucketRb.endRecord(false)
    }
    tsShard.initTimeBuckets()
    timeBucketRb.optimalContainerBytes(true).foreach { bytes =>
      tsShard.extractTimeBucket(new IndexData(1, 0, RecordContainer(bytes)))
    }
    tsShard.commitPartKeyIndexBlocking()
    partKeys.zipWithIndex.foreach { case (off, i) =>
      val readPartKey = tsShard.partKeyIndex.partKeyFromPartId(i).get
      val expectedPartKey = dataset1.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, off)
      readPartKey.bytes.drop(readPartKey.offset).take(readPartKey.length) shouldEqual expectedPartKey
      tsShard.partitions.get(i).partKeyBytes shouldEqual expectedPartKey
      tsShard.partSet.getWithPartKeyBR(UnsafeUtils.ZeroPointer, off).get.partID shouldEqual i
    }
  }

  import Iterators._

  it("should recoveryStream, skip some records, and receive a stream of offset updates") {
    // There are 4 subgroups within the shard.  Set up the watermarks like this:
    // 0 -> 2L, 1 -> 4L, 2 -> 6L, 3 -> 8L
    // A whole bunch of records should be skipped.  However, remember that each group of 5 records gets one offset.

    memStore.setup(dataset1, 0, TestData.storeConf)
    val initChunksWritten = chunksetsWritten
    val checkpoints = Map(0 -> 2L, 1 -> 4L, 2 -> 6L, 3 -> 8L)

    // val stream = Observable.fromIterable(linearMultiSeries().take(100).grouped(5).toSeq.map(records(dataset1, _)))
    val stream = Observable.fromIterable(groupedRecords(dataset1, linearMultiSeries()))
    val offsets = memStore.recoverStream(dataset1.ref, 0, stream, checkpoints, 4L)
                          .until(_ >= 50L).toListL.runAsync.futureValue

    offsets shouldEqual Seq(7L, 11L, 15L, 19L)
    // no flushes
    chunksetsWritten shouldEqual initChunksWritten

    // Should have less than 50 records ingested
    // Try reading - should be able to read optimized chunks too
    val splits = memStore.getScanSplits(dataset1.ref, 1)
    val data1 = memStore.scanRows(dataset1, Seq(1), FilteredPartitionScan(splits.head))
                        .map(_.getDouble(0)).toSeq
    data1.length shouldEqual 71
  }

  it("should truncate shards properly") {
    memStore.setup(dataset1, 0, TestData.storeConf)
    val data = records(dataset1, multiSeriesData().take(20))   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    memStore.commitIndexForTesting(dataset1.ref)
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexNames(dataset1.ref).toSeq should equal (Seq(("series", 0)))

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
    memStore.commitIndexForTesting(dataset1.ref)
    endTime
  }

  it("should be able to evict partitions properly, flush, and still query") {
    memStore.setup(dataset1, 0, TestData.storeConf)

    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(dataset1, linearMultiSeries().take(10))
    memStore.ingest(dataset1.ref, 0, data)

    memStore.commitIndexForTesting(dataset1.ref)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexValues(dataset1.ref, 0, "series").toSeq should have length (10)

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

    // Check partitions are now 2 to 21, 0 and 1 got evicted
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val parts = memStore.scanPartitions(dataset1, Seq(0, 1), FilteredPartitionScan(split))
                        .toListL.runAsync
                        .futureValue
                        .asInstanceOf[Seq[TimeSeriesPartition]]
    parts.map(_.partID).toSet shouldEqual (2 to 21).toSet
  }

  it("should be able to ODP/query partitions evicted from memory structures when doing index/tag query") {
    memStore.setup(dataset1, 0, TestData.storeConf)

    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(dataset1, linearMultiSeries().take(10))
    memStore.ingest(dataset1.ref, 0, data)

    memStore.commitIndexForTesting(dataset1.ref)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexValues(dataset1.ref, 0, "series").toSeq should have length (10)

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
    parts.map(_.partID) shouldEqual Seq(22)    // newly created partitions get a new ID
    dataset1.partKeySchema.asJavaString(parts.head.partKeyBase, parts.head.partKeyOffset, 0) shouldEqual "Series 0"
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 21
  }

  it("should be able to skip ingestion/add partitions if there is no more space left") {
    memStore.setup(dataset1, 0, TestData.storeConf)

    // Ingest normal multi series data with 10 partitions.  Should have 10 partitions.
    val data = records(dataset1, linearMultiSeries().take(10))
    memStore.ingest(dataset1.ref, 0, data)

    memStore.commitIndexForTesting(dataset1.ref)

    memStore.numPartitions(dataset1.ref, 0) shouldEqual 10
    memStore.indexValues(dataset1.ref, 0, "series").toSeq should have length (10)

    // Don't mark any partitions for eviction
    // Now, ingest 23 partitions.  Last two partitions cannot be added.  Check numpartitions, stats, index
    val data2 = records(dataset1, linearMultiSeries(numSeries = 23).take(23))
    memStore.ingest(dataset1.ref, 0, data2)

    memStore.getShardE(dataset1.ref, 0).addPartitionsDisabled() shouldEqual true
    memStore.numPartitions(dataset1.ref, 0) shouldEqual 21   // due to the way the eviction policy works
    memStore.getShardE(dataset1.ref, 0).evictionWatermark shouldEqual 0

    // Check partitions are now 0 to 20, 21/22 did not get added
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val parts = memStore.scanPartitions(dataset1, Seq(0, 1), FilteredPartitionScan(split))
                        .toListL.runAsync
                        .futureValue
                        .asInstanceOf[Seq[TimeSeriesPartition]]
    parts.map(_.partID).toSet shouldEqual (0 to 20).toSet
  }
}
