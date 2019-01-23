package filodb.core.store

import com.typesafe.config.ConfigFactory
import monix.reactive.Observable
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, FlushStream, TimeSeriesMemStore}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.format.ZeroCopyUTF8String._

// TODO: figure out what to do with this..  most of the tests are really irrelevant
trait ColumnStoreSpec extends FlatSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import NamesTestData._
  import TestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  implicit val s = monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  def colStore: ColumnStore
  def metaStore: MetaStore
  val policy = new FixedMaxPartitionsEvictionPolicy(100)   // Since 99 GDELT rows, this will never evict
  val memStore = new TimeSeriesMemStore(config, colStore, metaStore, Some(policy))

  val datasetDb2 = dataset.copy(database = Some("unittest2"))

  // First create the tables in C*
  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore.initialize().futureValue
    colStore.initialize(dataset.ref).futureValue
    colStore.initialize(GdeltTestData.dataset2.ref).futureValue
    colStore.initialize(datasetDb2.ref).futureValue
  }

  before {
    colStore.truncate(dataset.ref).futureValue
    colStore.truncate(GdeltTestData.dataset2.ref).futureValue
    colStore.truncate(datasetDb2.ref).futureValue
    memStore.reset()
    metaStore.clearAllData()
  }

  val partScan = SinglePartitionScan(defaultPartKey, 0)

  implicit val keyType = SingleKeyTypes.LongKeyType

  // NOTE: The test below purposefully does not use any of the read APIs so that if only the read code
  // breaks, this test can independently test for write failures
  "write" should "return NotApplied and not write if no ChunkSets" in {
    whenReady(colStore.write(dataset, Observable.empty)) { response =>
      response should equal (NotApplied)
    }
  }

  it should "append new rows successfully" ignore {
    whenReady(colStore.write(dataset, chunkSetStream(names take 3))) { response =>
      response should equal (Success)
    }

    // last 3 rows, should get appended to first 3 in same partition
    whenReady(colStore.write(dataset, chunkSetStream(names drop 3))) { response =>
      response should equal (Success)
    }

    val rows = memStore.scanRows(dataset, Seq(0, 1, 2), partScan)
                       .map(r => (r.getLong(2), r.filoUTF8String(0))).toSeq
    rows.map(_._1) should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    rows.map(_._2) should equal (utf8FirstNames)
  }

  "scanRows SinglePartitionScan" should "return no chunksets if cannot find chunk (SinglePartitionRangeScan)" in {
    colStore.write(dataset, chunkSetStream()).futureValue should equal (Success)

    // partition exists but no chunks in range > 1000, this should find nothing
    val noChunkScan = RowKeyChunkScan(BinaryRecord(dataset, Seq(1000L)),
                                      BinaryRecord(dataset, Seq(2000L)))
    val parts = colStore.readRawPartitions(dataset, Seq(0, 1, 2), partScan, noChunkScan).toListL.runAsync.futureValue
    parts should have length (1)
    parts.head.chunkSets should have length (0)
  }

  it should "return empty iterator if cannot find partition or version" in {
    // Don't write any data
    colStore.readRawPartitions(dataset, Seq(0, 1, 2), partScan)
            .toListL.runAsync.futureValue should have length (0)
  }

  "scanRows" should "read back rows that were written" ignore {
    whenReady(colStore.write(dataset, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val rowIt = memStore.scanRows(dataset, Seq(0, 1, 2), FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))

    // check that can read from same partition again
    val rowIt2 = memStore.scanRows(dataset, Seq(2), FilteredPartitionScan(paramSet.head))
    rowIt2.map(_.getLong(0)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
  }

  it should "read back rows written in another database" ignore {
    whenReady(colStore.write(datasetDb2, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val rowIt = memStore.scanRows(datasetDb2, Seq(0, 1, 2), FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))

    // Check that original keyspace/database has no data
    memStore.scanRows(dataset, Seq(0), partScan).toSeq should have length (0)
  }

  it should "read back rows written with multi-column row keys" ignore {
    import GdeltTestData._
    val stream = toChunkSetStream(dataset2, partBuilder2.addFromObjects(197901), dataRows(dataset2))
    colStore.write(dataset2, stream).futureValue should equal (Success)

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val rowIt = memStore.scanRows(dataset2, dataset2.colIDs("NumArticles").get,
                                  FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getInt(0)).sum should equal (492)
  }

  // TODO: FilteredPartitionScan() for ColumnStores does not work without an index right now
  ignore should "filter rows written with single partition key" in {
    import GdeltTestData._
    memStore.setup(dataset2, 0, TestData.storeConf)
    val stream = Observable.now(records(dataset2))
    // Force flush of all groups at end
    memStore.ingestStream(dataset2.ref, 0, stream ++ FlushStream.allGroups(4), s, 86400).futureValue

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val filter = ColumnFilter("MonthYear", Filter.Equals(197902))
    val method = FilteredPartitionScan(paramSet.head, Seq(filter))
    val rowIt = memStore.scanRows(dataset2, dataset2.colIDs("NumArticles").get, method)
    rowIt.map(_.getInt(0)).sum should equal (22)
  }

  // TODO: redo this test for several reasons.  First, having to sort by a non-time row key is not realistic anymore.
  // Second switch to new BR2 format.
  ignore should "range scan by row keys and filter rows with single partition key" in {
    import GdeltTestData._
    // Requirement: Must ingest 50 rows per chunk
    // val partsRows = getRowsByPartKey(dataset2)
    // partsRows.foreach { case (part, rows) =>
    //   rows.grouped(50).foreach { rowset =>
    //     val sorted = rowset.sortBy(r => (r.getString(3), r.getInt(0)))
    //     val stream = toChunkSetStream(dataset2, part, sorted, 50)
    //     colStore.write(dataset2, stream).futureValue should equal (Success)
    //   }
    // }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    // First 50 rows have no blank Actor2Code's.  We range scan on ("", 50) to ("", 99), which
    // does exist in second 50 rows in both partitions 197901 and 197902.
    // TODO: switch this to EventID, Actor2Code scan.
    val method1 = FilteredPartitionScan(paramSet.head)
    val rowRange = RowKeyChunkScan(BinaryRecord(dataset2, Seq("", 50)),
                                   BinaryRecord(dataset2, Seq("", 99)))
    val rows = memStore.scanRows(dataset2, dataset2.colIDs("GLOBALEVENTID", "MonthYear").get,
                                 method1, rowRange)
                       .map(r => (r.getInt(0), r.getInt(1))).toList
    rows.length should equal (49)
    rows.map(_._2).toSet should equal (Set(197901, 197902))
    rows.map(_._1).min should equal (50)

    // Ask for only partition 197902 and row keys that don't exist, there should be no rows
    val filter2 = ColumnFilter("MonthYear", Filter.Equals(197902))
    val method2 = FilteredPartitionScan(paramSet.head, Seq(filter2))
    val rowRange2 = RowKeyChunkScan(BinaryRecord(dataset2, Seq("", 0)),
                                    BinaryRecord(dataset2, Seq("", 2)))
    val rowIter2 = memStore.scanRows(dataset2, Seq(0), method2, rowRange2)
    rowIter2.toSeq.length should equal (0)

    // Should be able to filter chunks by just the first rowkey column. First V is id=51
    val rowRange3 = RowKeyChunkScan(BinaryRecord(dataset2, Seq("V")),
                                    BinaryRecord(dataset2, Seq("Z")))
    val rowIter3 = memStore.scanRows(dataset2, Seq(0), method1, rowRange3)
    rowIter3.length should equal (41)
  }

  // TODO: FilteredPartitionScan() for ColumnStores does not work without an index right now
  ignore should "filter rows written with multiple column partition keys" in {
    import GdeltTestData._
    memStore.setup(dataset3, 0, TestData.storeConf)
    val stream = Observable.now(records(dataset3))
    // Force flush of all groups at end
    memStore.ingestStream(dataset3.ref, 0, stream ++ FlushStream.allGroups(4), s, 86400).futureValue

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    // Test 1:  IN query on first column only
    val filter1 = ColumnFilter("Actor2Code", Filter.In(Set("JPN".utf8, "KHM".utf8)))
    val method1 = FilteredPartitionScan(paramSet.head, Seq(filter1))

    val readSegs1 = colStore.stats.readPartitions
    val rows = memStore.scanRows(dataset3, dataset3.colIDs("NumArticles", "Actor2Code").get, method1).toSeq
    rows.map(_.getInt(0)).sum should equal (30)
    rows.map(_.filoUTF8String(1)).toSet should equal (Set("JPN".utf8, "KHM".utf8))
    (colStore.stats.readPartitions - readSegs1) should equal (2)

    // Test 2: = filter on both partition columns
    val filters = Seq(ColumnFilter("Actor2Code", Filter.Equals("JPN".utf8)),
                      ColumnFilter("Year",       Filter.Equals(1979)))
    val method2 = FilteredPartitionScan(paramSet.head, filters)
    val rowIt = memStore.scanRows(dataset3, dataset3.colIDs("NumArticles").get, method2)
    rowIt.map(_.getInt(0)).sum should equal (10)
  }

  // "rangeVectors api" should "return Range Vectors for given filter and read all rows" in {
  ignore should "return Range Vectors for given filter and read all rows" in {
    import GdeltTestData._
    memStore.setup(dataset2, 0, TestData.storeConf)
    val stream = Observable.now(records(dataset2))
    // Force flush of all groups at end
    memStore.ingestStream(dataset2.ref, 0, stream ++ FlushStream.allGroups(4), s, 86400).futureValue

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val filter = ColumnFilter("MonthYear", Filter.Equals(197902))
    val method = FilteredPartitionScan(paramSet.head, Seq(filter))
    val rangeVectorObs = memStore.rangeVectors(dataset2, dataset2.colIDs("NumArticles").get,
                                               method, AllChunkScan)
    val rangeVectors = rangeVectorObs.toListL.runAsync.futureValue

    rangeVectors should have length (1)
    rangeVectors.head.key.labelValues.head._1.asNewString shouldEqual "MonthYear"
    rangeVectors.head.key.labelValues.head._2.asNewString shouldEqual "197902"
    rangeVectors.head.rows.map(_.getInt(0)).sum should equal (22)
    rangeVectors.head.key.sourceShards shouldEqual Seq(0)
  }
}