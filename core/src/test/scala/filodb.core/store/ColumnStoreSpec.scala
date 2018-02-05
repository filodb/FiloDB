package filodb.core.store

import com.typesafe.config.ConfigFactory
import monix.reactive.Observable
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.format.ZeroCopyUTF8String._

trait ColumnStoreSpec extends FlatSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import NamesTestData._
  import TestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  def colStore: ColumnStore

  val datasetDb2 = dataset.copy(database = Some("unittest2"))

  // First create the tables in C*
  override def beforeAll(): Unit = {
    super.beforeAll()
    colStore.initialize(dataset.ref).futureValue
    colStore.initialize(GdeltTestData.dataset2.ref).futureValue
    colStore.initialize(datasetDb2.ref).futureValue
  }

  before {
    colStore.truncate(dataset.ref).futureValue
    colStore.truncate(GdeltTestData.dataset2.ref).futureValue
    colStore.truncate(datasetDb2.ref).futureValue
  }

  val partScan = SinglePartitionScan(defaultPartKey)

  implicit val keyType = SingleKeyTypes.LongKeyType

  // NOTE: The test below purposefully does not use any of the read APIs so that if only the read code
  // breaks, this test can independently test for write failures
  "write" should "NOOP if the segment is empty" in {
    whenReady(colStore.write(dataset, Observable.empty)) { response =>
      response should equal (NotApplied)
    }
  }

  it should "append new rows successfully" in {
    whenReady(colStore.write(dataset, chunkSetStream(names take 3))) { response =>
      response should equal (Success)
    }

    // last 3 rows, should get appended to first 3 in same partition
    whenReady(colStore.write(dataset, chunkSetStream(names drop 3))) { response =>
      response should equal (Success)
    }

    val rows = colStore.scanRows(dataset, Seq(0, 1, 2), partScan)
                       .map(r => (r.getLong(2), r.filoUTF8String(0))).toSeq
    rows.map(_._1) should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    rows.map(_._2) should equal (utf8FirstNames)
  }

  ignore should "replace rows to a previous chunk successfully" in {
    val sortedNames = names.sortBy(_._3.get)
    whenReady(colStore.write(dataset, chunkSetStream(sortedNames drop 1))) { response =>
      response should equal (Success)
    }

    // NOTE: previous row key chunks are not currently cached

    // Writing 2nd chunkset, repeat 1 row and add another row.
    val sortedAltNames = altNames.sortBy(_._3.get)
    whenReady(colStore.write(dataset, chunkSetStream(sortedAltNames take 2))) { response =>
      response should equal (Success)
    }

    // First row (names(1)) should be skipped, and last rows should be from second chunk (chunkID order)
    val rows = colStore.scanRows(dataset, Seq(0, 1, 2), partScan)
                       .map(r => (r.getLong(2), r.filoUTF8String(0))).toSeq
    rows.map(_._1) should equal (Seq(28L, 29L, 39L, 40L, 24L, 25L))
    rows.map(_._2) should equal (sortedUtf8Firsts.drop(2) ++ Seq("Stacy".utf8, "Amari".utf8))
  }

  "scanChunks SinglePartitionScan" should "read chunks back that were written" in {
    whenReady(colStore.write(dataset, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val readSegs1 = colStore.stats.readPartitions
    val chunks = colStore.scanChunks(dataset, Seq(0, 1, 2), partScan).toSeq
    chunks should have length (1)
    chunks.head.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    (colStore.stats.readPartitions - readSegs1) should equal (1)
  }

  it should "return empty iterator if cannot find chunk (SinglePartitionRangeScan)" in {
    colStore.write(dataset, chunkSetStream()).futureValue should equal (Success)

    // partition exists but no chunks in range > 1000, this should find nothing
    val noChunkScan = RowKeyChunkScan(BinaryRecord(dataset, Seq(1000L)),
                                      BinaryRecord(dataset, Seq(2000L)))
    colStore.scanChunks(dataset, Seq(0, 1, 2), partScan, noChunkScan).toSeq should have length (0)
  }

  it should "return empty iterator if cannot find partition or version" in {
    // Don't write any data
    colStore.scanChunks(dataset, Seq(0, 1, 2), partScan).toSeq should have length (0)
  }

  "scanRows" should "read back rows that were written" in {
    whenReady(colStore.write(dataset, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val rowIt = colStore.scanRows(dataset, Seq(0, 1, 2), FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))

    // check that can read from same partition again
    val rowIt2 = colStore.scanRows(dataset, Seq(2), FilteredPartitionScan(paramSet.head))
    rowIt2.map(_.getLong(0)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
  }

  it should "read back rows written in another database" in {
    whenReady(colStore.write(datasetDb2, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val rowIt = colStore.scanRows(datasetDb2, Seq(0, 1, 2), FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))

    // Check that original keyspace/database has no data
    colStore.scanRows(dataset, Seq(0), partScan).toSeq should have length (0)
  }

  it should "read back rows written with multi-column row keys" in {
    import GdeltTestData._
    val stream = toChunkSetStream(dataset2, dataset2.partKey(197901), dataRows(dataset2))
    colStore.write(dataset2, stream).futureValue should equal (Success)

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val rowIt = colStore.scanRows(dataset2, dataset2.colIDs("NumArticles").get,
                                  FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getInt(0)).sum should equal (492)
  }

  it should "filter rows written with single partition key" in {
    import GdeltTestData._
    val streams = getStreamsByPartKey(dataset2)
    streams.foreach { s =>
      colStore.write(dataset2, s).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val filter = ColumnFilter("MonthYear", Filter.Equals(197902))
    val method = FilteredPartitionScan(paramSet.head, Seq(filter))
    val rowIt = colStore.scanRows(dataset2, dataset2.colIDs("NumArticles").get, method)
    rowIt.map(_.getInt(0)).sum should equal (22)
  }

  it should "range scan by row keys and filter rows with single partition key" in {
    import GdeltTestData._
    // Requirement: Must ingest 50 rows per chunk
    val partsRows = getRowsByPartKey(dataset2)
    partsRows.foreach { case (part, rows) =>
      rows.grouped(50).foreach { rowset =>
        val sorted = rowset.sortBy(r => (r.getString(3), r.getInt(0)))
        val stream = toChunkSetStream(dataset2, part, sorted, 50)
        colStore.write(dataset2, stream).futureValue should equal (Success)
      }
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    // First 50 rows have no blank Actor2Code's.  We range scan on ("", 50) to ("", 99), which
    // does exist in second 50 rows in both partitions 197901 and 197902.
    val method1 = FilteredPartitionScan(paramSet.head)
    val rowRange = RowKeyChunkScan(BinaryRecord(dataset2, Seq("", 50)),
                                   BinaryRecord(dataset2, Seq("", 99)))
    val rows = colStore.scanRows(dataset2, dataset2.colIDs("GLOBALEVENTID", "MonthYear").get,
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
    val rowIter2 = colStore.scanRows(dataset2, Seq(0), method2, rowRange2)
    rowIter2.toSeq.length should equal (0)

    // Should be able to filter chunks by just the first rowkey column. First V is id=51
    val rowRange3 = RowKeyChunkScan(BinaryRecord(dataset2, Seq("V")),
                                    BinaryRecord(dataset2, Seq("Z")))
    val rowIter3 = colStore.scanRows(dataset2, Seq(0), method1, rowRange3)
    rowIter3.length should equal (41)
  }

  it should "range scan by row keys (SinglePartitionRowKeyScan)" in {
    import GdeltTestData._
    val partsRows = getRowsByPartKey(dataset4)
    partsRows.foreach { case (part, rows) =>
      rows.grouped(10).foreach { rowset =>
        val sorted = rowset.sortBy(r => (r.filoUTF8String(3), r.getInt(0)))
        val stream = toChunkSetStream(dataset4, part, sorted, 10)
        colStore.write(dataset4, stream).futureValue should equal (Success)
      }
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    // This should span multiple chunks and properly test comparison of binary row keys
    // Matching chunks (each one 10 lines):
    // AFR/0-GOV/9, GOV/10-IRN/19, /53-ZMB/57, /65-VATGOV/61, /70-KHM/73, CHL/88-ITA/87, /91-GOV/90
    val startKey = BinaryRecord(dataset4, Seq("F", -1))
    val endKey = BinaryRecord(dataset4, Seq("H", 100))
    val method = SinglePartitionScan(dataset4.partKey(1979))
    val range1 = RowKeyChunkScan(startKey, endKey)
    val rowIt = colStore.scanRows(dataset4, dataset4.colIDs("GLOBALEVENTID", "MonthYear").get, method, range1)
    val rows = rowIt.map(r => (r.getInt(0), r.getInt(1))).toList
    rows.length should equal (69)   // 7 chunks, last one only has 9 rows
    rows.map(_._2).toSet should equal (Set(197901, 197902))
    // Verify every chunk that should be there is actually there
    rows.map(_._1).toSet.intersect(Set(0, 10, 20, 30, 40, 53, 65, 70, 88, 91)) should equal (
                                   Set(0, 10, 53, 65, 70, 88, 91))

    val emptyScan = RowKeyChunkScan(endKey, startKey)
    colStore.scanRows(dataset4, Seq(0), method, emptyScan).length should equal (0)

    val key0 = BinaryRecord(dataset4, Seq("a", -1))  // All the Actor2Codes are uppercase, last one is Z
    val key1 = BinaryRecord(dataset4, Seq("b", 100))
    val emptyScan2 = RowKeyChunkScan(key0, key1)
    colStore.scanRows(dataset4, Seq(0), method, emptyScan2).length should equal (0)
  }

  it should "filter rows written with multiple column partition keys" in {
    import GdeltTestData._
    val streams = getStreamsByPartKey(dataset3)
    streams.foreach { case s =>
      colStore.write(dataset3, s).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    // Test 1:  IN query on first column only
    val filter1 = ColumnFilter("Actor2Code", Filter.In(Set("JPN".utf8, "KHM".utf8)))
    val method1 = FilteredPartitionScan(paramSet.head, Seq(filter1))

    val readSegs1 = colStore.stats.readPartitions
    val rows = colStore.scanRows(dataset3, dataset3.colIDs("NumArticles", "Actor2Code").get, method1).toSeq
    rows.map(_.getInt(0)).sum should equal (30)
    rows.map(_.filoUTF8String(1)).toSet should equal (Set("JPN".utf8, "KHM".utf8))
    (colStore.stats.readPartitions - readSegs1) should equal (2)

    // Test 2: = filter on both partition columns
    val filters = Seq(ColumnFilter("Actor2Code", Filter.Equals("JPN".utf8)),
                      ColumnFilter("Year",       Filter.Equals(1979)))
    val method2 = FilteredPartitionScan(paramSet.head, filters)
    val rowIt = colStore.scanRows(dataset3, dataset3.colIDs("NumArticles").get, method2)
    rowIt.map(_.getInt(0)).sum should equal (10)
  }

  "partition list api" should "allow reading and writing partition list for shard" in {
    import monix.execution.Scheduler.Implicits.global

    val writtenKeys = Range(0, 1024).map(i => dataset.partKey(i))
    val additions = colStore.addPartitions(dataset, writtenKeys.toIterator, 0).futureValue
    additions shouldBe Success
    val readKeys = colStore.scanPartitionKeys(dataset, 0).toListL.runAsync.futureValue
    writtenKeys.toSet shouldEqual readKeys.toSet
  }

  "partitionChunks" should "return PartitionChunks with partition filter and read all rows" in {
    import GdeltTestData._
    import monix.execution.Scheduler.Implicits.global

    val streams = getStreamsByPartKey(dataset2)
    streams.foreach { s =>
      colStore.write(dataset2, s).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val filter = ColumnFilter("MonthYear", Filter.Equals(197902))
    val method = FilteredPartitionScan(paramSet.head, Seq(filter))
    val partVectObs = colStore.partitionVectors(dataset2, dataset2.colIDs("NumArticles").get, method)
    val partVectors = partVectObs.toListL.runAsync.futureValue

    partVectors should have length (1)
    partVectors.head.info.get.partKey shouldEqual dataset2.partKey(197902)
    partVectors.head.allRowsIterator.map(_.getInt(0)).sum should equal (22)
  }
}