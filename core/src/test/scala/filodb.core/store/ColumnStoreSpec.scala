package filodb.core.store

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import monix.reactive.Observable
import org.velvia.filo.ZeroCopyUTF8String._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.postfixOps

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.{Dataset, Column, DataColumn, Projection, RichProjection}
import filodb.core.Types
import filodb.core.query.{KeyFilter, Filter, ColumnFilter}

import org.scalatest.{FlatSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

trait ColumnStoreSpec extends FlatSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import NamesTestData._
  import TestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  def colStore: ColumnStore

  val projectionDb2 = projection.withDatabase("unittest2")

  // First create the tables in C*
  override def beforeAll() {
    super.beforeAll()
    colStore.initializeProjection(dataset.projections.head).futureValue
    colStore.initializeProjection(GdeltTestData.dataset2.projections.head).futureValue
    colStore.initializeProjection(projectionDb2.projection).futureValue
  }

  before {
    colStore.clearProjectionData(dataset.projections.head).futureValue
    colStore.clearProjectionData(GdeltTestData.dataset2.projections.head).futureValue
    colStore.clearProjectionData(projectionDb2.projection).futureValue
  }

  val partScan = SinglePartitionScan(defaultPartKey)

  implicit val keyType = SingleKeyTypes.LongKeyType

  // NOTE: The test below purposefully does not use any of the read APIs so that if only the read code
  // breaks, this test can independently test for write failures
  "write" should "NOOP if the segment is empty" in {
    whenReady(colStore.write(projection, 0, Observable.empty)) { response =>
      response should equal (NotApplied)
    }
  }

  it should "append new rows successfully" in {
    whenReady(colStore.write(projection, 0, chunkSetStream(names take 3))) { response =>
      response should equal (Success)
    }

    // last 3 rows, should get appended to first 3 in same partition
    whenReady(colStore.write(projection, 0, chunkSetStream(names drop 3))) { response =>
      response should equal (Success)
    }

    val rows = colStore.scanRows(projection, schema, 0, partScan)
                       .map(r => (r.getLong(2), r.filoUTF8String(0))).toSeq
    rows.map(_._1) should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    rows.map(_._2) should equal (utf8FirstNames)
  }

  ignore should "replace rows to a previous chunk successfully" in {
    val sortedNames = names.sortBy(_._3.get)
    whenReady(colStore.write(projection, 0, chunkSetStream(sortedNames drop 1))) { response =>
      response should equal (Success)
    }

    // NOTE: previous row key chunks are not currently cached

    // Writing 2nd chunkset, repeat 1 row and add another row.
    val sortedAltNames = altNames.sortBy(_._3.get)
    whenReady(colStore.write(projection, 0, chunkSetStream(sortedAltNames take 2))) { response =>
      response should equal (Success)
    }

    // First row (names(1)) should be skipped, and last rows should be from second chunk (chunkID order)
    val rows = colStore.scanRows(projection, schema, 0, partScan)
                       .map(r => (r.getLong(2), r.filoUTF8String(0))).toSeq
    rows.map(_._1) should equal (Seq(28L, 29L, 39L, 40L, 24L, 25L))
    rows.map(_._2) should equal (sortedUtf8Firsts.drop(2) ++ Seq("Stacy".utf8, "Amari".utf8))
  }

  "scanChunks SinglePartitionScan" should "read chunks back that were written" in {
    whenReady(colStore.write(projection, 0, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val readSegs1 = colStore.stats.readPartitions
    val chunks = colStore.scanChunks(projection, schema, 0, partScan).toSeq
    chunks should have length (1)
    chunks.head.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    (colStore.stats.readPartitions - readSegs1) should equal (1)
  }

  it should "return empty iterator if cannot find chunk (SinglePartitionRangeScan)" in {
    colStore.write(projection, 0, chunkSetStream()).futureValue should equal (Success)

    // partition exists but no chunks in range > 1000, this should find nothing
    val noChunkScan = RowKeyChunkScan(BinaryRecord(projection, Seq(1000L)),
                                      BinaryRecord(projection, Seq(2000L)))
    colStore.scanChunks(projection, schema, 0, partScan, noChunkScan).toSeq should have length (0)
  }

  it should "return empty iterator if cannot find partition or version" in {
    // Don't write any data
    colStore.scanChunks(projection, schema, 0, partScan).toSeq should have length (0)
  }

  // In the real world not every column will be ingested all the time, so there might be blank chunks
  // and not every column requested will return
  it should "throw an exception if cannot find some columns" in {
    whenReady(colStore.write(projection, 0, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val fakeCol = DataColumn(5, "notACol", dataset.name, 0, Column.ColumnType.StringColumn)
    val columns = Seq(fakeCol, schema(2))   // fakeCol, age
    intercept[IllegalArgumentException] {
      colStore.scanChunks(projection, columns, 0, partScan).toSeq
    }
  }

  "scanRows" should "read back rows that were written" in {
    whenReady(colStore.write(projection, 0, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val rowIt = colStore.scanRows(projection, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))

    // check that can read from same partition again
    val rowIt2 = colStore.scanRows(projection, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIt2.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
  }

  it should "read back rows written in another database" in {
    whenReady(colStore.write(projectionDb2, 0, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val rowIt = colStore.scanRows(projectionDb2, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))

    // Check that original keyspace/database has no data
    colStore.scanRows(projection, schema, 0, partScan).toSeq should have length (0)
  }

  it should "read back rows written with multi-column row keys" in {
    import GdeltTestData._
    val stream = toChunkSetStream(projection2, projection2.partKey(197901), readers)
    colStore.write(projection2, 0, stream).futureValue should equal (Success)

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val rowIt = colStore.scanRows(projection2, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getInt(6)).sum should equal (492)
  }

  it should "filter rows written with single partition key" in {
    import GdeltTestData._
    val streams = getStreamsByPartKey(projection2)
    streams.foreach { s =>
      colStore.write(projection2, 0, s).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val filter = ColumnFilter("MonthYear", Filter.Equals(197902))
    val method = FilteredPartitionScan(paramSet.head, Seq(filter))
    val rowIt = colStore.scanRows(projection2, schema, 0, method)
    rowIt.map(_.getInt(6)).sum should equal (22)
  }

  it should "range scan by row keys and filter rows with single partition key" in {
    import GdeltTestData._
    // Requirement: Must ingest 50 rows per chunk
    val partsRows = getRowsByPartKey(projection2)
    partsRows.foreach { case (part, rows) =>
      rows.grouped(50).foreach { rowset =>
        val sorted = rowset.sortBy(r => (r.getString(4), r.getInt(0)))
        val stream = toChunkSetStream(projection2, part, sorted, 50)
        colStore.write(projection2, 0, stream).futureValue should equal (Success)
      }
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    // First 50 rows have no blank Actor2Code's.  We range scan on ("", 50) to ("", 99), which
    // does exist in second 50 rows in both partitions 197901 and 197902.
    val method1 = FilteredPartitionScan(paramSet.head)
    val rowRange = RowKeyChunkScan(BinaryRecord(projection2, Seq("", 50)),
                                   BinaryRecord(projection2, Seq("", 99)))
    val rows = colStore.scanRows(projection2, schema, 0, method1, rowRange)
                       .map(r => (r.getInt(0), r.getInt(2))).toList
    rows.length should equal (49)
    rows.map(_._2).toSet should equal (Set(197901, 197902))
    rows.map(_._1).min should equal (50)

    // Ask for only partition 197902 and row keys that don't exist, there should be no rows
    val filter2 = ColumnFilter("MonthYear", Filter.Equals(197902))
    val method2 = FilteredPartitionScan(paramSet.head, Seq(filter2))
    val rowRange2 = RowKeyChunkScan(BinaryRecord(projection2, Seq("", 0)),
                                    BinaryRecord(projection2, Seq("", 2)))
    val rowIter2 = colStore.scanRows(projection2, schema, 0, method2, rowRange2)
    rowIter2.toSeq.length should equal (0)

    // Should be able to filter chunks by just the first rowkey column. First V is id=51
    val rowRange3 = RowKeyChunkScan(BinaryRecord(projection2, Seq("V")),
                                    BinaryRecord(projection2, Seq("Z")))
    val rowIter3 = colStore.scanRows(projection2, schema, 0, method1, rowRange3)
    rowIter3.length should equal (41)
  }

  it should "range scan by row keys (SinglePartitionRowKeyScan)" in {
    import GdeltTestData._
    val partsRows = getRowsByPartKey(projection4)
    partsRows.foreach { case (part, rows) =>
      rows.grouped(10).foreach { rowset =>
        val sorted = rowset.sortBy(r => (r.filoUTF8String(4), r.getInt(0)))
        val stream = toChunkSetStream(projection4, part, sorted, 10)
        colStore.write(projection4, 0, stream).futureValue should equal (Success)
      }
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    // This should span multiple chunks and properly test comparison of binary row keys
    // Matching chunks (each one 10 lines):
    // AFR/0-GOV/9, GOV/10-IRN/19, /53-ZMB/57, /65-VATGOV/61, /70-KHM/73, CHL/88-ITA/87, /91-GOV/90
    val startKey = BinaryRecord(projection4, Seq("F", -1))
    val endKey = BinaryRecord(projection4, Seq("H", 100))
    val method = SinglePartitionScan(projection4.partKey(1979))
    val range1 = RowKeyChunkScan(startKey, endKey)
    val rowIt = colStore.scanRows(projection4, schema, 0, method, range1)
    val rows = rowIt.map(r => (r.getInt(0), r.getInt(2))).toList
    rows.length should equal (69)   // 7 chunks, last one only has 9 rows
    rows.map(_._2).toSet should equal (Set(197901, 197902))
    // Verify every chunk that should be there is actually there
    rows.map(_._1).toSet.intersect(Set(0, 10, 20, 30, 40, 53, 65, 70, 88, 91)) should equal (
                                   Set(0, 10, 53, 65, 70, 88, 91))

    val emptyScan = RowKeyChunkScan(endKey, startKey)
    colStore.scanRows(projection4, schema, 0, method, emptyScan).length should equal (0)

    val key0 = BinaryRecord(projection4, Seq("a", -1))  // All the Actor2Codes are uppercase, last one is Z
    val key1 = BinaryRecord(projection4, Seq("b", 100))
    val emptyScan2 = RowKeyChunkScan(key0, key1)
    colStore.scanRows(projection4, schema, 0, method, emptyScan2).length should equal (0)
  }

  import SingleKeyTypes._

  it should "filter rows written with multiple column partition keys" in {
    import GdeltTestData._
    val streams = getStreamsByPartKey(projection3)
    streams.foreach { case s =>
      colStore.write(projection3, 0, s).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    // Test 1:  IN query on first column only
    val filter1 = ColumnFilter("Actor2Code", Filter.In(Set("JPN".utf8, "KHM".utf8)))
    val method1 = FilteredPartitionScan(paramSet.head, Seq(filter1))

    val readSegs1 = colStore.stats.readPartitions
    val rows = colStore.scanRows(projection3, schema, 0, method1).toSeq
    rows.map(_.getInt(6)).sum should equal (30)
    rows.map(_.filoUTF8String(4)).toSet should equal (Set("JPN".utf8, "KHM".utf8))
    (colStore.stats.readPartitions - readSegs1) should equal (2)

    // Test 2: = filter on both partition columns
    val filters = Seq(ColumnFilter("Actor2Code", Filter.Equals("JPN".utf8)),
                      ColumnFilter("Year",       Filter.Equals(1979)))
    val method2 = FilteredPartitionScan(paramSet.head, filters)
    val rowIt = colStore.scanRows(projection3, schema, 0, method2)
    rowIt.map(_.getInt(6)).sum should equal (10)
  }
}