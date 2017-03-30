package filodb.core.store

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import org.velvia.filo.ZeroCopyUTF8String._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.postfixOps

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.{Dataset, Column, DataColumn, Projection, RichProjection}
import filodb.core.Types
import filodb.core.query.KeyFilter

import org.scalatest.{FlatSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

trait ColumnStoreSpec extends FlatSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import NamesTestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  def colStore: ColumnStore with ColumnStoreScanner

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
    colStore.reset()
  }

  val partScan = SinglePartitionScan(defaultPartKey)

  implicit val keyType = SingleKeyTypes.LongKeyType

  protected def ourState(proj: RichProjection = projection): SegmentState =
    ColumnStoreSegmentState(proj, schema, 0, colStore)(SegmentInfo(defaultPartKey, 0).basedOn(proj))

  protected def ourState(segment: ChunkSetSegment): SegmentState =
    ColumnStoreSegmentState(segment.projection, segment.projection.columns, 0, colStore)(segment.segInfo)


  // NOTE: The test below purposefully does not use any of the read APIs so that if only the read code
  // breaks, this test can independently test for write failures
  "appendSegment" should "NOOP if the segment is empty" in {
    val state = ourState()
    val segment = getWriterSegment()
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (NotApplied)
    }
  }

  it should "append new rows successfully" in {
    val state = ourState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names take 3))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    // Writing segment2, last 3 rows, should get appended to first 3 in same partition
    val segment2 = getWriterSegment()
    segment2.addChunkSet(state, mapper(names drop 3))
    whenReady(colStore.appendSegment(projection, segment2, 0)) { response =>
      response should equal (Success)
    }

    val rows = colStore.scanRows(projection, schema, 0, partScan)
                       .map(r => (r.getLong(2), r.filoUTF8String(0))).toSeq
    rows.map(_._1) should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    rows.map(_._2) should equal (utf8FirstNames)
  }

  it should "replace rows to a previous chunk successfully" in {
    val state = ourState()
    val segment = getWriterSegment()
    val sortedNames = names.sortBy(_._3.get)
    val sortedAltNames = altNames.sortBy(_._3.get)
    segment.addChunkSet(state, mapper(sortedNames drop 1))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    // NOTE: previous row key chunks are not currently cached

    // Writing segment2, repeat 1 row and add another row.  Should read orig segment from disk.
    val segment2 = getWriterSegment()
    segment2.addChunkSet(state, mapper(sortedAltNames take 2))
    whenReady(colStore.appendSegment(projection, segment2, 0)) { response =>
      response should equal (Success)
    }

    // First row (names(1)) should be skipped, and last rows should be from second chunk (chunkID order)
    val rows = colStore.scanRows(projection, schema, 0, partScan)
                       .map(r => (r.getLong(2), r.filoUTF8String(0))).toSeq
    rows.map(_._1) should equal (Seq(28L, 29L, 39L, 40L, 24L, 25L))
    rows.map(_._2) should equal (sortedUtf8Firsts.drop(2) ++ Seq("Stacy".utf8, "Amari".utf8))
  }

  // The first row key column is a computed column, so this test also ensures that retrieving the original
  // source columns works.
  it should "replace rows with multi row keys to an uncached segment" in {
    import GdeltTestData._
    val segmentsRows = getSegments(projection2.partKey(197901))
    val segs = segmentsRows.map { case (seg, lines) =>
      val state = ourState(seg)
      seg.addChunkSet(state, lines.sortBy(_.getString(4)))
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
      seg
    }

    // Simulate removal of segment state from cache by creating a new state reading from column store
    val segment2 = new ChunkSetSegment(projection2, segmentsRows.head._1.segInfo)
    segment2.addChunkSet(ourState(segs.head), altReaders.take(3))
    colStore.appendSegment(projection2, segment2, 0).futureValue should equal (Success)

    val method = SinglePartitionScan(projection2.partKey(197901))
    val ints = colStore.scanRows(projection2, schema, 0, method).map(_.getInt(6)).toSeq
    ints should have length 99
    ints.sum should equal (492 - 12)    // 492 is original sum of #Articles all lines, minus diff in alt lines
  }

  "scanChunks SinglePartitionScan" should "read chunks back that were written" in {
    val state = ourState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val readSegs1 = colStore.stats.readPartitions
    val chunks = colStore.scanChunks(projection, schema, 0, partScan).toSeq
    chunks should have length (1)
    chunks.head.info should equal (segment.chunkSets.head.info)
    chunks.head.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    (colStore.stats.readPartitions - readSegs1) should equal (1)
  }

  it should "return empty iterator if cannot find chunk (SinglePartitionRangeScan)" in {
    val state = ourState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    colStore.appendSegment(projection, segment, 0).futureValue should equal (Success)

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
  it should "return empty chunks if cannot find some columns" in {
    val state = ourState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val fakeCol = DataColumn(5, "notACol", dataset.name, 0, Column.ColumnType.StringColumn)
    val columns = Seq(fakeCol, schema(2))   // fakeCol, age
    val chunks = colStore.scanChunks(projection, columns, 0, partScan).toSeq
    chunks should have length (1)
    // Should still be able to pull out the valid column
    chunks.head.rowIterator().map(_.getLong(1)).toSeq should equal (names.map(_._3.get))
    // Invalid column translates to empty vector, with no values available
    chunks.head.rowIterator().filter(_.notNull(0)).toSeq should equal (Nil)
  }

  "scanRows" should "read back rows that were written" in {
    val state = ourState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
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
    val state = ourState(projectionDb2)
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    whenReady(colStore.appendSegment(projectionDb2, segment, 0)) { response =>
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
    val segmentsRows = getSegments(projection2.partKey(197901))
    segmentsRows.foreach { case (seg, rows) =>
      seg.addChunkSet(ourState(seg), rows)
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val rowIt = colStore.scanRows(projection2, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getInt(6)).sum should equal (492)
  }

  it should "filter rows written with single partition key" in {
    import GdeltTestData._
    val segmentsRows = getSegmentsByPartKey(projection2)
    segmentsRows.foreach { case (seg, rows) =>
      seg.addChunkSet(ourState(seg), rows)
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val filterFunc = KeyFilter.makePartitionFilterFunc(projection2, KeyFilter.equalsFunc(197902))
    val method = FilteredPartitionScan(paramSet.head, filterFunc)
    val rowIt = colStore.scanRows(projection2, schema, 0, method)
    rowIt.map(_.getInt(6)).sum should equal (22)
  }

  it should "range scan by row keys and filter rows with single partition key" in {
    import GdeltTestData._
    // Requirement: Must ingest 50 rows per chunk
    val segmentsRows = getSegmentsByPartKey(projection2)
    segmentsRows.foreach { case (seg, rows) =>
      rows.grouped(50).foreach { rowset =>
        val sorted = rowset.sortBy(r => (r.getString(4), r.getInt(0)))
        seg.addChunkSet(ourState(seg), sorted)
      }
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
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
    val filterFunc = KeyFilter.equalsFunc(197902)
    val method2 = FilteredPartitionScan(paramSet.head, filterFunc)
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
    val segmentsRows = getSegmentsByPartKey(projection4)
    segmentsRows.foreach { case (seg, rows) =>
      val segState = ourState(seg)
      // TODO: move grouping and sorting to TestData
      rows.grouped(10).foreach { rowGroup =>
        val sorted = rowGroup.sortBy(r => (r.filoUTF8String(4), r.getInt(0)))
        seg.addChunkSet(segState, sorted)
      }
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
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
    val segmentsRows = getSegmentsByPartKey(projection3)
    segmentsRows.foreach { case (seg, rows) =>
      seg.addChunkSet(ourState(seg), rows)
      colStore.appendSegment(projection3, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    // Test 1:  IN query on first column only
    val inFilter = KeyFilter.inFunc(Set("JPN".utf8, "KHM".utf8))
    val filter1 = KeyFilter.makePartitionFilterFunc(projection3, inFilter)
    val method1 = FilteredPartitionScan(paramSet.head, filter1)

    val readSegs1 = colStore.stats.readPartitions
    val rows = colStore.scanRows(projection3, schema, 0, method1).toSeq
    rows.map(_.getInt(6)).sum should equal (30)
    rows.map(_.filoUTF8String(4)).toSet should equal (Set("JPN".utf8, "KHM".utf8))
    (colStore.stats.readPartitions - readSegs1) should equal (2)

    // Test 2: = filter on both partition columns
    val eqFilters = Array(KeyFilter.equalsFunc("JPN".utf8),
                          KeyFilter.equalsFunc(1979))
    val filter2 = KeyFilter.makePartitionFilterFunc(projection3, Array(0, 1), eqFilters)
    val method2 = FilteredPartitionScan(paramSet.head, filter2)
    val rowIt = colStore.scanRows(projection3, schema, 0, method2)
    rowIt.map(_.getInt(6)).sum should equal (10)
  }
}