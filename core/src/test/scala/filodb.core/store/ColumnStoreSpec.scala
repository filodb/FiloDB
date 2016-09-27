package filodb.core.store

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.{Dataset, Column, DataColumn, Projection, RichProjection}
import filodb.core.Types
import filodb.core.query.KeyFilter

import org.scalatest.{FlatSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

trait ColumnStoreSpec extends FlatSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import scala.concurrent.ExecutionContext.Implicits.global
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

  val partScan = SinglePartitionScan("/0")

  implicit val keyType = SingleKeyTypes.LongKeyType

  protected def getState(proj: RichProjection = projection): SegmentState =
    new SegmentState(projection, schema, Nil, colStore, 0, 15 seconds)(
                     SegmentInfo("/0", 0).basedOn(projection))

  // NOTE: The test below purposefully does not use any of the read APIs so that if only the read code
  // breaks, this test can independently test for write failures
  "appendSegment" should "NOOP if the segment is empty" in {
    val state = getState()
    val segment = getWriterSegment()
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (NotApplied)
    }
  }

  it should "append new rows successfully" in {
    val state = getState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names take 3))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    // Writing segment2, last 3 rows, should get appended to first 3 in same segment
    val segment2 = getWriterSegment()
    segment2.addChunkSet(state, mapper(names drop 3))
    whenReady(colStore.appendSegment(projection, segment2, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.scanSegments(projection, schema, 0, partScan)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
      readSeg.rowIterator().map(_.getString(0)).toSeq should equal (firstNames)
    }
  }

  it should "replace rows to a previous chunk successfully" in {
    val state = getState()
    val segment = getWriterSegment()
    val sortedNames = names.sortBy(_._3.get)
    segment.addChunkSet(state, mapper(sortedNames drop 1))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    // NOTE: previous row key chunks are not currently cached

    // Writing segment2, repeat 1 row and add another row.  Should read orig segment from disk.
    val segment2 = getWriterSegment()
    segment2.addChunkSet(state, mapper(sortedNames take 2))
    whenReady(colStore.appendSegment(projection, segment2, 0)) { response =>
      response should equal (Success)
    }

    // First row (names(1)) should be skipped, and last rows should be from second chunk
    whenReady(colStore.scanSegments(projection, schema, 0, partScan)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(28L, 29L, 39L, 40L, 24L, 25L))
      readSeg.rowIterator().map(_.getString(0)).toSeq should equal (sortedFirstNames.drop(2) ++
                                                                    sortedFirstNames.take(2))
    }
  }

  // The first row key column is a computed column, so this test also ensures that retrieving the original
  // source columns works.
  it should "replace rows with multi row keys to an uncached segment" in {
    import GdeltTestData._
    val segmentsStates = getSegments(colStore, 197901.asInstanceOf[projection2.PK])
    segmentsStates.foreach { case (seg, _) =>
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    // TODO: remove cache and force read from column store
    val segment2 = new ChunkSetSegment(projection2, segmentsStates.head._1.segInfo.basedOn(projection2))
    segment2.addChunkSet(segmentsStates.head._2, readers.take(3))
    colStore.appendSegment(projection2, segment2, 0).futureValue should equal (Success)

    whenReady(colStore.scanSegments(projection2, schema, 0, SinglePartitionScan(197901))) { segIter =>
      val segments = segIter.toSeq
      segments should have length (2)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.rowIterator().map(_.getInt(6)).sum should equal (288)
    }
  }

  "readChunks" should "return correct number of chunks in chunkID range" in {
    val state = getState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    colStore.appendSegment(projection, segment, 0).futureValue should equal (Success)

    val chunks1 = colStore.readChunks(datasetRef, 0, Seq("first"),
                                      segment.binaryPartition, segment.segmentId,
                                      (1, 2)).futureValue
    chunks1.head.chunks should equal (Nil)

    val chunks2 = colStore.readChunks(datasetRef, 0, Seq("first"),
                                      segment.binaryPartition, segment.segmentId,
                                      (0, 1)).futureValue
    chunks2.head.chunks should have length (1)
  }

  "scanSegments SinglePartitionScan" should "read segments back that were written" in {
    val state = getState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val readSegs1 = colStore.stats.readSegments
    whenReady(colStore.scanSegments(projection, schema, 0, partScan)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    }
    (colStore.stats.readSegments - readSegs1) should equal (1)
  }

  it should "return empty iterator if cannot find segment (SinglePartitionRangeScan)" in {
    val state = getState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    colStore.appendSegment(projection, segment, 0).futureValue should equal (Success)

    // partition exists but only for segment key 0, this should find nothing
    val rangeScanNoSegment = SinglePartitionRangeScan(KeyRange("/0", 1000, 1000, false))
    whenReady(colStore.scanSegments(projection, schema, 0, rangeScanNoSegment)) { segIter =>
      segIter.toSeq should have length (0)
    }
  }

  it should "return empty iterator if cannot find partition or version" in {
    whenReady(colStore.scanSegments(projection, schema, 0, partScan)) { segIter =>
      segIter.toSeq should have length (0)
    }
  }

  it should "return segment with empty chunks if cannot find columns" in {
    val state = getState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val fakeCol = DataColumn(5, "notACol", dataset.name, 0, Column.ColumnType.StringColumn)
    whenReady(colStore.scanSegments(projection, Seq(fakeCol), 0, partScan)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Nil)
    }
  }

  "scanSegments FilteredPartitionScan" should "read segments back that were written" in {
    val state = getState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    whenReady(colStore.scanSegments(projection, schema, 0, FilteredPartitionScan(paramSet.head))) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    }
  }

  "scanRows" should "read back rows that were written" in {
    val state = getState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    whenReady(colStore.scanRows(projection, schema, 0, FilteredPartitionScan(paramSet.head))) { rowIter =>
      rowIter.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    }

    // check that can read from same segment again
    whenReady(colStore.scanRows(projection, schema, 0, FilteredPartitionScan(paramSet.head))) { rowIter =>
      rowIter.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    }
  }

  it should "read back rows written in another database" in {
    val state = getState(projectionDb2)
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names))
    whenReady(colStore.appendSegment(projectionDb2, segment, 0)) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    whenReady(colStore.scanRows(projectionDb2, schema, 0, FilteredPartitionScan(paramSet.head))) { rowIter =>
      rowIter.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    }

    // Check that original keyspace/database has no data
    whenReady(colStore.scanSegments(projection, schema, 0, partScan)) { segIter =>
      segIter.toSeq should have length (0)
    }
  }

  it should "read back rows written with multi-column row keys" in {
    import GdeltTestData._
    val segmentsStates = getSegments(colStore, 197901.asInstanceOf[projection2.PK])
    segmentsStates.foreach { case (seg, _) =>
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    whenReady(colStore.scanRows(projection2, schema, 0, FilteredPartitionScan(paramSet.head))) { rowIter =>
      rowIter.map(_.getInt(6)).sum should equal (492)
    }
  }

  it should "filter rows written with single partition key" in {
    import GdeltTestData._
    val segments = getSegmentsByPartKey(colStore, projection2)
    segments.foreach { seg =>
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val filterFunc = KeyFilter.equalsFunc(projection2.partitionType)(197902.asInstanceOf[projection2.PK])
    val method = FilteredPartitionScan(paramSet.head, filterFunc)
    whenReady(colStore.scanRows(projection2, schema, 0, method)) { rowIter =>
      rowIter.map(_.getInt(6)).sum should equal (22)
    }
  }

  it should "range scan by segment key and filter rows with single partition key" in {
    import GdeltTestData._
    val segments = getSegmentsByPartKey(colStore, projection2)
    segments.foreach { seg =>
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    // First filter by segment range only.  There are two possible segment key values: 0 or 50, and
    // two partitions, 197901 and 197902.  197902 only has segment 50 (IDs 91-98)
    // There should be 49 rows and two partitions if we only ask for seg 50 across both partitions
    val method1 = FilteredPartitionRangeScan(paramSet.head, SegmentRange(50, 50))
    whenReady(colStore.scanRows(projection2, schema, 0, method1)) { rowIter =>
      val rows = rowIter.map(r => (r.getInt(0), r.getInt(2))).toList
      rows.length should equal (49)
      rows.map(_._2).toSet should equal (Set(197901, 197902))
      rows.map(_._1).min should equal (50)
    }

    // Ask for only seg 0 and partition 197902, there should be no rows
    val filterFunc = KeyFilter.equalsFunc(projection2.partitionType)(197902.asInstanceOf[projection2.PK])
    val method2 = FilteredPartitionRangeScan(paramSet.head, SegmentRange(0, 0), filterFunc)
    whenReady(colStore.scanRows(projection2, schema, 0, method2)) { rowIter =>
      rowIter.toSeq.length should equal (0)
    }
  }

  import SingleKeyTypes._

  it should "filter rows written with multiple column partition keys" in {
    import GdeltTestData._
    val segments = getSegmentsByPartKey(colStore, projection3)
    segments.foreach { seg =>
      colStore.appendSegment(projection3, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    // Test 1:  IN query on first column only
    val inFilter = KeyFilter.inFunc(StringKeyType)(Set("JPN", "KHM"))
    val filter1 = KeyFilter.makePartitionFilterFunc(projection3, Seq(0), Seq(inFilter))
    val method1 = FilteredPartitionScan(paramSet.head, filter1)
    val readSegs1 = colStore.stats.readSegments
    whenReady(colStore.scanRows(projection3, schema, 0, method1)) { rowIter =>
      val rows = rowIter.toSeq
      rows.map(_.getInt(6)).sum should equal (30)
      rows.map(_.getString(4)).toSet should equal (Set("JPN", "KHM"))
    }
    (colStore.stats.readSegments - readSegs1) should equal (2)

    // Test 2: = filter on both partition columns
    val eqFilters = Seq(KeyFilter.equalsFunc(StringKeyType)("JPN"),
                        KeyFilter.equalsFunc(IntKeyType)(1979))
    val filter2 = KeyFilter.makePartitionFilterFunc(projection3, Seq(0, 1), eqFilters)
    val method2 = FilteredPartitionScan(paramSet.head, filter2)
    whenReady(colStore.scanRows(projection3, schema, 0, method2)) { rowIter =>
      rowIter.map(_.getInt(6)).sum should equal (10)
    }
  }
}