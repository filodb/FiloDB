package filodb.core.store

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  def colStore: CachedMergingColumnStore

  // First create the tables in C*
  override def beforeAll() {
    super.beforeAll()
    colStore.initializeProjection(dataset.projections.head).futureValue
    colStore.initializeProjection(GdeltTestData.dataset2.projections.head).futureValue
  }

  before {
    colStore.clearProjectionData(dataset.projections.head).futureValue
    colStore.clearProjectionData(GdeltTestData.dataset2.projections.head).futureValue
    colStore.clearSegmentCache()
  }

  val segInfo = SegmentInfo("partition", 0).basedOn(projection)
  val partScan = SinglePartitionScan("partition")

  val bytes1 = ByteBuffer.wrap("apple".getBytes("UTF-8"))
  val bytes2 = ByteBuffer.wrap("orange".getBytes("UTF-8"))

  implicit val keyType = SingleKeyTypes.LongKeyType
  val rowIndex = new UpdatableChunkRowMap

  val baseSegment = new GenericSegment(projection, rowIndex)(segInfo)
  baseSegment.addChunks(0, Map("columnA" -> bytes1, "columnB" -> bytes2))
  baseSegment.addChunks(1, Map("columnA" -> bytes1, "columnB" -> bytes2))
  rowIndex.index = rowIndex.index ++
                     Map(500L -> (0 -> 0), 1000L -> (1 -> 0), 600L -> (0 -> 1), 700L -> (0 -> 2))

  // NOTE: The test below purposefully does not use any of the read APIs so that if only the read code
  // breaks, this test can independently test for write failures
  "appendSegment" should "NOOP if the segment is empty" in {
    val segment = getRowWriter()
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (NotApplied)
    }
  }

  it should "append new rows to a cached segment successfully" in {
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names take 3))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    // Writing segment2, last 3 rows, should get appended to first 3 in same segment
    val segment2 = getRowWriter()
    segment2.addRowsAsChunk(mapper(names drop 3))
    whenReady(colStore.appendSegment(projection, segment2, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.scanSegments(projection, schema, 0, partScan)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
      readSeg.rowIterator().map(_.getString(0)).toSeq should equal (firstNames)
    }
  }

  it should "replace rows to an uncached segment successfully" in {
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names drop 1))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    colStore.clearSegmentCache()

    // Writing segment2, repeat 1 row and add another row.  Should read orig segment from disk.
    val segment2 = getRowWriter()
    segment2.addRowsAsChunk(mapper(names take 2))
    whenReady(colStore.appendSegment(projection, segment2, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.scanSegments(projection, schema, 0, partScan)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
      readSeg.rowIterator().map(_.getString(0)).toSeq should equal (firstNames)
    }
  }

  // The first row key column is a computed column, so this test also ensures that retrieving the original
  // source columns works.
  it should "replace rows with multi row keys to an uncached segment" in {
    import GdeltTestData._
    val segments = getSegments(197901.asInstanceOf[projection2.PK])
    segments.foreach { seg =>
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    colStore.clearSegmentCache()

    val segment2 = new RowWriterSegment(projection2, schema)(segments.head.segInfo.basedOn(projection2))
    segment2.addRowsAsChunk(readers.toIterator.take(3))
    colStore.appendSegment(projection2, segment2, 0).futureValue should equal (Success)

    whenReady(colStore.scanSegments(projection2, schema, 0, SinglePartitionScan(197901))) { segIter =>
      val segments = segIter.toSeq
      segments should have length (2)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.rowIterator().map(_.getInt(6)).sum should equal (288)
    }
  }

  "scanSegments SinglePartitionScan" should "read segments back that were written" in {
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.scanSegments(projection, schema, 0, partScan)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.getChunks.toSet should equal (segment.getChunks.toSet)
      readSeg.index.rowNumIterator.toSeq should equal (segment.index.rowNumIterator.toSeq)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
    }
  }

  it should "return empty iterator if cannot find segment (SinglePartitionRangeScan)" in {
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names))
    colStore.appendSegment(projection, segment, 0).futureValue should equal (Success)

    // partition exists but only for segment key 0, this should find nothing
    val rangeScanNoSegment = SinglePartitionRangeScan(KeyRange("partition", 1000, 1000, false))
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
    whenReady(colStore.appendSegment(projection, baseSegment, 0)) { response =>
      response should equal (Success)
    }

    val fakeCol = DataColumn(5, "notACol", dataset.name, 0, Column.ColumnType.StringColumn)
    whenReady(colStore.scanSegments(projection, Seq(fakeCol), 0, partScan)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      segments.head.getChunks.toSet should equal (Set(("notACol", 0, null), ("notACol", 1, null)))
    }
  }

  "scanSegments FilteredPartitionScan" should "read segments back that were written" in {
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.name, 1)
    paramSet should have length (1)

    whenReady(colStore.scanSegments(projection, schema, 0, FilteredPartitionScan(paramSet.head))) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.getChunks.toSet should equal (segment.getChunks.toSet)
      readSeg.index.rowNumIterator.toSeq should equal (segment.index.rowNumIterator.toSeq)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
    }
  }

  "scanRows" should "read back rows that were written" in {
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.name, 1)
    paramSet should have length (1)

    whenReady(colStore.scanRows(projection, schema, 0, FilteredPartitionScan(paramSet.head))) { rowIter =>
      rowIter.map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
    }
  }

  it should "read back rows written with multi-column row keys" in {
    import GdeltTestData._
    val segments = getSegments(197901.asInstanceOf[projection2.PK])
    segments.foreach { seg =>
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.name, 1)
    paramSet should have length (1)

    whenReady(colStore.scanRows(projection2, schema, 0, FilteredPartitionScan(paramSet.head))) { rowIter =>
      rowIter.map(_.getInt(6)).sum should equal (492)
    }
  }

  it should "filter rows written with single partition key" in {
    import GdeltTestData._
    val segments = getSegmentsByPartKey(projection2)
    segments.foreach { seg =>
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.name, 1)
    paramSet should have length (1)

    val filterFunc = KeyFilter.equalsFunc(projection2.partitionType)(197902.asInstanceOf[projection2.PK])
    val method = FilteredPartitionScan(paramSet.head, filterFunc)
    whenReady(colStore.scanRows(projection2, schema, 0, method)) { rowIter =>
      rowIter.map(_.getInt(6)).sum should equal (22)
    }
  }

  it should "range scan by segment key and filter rows with single partition key" in {
    import GdeltTestData._
    val segments = getSegmentsByPartKey(projection2)
    segments.foreach { seg =>
      colStore.appendSegment(projection2, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.name, 1)
    paramSet should have length (1)

    // First filter by segment range only.  There are two possible segment key values: 0 or 50, and
    // two partitions, 197901 and 197902.  197902 only has segment 50 (IDs 91-98)
    // There should be 49 rows and two partitions if we only ask for seg 50 across both partitions
    val method1 = FilteredPartitionRangeScan(paramSet.head, 50, 50)
    whenReady(colStore.scanRows(projection2, schema, 0, method1)) { rowIter =>
      val rows = rowIter.map(r => (r.getInt(0), r.getInt(2))).toList
      rows.length should equal (49)
      rows.map(_._2).toSet should equal (Set(197901, 197902))
      rows.map(_._1).min should equal (50)
    }

    // Ask for only seg 0 and partition 197902, there should be no rows
    val filterFunc = KeyFilter.equalsFunc(projection2.partitionType)(197902.asInstanceOf[projection2.PK])
    val method2 = FilteredPartitionRangeScan(paramSet.head, 0, 0, filterFunc)
    whenReady(colStore.scanRows(projection2, schema, 0, method2)) { rowIter =>
      rowIter.toSeq.length should equal (0)
    }
  }

  import SingleKeyTypes._

  it should "filter rows written with multiple column partition keys" in {
    import GdeltTestData._
    val segments = getSegmentsByPartKey(projection3)
    segments.foreach { seg =>
      colStore.appendSegment(projection3, seg, 0).futureValue should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.name, 1)
    paramSet should have length (1)

    // Test 1:  IN query on first column only
    val inFilter = KeyFilter.inFunc(StringKeyType)(Set("JPN", "KHM"))
    val filter1 = KeyFilter.makePartitionFilterFunc(projection3, Seq(0), Seq(inFilter))
    val method1 = FilteredPartitionScan(paramSet.head, filter1)
    whenReady(colStore.scanRows(projection3, schema, 0, method1)) { rowIter =>
      val rows = rowIter.toSeq
      rows.map(_.getInt(6)).sum should equal (30)
      rows.map(_.getString(4)).toSet should equal (Set("JPN", "KHM"))
    }

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