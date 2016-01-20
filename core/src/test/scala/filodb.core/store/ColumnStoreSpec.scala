package filodb.core.store

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.{Dataset, Column, DataColumn, Projection, RichProjection}
import filodb.core.Types

import org.scalatest.{FlatSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

trait ColumnStoreSpec extends FlatSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import scala.concurrent.ExecutionContext.Implicits.global
  import NamesTestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val config = ConfigFactory.load("application_test.conf")
  def colStore: CachedMergingColumnStore

  // First create the tables in C*
  override def beforeAll() {
    super.beforeAll()
    colStore.initializeProjection(dataset.projections.head).futureValue
  }

  before {
    colStore.clearProjectionData(dataset.projections.head).futureValue
    colStore.clearSegmentCache()
  }

  val segInfo = SegmentInfo("partition", 0).basedOn(projection)
  val keyRange = KeyRange("partition", 0, 0, endExclusive = false)

  val bytes1 = ByteBuffer.wrap("apple".getBytes("UTF-8"))
  val bytes2 = ByteBuffer.wrap("orange".getBytes("UTF-8"))

  implicit val keyType = LongKeyType
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

    whenReady(colStore.readSegments(projection, schema, keyRange, 0)) { segIter =>
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

    whenReady(colStore.readSegments(projection, schema, keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
      readSeg.rowIterator().map(_.getString(0)).toSeq should equal (firstNames)
    }
  }

  "readSegments" should "read segments back that were written" in {
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.readSegments(projection, schema, keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.getChunks.toSet should equal (segment.getChunks.toSet)
      readSeg.index.rowNumIterator.toSeq should equal (segment.index.rowNumIterator.toSeq)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
    }
  }

  it should "return empty iterator if cannot find segment" in {
    whenReady(colStore.readSegments(projection, schema, keyRange, 0)) { segIter =>
      segIter.toSeq should have length (0)
    }
  }

  it should "return empty iterator if cannot find partition or version" in (pending)

  it should "return segment with empty chunks if cannot find columns" in {
    whenReady(colStore.appendSegment(projection, baseSegment, 0)) { response =>
      response should equal (Success)
    }

    val fakeCol = DataColumn(5, "notACol", dataset.name, 0, Column.ColumnType.StringColumn)
    whenReady(colStore.readSegments(projection, Seq(fakeCol), keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      segments.head.getChunks.toSet should equal (Set(("notACol", 0, null), ("notACol", 1, null)))
    }
  }

  "scanSegments" should "read segments back that were written" in {
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names))
    whenReady(colStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.name)
    paramSet should have length (1)

    whenReady(colStore.scanSegments(projection, schema, 0, params = paramSet.head)()) { segIter =>
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

    val paramSet = colStore.getScanSplits(dataset.name)
    paramSet should have length (1)

    whenReady(colStore.scanRows(projection, schema, 0, params = paramSet.head)()) { rowIter =>
      rowIter.map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
    }
  }
}