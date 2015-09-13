package filodb.cassandra.columnstore

import com.typesafe.config.ConfigFactory
import com.websudos.phantom.testkit._
import java.nio.ByteBuffer
import org.scalatest.BeforeAndAfter
import org.velvia.filo.TupleRowIngestSupport
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.Column
import filodb.core.Types

class CassandraColumnStoreSpec extends CassandraFlatSpec with BeforeAndAfter {
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.websudos.phantom.dsl._
  import filodb.core.columnstore._
  import SegmentSpec._

  implicit val keySpace = KeySpace("unittest")
  val colStore = new CassandraColumnStore(ConfigFactory.load(),
                                          { x => schema(2) })
  val dataset = "foo"

  val (chunkTable, rowMapTable) = Await.result(colStore.getSegmentTables(dataset), 3 seconds)

  // First create the tables in C*
  override def beforeAll() {
    super.beforeAll()
    // Note: This is a CREATE TABLE IF NOT EXISTS
    Await.result(chunkTable.create.ifNotExists.future(), 3 seconds)
    Await.result(rowMapTable.create.ifNotExists.future(), 3 seconds)
  }

  before {
    Await.result(chunkTable.truncate.future(), 3 seconds)
    Await.result(rowMapTable.truncate.future(), 3 seconds)
    colStore.clearSegmentCache()
  }

  val keyRange = KeyRange(dataset, "partition", 0L, 10000L)

  val bytes1 = ByteBuffer.wrap("apple".getBytes("UTF-8"))
  val bytes2 = ByteBuffer.wrap("orange".getBytes("UTF-8"))

  val rowIndex = new UpdatableChunkRowMap[Long]

  val baseSegment = new GenericSegment(keyRange, rowIndex)
  baseSegment.addChunks(0, Map("columnA" -> bytes1, "columnB" -> bytes2))
  baseSegment.addChunks(1, Map("columnA" -> bytes1, "columnB" -> bytes2))
  rowIndex.index = rowIndex.index ++
                     Map(500L -> (0 -> 0), 1000L -> (1 -> 0), 600L -> (0 -> 1), 700L -> (0 -> 2))

  private def getChunkRowMap[K](segment: Segment[K]): Future[BinaryChunkRowMap] =
    rowMapTable.getChunkMaps(segment.partition, 0, segment.segmentId, segment.keyRange.binaryEnd).
                collect {
      case Seq(ChunkRowMapRecord(_, chunkIds, rowNums, nextId)) =>
        new BinaryChunkRowMap(chunkIds, rowNums, nextId)
      case x: Seq[_] => throw new RuntimeException("Got back unexpected chunkMaps " + x)
    }

  // NOTE: The test below purposefully does not use any of the read APIs so that if only the read code
  // breaks, this test can independently test for write failures
  "appendSegment" should "write a segment into an empty table" in {
    whenReady(colStore.appendSegment(baseSegment, 0)) { response =>
      response should equal (Success)
    }

    whenReady(chunkTable.select(_.columnName, _.chunkId, _.data).fetch()) { data =>
      data should equal (Seq(("columnA", 0, bytes1),
                             ("columnA", 1, bytes1),
                             ("columnB", 0, bytes2),
                             ("columnB", 1, bytes2)))
    }

    whenReady(getChunkRowMap(baseSegment)) { binRowMap =>
      binRowMap.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 1))
      binRowMap.rowNumIterator.toSeq should equal (Seq(0, 1, 2, 0))
    }
  }

  it should "NOOP if the segment is empty" in {
    val segment = getRowWriter(keyRange)
    whenReady(colStore.appendSegment(segment, 0)) { response =>
      response should equal (NotApplied)
    }
  }

  it should "append new rows to a cached segment successfully" in {
    val segment = getRowWriter(keyRange)
    segment.addRowsAsChunk(names take 3, getSortKey _)
    whenReady(colStore.appendSegment(segment, 0)) { response =>
      response should equal (Success)
    }

    // Writing segment2, last 3 rows, should get appended to first 3 in same segment
    val segment2 = getRowWriter(keyRange)
    segment2.addRowsAsChunk(names drop 3, getSortKey _)
    whenReady(colStore.appendSegment(segment2, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.readSegments(schema, keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment[Long]]
      readSeg.keyRange should equal (segment.keyRange)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
      readSeg.rowIterator().map(_.getString(0)).toSeq should equal (firstNames)
    }
  }

  it should "replace rows to an uncached segment successfully" in {
    val segment = getRowWriter(keyRange)
    segment.addRowsAsChunk(names drop 1, getSortKey _)
    whenReady(colStore.appendSegment(segment, 0)) { response =>
      response should equal (Success)
    }

    colStore.clearSegmentCache()

    // Writing segment2, repeat 1 row and add another row.  Should read orig segment from disk.
    val segment2 = getRowWriter(keyRange)
    segment2.addRowsAsChunk(names take 2, getSortKey _)
    whenReady(colStore.appendSegment(segment2, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.readSegments(schema, keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment[Long]]
      readSeg.keyRange should equal (segment.keyRange)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
      readSeg.rowIterator().map(_.getString(0)).toSeq should equal (firstNames)
    }
  }

  "readSegments" should "read segments back that were written" in {
    val segment = getRowWriter(keyRange)
    segment.addRowsAsChunk(names, getSortKey _)
    whenReady(colStore.appendSegment(segment, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.readSegments(schema, keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment[Long]]
      readSeg.keyRange should equal (segment.keyRange)
      readSeg.getChunks.toSet should equal (segment.getChunks.toSet)
      readSeg.index.rowNumIterator.toSeq should equal (segment.index.rowNumIterator.toSeq)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
    }
  }

  it should "return empty iterator if cannot find segment" in {
    whenReady(colStore.readSegments(schema, keyRange, 0)) { segIter =>
      segIter.toSeq should have length (0)
    }
  }

  it should "return empty iterator if cannot find partition or version" in (pending)

  it should "return segment with empty chunks if cannot find columns" in {
    whenReady(colStore.appendSegment(baseSegment, 0)) { response =>
      response should equal (Success)
    }

    val fakeCol = Column("notACol", dataset, 0, Column.ColumnType.StringColumn)
    whenReady(colStore.readSegments(Seq(fakeCol), keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      segments.head.getChunks.toSet should equal (Set(("notACol", 0, null), ("notACol", 1, null)))
    }
  }
}