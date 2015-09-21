package filodb.core.columnstore

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import java.nio.ByteBuffer
import org.velvia.filo.{ColumnParser, RowReader, TupleRowReader}

import org.scalatest.FunSpec
import org.scalatest.Matchers

object SegmentSpec {
  implicit val keyHelper = LongKeyHelper(10000L)

  val schema = Seq(Column("first", "dataset1", 0, Column.ColumnType.StringColumn),
                   Column("last", "dataset1", 0, Column.ColumnType.StringColumn),
                   Column("age", "dataset1", 0, Column.ColumnType.LongColumn))

  def mapper(rows: Seq[Product]): Iterator[RowReader] = rows.map(TupleRowReader).toIterator

  val dataset = Dataset("dataset", "age")
  val projection = RichProjection(dataset, schema)

  val names = Seq((Some("Khalil"), Some("Mack"), Some(24L)),
                  (Some("Ndamukong"), Some("Suh"), Some(28L)),
                  (Some("Rodney"), Some("Hudson"), Some(25L)),
                  (Some("Jerry"),  None,           Some(40L)),
                  (Some("Peyton"), Some("Manning"), Some(39L)),
                  (Some("Terrance"), Some("Knighton"), Some(29L)))

  def getRowWriter(keyRange: KeyRange[Long]): RowWriterSegment[Long] =
    new RowWriterSegment(keyRange, schema)

  def getSortKey(r: RowReader): Long = r.getLong(2)

  val firstNames = Seq("Khalil", "Rodney", "Ndamukong", "Terrance", "Peyton", "Jerry")
}

class SegmentSpec extends FunSpec with Matchers {
  import SegmentSpec._
  val keyRange = KeyRange("dataset", "partition", 0L, 10000L)

  val bytes1 = ByteBuffer.wrap("apple".getBytes("UTF-8"))
  val bytes2 = ByteBuffer.wrap("orange".getBytes("UTF-8"))

  it("GenericSegment should add and get chunks back out") {
    val rowIndex = new UpdatableChunkRowMap[Long]
    val segment = new GenericSegment(keyRange, rowIndex)
    segment.isEmpty should equal (true)
    segment.addChunks(0, Map("columnA" -> bytes1, "columnB" -> bytes2))
    segment.addChunks(1, Map("columnA" -> bytes1, "columnB" -> bytes2))
    segment.isEmpty should equal (true)
    rowIndex.update(0L, 0, 0)
    segment.isEmpty should equal (false)

    segment.getColumns should equal (Set("columnA", "columnB"))
    segment.getChunks.toSet should equal (Set(("columnA", 0, bytes1),
                                              ("columnA", 1, bytes1),
                                              ("columnB", 0, bytes2),
                                              ("columnB", 1, bytes2)))
  }

  it("RowWriterSegment should add rows and chunkify properly") {
    val segment = getRowWriter(keyRange)
    segment.addRowsAsChunk(mapper(names), getSortKey _)

    segment.index.nextChunkId should equal (1)
    segment.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 0, 0, 0))
    segment.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 5, 4, 3))
    segment.getChunks.toSeq should have length (3)
    segment.getColumns should equal (Set("first", "last", "age"))

    // Write some of the rows as another chunk and make sure index updates properly
    // NOTE: this is row merging in operation!
    segment.addRowsAsChunk(mapper(names.drop(4)), getSortKey _)

    segment.index.nextChunkId should equal (2)
    segment.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 1, 1, 0))
    segment.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 1, 0, 3))
    segment.getChunks.toSeq should have length (6)
  }

  it("RowReaderSegment should read back rows in sort key order") {
    val segment = getRowWriter(keyRange)
    segment.addRowsAsChunk(mapper(names), getSortKey _)
    val readSeg = RowReaderSegment(segment, schema)

    readSeg.getColumns should equal (Set("first", "last", "age"))
    readSeg.rowIterator().map(_.getString(0)).toSeq should equal (firstNames)

    // Should be able to obtain another rowIterator
    readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))

    readSeg.rowChunkIterator().map { case (reader, id, rowNo) => (reader.getString(0), id, rowNo) }.
      take(2).toSeq should equal (Seq(("Khalil", 0, 0), ("Rodney", 0, 2)))
  }
}