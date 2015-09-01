package filodb.core.datastore2

import filodb.core.metadata.Column
import java.nio.ByteBuffer
import org.velvia.filo.{ColumnParser, TupleRowIngestSupport}

import org.scalatest.FunSpec
import org.scalatest.Matchers

class SegmentSpec extends FunSpec with Matchers {
  implicit val keyHelper = TimestampKeyHelper(10000L)
  val keyRange = KeyRange("dataset", "partition", 0L, 10000L)

  val bytes1 = ByteBuffer.wrap("apple".getBytes("UTF-8"))
  val bytes2 = ByteBuffer.wrap("orange".getBytes("UTF-8"))

  val rowIndex = new UpdatableChunkRowMap[Long]

  it("GenericSegment should add and get chunks back out") {
    val segment = new GenericSegment(keyRange, rowIndex)
    segment.addChunks(0, Map("columnA" -> bytes1, "columnB" -> bytes2))
    segment.addChunks(1, Map("columnA" -> bytes1, "columnB" -> bytes2))

    segment.getColumns should equal (Set("columnA", "columnB"))
    segment.getChunks.toSet should equal (Set(("columnA", 0, bytes1),
                                              ("columnA", 1, bytes1),
                                              ("columnB", 0, bytes2),
                                              ("columnB", 1, bytes2)))
  }

  val schema = Seq(Column("first", "dataset1", 0, Column.ColumnType.StringColumn),
                   Column("last", "dataset1", 0, Column.ColumnType.StringColumn),
                   Column("age", "dataset1", 0, Column.ColumnType.LongColumn))

  val support = TupleRowIngestSupport

  val names = Seq((Some("Khalil"), Some("Mack"), Some(24L)),
                  (Some("Ndamukong"), Some("Suh"), Some(28L)),
                  (Some("Rodney"), Some("Hudson"), Some(25L)),
                  (Some("Jerry"),  None,           Some(40L)),
                  (Some("Peyton"), Some("Manning"), Some(39L)),
                  (Some("Terrance"), Some("Knighton"), Some(29L)))

  it("RowWriterSegment should add rows and chunkify properly") {
    val segment = new RowWriterSegment(keyRange, schema, support,
                                       { p: Product => p.productElement(2).asInstanceOf[Option[Long]].get })
    segment.addRowsAsChunk(names)

    segment.index.nextChunkId should equal (1)
    segment.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 0, 0, 0))
    segment.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 5, 4, 3))
    segment.getChunks.toSeq should have length (3)

    // Write some of the rows as another chunk and make sure index updates properly
    // NOTE: this is row merging in operation!
    segment.addRowsAsChunk(names.drop(4))

    segment.index.nextChunkId should equal (2)
    segment.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 1, 1, 0))
    segment.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 1, 0, 3))
    segment.getChunks.toSeq should have length (6)
  }

  it("RowReaderSegment should read back rows in sort key order") {
    val segment = new RowWriterSegment(keyRange, schema, support,
                                       { p: Product => p.productElement(2).asInstanceOf[Option[Long]].get })
    segment.addRowsAsChunk(names)
    val (chunkIdBuf, rowNumBuf) = segment.index.serialize()
    val binChunkMap = new BinaryChunkRowMap(chunkIdBuf, rowNumBuf, segment.index.nextChunkId)
    val readSeg = new RowReaderSegment(keyRange, binChunkMap, schema)
    segment.getChunks.foreach { case (col, id, bytes) => readSeg.addChunk(id, col, bytes) }

    readSeg.rowIterator().map(_.getString(0)).toSeq should equal (Seq(
               "Khalil", "Rodney", "Ndamukong", "Terrance", "Peyton", "Jerry"))

    // Should be able to obtain another rowIterator
    readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
  }
}