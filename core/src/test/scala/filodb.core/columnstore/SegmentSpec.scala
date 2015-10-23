package filodb.core.columnstore

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import java.nio.ByteBuffer
import org.velvia.filo.{RowReader, TupleRowReader}

import org.scalatest.FunSpec
import org.scalatest.Matchers

object SegmentSpec {
  implicit val keyHelper = LongKeyHelper(10000L)

  val schema = Seq(Column("first", "dataset", 0, Column.ColumnType.StringColumn),
                   Column("last", "dataset", 0, Column.ColumnType.StringColumn),
                   Column("age", "dataset", 0, Column.ColumnType.LongColumn))

  def mapper(rows: Seq[Product]): Iterator[RowReader] = rows.map(TupleRowReader).toIterator

  val dataset = Dataset("dataset", "age")
  val projection = RichProjection[Long](dataset, schema)

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

  // OK, what we want is to test multiple partitions, segments, multiple chunks per segment too.
  // With default segmentSize of 10000, change chunkSize to say 100.
  // Thus let's have the following:
  // "nfc"  0-99  10000-10099 10100-10199  20000-20099 20100-20199 20200-20299
  // "afc"  the same
  // 1200 rows total, 6 segments (3 x 2 partitions)
  // No need to test out of order since that's covered by other things (but we can scramble the rows
  // just for fun)
  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )

  val largeDataset = dataset.copy(options = Dataset.DefaultOptions.copy(chunkSize = 100),
                                  partitionColumn = "league")

  val lotLotNames = {
    for { league <- Seq("nfc", "afc")
          numChunks <- 0 to 2
          chunk  <- 0 to numChunks
          startRowNo = numChunks * 10000 + chunk * 100
          rowNo  <- startRowNo to (startRowNo + 99) }
    yield { (names(rowNo % 6)._1, names(rowNo % 6)._2, Some(rowNo.toLong), Some(league)) }
  }
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

  it("RowWriter and RowReader should work for rows with string sort keys") {
    implicit val stringHelper = new StringKeyHelper(1)
    val stringKeyRange = KeyRange("dataset", "partition", "000", "zzz")
    val segment = new RowWriterSegment(stringKeyRange, schema)
    segment.addRowsAsChunk(mapper(names), (r: RowReader) => r.getString(0))
    val readSeg = RowReaderSegment(segment, schema)

    val sortedNames = Seq("Jerry", "Khalil", "Ndamukong", "Peyton", "Rodney", "Terrance")
    readSeg.rowIterator().map(_.getString(0)).toSeq should equal (sortedNames)
  }
}