package filodb.core.store

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import java.nio.ByteBuffer
import org.velvia.filo.{RowReader, TupleRowReader}

import org.scalatest.FunSpec
import org.scalatest.Matchers

class SegmentSpec extends FunSpec with Matchers {
  import NamesTestData._

  val segInfo = SegmentInfo("partition", 0)

  val bytes1 = ByteBuffer.wrap("apple".getBytes("UTF-8"))
  val bytes2 = ByteBuffer.wrap("orange".getBytes("UTF-8"))

  it("GenericSegment should add and get chunks back out") {
    import SingleKeyTypes.LongKeyType
    val rowIndex = new UpdatableChunkRowMap
    val segment = new GenericSegment(projection, rowIndex)(segInfo.basedOn(projection))
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
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names))

    segment.index.nextChunkId should equal (1)
    segment.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 0, 0, 0))
    segment.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 5, 4, 3))
    segment.getChunks.toSeq should have length (4)
    segment.getColumns should equal (Set("first", "last", "age", "seg"))

    // Write some of the rows as another chunk and make sure index updates properly
    // NOTE: this is row merging in operation!
    segment.addRowsAsChunk(mapper(names.drop(4)))

    segment.index.nextChunkId should equal (2)
    segment.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 1, 1, 0))
    segment.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 1, 0, 3))
    // Remember that we have to include the seg column
    segment.getChunks.toSeq should have length (8)
  }

  it("RowReaderSegment should read back rows in sort key order") {
    val segment = getRowWriter()
    segment.addRowsAsChunk(mapper(names))
    val readSeg = RowReaderSegment(segment, schema)

    readSeg.getColumns should equal (Set("first", "last", "age", "seg"))
    readSeg.rowIterator().map(_.getString(0)).toSeq should equal (firstNames)

    // Should be able to obtain another rowIterator
    readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))

    readSeg.rowChunkIterator().map { case (reader, id, rowNo) => (reader.getString(0), id, rowNo) }.
      take(2).toSeq should equal (Seq(("Khalil", 0, 0), ("Rodney", 0, 2)))
  }

  it("RowWriter and RowReader should work for rows with string row keys") {
    val stringProj = RichProjection(Dataset("a", "first", "seg"), schema)
    val segment = new RowWriterSegment(stringProj, schema)(segInfo.basedOn(stringProj))
    segment.addRowsAsChunk(mapper(names))
    val readSeg = RowReaderSegment(segment, schema)

    val sortedNames = Seq("Jerry", "Khalil", "Ndamukong", "Peyton", "Rodney", "Terrance")
    readSeg.rowIterator().map(_.getString(0)).toSeq should equal (sortedNames)
  }

  it("RowWriter and RowReader should work for rows with multi-column row keys") {
    import GdeltTestData._
    val segInfo = SegmentInfo(197901, "0").basedOn(projection2)
    val segment = new RowWriterSegment(projection2, schema)(segInfo)
    segment.addRowsAsChunk(readers.toIterator.take(20))
    val readSeg = RowReaderSegment(segment, schema)

    // Sum up all the NumArticles for first 20 rows
    readSeg.rowIterator().map(_.getInt(6)).sum should equal (127)
  }
}