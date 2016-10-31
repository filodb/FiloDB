package filodb.core.store

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import java.nio.ByteBuffer
import org.velvia.filo.{RowReader, TupleRowReader}
import scodec.bits._

import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

class SegmentSpec extends FunSpec with Matchers with BeforeAndAfter {
  import NamesTestData._

  val segInfo = SegmentInfo("partition", 0)

  def rowReaderSegFromChunkSet(projection: RichProjection,
                               _segInfo: SegmentInfo[_, _],
                               chunkSet: ChunkSet,
                               schema: Seq[Column]): RowReaderSegment = {
    val seg = new RowReaderSegment(projection, _segInfo,
                                   Seq((chunkSet.info, Array[Int]())), schema)
    chunkSet.chunks.foreach { case (colId, bytes) => seg.addChunk(chunkSet.info.id, colId, bytes) }
    seg
  }

  import SingleKeyTypes._

  it("SegmentState should add chunk info properly and update state for append only") {
    val state = getState()
    state.nextChunkId should equal (0)

    val chunkSet1 = ChunkSet(state, mapper(names take 4).toIterator)
    chunkSet1.info.id should equal (0)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.info.firstKey should equal (firstKey)
    chunkSet1.info.lastKey should equal (keyForName(3))
    chunkSet1.skips should equal (Nil)
    chunkSet1.chunks.size should equal (4)
    chunkSet1.chunks.keySet should equal (Set("first", "last", "age", "seg"))
    state.store(chunkSet1)
    state.add(chunkSet1)
    state.nextChunkId should equal (1)
    state.numChunks should equal (1)

    // Add new rows.  These do not replace previous ones at all.
    val chunkSet2 = ChunkSet(state, mapper(names drop 4).toIterator)
    chunkSet2.info.id should equal (1)
    chunkSet2.info.numRows should equal (2)
    chunkSet2.skips should equal (Nil)
    chunkSet2.chunks.size should equal (4)
    state.add(chunkSet2)
    state.nextChunkId should equal (2)
  }

  it("SegmentState should add chunk info properly when SegmentState prepopulated") {
    val info1 = ChunkSetInfo(4, 10, firstKey, lastKey)
    val state = new TestSegmentState(projection, schema)
    state.add(Seq((info1, emptyFilter)))
    state.rowKeyChunks(4) = Array.fill[ByteBuffer](projection.rowKeyColumns.length)(null)
    state.nextChunkId should equal (5)

    val chunkSet1 = ChunkSet(state, mapper(names take 4).toIterator)
    state.add(chunkSet1)
    state.nextChunkId should equal (6)
    state.numChunks should equal (2)
    chunkSet1.info.id should equal (5)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.skips should have length (0)
  }

  it("SegmentState should add skip lists properly when new rows replace previous chunks") {
    val state = getState()
    state.nextChunkId should equal (0)

    val sortedNames = names.sortBy(_._3.get)
    val chunkSet1 = ChunkSet.withSkips(state, mapper(sortedNames take 4).toIterator)
    state.add(chunkSet1)
    state.nextChunkId should equal (1)
    state.numChunks should equal (1)
    chunkSet1.info.id should equal (0)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.skips should equal (Nil)
    state.store(chunkSet1)

    // Add new rows.  These replace two of the previous rows - namely in rows 2, 3
    val chunkSet2 = ChunkSet.withSkips(state, mapper(sortedNames drop 2).toIterator)
    state.add(chunkSet2)
    state.nextChunkId should equal (2)
    chunkSet2.info.id should equal (1)
    chunkSet2.info.numRows should equal (4)
    chunkSet2.skips should equal (Seq(ChunkRowSkipIndex(0, Array(2, 3))))
  }

  it("SegmentState should not add skip lists if detectSkips=false") {
    val state = getState(0)
    state.nextChunkId should equal (0)

    val sortedNames = names.sortBy(_._3.get)
    val chunkSet1 = ChunkSet(state, mapper(sortedNames take 4).toIterator)
    state.add(chunkSet1)
    state.nextChunkId should equal (1)
    state.numChunks should equal (1)
    chunkSet1.skips should equal (Nil)
    state.store(chunkSet1)

    // Add new rows.  These replace two of the previous rows - namely in rows 2, 3
    val chunkSet2 = ChunkSet(state, mapper(sortedNames drop 2).toIterator)
    state.add(chunkSet2)
    state.nextChunkId should equal (2)
    chunkSet2.info.id should equal (1)
    chunkSet2.info.numRows should equal (4)
    chunkSet2.skips should equal (Nil)
  }

  it("RowWriter and RowReader should work for rows with string row keys") {
    val stringProj = RichProjection(Dataset("a", "first", "seg"), schema)
    val state = new TestSegmentState(stringProj, schema)
    val chunkSet = ChunkSet(state, mapper(names).toIterator)
    val readSeg = rowReaderSegFromChunkSet(stringProj, segInfo, chunkSet, schema)

    readSeg.rowIterator().map(_.getString(0)).toSeq should equal (names.map(_._1.get))
  }

  it("RowWriter and RowReader should work for rows with multi-column row keys") {
    import GdeltTestData._
    val segInfo = SegmentInfo(197901, "0").basedOn(projection2)
    val state = new TestSegmentState(projection2, schema)
    val chunkSet = ChunkSet(state, readers.take(20).toIterator)
    val readSeg = rowReaderSegFromChunkSet(projection2, segInfo, chunkSet, schema)

    // Sum up all the NumArticles for first 20 rows
    readSeg.rowIterator().map(_.getInt(6)).sum should equal (127)
  }
}