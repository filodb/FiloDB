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

  val rowKeyChunks = new collection.mutable.HashMap[Types.ChunkID, Array[ByteBuffer]]

  before {
    rowKeyChunks.clear()
  }

  private def storeChunkSet(chunkSet: ChunkSet): Unit = {
    val rowKeyColNames = projection.rowKeyColumns.map(_.name)
    val chunkArray = rowKeyColNames.map(chunkSet.chunks).toArray
    rowKeyChunks(chunkSet.info.id) = chunkArray
  }

  import SingleKeyTypes._

  it("SegmentState should add chunk info properly and update state for append only") {
    val state = getState(rowKeyChunks.apply _)
    state.nextChunkId should equal (0)

    val chunkSet1 = state.newChunkSet(mapper(names take 4).toIterator)
    state.nextChunkId should equal (1)
    state.infoMap.size should equal (1)
    chunkSet1.info.id should equal (0)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.info.firstKey should equal (firstKey)
    chunkSet1.info.lastKey should equal (keyForName(3))
    chunkSet1.skips should equal (Nil)
    chunkSet1.chunks.size should equal (4)
    chunkSet1.chunks.keySet should equal (Set("first", "last", "age", "seg"))
    storeChunkSet(chunkSet1)

    // Add new rows.  These do not replace previous ones at all.
    val chunkSet2 = state.newChunkSet(mapper(names drop 4).toIterator)
    state.nextChunkId should equal (2)
    chunkSet2.info.id should equal (1)
    chunkSet2.info.numRows should equal (2)
    chunkSet2.skips should equal (Nil)
    chunkSet2.chunks.size should equal (4)
  }

  it("SegmentState should add chunk info properly when SegmentState prepopulated") {
    val info1 = ChunkSetInfo(4, 10, firstKey, lastKey)
    rowKeyChunks(4) = Array.fill[ByteBuffer](projection.rowKeyColumns.length)(null)
    val state = new SegmentState(projection, schema, Seq(info1), rowKeyChunks.apply _)(
                                 segInfo.basedOn(projection))

    state.nextChunkId should equal (5)
    val chunkSet1 = state.newChunkSet(mapper(names take 4).toIterator)
    state.nextChunkId should equal (6)
    state.infoMap.size should equal (2)
    chunkSet1.info.id should equal (5)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.skips should have length (0)
  }

  it("SegmentState should add skip lists properly when new rows replace previous chunks") {
    val state = getState(rowKeyChunks.apply _)
    state.nextChunkId should equal (0)

    val sortedNames = names.sortBy(_._3.get)
    val chunkSet1 = state.newChunkSet(mapper(sortedNames take 4).toIterator)
    state.nextChunkId should equal (1)
    state.infoMap.size should equal (1)
    chunkSet1.info.id should equal (0)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.skips should equal (Nil)
    storeChunkSet(chunkSet1)

    // Add new rows.  These replace two of the previous rows - namely in rows 2, 3
    val chunkSet2 = state.newChunkSet(mapper(sortedNames drop 2).toIterator)
    state.nextChunkId should equal (2)
    chunkSet2.info.id should equal (1)
    chunkSet2.info.numRows should equal (4)
    chunkSet2.skips should equal (Seq(ChunkRowSkipIndex(0, Array(2, 3))))
  }

  it("RowWriter and RowReader should work for rows with string row keys") {
    val stringProj = RichProjection(Dataset("a", "first", "seg"), schema)
    val state = new SegmentState(stringProj, schema, Nil, rowKeyChunks.apply _)(segInfo.basedOn(stringProj))
    val chunkSet = state.newChunkSet(mapper(names).toIterator)
    val readSeg = rowReaderSegFromChunkSet(stringProj, segInfo, chunkSet, schema)

    readSeg.rowIterator().map(_.getString(0)).toSeq should equal (names.map(_._1.get))
  }

  it("RowWriter and RowReader should work for rows with multi-column row keys") {
    import GdeltTestData._
    val segInfo = SegmentInfo(197901, "0").basedOn(projection2)
    val state = new SegmentState(projection2, schema, Nil, rowKeyChunks.apply _)(segInfo)
    val chunkSet = state.newChunkSet(readers.take(20).toIterator)
    val readSeg = rowReaderSegFromChunkSet(projection2, segInfo, chunkSet, schema)

    // Sum up all the NumArticles for first 20 rows
    readSeg.rowIterator().map(_.getInt(6)).sum should equal (127)
  }
}