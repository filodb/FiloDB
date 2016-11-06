package filodb.core.store

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import org.velvia.filo.{RowReader, TupleRowReader}
import scodec.bits._

import org.scalatest.FunSpec
import org.scalatest.Matchers

class SegmentSpec extends FunSpec with Matchers {
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

    val chunkSet1 = state.newChunkSet(mapper(names take 4))
    state.nextChunkId should equal (1)
    state.infoMap.size should equal (1)
    chunkSet1.info.id should equal (0)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.info.firstKey should equal (LongKeyType.toBytes(24L))
    chunkSet1.info.lastKey should equal (LongKeyType.toBytes(40L))
    chunkSet1.skips should equal (Nil)
    chunkSet1.chunks.size should equal (4)
    chunkSet1.chunks.keySet should equal (Set("first", "last", "age", "seg"))

    // Add new rows.  These do not replace previous ones at all.
    val chunkSet2 = state.newChunkSet(mapper(names drop 4))
    state.nextChunkId should equal (2)
    chunkSet2.info.id should equal (1)
    chunkSet2.info.numRows should equal (2)
    chunkSet2.skips should equal (Nil)
    chunkSet2.chunks.size should equal (4)
  }

  it("SegmentState should add chunk info properly when SegmentState prepopulated") {
    val info1 = ChunkSetInfo(4, 10, ByteVector(10), ByteVector(15))
    val state = new SegmentState(projection, schema, Seq(info1))(segInfo.basedOn(projection))

    state.nextChunkId should equal (5)
    val chunkSet1 = state.newChunkSet(mapper(names take 4))
    state.nextChunkId should equal (6)
    state.infoMap.size should equal (2)
    chunkSet1.info.id should equal (5)
    chunkSet1.info.numRows should equal (4)
  }

  it("SegmentState should add skip lists properly when new rows replace previous chunks") (pending)

  it("RowWriter and RowReader should work for rows with string row keys") {
    val stringProj = RichProjection(Dataset("a", "first", "seg"), schema)
    val state = new SegmentState(stringProj, schema, Nil)(segInfo.basedOn(stringProj))
    val chunkSet = state.newChunkSet(mapper(names))
    val readSeg = rowReaderSegFromChunkSet(stringProj, segInfo, chunkSet, schema)

    readSeg.rowIterator().map(_.getString(0)).toSeq should equal (names.map(_._1.get))
  }

  it("RowWriter and RowReader should work for rows with multi-column row keys") {
    import GdeltTestData._
    val segInfo = SegmentInfo(197901, "0").basedOn(projection2)
    val state = new SegmentState(projection2, schema, Nil)(segInfo)
    val chunkSet = state.newChunkSet(readers.take(20))
    val readSeg = rowReaderSegFromChunkSet(projection2, segInfo, chunkSet, schema)

    // Sum up all the NumArticles for first 20 rows
    readSeg.rowIterator().map(_.getInt(6)).sum should equal (127)
  }
}