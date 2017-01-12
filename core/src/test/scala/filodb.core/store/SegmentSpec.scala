package filodb.core.store

import java.nio.ByteBuffer

import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.query.{ChunkSetReader, RowkeyPartitionChunkIndex}

// TODO: probably all of these tests can be migrated to PartitionChunkIndexSpec and ChunkSetReaderSpec
class SegmentSpec extends FunSpec with Matchers with BeforeAndAfter {
  import NamesTestData._

  it("SegmentState should add chunk info properly and update state for append only") {
    val state = getState()
    val origId = state.nextChunkId
    Thread sleep 10   // Just to be sure the timeUUID will be greater

    val chunkSet1 = ChunkSet(state, mapper(names take 4).toIterator)
    chunkSet1.info.id should be > (origId)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.info.firstKey should equal (firstKey)
    chunkSet1.info.lastKey should equal (keyForName(3))
    chunkSet1.skips should equal (Nil)
    chunkSet1.chunks.size should equal (4)
    chunkSet1.chunks.keySet should equal (Set("first", "last", "age", "seg"))
    state.store(chunkSet1)
    state.add(chunkSet1)
    state.numChunks should equal (1)

    Thread sleep 10   // Just to be sure the timeUUID will be greater
    // Add new rows.  These do not replace previous ones at all.
    val chunkSet2 = ChunkSet(state, mapper(names drop 4).toIterator)
    chunkSet2.info.id should be > (chunkSet1.info.id)
    chunkSet2.info.numRows should equal (2)
    chunkSet2.skips should equal (Nil)
    chunkSet2.chunks.size should equal (4)
    state.add(chunkSet2)
  }

  it("SegmentState should add chunk info properly when SegmentState prepopulated") {
    val index = new RowkeyPartitionChunkIndex(defaultPartKey, projection)
    val state = new TestSegmentState(projection, index, schema, SegmentStateSettings())
    val info1 = ChunkSetInfo(state.nextChunkId, 10, firstKey, lastKey)
    index.add(info1, Nil)
    state.rowKeyChunks((firstKey, 4)) = Array.fill[ByteBuffer](projection.rowKeyColumns.length)(null)

    Thread sleep 10   // Just to be sure the timeUUID will be greater
    val chunkSet1 = ChunkSet(state, mapper(names take 4).toIterator)
    state.add(chunkSet1)
    state.numChunks should equal (2)
    chunkSet1.info.id should be > (info1.id)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.skips should have length (0)
  }

  it("SegmentState should add skip lists properly when new rows replace previous chunks") {
    val state = getState()

    val sortedNames = names.sortBy(_._3.get)
    val chunkSet1 = ChunkSet.withSkips(state, mapper(sortedNames take 4).toIterator)
    state.add(chunkSet1)
    state.numChunks should equal (1)
    chunkSet1.info.numRows should equal (4)
    chunkSet1.skips should equal (Nil)
    state.store(chunkSet1)

    Thread sleep 10   // Just to be sure the timeUUID will be greater
    // Add new rows.  These replace two of the previous rows - namely in rows 2, 3
    val chunkSet2 = ChunkSet.withSkips(state, mapper(sortedNames drop 2).toIterator)
    state.add(chunkSet2)
    chunkSet2.info.id should be > (chunkSet1.info.id)
    chunkSet2.info.numRows should equal (4)
    chunkSet2.skips should equal (Seq(ChunkRowSkipIndex(chunkSet1.info.id, Array(2, 3))))
  }

  it("SegmentState should not add skip lists if detectSkips=false") {
    val state = getState(0)

    val sortedNames = names.sortBy(_._3.get)
    val chunkSet1 = ChunkSet(state, mapper(sortedNames take 4).toIterator)
    state.add(chunkSet1)
    state.numChunks should equal (1)
    chunkSet1.skips should equal (Nil)
    state.store(chunkSet1)

    Thread sleep 10   // Just to be sure the timeUUID will be greater
    // Add new rows.  These replace two of the previous rows - namely in rows 2, 3
    val chunkSet2 = ChunkSet(state, mapper(sortedNames drop 2).toIterator)
    state.add(chunkSet2)
    chunkSet2.info.id should be > (chunkSet1.info.id)
    chunkSet2.info.numRows should equal (4)
    chunkSet2.skips should equal (Nil)
  }

  it("RowWriter and RowReader should work for rows with string row keys") {
    val stringProj = RichProjection(Dataset("a", "first", "seg"), schema)
    val state = new TestSegmentState(stringProj, schema)
    val chunkSet = ChunkSet(state, mapper(names).toIterator)
    val reader = ChunkSetReader(chunkSet, schema)

    reader.rowIterator().map(_.filoUTF8String(0)).toSeq should equal (utf8FirstNames)
  }

  it("RowWriter and RowReader should work for rows with multi-column row keys") {
    import GdeltTestData._
    val state = new TestSegmentState(projection2, schema, projection2.partKey(197901))
    val chunkSet = ChunkSet(state, readers.take(20).toIterator)
    val reader = ChunkSetReader(chunkSet, schema)

    // Sum up all the NumArticles for first 20 rows
    reader.rowIterator().map(_.getInt(6)).sum should equal (127)
  }
}