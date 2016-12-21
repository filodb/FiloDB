package filodb.core.query

import filodb.core.store.ChunkSetInfo
import scodec.bits._

import org.scalatest.{FunSpec, Matchers}

class PartitionChunkIndexSpec extends FunSpec with Matchers {
  import filodb.core.NamesTestData._
  import PartitionChunkIndex.emptySkips

  val info1 = ChunkSetInfo(100L, 3, firstKey, lastKey)
  val info2 = ChunkSetInfo(99L, 3, keyForName(1), lastKey)

  describe("RowkeyPartitionChunkIndex") {
    it("should add out of order chunks and return in rowkey order") {
      val newIndex = new RowkeyPartitionChunkIndex(ByteVector(0), projection)

      // Initial index should be empty
      newIndex.numChunks should equal (0)

      // Add chunk info with no skips. Note that info1 has lower firstKey but higher id, on purpose
      newIndex.add(info1, Nil)
      newIndex.numChunks should equal (1)

      newIndex.add(info2, Nil)
      newIndex.numChunks should equal (2)

      newIndex.allChunks.toList.map(_._1) should equal (List(info1, info2))

      newIndex.rowKeyRange(firstKey, firstKey).toList.map(_._1) should equal (List(info1))
    }

    it("should return no chunks if rowKeyRange startKey is greater than endKey") {
      val newIndex = new RowkeyPartitionChunkIndex(ByteVector(0), projection)
      newIndex.add(info1, Nil)
      newIndex.add(info2, Nil)

      newIndex.rowKeyRange(lastKey, firstKey).toList.map(_._1) should equal (Nil)
    }

    it("should handle skips") (pending)
  }

  describe("ChunkIDPartitionChunkIndex") {
    it("should add out of order chunks and return in chunkID order") {
      val newIndex = new ChunkIDPartitionChunkIndex(ByteVector(0), projection)

      // Initial index should be empty
      newIndex.numChunks should equal (0)

      // Add chunk info with no skips. Note that info1 has lower firstKey but higher id, on purpose
      newIndex.add(info1, Nil)
      newIndex.numChunks should equal (1)

      newIndex.add(info2, Nil)
      newIndex.numChunks should equal (2)

      newIndex.allChunks.toList.map(_._1) should equal (List(info2, info1))

      newIndex.rowKeyRange(firstKey, firstKey).toList.map(_._1) should equal (List(info1))
    }
  }
}