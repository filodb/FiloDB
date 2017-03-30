package filodb.core.query

import filodb.core.store.{ChunkSetInfo, ChunkRowSkipIndex}
import scodec.bits._

import org.scalatest.{FunSpec, Matchers}

class PartitionChunkIndexSpec extends FunSpec with Matchers {
  import filodb.core.NamesTestData._
  import PartitionChunkIndex.emptySkips
  import collection.JavaConverters._

  val info1 = ChunkSetInfo(100L, 3, firstKey, lastKey)
  val info2 = ChunkSetInfo(99L, 3, keyForName(1), lastKey)

  describe("RowkeyPartitionChunkIndex") {
    it("should add out of order chunks and return in rowkey order") {
      val newIndex = new RowkeyPartitionChunkIndex(defaultPartKey, projection)

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
      val newIndex = new RowkeyPartitionChunkIndex(defaultPartKey, projection)
      newIndex.add(info1, Nil)
      newIndex.add(info2, Nil)

      newIndex.rowKeyRange(lastKey, firstKey).toList.map(_._1) should equal (Nil)
    }
  }

  describe("ChunkIDPartitionChunkIndex") {
    it("should add out of order chunks and return in chunkID order") {
      val newIndex = new ChunkIDPartitionChunkIndex(defaultPartKey, projection)

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

    it("should handle skips") {
      val newIndex = new ChunkIDPartitionChunkIndex(defaultPartKey, projection)
      val origInfo = info1.copy(id = 9)
      val info2 = info1.copy(id = 14)
      newIndex.add(origInfo, Nil)
      newIndex.add(info1, Seq(ChunkRowSkipIndex(9, Array(2, 5))))
      newIndex.add(info2, Seq(ChunkRowSkipIndex(9, Array(3, 5, 8))))

      val infosAndSkips = newIndex.allChunks.toList
      infosAndSkips should have length (3)
      infosAndSkips(0)._1.id should equal (9)
      infosAndSkips(0)._2.toList.asScala should equal (Seq(2, 3, 5, 8))
    }
  }
}