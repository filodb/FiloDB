package filodb.core.columnstore

import filodb.core._
import java.nio.ByteBuffer

import org.scalatest.FunSpec
import org.scalatest.Matchers

class ChunkRowMapSpec extends FunSpec with Matchers {
  describe("UpdatableChunkRowMap") {
    implicit val keyHelper = LongKeyHelper
    it("should be able to add individual key -> (chunkId, rowNum) pairs") {
      val rowIndex = new UpdatableChunkRowMap[Long]
      rowIndex.update(1001L, 0, 0)
      rowIndex.update(1002L, 0, 1)
      rowIndex.update(1002L, 1, 0)
      rowIndex.chunkIdIterator.toSeq should equal (Seq(0, 1))
      rowIndex.rowNumIterator.toSeq should equal (Seq(0, 0))
      rowIndex.nextChunkId should equal (2)
    }

    it("should be able to create a new index from items") {
      val rowIndex = new UpdatableChunkRowMap[Long]
      val newIndex = rowIndex ++ Seq(1001L -> (0 -> 0), 1002L -> (0 -> 1), 1002L -> (1 -> 0))
      newIndex.chunkIdIterator.toSeq should equal (Seq(0, 1))
      newIndex.rowNumIterator.toSeq should equal (Seq(0, 0))
      newIndex.nextChunkId should equal (2)
    }

    it("should be able to ++ another treeMap or sequence of items") {
      val rowIndex = new UpdatableChunkRowMap[Long]
      rowIndex.update(1001L, 0, 0)
      rowIndex.update(1002L, 0, 1)

      val newItems = Seq(1002L -> (1 -> 0), 999L -> (1 -> 2))

      val index2 = rowIndex ++ newItems
      index2.chunkIdIterator.toSeq should equal (Seq(1, 0, 1))
      index2.rowNumIterator.toSeq should equal (Seq(2, 0, 0))
    }

    it("should be able to serialize to Filo vectors") {
      val rowIndex = new UpdatableChunkRowMap[Long]
      rowIndex.update(1001L, 0, 0)
      rowIndex.update(1002L, 0, 1)
      val (chunkIdBuf, rowNumBuf) = rowIndex.serialize()

      val readIndex = new BinaryChunkRowMap(chunkIdBuf, rowNumBuf, 2)
      readIndex.chunkIdIterator.toSeq should equal (Seq(0, 0))
      readIndex.rowNumIterator.toSeq should equal (Seq(0, 1))
    }
  }
}