package filodb.core.datastore2

import java.nio.ByteBuffer

import org.scalatest.FunSpec
import org.scalatest.Matchers

class ChunkRowMapSpec extends FunSpec with Matchers {
  describe("UpdatableChunkRowMap") {
    implicit val keyHelper = TimestampKeyHelper(10000L)
    it("should be able to add individual key -> (chunkId, rowNum) pairs") {
      val rowIndex = new UpdatableChunkRowMap[Long]
      rowIndex.update(1001L, 0, 0)
      rowIndex.update(1002L, 0, 1)
      rowIndex.update(1002L, 1, 0)
      rowIndex.chunkIdIterator.toSeq should equal (Seq(0, 1))
      rowIndex.rowNumIterator.toSeq should equal (Seq(0, 0))
      rowIndex.nextChunkId should equal (2)
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