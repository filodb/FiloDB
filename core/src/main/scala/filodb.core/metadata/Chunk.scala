package filodb.core.metadata

import java.nio.ByteBuffer

import filodb.core.Types._


trait Chunk {

  def segmentInfo: SegmentInfo

  def columnVectors: Array[ByteBuffer]

  def keys: Seq[_]

}

trait ChunkWithId extends Chunk {
  def chunkId: ChunkId
}

trait ChunkWithMeta extends ChunkWithId {

  def chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]]

  def numRows: Int

  override def toString: String = s"Chunk($segmentInfo/ $chunkId) rows($numRows)"

}

case class FlushedChunk(segmentInfo: SegmentInfo,
                        keys: Seq[Any],
                        sortedKeyRange: KeyRange[Any],
                        columnVectors: Array[ByteBuffer]) extends Chunk

case class DefaultChunk(chunkId: ChunkId,
                        keys: Seq[Any],
                        segmentInfo: SegmentInfo,
                        columnVectors: Array[ByteBuffer],
                        numRows: Int,
                        chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]] = None) extends ChunkWithMeta
