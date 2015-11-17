package filodb.core.metadata

import java.nio.ByteBuffer

import filodb.core.Types._
import scodec.bits.ByteVector


trait Chunk[R,S] {

  def chunkId: ChunkId

  def segmentInfo: SegmentInfo[R,S]

  def columnVectors: Array[ByteBuffer]

  def chunkOverrides: Option[Seq[(ChunkId, Seq[R])]]

  def numRows: Int

  override def toString: String = s"Chunk($segmentInfo/ $chunkId) rows($numRows)"
}


case class DefaultChunk[R,S](chunkId: ChunkId,
                           segmentInfo: SegmentInfo[R,S],
                           columnVectors: Array[ByteBuffer],
                           numRows: Int,
                           chunkOverrides: Option[Seq[(ChunkId, Seq[R])]] = None) extends Chunk[R,S]
