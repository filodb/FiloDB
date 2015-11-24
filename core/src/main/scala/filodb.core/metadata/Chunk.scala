package filodb.core.metadata

import java.nio.ByteBuffer

import filodb.core.Types._
import org.velvia.filo.{FastFiloRowReader, RowReader}


trait Chunk {

  def columnVectors: Array[ByteBuffer]

  def keys: Seq[_]

}

trait ChunkWithId extends Chunk {
  def chunkId: ChunkId
}


trait ChunkWithMeta extends ChunkWithId {

  def chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]]

  def numRows: Int

  override def toString: String = s"Chunk($$chunkId) rows($numRows)"

}

case class FlushedChunk(projection: Projection,
                        partition: Any,
                        segment: Any,
                        keys: Seq[Any],
                        sortedKeyRange: KeyRange[Any],
                        columnVectors: Array[ByteBuffer]) extends Chunk

case class DefaultChunk(chunkId: ChunkId,
                        keys: Seq[Any],
                        columnVectors: Array[ByteBuffer],
                        numRows: Int,
                        chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]] = None) extends ChunkWithMeta
