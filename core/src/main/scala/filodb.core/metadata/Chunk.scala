package filodb.core.metadata

import java.io.{DataInputStream, DataOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import filodb.core.Types._
import filodb.core.reprojector.Reprojector.SegmentFlush
import it.unimi.dsi.io.ByteBufferInputStream
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

  override def toString: String = s"Chunk($chunkId) rows($numRows)"

}

case class DefaultChunk(chunkId: ChunkId,
                        keys: Seq[Any],
                        columnVectors: Array[ByteBuffer],
                        numRows: Int,
                        chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]] = None) extends ChunkWithMeta


object SimpleChunk {
  def chunkOverridesAsByteBuffer(chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]]): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val os = new DataOutputStream(baos)
    chunkOverrides match {
      case Some(overrides) =>
        os.writeInt(overrides.length)
        overrides.foreach { case (cid, seq) =>
          os.writeInt(cid)
          os.writeInt(seq.length)
          if (seq.nonEmpty) seq.foreach(os.writeInt)
        }
      case None => os.writeInt(0)
    }
    os.flush()
    baos.flush()
    ByteBuffer.wrap(baos.toByteArray)
  }

}


case class SimpleChunk(projection: Projection, chunkId: ChunkId,
                       namedVectors: Map[String, ByteBuffer],
                       numRows: Int,
                       chunkOverrideBuffer: ByteBuffer) extends ChunkWithMeta {

  private val classes = projection.schema.map(_.columnType.clazz).toArray

  override def keys: Seq[_] = {
    val reader = new FastFiloRowReader(columnVectors, classes)
    val k = Array[Any](numRows)
    (0 to numRows).foreach { i =>
      reader.rowNo = i
      k(i) = projection.keyFunction(reader)
    }
    k.toSeq
  }

  override def chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]] = {
    val is = new ByteBufferInputStream(chunkOverrideBuffer)
    val in = new DataInputStream(is)
    val length = in.readInt()
    if (length > 0) {
      val chunks = (0 to length).map { i =>
        val chunkId = in.readInt()
        val rowIdLength = in.readInt()
        val rowIds = (0 to rowIdLength).map(j => in.readInt()).toSeq
        chunkId -> rowIds
      }
      Some(chunks)
    } else {
      None
    }
  }

  override def columnVectors: Array[ByteBuffer] = projection.schema.map(col => namedVectors(col.name)).toArray
}

