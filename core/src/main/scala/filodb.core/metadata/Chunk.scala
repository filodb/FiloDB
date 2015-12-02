package filodb.core.metadata

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import filodb.core.KeyType
import filodb.core.Types._
import it.unimi.dsi.io.ByteBufferInputStream
import scodec.bits.ByteVector


trait KeySet {
  def keys: Seq[_]
}

case class SimpleKeySet(keys: Seq[_]) extends KeySet

trait Chunk extends KeySet {

  def columns: Seq[ColumnId]

  def columnVectors: Array[ByteBuffer]

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
                        columns: Seq[ColumnId],
                        columnVectors: Array[ByteBuffer],
                        numRows: Int,
                        chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]] = None) extends ChunkWithMeta


object SimpleChunk {
  def metadataAsByteBuffer(numRows: Int, chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]]): ByteBuffer = {
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
    os.writeInt(numRows)
    os.flush()
    baos.flush()
    ByteBuffer.wrap(baos.toByteArray)
  }

  def keysAsByteBuffer(keys: Seq[_], keyType: KeyType):ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val os = new DataOutputStream(baos)
    keys.foreach { key =>
      val (l, keyBytes) = keyType.toBytes(key.asInstanceOf[keyType.T])
      os.writeInt(l)
      os.write(keyBytes.toArray)
    }
    os.flush()
    baos.flush()
    ByteBuffer.wrap(baos.toByteArray)
  }

  def keysFromByteBuffer(keyBuffer: ByteBuffer, keyType: KeyType):Seq[_] = {
    val is = new ByteBufferInputStream(keyBuffer)
    val in = new DataInputStream(is)
    val length = in.readInt()
    (0 until length).map { i =>
      val keyLength = in.readInt()
      val byteArray = new Array[Byte](keyLength)
      in.read(byteArray)
      keyType.fromBytes(ByteVector(byteArray))
    }
  }

}


case class SimpleChunk(projection: Projection,
                       columns: Seq[ColumnId],
                       chunkId: ChunkId,
                       columnVectors: Array[ByteBuffer],
                       keyBuffer: ByteBuffer,
                       metadataBuffer: ByteBuffer) extends ChunkWithMeta {


  private val metadata = metaDataFromByteBuffer(metadataBuffer)

  def chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]] = metadata._2

  def numRows: Int = metadata._1

  override def keys: Seq[_] =
    SimpleChunk.keysFromByteBuffer(keyBuffer, projection.keyType)

  private def metaDataFromByteBuffer(byteBuffer: ByteBuffer) = {
    val is = new ByteBufferInputStream(byteBuffer)
    val in = new DataInputStream(is)
    val length = in.readInt()
    val overrides = if (length > 0) {
      val chunks = (0 until length).map { i =>
        val chunkId = in.readInt()
        val rowIdLength = in.readInt()
        val rowIds = (0 until rowIdLength).map(j => in.readInt()).toSeq
        chunkId -> rowIds
      }
      Some(chunks)
    } else {
      None
    }
    val numRows = in.readInt()
    (numRows, overrides)
  }

}

