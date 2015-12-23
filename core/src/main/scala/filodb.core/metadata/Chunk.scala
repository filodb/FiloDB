package filodb.core.metadata

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import com.typesafe.scalalogging.slf4j.StrictLogging
import filodb.core.KeyType
import filodb.core.Types._
import filodb.core.util.ByteBufferOutputStream
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

  def metaDataByteSize: Int =
    4 + chunkOverrides.fold(0)(f =>
      f.map { case (cid, seq) => seq.length }.sum
    ) + 100

  def keySize(keyType: KeyType): Int = 4 + keys.map { key =>
    keyType.size(key.asInstanceOf[keyType.T])
  }.sum + 100
}

case class DefaultChunk(chunkId: ChunkId,
                        keys: Seq[Any],
                        columns: Seq[ColumnId],
                        columnVectors: Array[ByteBuffer],
                        numRows: Int,
                        chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]] = None) extends ChunkWithMeta


object SimpleChunk extends StrictLogging {
  def writeMetadata(byteBuffer: ByteBuffer, numRows: Int, chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]]): Unit = {
    val baos = new ByteBufferOutputStream(byteBuffer)
    val os = new DataOutputStream(baos)
    chunkOverrides match {
      case Some(overrides) =>
        val length = overrides.length
        logger.debug(s"Chunk Metadata num overrides is $length")
        os.writeInt(length)
        overrides.foreach { case (cid, positions) =>
          val posLength = positions.length
          logger.debug(s"Chunk Metadata writing $posLength overridden positions for $cid")
          os.writeInt(cid)
          os.writeInt(posLength)
          if (positions.nonEmpty) positions.foreach(os.writeInt)
        }
      case None => os.writeInt(0)
    }
    os.writeInt(numRows)
    os.flush()
    baos.flush()
    byteBuffer.flip()
  }

  def writeKeys(byteBuffer: ByteBuffer, keys: Seq[_], keyType: KeyType): Unit = {
    val baos = new ByteBufferOutputStream(byteBuffer)
    val os = new DataOutputStream(baos)
    os.writeInt(keys.length)
    keys.foreach { key =>
      val (l, keyBytes) = keyType.toBytes(key.asInstanceOf[keyType.T])
      os.writeInt(l)
      os.write(keyBytes.toArray)
    }
    os.flush()
    baos.flush()
    byteBuffer.flip()
  }

  def keysFromByteBuffer(keyBuffer: ByteBuffer, keyType: KeyType): Seq[_] = {
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

  def metaDataFromByteBuffer(byteBuffer: ByteBuffer)
  : (Int, Option[Seq[(ChunkId, Seq[Int])]]) = {
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


case class SimpleChunk(projection: Projection,
                       columns: Seq[ColumnId],
                       chunkId: ChunkId,
                       columnVectors: Array[ByteBuffer],
                       keyBuffer: ByteBuffer,
                       metadataBuffer: ByteBuffer) extends ChunkWithMeta {


  private val metadata = SimpleChunk.metaDataFromByteBuffer(metadataBuffer)

  def chunkOverrides: Option[Seq[(ChunkId, Seq[Int])]] = metadata._2

  def numRows: Int = metadata._1

  override def keys: Seq[_] =
    SimpleChunk.keysFromByteBuffer(keyBuffer, projection.keyType)


}

