package filodb.cassandra.columnstore

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import com.typesafe.config.Config
import filodb.core.Messages._
import filodb.core.Types.{ChunkId, ColumnId}
import filodb.core.metadata._
import filodb.core.store.ChunkStore
import it.unimi.dsi.io.ByteBufferInputStream
import org.velvia.filo.FastFiloRowReader

import scala.concurrent.Future

class CassandraChunkStore(config: Config) extends ChunkStore {
  val chunkTable = new ChunkTable(config)

  override def appendChunk(projection: Projection,
                           partition: Any,
                           segment: Any,
                           chunk: ChunkWithMeta): Future[Boolean] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])
    val segmentId = segment.toString
    chunkTable.writeChunks(projection, pk.toByteBuffer, segmentId, chunk).map {
      case Success => true
      case _ => false
    }
  }


  override def getAllChunksForSegments(projection: Projection,
                                       partition: Any,
                                       segmentRange: KeyRange[_],
                                       columns: Seq[ColumnId]): Future[Seq[(Any, Seq[ChunkWithMeta])]] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])

    val chunks = chunkTable.readChunksForSegmentRange(projection, pk.toByteBuffer,
      segmentRange.start.toString, segmentRange.end.toString,
      columns)
    chunks
  }

  override def getSegmentChunks(projection: Projection,
                                partition: Any,
                                segment: Any,
                                columns: Seq[String],
                                chunkIds: Seq[ChunkId]): Future[Seq[ChunkWithId]] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])
    val segmentId = segment.toString
    chunkTable.readChunks(projection, pk.toByteBuffer, segmentId, columns, chunkIds)
  }

}


case class SimpleChunk(projection: Projection, chunkId: ChunkId,
                       namedVectors: Map[String, ByteBuffer],
                       numRows: Int,
                       chunkOverrideBuffer: ByteBuffer) extends ChunkWithMeta {

  private val classes = projection.schema.map(_.columnType.clazz).toArray

  override def keys: Seq[_] = {
    val reader = new FastFiloRowReader(columnVectors, classes)
    val k = Array[_](numRows)
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

