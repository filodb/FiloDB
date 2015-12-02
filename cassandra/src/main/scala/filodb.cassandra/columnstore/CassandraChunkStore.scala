package filodb.cassandra.columnstore

import filodb.core.Messages
import filodb.core.Messages._
import filodb.core.Types.{ChunkId, ColumnId}
import filodb.core.metadata._
import filodb.core.store.ChunkStore
import filodb.core.util.Iterators._
import scala.concurrent.Future

trait CassandraChunkStore extends ChunkStore {

  import scala.concurrent.ExecutionContext.Implicits.global

  def chunkTable: ChunkTable

  final val META_COLUMN_NAME = "_metadata_"
  final val KEYS_COLUMN_NAME = "_keys_"

  override def appendChunk(projection: Projection,
                           partition: Any,
                           segment: Any,
                           chunk: ChunkWithMeta): Future[Boolean] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])._2.toByteBuffer
    val segmentId = segment.toString
    var responses = projection.schema.zipWithIndex.map { case (col, i) =>
      val chunkData = chunk.columnVectors(i)
      responseToBoolean(chunkTable.writeChunks(projection, pk, col.name, segmentId, chunk.chunkId, chunkData))
    }
    val metadataBuf = SimpleChunk.metadataAsByteBuffer(chunk.numRows, chunk.chunkOverrides)
    val keysBuf = SimpleChunk.keysAsByteBuffer(chunk.keys, projection.keyType)
    val metaInsert = responseToBoolean(
      chunkTable.writeChunks(projection, pk, META_COLUMN_NAME, segmentId, chunk.chunkId, metadataBuf))
    val keysInsert = responseToBoolean(
      chunkTable.writeChunks(projection, pk, KEYS_COLUMN_NAME, segmentId, chunk.chunkId, keysBuf))
    responses = responses :+ metaInsert
    responses = responses :+ keysInsert
    (Future sequence responses).map { results =>
      results.reduce(_ & _)
    }
  }

  private def responseToBoolean(f: Future[Messages.Response]) = {
    f.map {
      case Success => true
      case _ => false
    }
  }

  override def getAllChunksForSegments(projection: Projection,
                                       partition: Any,
                                       segmentRange: KeyRange[_],
                                       columns: Seq[ColumnId]): Future[Seq[(Any, Seq[ChunkWithMeta])]] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])._2.toByteBuffer

    for {
      dataResult <- Future sequence columns.map { col =>
        chunkTable.getDataBySegmentAndChunk(projection, pk, col,
          segmentRange.start.toString,
          segmentRange.end.toString)
      }
      metaResult <- chunkTable.getChunkData(projection, pk, META_COLUMN_NAME,
        segmentRange.start.toString,
        segmentRange.end.toString)

      keysResult <- chunkTable.getDataBySegmentAndChunk(projection, pk, KEYS_COLUMN_NAME,
        segmentRange.start.toString,
        segmentRange.end.toString)


      segmentChunks = metaResult.map { case (segmentId, chunkId, metadata) =>
        val colBuffers = (0 until columns.length).map(i => dataResult(i)((segmentId, chunkId))).toArray
        val keysBuffer = keysResult((segmentId, chunkId))
        (segmentId, chunkId) -> SimpleChunk(projection, columns, chunkId, colBuffers, keysBuffer, metadata)
      }.iterator

      res = segmentChunks.sortedGroupBy(i => i._1._1).map { case (segmentId, seq) =>
        (segmentId, seq.map(_._2).toSeq)
      }.toSeq
    } yield res

  }

  override def getKeySets(projection: Projection,
                          partition: Any,
                          segment: Any,
                          columns: Seq[String],
                          chunkIds: Seq[ChunkId]): Future[Seq[(ChunkId, Seq[_])]] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])._2.toByteBuffer
    val segmentId = segment.toString
    chunkTable.
      getColumnData(projection, pk,
        KEYS_COLUMN_NAME,
        segmentId,
        chunkIds.toList).map { seq =>
      seq.map { case (chunkId, bb) =>
        (chunkId, SimpleChunk.keysFromByteBuffer(bb, projection.keyType))
      }
    }
  }

}
