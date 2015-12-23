package filodb.cassandra.columnstore

import com.typesafe.scalalogging.slf4j.StrictLogging
import filodb.core.Messages._
import filodb.core.Types.ChunkId
import filodb.core.metadata._
import filodb.core.query.ScanInfo
import filodb.core.store.ChunkStore
import filodb.core.util.Iterators._
import filodb.core.util.MemoryPool

import scala.concurrent.Future

trait CassandraChunkStore extends ChunkStore with MemoryPool with StrictLogging{

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

    val metaDataSize = chunk.metaDataByteSize
    logger.debug(s"Acquiring buffer of size $metaDataSize for ChunkMetadata")
    val metadataBuf = acquire(metaDataSize)
    val keySize = chunk.keySize(projection.keyType)
    logger.debug(s"Acquiring buffer of size $keySize for Chunk Key Buffer")
    val keysBuf = acquire(keySize)
    SimpleChunk.writeMetadata(metadataBuf, chunk.numRows, chunk.chunkOverrides)
    SimpleChunk.writeKeys(keysBuf, chunk.keys, projection.keyType)

    chunkTable.writeChunks(projection, pk,
      projection.columnNames ++ Seq(META_COLUMN_NAME, KEYS_COLUMN_NAME),
      segmentId, chunk.chunkId,
      chunk.columnVectors ++ Seq(metadataBuf, keysBuf))
      .map {
      case Success => {
        release(metadataBuf)
        release(keysBuf)
        true
      }
      case _ => {
        release(metadataBuf)
        release(keysBuf)
        false
      }
    }
  }


  override def getChunks(scanInfo: ScanInfo): Future[Seq[((Any, Any), Seq[ChunkWithMeta])]] = {
    val columns = scanInfo.columns
    val projection = scanInfo.projection
    for {
      metaResult <- chunkTable.getChunkData(scanInfo, META_COLUMN_NAME)

      dataResult <- Future sequence columns.map { col =>
        chunkTable.getDataBySegmentAndChunk(scanInfo, col)
      }

      keysResult <- chunkTable.getDataBySegmentAndChunk(scanInfo, KEYS_COLUMN_NAME)


      segmentChunks = metaResult.map { case (pk, segmentId, chunkId, metadata) =>
        val colBuffers = (0 until columns.length).map(i => dataResult(i)((pk, segmentId, chunkId))).toArray
        val keysBuffer = keysResult((pk, segmentId, chunkId))
        (pk, segmentId, chunkId) -> SimpleChunk(projection, columns, chunkId, colBuffers, keysBuffer, metadata)
      }.iterator

      res = segmentChunks.sortedGroupBy(i => (i._1._1, i._1._2)).map { case (segmentId, seq) =>
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
