package filodb.cassandra.columnstore

import filodb.core.Messages._
import filodb.core.Types.{ChunkId, ColumnId}
import filodb.core.metadata._
import filodb.core.store.ChunkStore

import scala.concurrent.Future

trait CassandraChunkStore extends ChunkStore {
  def chunkTable: ChunkTable

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
