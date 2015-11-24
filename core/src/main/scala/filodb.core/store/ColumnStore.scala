package filodb.core.store

import java.util.UUID

import filodb.core.Types._
import filodb.core.metadata._

import scala.concurrent.Future

trait ChunkStore {
  def appendChunk(projection: Projection,
                  partition: Any,
                  segment: Any,
                  chunk: ChunkWithMeta): Future[Boolean]

  def getSegmentChunks(projection: Projection,
                       partition: Any,
                       segment: Any,
                       columns: Seq[ColumnId],
                       chunkIds: Seq[ChunkId]): Future[Seq[ChunkWithId]]

  def getAllChunksForSegments(projection: Projection,
                              partitionKey: Any,
                              segmentRange: KeyRange[_],
                              columns: Seq[ColumnId])
  : Future[Seq[(Any, Seq[ChunkWithMeta])]]
}

trait SummaryStore {
  type SegmentVersion = UUID

  /**
   * Atomically compare and swap the new SegmentSummary for this SegmentID
   */
  def compareAndSwapSummary(projection: Projection,
                            partition: Any,
                            segment: Any,
                            oldVersion: Option[SegmentVersion],
                            segmentVersion: SegmentVersion,
                            segmentSummary: SegmentSummary): Future[Boolean]

  def readSegmentSummary(projection: Projection,
                         partition: Any,
                         segment: Any)
  : Future[Option[(SegmentVersion, SegmentSummary)]]

  def newVersion: SegmentVersion

}


trait ColumnStore {
  self: ChunkStore with SummaryStore =>

  import scala.concurrent.ExecutionContext.Implicits.global

  def readSegments(projection: Projection,
                   partitionKey: Any,
                   columns: Seq[ColumnId],
                   segmentRange: KeyRange[_]): Future[Seq[Segment]] =
    for {
      segmentData <- getAllChunksForSegments(projection, partitionKey, segmentRange, columns)
      mapping = segmentData.map { case (segmentId, data) =>
        DefaultSegment(projection, partitionKey, segmentId, data)
      }
    } yield mapping

  /**
   * The latest segment summary is read along with the version
   * A new summary adding this flush chunks and keys is built.
   * We attempt to CAS the new summary.
   * If the attempt passes, we append the new chunks to the Segment.
   * There is a particular thorny edge case where the summary is stored but the chunk is not.
   */
  def flushToSegment(projection: Projection,
                     partition: Any,
                     segment: Any,
                     flushedChunk: FlushedChunk): Future[Boolean] = {
    for {
      versionAndSummary <- readSegmentSummary(projection, partition, segment)
      (oldVersion, segmentSummary) = versionAndSummary match {
        case Some(vAndS) => (Some(vAndS._1), vAndS._2)
        case None => (None, DefaultSegmentSummary(projection.keyType, None))
      }

      chunkId = segmentSummary.nextChunkId
      newSummary = segmentSummary.withKeys(chunkId, flushedChunk.keys)

      possibleOverrides = segmentSummary.possibleOverrides(flushedChunk.keys)

      possiblyOverriddenChunks <- possibleOverrides.map { o =>
        getSegmentChunks(projection, partition, segment, projection.keyColumns, o)
      }.getOrElse(Future(List.empty[ChunkWithId]))

      chunkOverrides = segmentSummary.actualOverrides(flushedChunk.keys, possiblyOverriddenChunks)

      newChunk = new DefaultChunk(chunkId,
        flushedChunk.keys,
        flushedChunk.columnVectors,
        flushedChunk.keys.length,
        if (chunkOverrides.isEmpty) None else Some(chunkOverrides))

      swapResult <- compareAndSwapSummary(projection, partition, segment, oldVersion, newVersion, newSummary)
      // hoping that chunk will be appended always. If this fails, the db is corrupted!!
      result <- if (swapResult) appendChunk(projection, partition, segment, newChunk) else Future(false)
    } yield result
  }


}

