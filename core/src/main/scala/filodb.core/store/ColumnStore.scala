package filodb.core.store

import java.util.UUID

import filodb.core.Types._
import filodb.core.metadata._

import scala.concurrent.Future

trait ChunkStore {
  def appendChunk(chunk: ChunkWithMeta): Future[Boolean]

  def getChunks(segment: SegmentInfo,
                chunkIds: Seq[ChunkId]): Seq[ChunkWithId]

  def getAllChunksForSegments(segmentInfoSeq: Seq[SegmentInfo],
                              columns: Seq[ColumnId])
  : Future[Map[SegmentInfo, Seq[ChunkWithMeta]]]
}

trait SummaryStore {
  type SegmentVersion = UUID

  /**
   * Atomically compare and swap the new SegmentSummary for this SegmentID
   */
  def compareAndSwapSummary(segmentVersion: SegmentVersion,
                            segmentInfo: SegmentInfo,
                            segmentSummary: SegmentSummary): Future[Boolean]

  def readSegmentSummaries(projectionId: Projection,
                           partitionKey: Any,
                           segmentRange: KeyRange[_]): Future[Seq[SegmentSummary]]

  def readSegmentSummary(segmentInfo: SegmentInfo)
  : Future[Option[(SegmentVersion, SegmentSummary)]]

}


trait ColumnStore {
  self: ChunkStore with SummaryStore =>

  import scala.concurrent.ExecutionContext.Implicits.global

  def readSegments(projection: Projection,
                   partitionKey: Any,
                   columns: Seq[ColumnId],
                   segmentRange: KeyRange[_]): Future[Seq[Segment]] =
    for {
      segmentMeta <- readSegmentSummaries(projection, partitionKey, segmentRange)
      segmentData <- getAllChunksForSegments(segmentMeta.map(_.segmentInfo), columns)
      mapping = segmentMeta.map(meta => DefaultSegment(meta.segmentInfo, segmentData(meta.segmentInfo)))
    } yield mapping

  /**
   * The latest segment summary is read along with the version
   * A new summary adding this flush chunks and keys is built.
   * We attempt to CAS the new summary.
   * If the attempt passes, we append the new chunks to the Segment.
   * There is a particular thorny edge case where the summary is stored but the chunk is not.
   */
  def flushToSegment(projection: Projection,
                     flushedChunk: FlushedChunk): Future[Boolean] = {
    for {
      s <- readSegmentSummary(flushedChunk.segmentInfo)
      segmentSummary = s.map(_._2).getOrElse(
        DefaultSegmentSummary(projection.keyType, flushedChunk.segmentInfo, None))

      segmentVersion = s.fold(UUID.randomUUID())(_._1)
      chunkId = segmentSummary.nextChunkId
      (chunkSummary, newSummary) = segmentSummary.withKeys(chunkId, flushedChunk.keys, flushedChunk.sortedKeyRange)

      possibleOverrides = segmentSummary.possibleOverrides(flushedChunk.keys)
      possiblyOverriddenChunks = possibleOverrides.map(o =>
        getChunks(flushedChunk.segmentInfo, o.seq))
        .getOrElse(List.empty[ChunkWithId])

      chunkOverrides = segmentSummary.actualOverrides(flushedChunk.keys, possiblyOverriddenChunks)

      newChunk = new DefaultChunk(chunkId,
        flushedChunk.keys,
        flushedChunk.segmentInfo,
        flushedChunk.columnVectors,
        flushedChunk.keys.length,
        if (chunkOverrides.isEmpty) None else Some(chunkOverrides))

      swapResult <- compareAndSwapSummary(segmentVersion, flushedChunk.segmentInfo, newSummary)
      // hoping that chunk will be appended always. If this fails, the db is corrupted!!
      result <- if (swapResult) appendChunk(newChunk) else Future(false)
    } yield result
  }


}

