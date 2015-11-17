package filodb.core.store

import java.util.UUID

import filodb.core.KeyType
import filodb.core.Types._
import filodb.core.metadata._
import filodb.core.reprojector.SegmentFlush

import scala.concurrent.Future

trait ChunkStore {
  def appendChunk[R,S](chunk: Chunk[R,S]): Future[Boolean]

  def readChunksForSegments[R,S](segmentInfoSeq: Seq[SegmentInfo[R,S]],
                            columns: Seq[ColumnId]): Future[Map[SegmentInfo[R,S], Seq[Chunk[R,S]]]]
}

trait SummaryStore {
  type SegmentVersion = UUID

  /**
   * Atomically compare and swap the new SegmentSummary for this SegmentID
   */
  def compareAndSwapSummary[R,S](segmentVersion: SegmentVersion,
                            segmentInfo: SegmentInfo[R,S],
                            segmentSummary: SegmentSummary[R,S]): Future[Boolean]

  def readSegmentSummaries[R,S](projection: ProjectionInfo[R,S],
                           partitionKey: PartitionKey,
                           keyRange: KeyRange[S]): Future[Seq[SegmentSummary[R,S]]]

  def readSegmentSummary[R,S](segmentInfo: SegmentInfo[R,S]): Future[Option[(SegmentVersion, SegmentSummary[R,S])]]

}


trait ColumnStore {
  self: ChunkStore with SummaryStore =>

  import scala.concurrent.ExecutionContext.Implicits.global

  def readSegments[R,S](projection: ProjectionInfo[R,S],
                   partitionKey: PartitionKey,
                   columns: Seq[ColumnId],
                   keyRange: KeyRange[S]): Future[Seq[Segment[R,S]]] =
    for {
      segmentMeta <- readSegmentSummaries(projection, partitionKey, keyRange)
      segmentData <- readChunksForSegments(segmentMeta.map(_.segmentInfo), columns)
      mapping = segmentMeta.map(meta => DefaultSegment(meta.segmentInfo, segmentData(meta.segmentInfo)))
    } yield mapping

  /**
   * The latest segment summary is read along with the version
   * A new summary adding this flush chunks and keys is built.
   * We attempt to CAS the new summary.
   * If the attempt passes, we append the new chunks to the Segment.
   * There is a particular thorny edge case where the summary is stored but the chunk is not.
   */
  def flushToSegment[R,S](projection: ProjectionInfo[R,S], segmentFlush: SegmentFlush[R,S]): Future[Boolean] = {
    for {
      s <- readSegmentSummary(segmentFlush.segmentInfo)
      segmentSummary = s.map(_._2).getOrElse(DefaultSegmentSummary(projection.rowKeyType,
        segmentFlush.segmentInfo, None))

      segmentVersion = s.map(_._1).getOrElse(UUID.randomUUID())
      chunkId = segmentSummary.nextChunkId
      (chunkSummary, newSummary) = segmentSummary.withSortKeys(chunkId, segmentFlush.rowKeys)
      chunkOverrides = segmentSummary.buildOverrides(projection.rowKeyType,segmentFlush.rowKeys, chunkSummary.keyRange)

      newChunk = new DefaultChunk(chunkId, segmentFlush.segmentInfo, segmentFlush.columnVectors,
        segmentFlush.rowKeys.length, chunkOverrides)

      swapResult <- compareAndSwapSummary(segmentVersion, segmentFlush.segmentInfo, newSummary)
      // hoping that chunk will be appended always. If this fails, the db is corrupted!!
      result <- if (swapResult) appendChunk(newChunk) else Future(false)
    } yield result
  }


}

