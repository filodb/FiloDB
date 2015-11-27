package filodb.core.store

import java.util.UUID

import filodb.core.Types._
import filodb.core.metadata._
import filodb.core.reprojector.Reprojector.SegmentFlush

import scala.concurrent.Future

trait ChunkStore {
  protected def appendChunk(projection: Projection,
                            partition: Any,
                            segment: Any,
                            chunk: ChunkWithMeta): Future[Boolean]

  protected def getSegmentChunks(projection: Projection,
                                 partition: Any,
                                 segment: Any,
                                 columns: Seq[ColumnId],
                                 chunkIds: Seq[ChunkId]): Future[Seq[ChunkWithId]]

  protected def getAllChunksForSegments(projection: Projection,
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
  protected def compareAndSwapSummary(projection: Projection,
                                      partition: Any,
                                      segment: Any,
                                      oldVersion: Option[SegmentVersion],
                                      segmentVersion: SegmentVersion,
                                      segmentSummary: SegmentSummary): Future[Boolean]

  protected def readSegmentSummary(projection: Projection,
                                   partition: Any,
                                   segment: Any)
  : Future[Option[(SegmentVersion, SegmentSummary)]]

  protected def newVersion: SegmentVersion

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
   * There is a particular thorny edge case where the summary is stored but the chunk is not.
   */
  def flushToSegment(projection: Projection,
                     partition: Any,
                     segment: Any,
                     segmentFlush: SegmentFlush): Future[Boolean] = {
    for {
    // first get version and summary for this segment
      (oldVersion, segmentSummary) <- getVersionAndSummaryWithDefaults(projection, partition, segment)
      // make a new chunk using this version of summary from the flush
      newChunk <- newChunkFromSummary(projection, partition, segment, segmentFlush, segmentSummary)
      // now make a new summary with keys from the new flush and the new chunk id
      newSummary = segmentSummary.withKeys(newChunk.chunkId, segmentFlush.keys)
      // Compare and swap the new summary and chunk if the version stayed the same
      result <- compareAndSwapSummaryAndChunk(projection, partition, segment,
        oldVersion, newVersion, newChunk, newSummary)

    } yield result
  }

  protected[store] def compareAndSwapSummaryAndChunk(projection: Projection,
                                              partition: Any,
                                              segment: Any,
                                              oldVersion: Option[SegmentVersion],
                                              newVersion: SegmentVersion,
                                              newChunk: ChunkWithMeta,
                                              newSummary: SegmentSummary): Future[Boolean] = {
    for {
    // Compare and swap the new summary if the version stayed the same
      swapResult <- compareAndSwapSummary(projection, partition, segment, oldVersion, newVersion, newSummary)
      // hoping that chunk will be appended always. If this fails, the db is corrupted!!
      result <- if (swapResult) appendChunk(projection, partition, segment, newChunk) else Future(false)
    } yield result
  }

  private[store] def getVersionAndSummaryWithDefaults(projection: Projection,
                                                      partition: Any,
                                                      segment: Any) = {
    for {
      versionAndSummary <- readSegmentSummary(projection, partition, segment)
      (oldVersion, segmentSummary) = versionAndSummary match {
        case Some(vAndS) => (Some(vAndS._1), vAndS._2)
        case None => (None, DefaultSegmentSummary(projection.keyType, None))
      }
    } yield (oldVersion, segmentSummary)
  }

  /**
   * For testing only
   */
  private[store] def newChunkFromSummary(projection: Projection,
                                         partition: Any,
                                         segment: Any,
                                         segmentFlush: SegmentFlush,
                                         segmentSummary: SegmentSummary) = {
    val chunkId = segmentSummary.nextChunkId
    val possibleOverrides = segmentSummary.possibleOverrides(segmentFlush.keys)
    for {
      possiblyOverriddenChunks <- possibleOverrides.map { o =>
        getSegmentChunks(projection, partition, segment, projection.keyColumns, o)
      }.getOrElse(Future(List.empty[ChunkWithId]))

      chunkOverrides = segmentSummary.actualOverrides(segmentFlush.keys, possiblyOverriddenChunks)

      newChunk = new DefaultChunk(chunkId,
        segmentFlush.keys,
        segmentFlush.columnVectors,
        segmentFlush.keys.length,
        if (chunkOverrides.isEmpty) None else Some(chunkOverrides))

    } yield newChunk
  }


}

