package filodb.core.datastore2

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}

/**
 * The ChunkMergingStrategy implements the storage-independent business logic to merge segments and
 * chunks/indices within segments.  It is the secret sauce that allows FiloDB to efficiently merge in
 * new changes and still have a read-optimized column store.  The workflow looks like this:
 *
 *   val oldSegment = segmentCache.getOrElsePut(key, readSegmentForCache(...))
 *   val mergedSegment = mergeSegments(oldSegment, newSegment)
 *   writeSegment(mergedSegment)
 *   segmentCache.update(key, pruneForCache(mergedSegment))
 *
 * It is likely that Strategies will need access to the MetaStore -- eg, what is the sort key for a dataset's
 * prime projection, what are all the columns that need to be rewritten etc.
 */
trait ChunkMergingStrategy {
  import Types._

  /**
   * Reads the minimum necessary amount of data to populate the segment cache for merging operations.
   * This reads back one segment.  For example, if the merging operation does not need existing chunks,
   * but only the sort key column, then this can read back just the ChunkRowMap and the sort key vectors.
   *
   * @param keyRange the keyRange of the segment to read.  It's important this corresponds to one segment.
   */
  def readSegmentForCache[K : SortKeyHelper](keyRange: KeyRange[K], version: Int): Future[Segment[K]]

  /**
   * Merges an existing segment cached using readSegmentForCache with a new partial segment to be inserted.
   * @param oldSegment the existing segment from the cache, populated using readSegmentForCache
   * @param newSegment the new partial segment containing only new data/rows to be inserted
   * @returns a merged Segment ready to be flushed to disk.  This typically will only include chunks that need
   *          to be updated or written to disk.
   */
  def mergeSegments[K](oldSegment: Segment[K], newSegment: Segment[K]): Segment[K]

  /**
   * Prunes a segment to only what needs to be cached.
   */
  def pruneForCache[K](segment: Segment[K]): Segment[K]
}

/**
 * The AppendingChunkMergingStrategy implements what is described in doc/sorted_chunk_merge.md
 * Basically instead of modifying existing chunks in place, it appends new data as new chunks, and modifies
 * a ChunkRowMap index so that chunks can be read in proper sort key order.
 * This strategy trades off some read speed for mostly append only and much faster write semantics
 * (only modifying the chunkRowMap in place), and also works for multi-version merging.
 *
 * @param columnStore the column store to use
 * @param getSortColumn a function that returns the sort column given a dataset
 */
class AppendingChunkMergingStrategy(columnStore: ColumnStore,
                                    getSortColumn: Types.TableName => Types.ColumnId)
                                   (implicit ec: ExecutionContext)
extends ChunkMergingStrategy with StrictLogging {
  import Types._

  // We only need to read back the sort column in order to merge with another segment's sort column
  def readSegmentForCache[K : SortKeyHelper](keyRange: KeyRange[K], version: Int): Future[Segment[K]] = {
    columnStore.readSegments(Set(getSortColumn(keyRange.dataset)), keyRange, version).map { iter =>
      iter.toSeq.headOption match {
        case Some(firstSegment) => firstSegment
        case None =>
          logger.debug(s"No segment/ChunkRowMap found for $keyRange, creating a new one")
          new GenericSegment(keyRange, new UpdatableChunkRowMap[K])
      }
    }
  }

  def mergeSegments[K](oldSegment: Segment[K], newSegment: Segment[K]): Segment[K] = {
    // TODO: actually implement the merge.  For now, just return the new segment.
    newSegment
  }

  // We only need to store the sort column
  def pruneForCache[K](segment: Segment[K]): Segment[K] = {
    val sortColumn = getSortColumn(segment.dataset)
    if (segment.getColumns == Set(sortColumn)) {
      segment
    } else {
      val prunedSeg = new GenericSegment(segment.keyRange, segment.index)
      segment.getChunks
             .filter { case (column, chunkId, chunk) => column == sortColumn }
             .foreach { case (_, chunkId, chunk) =>
        prunedSeg.addChunk(chunkId, sortColumn, chunk)
      }
      prunedSeg
    }
  }
}