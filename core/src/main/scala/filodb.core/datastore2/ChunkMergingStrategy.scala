package filodb.core.datastore2

import java.nio.ByteBuffer
import scala.concurrent.Future

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
   */
  def readSegmentForCache[K](dataset: TableName,
                             partition: String,
                             version: Int,
                             segmentId: ByteBuffer): Future[Segment[K]]

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