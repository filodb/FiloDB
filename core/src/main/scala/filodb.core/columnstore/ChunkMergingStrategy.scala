package filodb.core.columnstore

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.concurrent.{ExecutionContext, Future}

import filodb.core.metadata.{Column, RichProjection}
import filodb.core.Types._
import filodb.core._

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
  /**
   * Reads the minimum necessary amount of data to populate the segment cache for merging operations.
   * This reads back one segment.  For example, if the merging operation does not need existing chunks,
   * but only the sort key column, then this can read back just the ChunkRowMap and the sort key vectors.
   *
   * @param keyRange the keyRange of the segment to read.  It's important this corresponds to one segment.
   */
  def readSegmentForCache[K](projection: RichProjection[K],
                             keyRange: KeyRange[K],
                             version: Int): Future[Segment[K]]

  /**
   * Merges an existing segment cached using readSegmentForCache with a new partial segment to be inserted.
   * @param oldSegment the existing segment from the cache, populated using readSegmentForCache
   * @param newSegment the new partial segment containing only new data/rows to be inserted
   * @return a merged Segment ready to be flushed to disk.  This typically will only include chunks that need
   *          to be updated or written to disk.
   */
  def mergeSegments[K: SortKeyHelper](oldSegment: Segment[K],
                                      newSegment: Segment[K]): Segment[K]

  /**
   * Prunes a segment to only what needs to be cached.
   */
  def pruneForCache[K](projection: RichProjection[K], segment: Segment[K]): Segment[K]
}

/**
 * The AppendingChunkMergingStrategy implements what is described in doc/sorted_chunk_merge.md
 * Basically instead of modifying existing chunks in place, it appends new data as new chunks, and modifies
 * a ChunkRowMap index so that chunks can be read in proper sort key order.
 * This strategy trades off some read speed for mostly append only and much faster write semantics
 * (only modifying the chunkRowMap in place), and also works well for multi-version merging.
 *
 * @param columnStore the column store to use
 * @param getSortColumn a function that returns the sort column given a dataset
 */
class AppendingChunkMergingStrategy(columnStore: ColumnStore)
                                   (implicit ec: ExecutionContext)
extends ChunkMergingStrategy with StrictLogging {
  // We only need to read back the sort column in order to merge with another segment's sort column
  def readSegmentForCache[K](projection: RichProjection[K],
                             keyRange: KeyRange[K],
                             version: Int): Future[Segment[K]] = {
    implicit val helper = projection.helper
    columnStore.readSegments(Seq(projection.sortColumn), keyRange, version).map { iter =>
      iter.toSeq.headOption match {
        case Some(firstSegment) => firstSegment
        case None =>
          logger.debug(s"No segment/ChunkRowMap found for $keyRange, creating a new one")
          new GenericSegment(keyRange, new UpdatableChunkRowMap[K])
      }
    }
  }

  def mergeSegments[K: SortKeyHelper](oldSegment: Segment[K],
                                      newSegment: Segment[K]): Segment[K] = {
    // NOTE: This only works for single sort keys
    val sortKeyFunc = implicitly[SortKeyHelper[K]].getSortKeyFunc(Seq(0))

    // One should NEVER be allowed to merge segments from different places... unless we are perhaps
    // talking about splitting and merging, but that's outside the scope of this method
    require(oldSegment.keyRange.start == newSegment.keyRange.start,
      s"Cannot merge segments from different keyRanges (${oldSegment.keyRange}, ${newSegment.keyRange})")

    // How much to offset chunkIds in newSegment.  0 in newSegment == nextChunkId in oldSegment.
    val offsetChunkId = oldSegment.index.nextChunkId

    // Merge old ChunkRowMap with new segment's ChunkRowMap with chunkIds offset
    // NOTE: Working with a TreeMap is probably not the most efficient way to merge two sorted lists
    // since both indexes can be read in sort key order.  So, TODO: replace this with more efficient
    // sorted iterator merge.  Probably not an issue for now.
    val baseIndex = oldSegment match {
      case g: GenericSegment[K] =>
        g.index.asInstanceOf[UpdatableChunkRowMap[K]]
      case rr: RowReaderSegment[K] =>
        // This should be a segment read from disk via readSegmentForCache().  Cheat assume only sort col
        val items = rr.rowChunkIterator().map { case (reader, chunkId, rowNum) =>
          sortKeyFunc(reader) -> (chunkId -> rowNum)
        }
        UpdatableChunkRowMap(items.toSeq)
    }
    val offsetNewTree = newSegment.index.asInstanceOf[UpdatableChunkRowMap[K]].
                                   index.mapValues { case (chunkId, rowNum) =>
                                     (chunkId + offsetChunkId, rowNum)
                                   }

    // Translate chunkIds from newSegment to (oldSegment.nextChunkId + _)
    val mergedSegment = new GenericSegment(oldSegment.keyRange, baseIndex ++ offsetNewTree)
    newSegment.getChunks
              .foreach { case (column, chunkId, chunk) =>
                mergedSegment.addChunk(chunkId + offsetChunkId, column, chunk)
              }

    mergedSegment
  }

  // We only need to store the sort column
  def pruneForCache[K](projection: RichProjection[K], segment: Segment[K]): Segment[K] = {
    val sortColumn = projection.sortColumn.name
    if (segment.getColumns == Set(sortColumn)) {
      logger.trace(s"pruneForcache: segment only has ${segment.getColumns}, not pruning")
      segment
    } else {
      val prunedSeg = new GenericSegment(segment.keyRange, segment.index)
      segment.getChunks
             .filter { case (column, chunkId, chunk) => column == sortColumn }
             .foreach { case (_, chunkId, chunk) =>
        prunedSeg.addChunk(chunkId, sortColumn, chunk)
      }
      logger.trace(s"Pruned segment: $prunedSeg")
      prunedSeg
    }
  }
}