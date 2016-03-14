package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import filodb.core._
import filodb.core.metadata.RichProjection

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
  /**
   * Reads the minimum necessary amount of data to populate the segment cache for merging operations.
   * This reads back one segment.  For example, if the merging operation does not need existing chunks,
   * but only the sort key column, then this can read back just the ChunkRowMap and the sort key vectors.
   *
   * @param keyRange the keyRange of the segment to read.  It's important this corresponds to one segment.
   */
  def readSegmentForCache(projection: RichProjection,
                          version: Int)(
                          segInfo: SegmentInfo[projection.PK, projection.SK])
                         (implicit ec: ExecutionContext): Future[Segment]

  /**
   * Merges an existing segment cached using readSegmentForCache with a new partial segment to be inserted.
   * @param oldSegment the existing segment from the cache, populated using readSegmentForCache
   * @param newSegment the new partial segment containing only new data/rows to be inserted
   * @return a merged Segment ready to be flushed to disk.  This typically will only include chunks that need
   *          to be updated or written to disk.
   */
  def mergeSegments(oldSegment: Segment, newSegment: Segment): Segment

  /**
   * Prunes a segment to only what needs to be cached.
   */
  def pruneForCache(segment: Segment): Segment
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
extends ChunkMergingStrategy with StrictLogging {
  /**
   * Reads only the row key-relevant columns from a segment.
   * @param projection a RowKey-only projection, generated using toRowKeyOnlyProjection method.
   *                   This is crucial because such a segment has the row key functions recalculated
   *                   for a smaller number of columns, including computed columns.
   */
  def readSegmentForCache(projection: RichProjection,
                          version: Int)(
                          segInfo: SegmentInfo[projection.PK, projection.SK])
                         (implicit ec: ExecutionContext): Future[Segment] = {
    val keyRange = KeyRange(segInfo.partition, segInfo.segment, segInfo.segment, endExclusive = false)
    columnStore.scanSegments(projection, projection.columns, version,
                             SinglePartitionRangeScan(keyRange)).map { iter =>
      iter.toSeq.headOption match {
        case Some(firstSegment) => firstSegment
        case None =>
          logger.debug(s"No segment/ChunkRowMap found for $keyRange, creating a new one")
          new GenericSegment(projection,
                             new UpdatableChunkRowMap[projection.RK]()(projection.rowKeyType))(segInfo)
      }
    }
  }

  def mergeSegments(oldSegment: Segment, newSegment: Segment): Segment = {
    // One should NEVER be allowed to merge segments from different places... unless we are perhaps
    // talking about splitting and merging, but that's outside the scope of this method
    require(oldSegment.segInfo == newSegment.segInfo,
      s"Cannot merge different segments: orig ${oldSegment.segInfo} != new ${newSegment.segInfo}")

    // How much to offset chunkIds in newSegment.  0 in newSegment == nextChunkId in oldSegment.
    val offsetChunkId = oldSegment.index.nextChunkId

    val rowKeyFunc = oldSegment.projection.rowKeyFunc

    // Merge old ChunkRowMap with new segment's ChunkRowMap with chunkIds offset
    // NOTE: Working with a TreeMap is probably not the most efficient way to merge two sorted lists
    // since both indexes can be read in sort key order.  So, TODO: replace this with more efficient
    // sorted iterator merge.  Probably not an issue for now.
    val baseIndex = oldSegment match {
      case g: GenericSegment =>
        g.index.asInstanceOf[UpdatableChunkRowMap[oldSegment.projection.RK]]
      case rr: RowReaderSegment =>
        // This should be a segment read from disk via readSegmentForCache().  Cheat assume only sort col
        val items = rr.rowChunkIterator().map { case (reader, chunkId, rowNum) =>
          rowKeyFunc(reader) -> (chunkId -> rowNum)
        }
        UpdatableChunkRowMap(items.toSeq)(oldSegment.projection.rowKeyType)
    }
    val offsetNewTree = newSegment.index.asInstanceOf[UpdatableChunkRowMap[oldSegment.projection.RK]].
                                   index.mapValues { case (chunkId, rowNum) =>
                                     (chunkId + offsetChunkId, rowNum)
                                   }

    // Translate chunkIds from newSegment to (oldSegment.nextChunkId + _)
    val mergedSegment = new GenericSegment(oldSegment.projection,
                                           baseIndex ++ offsetNewTree)(oldSegment.segInfo)
    newSegment.getChunks
              .foreach { case (column, chunkId, chunk) =>
                mergedSegment.addChunk(chunkId + offsetChunkId, column, chunk)
              }

    mergedSegment
  }

  // We only need to store the row key columns
  def pruneForCache(segment: Segment): Segment = {
    val rowKeyProj = segment.projection.toRowKeyOnlyProjection
    val rowKeyColumns = rowKeyProj.columns.map(_.name)
    if (segment.getColumns == Set(rowKeyColumns)) {
      logger.trace(s"pruneForcache: segment only has ${segment.getColumns}, not pruning")
      segment
    } else {
      val prunedSeg = new GenericSegment(rowKeyProj, segment.index)(segment.segInfo.basedOn(rowKeyProj))
      segment.getChunks
             .filter { case (column, chunkId, chunk) => rowKeyColumns contains column }
             .foreach { case (column, chunkId, chunk) =>
        prunedSeg.addChunk(chunkId, column, chunk)
      }
      logger.trace(s"Pruned segment: $prunedSeg")
      prunedSeg
    }
  }
}