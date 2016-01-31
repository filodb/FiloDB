package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import org.velvia.filo.RowReader
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.existentials
import spray.caching._

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}

/**
 * High-level interface of a column store.  Writes and reads segments, which are pretty high level.
 * Most implementations will probably want to be based on something like the CachedmergingColumnStore
 * below, which implements much of the business logic and gives lower level primitives.  This trait
 * exists though to allow special implementations that want to use different lower level primitives.
 */
trait ColumnStore {
  import filodb.core.Types._
  import RowReaderSegment._

  def ec: ExecutionContext
  implicit val execContext = ec

  /**
   * Initializes the column store for a given dataset projection.  Must be called once before appending
   * segments to that projection.
   */
  def initializeProjection(projection: Projection): Future[Response]

  /**
   * Clears all data from the column store for that given projection, for all versions.
   * NOTE: please make sure there are no reprojections or writes going on before calling this
   */
  def clearProjectionData(projection: Projection): Future[Response]

  /**
   * Appends the segment to the column store.  The passed in segment must be somehow merged with an existing
   * segment to produce a new segment that has combined data such that rows with new unique primary keys
   * are appended and rows with existing primary keys will overwrite.  Also, the sort order must somehow be
   * preserved such that the chunk/row# in the ChunkRowMap can be read out in sort key order.
   * @param segment the partial Segment to write / merge to the columnar store
   * @param version the version # to write the segment to
   * @return Success. Future.failure(exception) otherwise.
   */
  def appendSegment(projection: RichProjection,
                    segment: Segment,
                    version: Int): Future[Response]

  /**
   * Reads segments from the column store, in order of primary key.
   * May not be designed for large amounts of data.
   * @param projection the Projection to read from
   * @param columns the set of columns to read back.  Order determines the order of columns read back
   *                in each row
   * @param version the version # to read from
   * @param keyRange describes the partition and range of keys to read back. NOTE: end range is exclusive!
   * @return An iterator over RowReaderSegment's
   */
  def readSegments(projection: RichProjection,
                   columns: Seq[Column],
                   version: Int)
                  (keyRange: KeyRange[projection.PK, projection.SK]): Future[Iterator[Segment]]

  /**
   * Scans over segments from multiple partitions of a dataset.  Designed for huge amounts of data.
   * The params determine things such as token ranges or some way of limiting the scanning.
   *
   * params:  see individual implementations, but
   *   segment_group_size   determines # of segments to read at once. Must be integer string.
   */
  def scanSegments(projection: RichProjection,
                   columns: Seq[Column],
                   version: Int,
                   params: Map[String, String] = Map.empty)
                  (partitionFilter: (projection.PK => Boolean) = (x: projection.PK) => true):
    Future[Iterator[Segment]]

  /**
   * Scans over segments, just like scanRows, but returns an iterator of RowReader
   * for all of those row-oriented applications.  Contains a high performance iterator
   * implementation, probably faster than trying to do it yourself.  :)
   */
  def scanRows(projection: RichProjection,
               columns: Seq[Column],
               version: Int,
               params: Map[String, String] = Map.empty,
               readerFactory: RowReaderFactory = DefaultReaderFactory)
              (partitionFilter: (projection.PK => Boolean) = (x: projection.PK) => true):
    Future[Iterator[RowReader]] = {
    for { segmentIt <- scanSegments(projection, columns, version, params)(partitionFilter) }
    yield {
      if (segmentIt.hasNext) {
        // TODO: fork this kind of code into a macro, called fastFlatMap.
        // That's what we really need...  :-p
        new Iterator[RowReader] {
          final def getNextRowIt: Iterator[RowReader] = {
            val readerSeg = segmentIt.next.asInstanceOf[RowReaderSegment]
            readerSeg.rowIterator(readerFactory)
          }

          var rowIt: Iterator[RowReader] = getNextRowIt

          final def hasNext: Boolean = {
            var _hasNext = rowIt.hasNext
            while (!_hasNext) {
              if (segmentIt.hasNext) {
                rowIt = getNextRowIt
                _hasNext = rowIt.hasNext
              } else {
                // all done. No more segments.
                return false
              }
            }
            _hasNext
          }

          final def next: RowReader = rowIt.next.asInstanceOf[RowReader]
        }
      } else {
        Iterator.empty
      }
    }
  }

  /**
   * Determines how to split the scanning of a dataset across a columnstore.
   * @param params implementation-specific flags to affect the scanning, including parallelism, locality etc
   * @return individual param maps for each split, to be fed to parallel/distributed scanSegments calls
   */
  def getScanSplits(dataset: TableName,
                    params: Map[String, String] = Map.empty): Seq[Map[String, String]]

  /**
   * Shuts down the ColumnStore, including any threads that might be hanging around
   */
  def shutdown(): Unit
}

case class ChunkedData(column: Types.ColumnId, chunks: Seq[(Types.SegmentId, Types.ChunkID, ByteBuffer)])

/**
 * A partial implementation of a ColumnStore, based on separating storage of chunks and ChunkRowMaps,
 * use of a segment cache to speed up merging, and a ChunkMergingStrategy to merge segments.  It defines
 * lower level primitives and implements the ColumnStore methods in terms of these primitives.
 */
trait CachedMergingColumnStore extends ColumnStore with StrictLogging {
  import filodb.core.Types._
  import filodb.core.Iterators._

  def segmentCache: Cache[Segment]

  def mergingStrategy: ChunkMergingStrategy

  implicit val ec: ExecutionContext

  def clearProjectionData(projection: Projection): Future[Response] = {
    // Clear out any entries from segmentCache first
    logger.info(s"Clearing out segment cache for dataset ${projection.dataset}")
    segmentCache.keys.foreach { case key @ (dataset, _, _, _) =>
      if (dataset == projection.dataset) segmentCache.remove(key)
    }
    logger.info(s"Clearing all columnar projection data for dataset ${projection.dataset}")
    clearProjectionDataInner(projection)
  }

  def clearProjectionDataInner(projection: Projection): Future[Response]

  /**
   *  == Lower level storage engine primitives ==
   */

  /**
   * Writes chunks to underlying storage.
   * @param chunks an Iterator over triples of (columnName, chunkId, chunk bytes)
   * @return Success. Future.failure(exception) otherwise.
   */
  def writeChunks(dataset: TableName,
                  partition: BinaryPartition,
                  version: Int,
                  segmentId: SegmentId,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response]

  def writeChunkRowMap(dataset: TableName,
                       partition: BinaryPartition,
                       version: Int,
                       segmentId: SegmentId,
                       chunkRowMap: ChunkRowMap): Future[Response]

  /**
   * Reads back all the chunks from multiple columns of a keyRange at once.  Note that no paging
   * is performed - so don't ask for too large of a range.  Recommendation is to read the ChunkRowMaps
   * first and use the info there to determine how much to read.
   * @param dataset the name of the dataset to read chunks from
   * @param columns the columns to read back chunks from
   * @param keyRange the binary range of segments to read from.  Note endExclusive flag.
   * @param version the version to read back
   * @return a sequence of ChunkedData, each triple is (segmentId, chunkId, bytes) for each chunk
   *          must be sorted in order of increasing segmentId
   */
  def readChunks(dataset: TableName,
                 columns: Set[ColumnId],
                 keyRange: BinaryKeyRange,
                 version: Int): Future[Seq[ChunkedData]]

  type ChunkMapInfo = (BinaryPartition, SegmentId, BinaryChunkRowMap)

  /**
   * Reads back all the ChunkRowMaps from multiple segments in a keyRange.
   * @param keyRange the range of segments to read from, note [start, end) <-- end is exclusive!
   * @param version the version to read back
   * @return a sequence of (segmentId, ChunkRowMap)'s
   */
  def readChunkRowMaps(dataset: TableName,
                       keyRange: BinaryKeyRange,
                       version: Int): Future[Iterator[ChunkMapInfo]]

  /**
   * Designed to scan over many many ChunkRowMaps from multiple partitions.  Intended for fast scanning
   * of many segments, such as reading all the data for one node from a Spark worker.
   * TODO: how to make passing stuff such as token ranges nicer, but still generic?
   *
   * @return an iterator over ChunkRowMaps.  Must be sorted by partitionkey first, then segment Id.
   */
  def scanChunkRowMaps(dataset: TableName,
                       version: Int,
                       params: Map[String, String]): Future[Iterator[ChunkMapInfo]]

  case class SegmentIndex[P, S](binPartition: BinaryPartition,
                                segmentId: SegmentId,
                                partition: P,
                                segment: S,
                                chunkMap: BinaryChunkRowMap)

  def toSegIndex(projection: RichProjection, chunkMapInfo: ChunkMapInfo):
        SegmentIndex[projection.PK, projection.SK] = {
    SegmentIndex(chunkMapInfo._1,
                 chunkMapInfo._2,
                 projection.partitionType.fromBytes(chunkMapInfo._1),
                 projection.segmentType.fromBytes(chunkMapInfo._2),
                 chunkMapInfo._3)
  }

  /**
   * == Caching and merging implementation of the high level functions ==
   */

  def clearSegmentCache(): Unit = { segmentCache.clear() }

  def appendSegment(projection: RichProjection,
                    segment: Segment,
                    version: Int): Future[Response] = {
    if (segment.isEmpty) return(Future.successful(NotApplied))
    for { oldSegment <- getSegFromCache(projection.toRowKeyOnlyProjection, segment, version)
          mergedSegment = mergingStrategy.mergeSegments(oldSegment, segment)
          writeChunksResp <- writeChunks(projection.datasetName, segment.binaryPartition, version,
                                         segment.segmentId, mergedSegment.getChunks)
          writeCRMapResp <- writeChunkRowMap(projection.datasetName, segment.binaryPartition, version,
                                         segment.segmentId, mergedSegment.index)
            if writeChunksResp == Success }
    yield {
      // Important!  Update the cache with the new merged segment.
      updateCache(projection, version, mergedSegment)
      writeCRMapResp
    }
  }

  def readSegments(projection: RichProjection,
                   columns: Seq[Column],
                   version: Int)
                  (keyRange: KeyRange[projection.PK, projection.SK]): Future[Iterator[Segment]] = {
    // TODO: implement actual paging and the iterator over segments.  Or maybe that should be implemented
    // at a higher level.
    val binKeyRange = projection.toBinaryKeyRange(keyRange)
    (for { rowMaps <- readChunkRowMaps(projection.datasetName, binKeyRange, version)
           chunks  <- readChunks(projection.datasetName,
                                 columns.map(_.name).toSet,
                                 binKeyRange,
                                 version) if rowMaps.nonEmpty }
    yield {
      val indexMaps = rowMaps.map(crm => toSegIndex(projection, crm))
      buildSegments(projection, indexMaps.toSeq, chunks, columns).toIterator
    }).recover {
      // No chunk maps found, so just return empty list of segments
      case e: java.util.NoSuchElementException => Iterator.empty
    }
  }

  // NOTE: this is more or less a single-threaded implementation.  Reads of chunks for multiple columns
  // happen in parallel, but we still block to wait for all of them to come back.
  def scanSegments(projection: RichProjection,
                   columns: Seq[Column],
                   version: Int,
                   params: Map[String, String] = Map.empty)
                  (partitionFilter: (projection.PK => Boolean) = (x: projection.PK) => true):
                     Future[Iterator[Segment]] = {
    val segmentGroupSize = params.getOrElse("segment_group_size", "3").toInt

    for { chunkmapsIt <- scanChunkRowMaps(projection.datasetName, version, params) }
    yield
    (for { // Group by partition key first
          (part, partChunkMaps) <- chunkmapsIt.map(crm => toSegIndex(projection, crm))
                                     .filter { case SegmentIndex(_, _, part, _, _) => partitionFilter(part) }
                                     .sortedGroupBy { case SegmentIndex(part, _, _, _, _) => part }
          // Subdivide chunk row maps in each partition so we don't read more than we can chew
          // TODO: make a custom grouping function based on # of rows accumulated
          groupChunkMaps <- partChunkMaps.grouped(segmentGroupSize) }
    yield {
      val chunkMaps = groupChunkMaps.toSeq
      val binKeyRange = BinaryKeyRange(part,
                                       chunkMaps.head.segmentId,
                                       chunkMaps.last.segmentId,
                                       endExclusive = false)
      val chunks = Await.result(readChunks(projection.datasetName, columns.map(_.name).toSet,
                                           binKeyRange, version), 5.minutes)
      buildSegments(projection, chunkMaps, chunks, columns).toIterator
    }).flatten
  }

  private def getSegFromCache(projection: RichProjection,
                              segment: Segment,
                              version: Int): Future[Segment] = {
    segmentCache((projection.datasetName, segment.binaryPartition, version, segment.segmentId)) {
      val newSegInfo = segment.segInfo.basedOn(projection)
      mergingStrategy.readSegmentForCache(projection, version)(newSegInfo)
    }
  }

  private def updateCache(projection: RichProjection,
                          version: Int,
                          newSegment: Segment): Unit = {
    // NOTE: Spray caching doesn't have an update() method, so we have to delete and then repopulate. :(
    // TODO: consider if we need to lock the code below. Probably not since ColumnStore is single-writer but
    // we might want to update the spray-caching API to have an update method.
    val key = (projection.datasetName, newSegment.binaryPartition, version, newSegment.segmentId)
    segmentCache.remove(key)
    segmentCache(key)(mergingStrategy.pruneForCache(newSegment))
  }

  import scala.util.control.Breaks._

  private def buildSegments[P, S](projection: RichProjection,
                                  rowMaps: Seq[SegmentIndex[P, S]],
                                  chunks: Seq[ChunkedData],
                                  schema: Seq[Column]): Seq[Segment] = {
    val segments = rowMaps.map { case SegmentIndex(_, _, partition, segStart, rowMap) =>
        val segInfo = SegmentInfo(partition, segStart)
        new RowReaderSegment(projection, segInfo, rowMap, schema)
    }
    chunks.foreach { case ChunkedData(columnName, chunkTriples) =>
      var segIndex = 0
      breakable {
        chunkTriples.foreach { case (segmentId, chunkId, chunkBytes) =>
          // Rely on the fact that chunks are sorted by segmentId, in the same order as the rowMaps
          val segmentKey = projection.segmentType.fromBytes(segmentId)
          val segInfo = segments(segIndex).segInfo
          while (segmentKey != segInfo.segment) {
            segIndex += 1
            if (segIndex >= segments.length) {
              logger.warn(s"Chunks with segmentId=$segmentKey (part ${segInfo.partition})" +
                           " with no rowmap; corruption?")
              break
            }
          }
          segments(segIndex).addChunk(chunkId, columnName, chunkBytes)
        }
      }
    }
    segments
  }
}