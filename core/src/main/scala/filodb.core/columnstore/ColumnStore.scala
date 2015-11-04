package filodb.core.columnstore

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
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
  def appendSegment[K](projection: RichProjection[K],
                       segment: Segment[K],
                       version: Int): Future[Response]

  /**
   * Reads segments from the column store, in order of primary key.
   * May not be designed for large amounts of data.
   * @param columns the set of columns to read back.  Order determines the order of columns read back
   *                in each row
   * @param keyRange describes the partition and range of keys to read back. NOTE: end range is exclusive!
   * @param version the version # to read from
   * @return An iterator over RowReaderSegment's
   */
  def readSegments[K: SortKeyHelper](columns: Seq[Column], keyRange: KeyRange[K], version: Int):
      Future[Iterator[Segment[K]]]

  /**
   * Scans over segments from multiple partitions of a dataset.  Designed for huge amounts of data.
   * The params determine things such as token ranges or some way of limiting the scanning.
   *
   * params:  see individual implementations, but
   *   segment_group_size   determines # of segments to read at once. Must be integer string.
   */
  def scanSegments[K: SortKeyHelper](columns: Seq[Column],
                                     dataset: TableName,
                                     version: Int,
                                     partitionFilter: (PartitionKey => Boolean) = (x => true),
                                     params: Map[String, String] = Map.empty): Future[Iterator[Segment[K]]]

  /**
   * Determines how to split the scanning of a dataset across a columnstore.
   * @param params implementation-specific flags to affect the scanning, including parallelism, locality etc
   * @return individual param maps for each split, to be fed to parallel/distributed scanSegments calls
   */
  def getScanSplits(dataset: TableName,
                    params: Map[String, String] = Map.empty): Seq[Map[String, String]]
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

  def segmentCache: Cache[Segment[_]]

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
                  partition: PartitionKey,
                  version: Int,
                  segmentId: SegmentId,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response]

  def writeChunkRowMap(dataset: TableName,
                       partition: PartitionKey,
                       version: Int,
                       segmentId: SegmentId,
                       chunkRowMap: ChunkRowMap): Future[Response]

  /**
   * Reads back all the chunks from multiple columns of a keyRange at once.  Note that no paging
   * is performed - so don't ask for too large of a range.  Recommendation is to read the ChunkRowMaps
   * first and use the info there to determine how much to read.
   * @param columns the columns to read back chunks from
   * @param keyRange the range of segments to read from.  Note endExclusive flag.
   * @param version the version to read back
   * @return a sequence of ChunkedData, each triple is (segmentId, chunkId, bytes) for each chunk
   *          must be sorted in order of increasing segmentId
   */
  def readChunks[K](columns: Set[ColumnId],
                    keyRange: KeyRange[K],
                    version: Int): Future[Seq[ChunkedData]]

  /**
   * Reads back all the ChunkRowMaps from multiple segments in a keyRange.
   * @param keyRange the range of segments to read from, note [start, end) <-- end is exclusive!
   * @param version the version to read back
   * @return a sequence of (segmentId, ChunkRowMap)'s
   */
  def readChunkRowMaps[K](keyRange: KeyRange[K], version: Int): Future[Seq[(SegmentId, BinaryChunkRowMap)]]

  type ChunkMapInfo = (PartitionKey, SegmentId, BinaryChunkRowMap)

  /**
   * Designed to scan over many many ChunkRowMaps from multiple partitions.  Intended for fast scanning
   * of many segments, such as reading all the data for one node from a Spark worker.
   * TODO: how to make passing stuff such as token ranges nicer, but still generic?
   *
   * @return an iterator over ChunkRowMaps.  Must be sorted by partitionkey first, then segment Id.
   */
  def scanChunkRowMaps(dataset: TableName,
                       version: Int,
                       partitionFilter: (PartitionKey => Boolean),
                       params: Map[String, String]): Future[Iterator[ChunkMapInfo]]

  /**
   * == Caching and merging implementation of the high level functions ==
   */

  def clearSegmentCache(): Unit = { segmentCache.clear() }

  def appendSegment[K](projection: RichProjection[K],
                       segment: Segment[K],
                       version: Int): Future[Response] = {
    if (segment.isEmpty) return(Future.successful(NotApplied))
    implicit val helper = projection.helper
    for { oldSegment <- getSegFromCache(projection, segment.keyRange, version)
          mergedSegment = mergingStrategy.mergeSegments(oldSegment, segment)
          writeChunksResp <- writeChunks(segment.dataset, segment.partition, version,
                                         segment.segmentId, mergedSegment.getChunks)
          writeCRMapResp <- writeChunkRowMap(segment.dataset, segment.partition, version,
                                         segment.segmentId, mergedSegment.index)
            if writeChunksResp == Success }
    yield {
      // Important!  Update the cache with the new merged segment.
      updateCache(projection, segment.keyRange, version, mergedSegment)
      writeCRMapResp
    }
  }

  def readSegments[K: SortKeyHelper](columns: Seq[Column], keyRange: KeyRange[K], version: Int):
      Future[Iterator[Segment[K]]] = {
    // TODO: implement actual paging and the iterator over segments.  Or maybe that should be implemented
    // at a higher level.
    (for { rowMaps <- readChunkRowMaps(keyRange, version)
          chunks   <- readChunks(columns.map(_.name).toSet, keyRange, version) if rowMaps.nonEmpty }
    yield {
      buildSegments(rowMaps, chunks, keyRange, columns).toIterator
    }).recover {
      // No chunk maps found, so just return empty list of segments
      case e: java.util.NoSuchElementException => Iterator.empty
    }
  }

  // NOTE: this is more or less a single-threaded implementation.  Reads of chunks for multiple columns
  // happen in parallel, but we still block to wait for all of them to come back.
  def scanSegments[K: SortKeyHelper](columns: Seq[Column],
                                     dataset: TableName,
                                     version: Int,
                                     partitionFilter: (PartitionKey => Boolean),
                                     params: Map[String, String]): Future[Iterator[Segment[K]]] = {
    val segmentGroupSize = params.getOrElse("segment_group_size", "3").toInt

    for { chunkmapsIt <- scanChunkRowMaps(dataset, version, partitionFilter, params) }
    yield
    (for { // Group by partition key first
          (part, partChunkMaps) <- chunkmapsIt.sortedGroupBy { case (part, seg, rowMap) => part }
          // Subdivide chunk row maps in each partition so we don't read more than we can chew
          // TODO: make a custom grouping function based on # of rows accumulated
          groupChunkMaps <- partChunkMaps.grouped(segmentGroupSize)
          chunkMaps = groupChunkMaps.toSeq
          keyRange = keyRangeFromMaps(chunkMaps, dataset, part) }
    yield {
      val rowMaps = chunkMaps.map { case (_, seg, rowMap) => (seg, rowMap) }
      val chunks = Await.result(readChunks(columns.map(_.name).toSet, keyRange, version),
                                5.minutes)
      buildSegments(rowMaps, chunks, keyRange, columns).toIterator
    }).flatten
  }

  private def keyRangeFromMaps[K: SortKeyHelper](chunkRowMaps: Seq[ChunkMapInfo],
                                                 dataset: TableName,
                                                 partition: PartitionKey): KeyRange[K] = {
    require(partition == chunkRowMaps.head._1)
    val helper = implicitly[SortKeyHelper[K]]
    KeyRange(dataset, partition,
             helper.fromBytes(chunkRowMaps.head._2),
             helper.fromBytes(chunkRowMaps.last._2),
             endExclusive = false)
  }


  private def getSegFromCache[K](projection: RichProjection[K],
                                 keyRange: KeyRange[K],
                                 version: Int): Future[Segment[K]] = {
    segmentCache((keyRange.dataset, keyRange.partition, version, keyRange.binaryStart))(
                 mergingStrategy.readSegmentForCache(projection, keyRange, version).
                 asInstanceOf[Future[Segment[_]]]).asInstanceOf[Future[Segment[K]]]
  }

  private def updateCache[K](projection: RichProjection[K],
                             keyRange: KeyRange[K],
                             version: Int,
                             newSegment: Segment[K]): Unit = {
    // NOTE: Spray caching doesn't have an update() method, so we have to delete and then repopulate. :(
    // TODO: consider if we need to lock the code below. Probably not since ColumnStore is single-writer but
    // we might want to update the spray-caching API to have an update method.
    val key = (keyRange.dataset, keyRange.partition, version, keyRange.binaryStart)
    segmentCache.remove(key)
    segmentCache(key)(mergingStrategy.pruneForCache(projection, newSegment).asInstanceOf[Segment[_]])
  }

  import scala.util.control.Breaks._

  // @param rowMaps a Seq of (segmentId, ChunkRowMap)
  private def buildSegments[K: SortKeyHelper](rowMaps: Seq[(SegmentId, BinaryChunkRowMap)],
                                              chunks: Seq[ChunkedData],
                                              origKeyRange: KeyRange[K],
                                              schema: Seq[Column]): Seq[Segment[K]] = {
    val helper = implicitly[SortKeyHelper[K]]
    val segments = rowMaps.map { case (segmentId, rowMap) =>
        val segStart = helper.fromBytes(segmentId)
        val segKeyRange = origKeyRange.copy(start = segStart, end = segStart, endExclusive = true)
        new RowReaderSegment(segKeyRange, rowMap, schema)
    }
    chunks.foreach { case ChunkedData(columnName, chunkTriples) =>
      var segIndex = 0
      breakable {
        chunkTriples.foreach { case (segmentId, chunkId, chunkBytes) =>
          // Rely on the fact that chunks are sorted by segmentId, in the same order as the rowMaps
          val segmentKey = helper.fromBytes(segmentId)
          while (segmentKey != segments(segIndex).keyRange.start) {
            segIndex += 1
            if (segIndex >= segments.length) {
              logger.warn(s"Chunks with segmentId=$segmentKey ($origKeyRange) with no rowmap; corruption?")
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