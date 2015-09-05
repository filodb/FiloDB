package filodb.core.columnstore

import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import spray.caching._

import filodb.core._
import filodb.core.metadata.Column
import RowReader.TypedFieldExtractor

/**
 * High-level interface of a column store.  Writes and reads segments, which are pretty high level.
 * Most implementations will probably want to be based on something like the CachedmergingColumnStore
 * below, which implements much of the business logic and gives lower level primitives.  This trait
 * exists though to allow special implementations that want to use different lower level primitives.
 */
trait ColumnStore {
  import filodb.core.Types._

  /**
   * Appends the segment to the column store.  The passed in segment must be somehow merged with an existing
   * segment to produce a new segment that has combined data such that rows with new unique primary keys
   * are appended and rows with existing primary keys will overwrite.  Also, the sort order must somehow be
   * preserved such that the chunk/row# in the ChunkRowMap can be read out in sort key order.
   * @param segment the partial Segment to write / merge to the columnar store
   * @param version the version # to write the segment to
   * @returns Success. Future.failure(exception) otherwise.
   */
  def appendSegment[K: SortKeyHelper: TypedFieldExtractor](segment: Segment[K],
                                                           version: Int): Future[Response]

  /**
   * Reads segments from the column store, in order of primary key.
   * @param columns the set of columns to read back.  Order determines the order of columns read back
   *                in each row
   * @param keyRange describes the partition and range of keys to read back. NOTE: end range is exclusive!
   * @param version the version # to read from
   * @returns An iterator over RowReaderSegment's
   */
  def readSegments[K: SortKeyHelper](columns: Seq[Column], keyRange: KeyRange[K], version: Int):
      Future[Iterator[Segment[K]]]
}

case class ChunkedData(column: Types.ColumnId, chunks: Seq[(ByteBuffer, Types.ChunkID, ByteBuffer)])

/**
 * A partial implementation of a ColumnStore, based on separating storage of chunks and ChunkRowMaps,
 * use of a segment cache to speed up merging, and a ChunkMergingStrategy to merge segments.  It defines
 * lower level primitives and implements the ColumnStore methods in terms of these primitives.
 */
trait CachedMergingColumnStore extends ColumnStore {
  import filodb.core.Types._

  def segmentCache: Cache[Segment[_]]

  def mergingStrategy: ChunkMergingStrategy

  implicit val ec: ExecutionContext

  /**
   *  == Lower level storage engine primitives ==
   */

  /**
   * Writes chunks to underlying storage.
   * @param chunks an Iterator over triples of (columnName, chunkId, chunk bytes)
   * @returns Success. Future.failure(exception) otherwise.
   */
  def writeChunks(dataset: TableName,
                  partition: PartitionKey,
                  version: Int,
                  segmentId: ByteBuffer,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response]

  def writeChunkRowMap(dataset: TableName,
                       partition: PartitionKey,
                       version: Int,
                       segmentId: ByteBuffer,
                       chunkRowMap: ChunkRowMap): Future[Response]

  /**
   * Reads back all the chunks from multiple columns of a keyRange at once.  Note that no paging
   * is performed - so don't ask for too large of a range.  Recommendation is to read the ChunkRowMaps
   * first and use the info there to determine how much to read.
   * @param columns the columns to read back chunks from
   * @param keyRange the range of segments to read from, note [start, end) <-- end is exclusive!
   * @param version the version to read back
   * @returns a sequence of ChunkedData, each triple is (segmentId, chunkId, bytes) for each chunk
   *          must be sorted in order of increasing segmentId
   */
  def readChunks[K](columns: Set[ColumnId],
                    keyRange: KeyRange[K],
                    version: Int): Future[Seq[ChunkedData]]

  /**
   * Reads back all the ChunkRowMaps from multiple segments in a keyRange.
   * @param keyRange the range of segments to read from, note [start, end) <-- end is exclusive!
   * @param version the version to read back
   * @returns a sequence of (segmentId, ChunkRowMap)'s
   */
  def readChunkRowMaps[K](keyRange: KeyRange[K], version: Int): Future[Seq[(ByteBuffer, BinaryChunkRowMap)]]

  /**
   * Designed to scan over many many ChunkRowMaps from multiple partitions.  Intended for fast scanning
   * of many segments, such as reading all the data for one node from a Spark worker.
   * TODO: how to make passing stuff such as token ranges nicer, but still generic?
   */
  def scanChunkRowMaps(dataset: TableName,
                       partitionFilter: (PartitionKey => Boolean),
                       params: Map[String, String])
                      (processFunc: (ChunkRowMap => Unit)): Future[Response]

  /**
   * == Caching and merging implementation of the high level functions ==
   */

  def clearSegmentCache(): Unit = { segmentCache.clear() }

  def appendSegment[K: SortKeyHelper: TypedFieldExtractor](segment: Segment[K],
                                                           version: Int): Future[Response] = {
    if (segment.isEmpty) return(Future.successful(NotApplied))
    for { oldSegment <- getSegFromCache(segment.keyRange, version)
          mergedSegment = mergingStrategy.mergeSegments(oldSegment, segment)
          writeChunksResp <- writeChunks(segment.dataset, segment.partition, version,
                                         segment.segmentId, mergedSegment.getChunks)
          writeCRMapResp <- writeChunkRowMap(segment.dataset, segment.partition, version,
                                         segment.segmentId, mergedSegment.index)
            if writeChunksResp == Success }
    yield {
      // Important!  Update the cache with the new merged segment.
      updateCache(segment.keyRange, version, mergedSegment)
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

  private def getSegFromCache[K: SortKeyHelper](keyRange: KeyRange[K], version: Int): Future[Segment[K]] = {
    segmentCache((keyRange.dataset, keyRange.partition, version, keyRange.binaryStart))(
                 mergingStrategy.readSegmentForCache(keyRange, version).
                 asInstanceOf[Future[Segment[_]]]).asInstanceOf[Future[Segment[K]]]
  }

  private def updateCache[K](keyRange: KeyRange[K], version: Int, newSegment: Segment[K]): Unit = {
    // NOTE: Spray caching doesn't have an update() method, so we have to delete and then repopulate. :(
    // TODO: consider if we need to lock the code below. Probably not since ColumnStore is single-writer but
    // we might want to update the spray-caching API to have an update method.
    val key = (keyRange.dataset, keyRange.partition, version, keyRange.binaryStart)
    segmentCache.remove(key)
    segmentCache(key)(mergingStrategy.pruneForCache(newSegment).asInstanceOf[Segment[_]])
  }

  // @param rowMaps a Seq of (segmentId, ChunkRowMap)
  private def buildSegments[K: SortKeyHelper](rowMaps: Seq[(ByteBuffer, BinaryChunkRowMap)],
                                              chunks: Seq[ChunkedData],
                                              origKeyRange: KeyRange[K],
                                              schema: Seq[Column]): Seq[Segment[K]] = {
    val helper = implicitly[SortKeyHelper[K]]
    val segments = rowMaps.map { case (segmentId, rowMap) =>
        val (segStart, segEnd) = helper.getSegment(helper.fromBytes(segmentId))
        val segKeyRange = origKeyRange.copy(start = segStart, end = segEnd)
        new RowReaderSegment(segKeyRange, rowMap, schema)
    }
    chunks.foreach { case ChunkedData(columnName, chunkTriples) =>
      var segIndex = 0
      chunkTriples.foreach { case (segmentId, chunkId, chunkBytes) =>
        // Rely on the fact that chunks are sorted by segmentId, in the same order as the rowMaps
        val segmentKey = helper.fromBytes(segmentId)
        while (segmentKey != segments(segIndex).keyRange.start) segIndex += 1
        segments(segIndex).addChunk(chunkId, columnName, chunkBytes)
      }
    }
    segments
  }
}