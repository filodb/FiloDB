package filodb.core.datastore2

import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core.messages.Response

/**
 * High-level interface of a column store.  Writes and reads segments, which are pretty high level.
 * Most implementations will probably want to be based on something like the CachedmergingColumnStore
 * below, which implements much of the business logic and gives lower level primitives.  This trait
 * exists though to allow special implementations that want to use different lower level primitives.
 */
trait ColumnStore {
  import Types._

  /**
   * Appends the segment to the column store.  The passed in segment must be somehow merged with an existing
   * segment to produce a new segment that has combined data such that rows with new unique primary keys
   * are appended and rows with existing primary keys will overwrite.  Also, the sort order must somehow be
   * preserved such that the chunk/row# in the ChunkRowMap can be read out in sort key order.
   * @param segment the partial Segment to write / merge to the columnar store
   * @param version the version # to write the segment to
   * @returns Success. Future.failure(exception) otherwise.
   */
  def appendSegment[K : SortKeyHelper](segment: Segment[K], version: Int): Future[Response]

  /**
   * Reads segments from the column store, in order of primary key.
   * @param columns the set of columns to read back
   * @param keyRange describes the partition and range of keys to read back. NOTE: end range is exclusive!
   * @param version the version # to read from
   * @returns An iterator over segments
   */
  def readSegments[K : SortKeyHelper](columns: Set[String], keyRange: KeyRange[K], version: Int):
      Future[Iterator[Segment[K]]]
}

case class ChunkedData(column: String, chunks: Seq[(ByteBuffer, Types.ChunkID, ByteBuffer)])

/**
 * A partial implementation of a ColumnStore, based on separating storage of chunks and ChunkRowMaps,
 * use of a segment cache to speed up merging, and a ChunkMergingStrategy to merge segments.  It defines
 * lower level primitives and implements the ColumnStore methods in terms of these primitives.
 */
trait CachedMergingColumnStore {
  import Types._

  /**
   *  == Lower level storage engine primitives ==
   */

  /**
   * Writes chunks to underlying storage.
   * @param chunks an Iterator over triples of (columnName, chunkId, chunk bytes)
   * @returns Success. Future.failure(exception) otherwise.
   */
  def writeChunks(dataset: TableName,
                  partition: String,
                  version: Int,
                  segmentId: ByteBuffer,
                  chunks: Iterator[(String, ChunkID, ByteBuffer)]): Future[Response]

  def writeChunkRowMap(dataset: TableName,
                       partition: String,
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
   */
  def readChunks[K](columns: Set[String],
                    keyRange: KeyRange[K],
                    version: Int): Future[Seq[ChunkedData]]

  def readChunkRowMaps[K](keyRange: KeyRange[K], version: Int): Future[Seq[ChunkRowMap]]

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
}