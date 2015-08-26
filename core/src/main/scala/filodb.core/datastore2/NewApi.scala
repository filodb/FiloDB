package filodb.core.datastore2

import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core.messages.{ErrorResponse, Response}

/**
 * Temporary home for new FiloDB API definitions, including column store and memtable etc.
 * Perhaps they should be moved into filodb.core.columnstore and filodb.core.reprojector
 *
 * NOT included: the MetadataStore, which contains column, partition, version definitions
 */
object Types {
  // A Chunk is a single columnar chunk for a given table, partition, column
  type Chunk = ByteBuffer
  type ColumnName = String
  type TableName = String
  type ChunkID = Long    // Each chunk is identified by segmentID and a long timestamp

  type SortOrder = Set[(ColumnName,Boolean)]
  type Rearrange[K] = (Segment[K]) => Segment[K]

  // TODO: support composite partition keys?
  type PartitionKey = String
}

/**
 * The MemTable serves these purposes:
 * 1) Holds incoming rows of data before being flushed
 * 2) Can extract rows of data in a given sort order, and remove them
 * 3) Can read rows of data in a given sort order for queries
 */
trait MemTable {
  def numRows: Long
  def tables: Set[String]
}

trait FlushPolicy {
  import Types._

  // this could check memory size, no of documents, time interval
  def shouldFlush(memtable: MemTable): Boolean

  // Determine the next table and partition to flush
  def nextFlushInfo(memtable: MemTable): (String, PartitionKey)
}

trait Row

/**
 * The streaming reprojector (TM) ingests new rows of data into a memtable, flushes them out as
 * columnar chunks to the ColumnStore based on some policy, and can read data out as part of queries.
 * It is a combination of the Write-Optimized Store (WOS) and Tuple Mover.
 *
 * NOTE:
 */
trait Reprojector {
  import Types._

  /**
   * Ingesting a new row requires one to know which partition to put the new row into.
   */
  def ingestNewRow(table: String, partition: PartitionKey, newRow: Row, memtable: MemTable): MemTable

  /**
   * This is a high level function that decides on its own which segment from which table to flush next.
   * In order to implement this, it needs to know the column schema for the table it is flushing, the
   * type of primary key, etc.
   */
  def flushToSegment[K](memtable: MemTable, sortOrder: SortOrder)(policy: FlushPolicy): Segment[K]

  def removeRows[K](memTable: MemTable, keyRange: KeyRange[K]): MemTable

  def readRows[K](memTable: MemTable, keyRange: KeyRange[K], sortOrder: SortOrder): Iterator[Row]
}

/**
 * Implementation of a column store.  Writes and reads segments, which are pretty high level.
 * Must be able to read and write both the columnar chunks and the SegmentRowIndex.
 * Hopefully, for fast I/O, the columns are stored together.  :)
 */
trait ColumnStore {
  import Types._

  /**
   * Appends the segment to the column store.  The chunks in a segment will be appended to the other chunks
   * already present in that segment, while the SegmentRowIndex will be merged such that the new row index
   * contains the correct sorted read order in order of PK.  It is assumed that the new chunks are newer
   * or will potentially overwrite the existing data.
   * @param segment the Segment to write / merge to the columnar store
   * @param version the version # to write the segment to
   * @returns Success, or other ErrorResponse
   */
  def appendSegment[K](segment: Segment[K], version: Int): Future[Response]

  /**
   * Reads segments from the column store, in order of primary key.
   * @param columns the set of columns to read back
   * @param keyRange describes the partition and range of keys to read back
   * @param version the version # to read from
   * @returns either an iterator over segments, or ErrorResponse
   */
  def readSegments[K](columns: Set[String], keyRange: KeyRange[K], version: Int):
      Future[Either[Iterator[Segment[K]], ErrorResponse]]
}
