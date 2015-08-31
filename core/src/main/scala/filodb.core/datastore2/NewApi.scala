package filodb.core.datastore2

import java.nio.ByteBuffer
import scala.concurrent.Future

/**
 * Temporary home for new FiloDB API definitions, including column store and memtable etc.
 * Perhaps they should be moved into filodb.core.columnstore and filodb.core.reprojector
 *
 * NOT included: the MetadataStore, which contains column, partition, version definitions
 */
object Types {
  // A Chunk is a single columnar chunk for a given table, partition, column
  type Chunk = ByteBuffer
  // TODO: Change ColumnId to an Int.  Would be more efficient, and allow renaming columns.
  type ColumnId = String
  type TableName = String
  type ChunkID = Int    // Each chunk is identified by segmentID and a long timestamp

  type SortOrder = Set[(ColumnId, Boolean)]
  type Rearrange[K] = (Segment[K]) => Segment[K]

  // TODO: support composite partition keys?
  type PartitionKey = String
}

// A range of keys, used for describing ingest rows as well as queries
case class KeyRange[K : SortKeyHelper](dataset: Types.TableName,
                                       partition: Types.PartitionKey,
                                       start: K, end: K) {
  val helper = implicitly[SortKeyHelper[K]]
  def binaryStart: ByteBuffer = helper.toBytes(start)
  def binaryEnd: ByteBuffer = helper.toBytes(end)
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
