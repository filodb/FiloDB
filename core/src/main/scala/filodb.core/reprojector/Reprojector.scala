package filodb.core.reprojector

import filodb.core.Types._

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

trait Row
