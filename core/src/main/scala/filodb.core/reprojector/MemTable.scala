package filodb.core.reprojector

import filodb.core.KeyRange
import filodb.core.Types._
import filodb.core.metadata.Dataset
import filodb.core.columnstore.RowReader

object MemTable {
  // Ingestion responses
  trait IngestionResponse
  case object Ingested extends IngestionResponse
  case object PleaseWait extends IngestionResponse  // Cannot quite ingest yet
}

/**
 * The MemTable serves these purposes:
 * 1) Holds incoming rows of data before being flushed
 * 2) Can extract rows of data in a given sort order, and remove them
 * 3) Can read rows of data in a given sort order for queries
 *
 * It definitely must be multithread safe, and very very fast.
 * It is flushed using a FlushPolicy which may be based on things like how stale the rows
 * for a given dataset are, or memory size, or other parameters.
 *
 * Data written to a MemTable should be logged via WAL or some other mechanism so it can be recovered in
 * case of failure.
 */
trait MemTable {
  import MemTable._

  def totalNumRows: Long
  def totalBytesUsed: Long
  def datasets: Set[String]

  val DefaultTopK = 10   // # of top results to return

  def mostStaleDatasets(k: Int = DefaultTopK): Seq[String]
  def mostStalePartitions(dataset: String, k: Int = DefaultTopK): Seq[PartitionKey]

  /**
   * === Row ingest, read, delete operations ===
   */

  /**
   * Ingests a bunch of new rows for a given table.  The rows will be grouped by partition key and then
   * sorted based on the superprojection sort order.
   * @param dataset the Dataset to ingest
   * @param rows the rows to ingest
   * @param timestamp the write timestamp to associate with the rows, used to calculate staleness
   * @returns Ingested or PleaseWait, if the MemTable is too full.
   */
  def ingestRows(dataset: Dataset, rows: Seq[RowReader], timestamp: Long): IngestionResponse

  def readRows[K](keyRange: KeyRange[K], sortOrder: SortOrder): Iterator[RowReader]

  def removeRows[K](keyRange: KeyRange[K]): Unit

  /**
   * Returns the key range encompassing all the rows in a given partition of a dataset.
   */
  def getKeyRange[K](dataset: Dataset, partition: PartitionKey): KeyRange[K]
}

