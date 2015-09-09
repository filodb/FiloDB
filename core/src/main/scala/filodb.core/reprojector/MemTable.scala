package filodb.core.reprojector

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.RowReader

object MemTable {
  // Ingestion responses
  trait IngestionResponse
  case object Ingested extends IngestionResponse
  case object PleaseWait extends IngestionResponse  // Cannot quite ingest yet
  case object BadSchema extends IngestionResponse

  val DefaultTopK = 10   // # of top results to return
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
  import RowReader._

  def datasets: Set[String]

  def close(): Unit

  /**
   * === Row ingest, read, delete operations ===
   */

  /**
   * Ingests a bunch of new rows for a given table.  The rows will be grouped by partition key and then
   * sorted based on the superprojection sort order.
   * @param dataset the Dataset to ingest
   * @param schema the columns to be ingested, in order of appearance in a row of data
   * @param rows the rows to ingest
   * @param timestamp the write timestamp to associate with the rows, used to calculate staleness
   * @returns Ingested or PleaseWait, if the MemTable is too full.
   */
  def ingestRows[K: TypedFieldExtractor: SortKeyHelper](dataset: Dataset,
                                                        schema: Seq[Column],
                                                        rows: Seq[RowReader],
                                                        timestamp: Long): IngestionResponse

  /**
   * Reads rows out. Note that inserts may be happening while rows are read, so results are not
   * guaranteed to be stable.
   */
  def readRows[K: SortKeyHelper](keyRange: KeyRange[K]): Iterator[RowReader]

  /**
   * Removes specific rows from a particular partition.  Note: we do NOT give a range here, because
   * inserts can be happening to the same range while keys are deleted.
   */
  def removeRows[K: SortKeyHelper](dataset: Dataset, partition: PartitionKey, keys: Seq[K]): Unit

  /**
   * == Operations to help triage rows for reprojection ==
   */

  /**
   * Returns the oldest row in the dataset.
   * @param skipPending if true, skip rows that are marked as pending
   * @returns None if there are no more available rows.
   */
  def oldestAvailableRow[K](dataset: Dataset, skipPending: Boolean = true): Option[(PartitionKey, K)]

  /**
   * Marks a bunch of rows as "pending", perhaps they are being written out to ColumnStore?
   */
  def markRowsAsPending[K](dataset: Dataset, partition: PartitionKey, keys: Seq[K]): Unit

  def numRows(dataset: Dataset): Long

  /**
   * == Common helper funcs ==
   */
  protected def getPartitioningFunc(dataset: Dataset, schema: Seq[Column]):
      Option[RowReader => PartitionKey] = {
    if (dataset.partitionColumn == Dataset.DefaultPartitionColumn) {
      Some(row => Dataset.DefaultPartitionKey)
    } else {
      val partitionColNo = schema.indexWhere(_.hasId(dataset.partitionColumn))
      if (partitionColNo < 0) None else Some(row => row.getString(partitionColNo))
    }
  }
}

