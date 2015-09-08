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

  def totalNumRows: Long
  def totalBytesUsed: Long
  def datasets: Set[String]

  def mostStaleDatasets(k: Int = DefaultTopK): Seq[String]
  def mostStalePartitions(dataset: String, k: Int = DefaultTopK): Seq[PartitionKey]

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
  def ingestRows[K: TypedFieldExtractor](dataset: Dataset,
                                         schema: Seq[Column],
                                         rows: Seq[RowReader],
                                         timestamp: Long): IngestionResponse

  def readRows[K](keyRange: KeyRange[K]): Iterator[RowReader]

  def removeRows[K](keyRange: KeyRange[K]): Unit

  /**
   * Returns the key range encompassing all the rows in a given partition of a dataset.
   */
  def getKeyRange[K: SortKeyHelper](dataset: Dataset, partition: PartitionKey): KeyRange[K]

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

