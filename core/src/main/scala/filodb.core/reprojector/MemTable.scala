package filodb.core.reprojector

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.RowReader
import scala.collection.mutable.HashMap

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset, RichProjection}

/**
 * The MemTable serves these purposes:
 * 1) Holds incoming rows of data before being flushed
 * 2) Can extract rows of data in a given sort order, and remove them
 * 3) Can read rows of data in a given sort order for queries
 *
 * The MemTable handles data for a single dataset or projection.  TODO: support multiple projections?
 *
 * Data written to a MemTable should be logged via WAL or some other mechanism so it can be recovered in
 * case of failure.
 */
trait MemTable[K] extends StrictLogging {
  import RowReader._

  def close(): Unit

  // A RichProjection with valid partitioning column.
  def projection: RichProjection[K]

  /**
   * === Row ingest, read, delete operations ===
   */

  /**
   * Ingests a bunch of new rows.
   * @param rows the rows to ingest.  For now, they must have the exact same columns, in the exact same order,
   *        as in the projection.
   * @param callback the function to call back when the MemTable has committed the new rows to disk.
   *        This is probably done asynchronously as we don't want to commit new rows with every write.
   */
  def ingestRows(rows: Seq[RowReader])(callback: => Unit): Unit

  /**
   * Reads rows out.  If reading from a MemTable actively inserting, the rows read might not reflect
   * latest updates.
   */
  def readRows(keyRange: KeyRange[K]): Iterator[RowReader]

  /**
   * Reads all rows of the memtable out, from every partition.  Partition ordering is not
   * guaranteed, but all sort keys K within the partition will be ordered.
   */
  def readAllRows(): Iterator[(PartitionKey, K, RowReader)]

  /**
   * Removes specific rows from a particular keyRange and version.  Can only remove rows
   * from the Locked buffer.
   */
  def removeRows(keyRange: KeyRange[K]): Unit

  def numRows: Int

  /**
   * Forces any new ingested rows to be committed to WAL/permanent storage so they will be recovered
   * in case the process dies.
   */
  def forceCommit(): Unit

  /**
   * Yes, this clears everything!  It's meant for testing only.
   */
  def clearAllData(): Unit
}
