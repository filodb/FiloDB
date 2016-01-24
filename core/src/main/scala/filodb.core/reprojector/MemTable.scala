package filodb.core.reprojector

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.RowReader
import scala.collection.mutable.HashMap

import filodb.core.KeyRange
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
 *
 * MemTables are owned by the DatasetCoordinatorActor, so writes do not have to be thread-safe.
 */
trait MemTable extends StrictLogging {
  import RowReader._

  def close(): Unit

  // A RichProjection with valid partitioning, segment, row key columns.
  val projection: RichProjection

  /**
   * === Row ingest, read, delete operations ===
   */

  /**
   * Ingests a bunch of new rows.  When this method returns, the rows will have been comitted to disk
   * such that a crash could be recoverable.
   * @param rows the rows to ingest.  For now, they must have the exact same columns, in the exact same order,
   *        as in the projection.  Also, the caller should do buffering; ingesting a very small number of rows
   *        might be extremely inefficient.
   */
  def ingestRows(rows: Seq[RowReader]): Unit

  /**
   * Reads rows out.  If reading from a MemTable actively inserting, the rows read might not reflect
   * latest updates.
   */
  def readRows(keyRange: KeyRange[projection.PK, projection.SK]): Iterator[RowReader]

  /**
   * Reads all rows of the memtable out, from every partition.  Partition ordering is not
   * guaranteed, but all sort keys K within the partition will be ordered.
   */
  def readAllRows(): Iterator[(projection.PK, projection.SK, projection.RK, RowReader)]

  def numRows: Int

  /**
   * Yes, this clears everything!  It's meant for testing only.
   */
  def clearAllData(): Unit
}
