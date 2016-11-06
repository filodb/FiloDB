package filodb.core.reprojector

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.RowReader
import scala.collection.mutable.HashMap

import filodb.core.KeyRange
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store.SegmentInfo

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
   * Reads rows out from one segment.
   * readRows is an unsafe read - it uses a fast FiloRowReader with mutable state so cannot be used in Seqs
   * but only Iterators.  safeReadRows is safe for use in Seqs but slower.
   */
  def readRows(partition: projection.PK, segment: projection.SK): Iterator[RowReader]

  def readRows(segInfo: SegmentInfo[projection.PK, projection.SK]): Iterator[RowReader] =
    readRows(segInfo.partition, segInfo.segment)

  def safeReadRows(segInfo: SegmentInfo[projection.PK, projection.SK]): Iterator[RowReader]

  /**
   * Reads all segments contained in the MemTable.  No particular order is guaranteed.
   */
  def getSegments(): Iterator[(projection.PK, projection.SK)]

  def numRows: Int

  /**
   * Yes, this clears everything!  It's meant for testing only.
   */
  def clearAllData(): Unit
}
