package filodb.core.reprojector

import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.{Dataset, Column}

/**
 * The Reprojector flushes rows out of the MemTable and writes out Segments to the ColumnStore.
 * All of the work should be done asynchronously.
 * The reprojector should be stateless.  It takes MemTables and creates Futures for reprojection tasks.
 */
trait Reprojector {
  /**
   * Does reprojection (columnar flushes from memtable) for a single dataset.
   * Should be completely stateless.
   * Does not need to reproject all the rows from the Locked memtable, but should progressively
   * delete rows from the memtable until there are none left.  This is how the reprojector marks progress:
   * by deleting rows that has been committed to ColumnStore.
   *
   * Failures:
   * The Scheduler only schedules one reprojection task at a time per (dataset, version), so if this fails,
   * then it can be rerun.
   *
   * Most likely this will involve scheduling a whole bunch of futures to write segments.
   * Be careful to do too much work, newTask is supposed to not take too much CPU time and use Futures
   * to do work asynchronously.  Also, scheduling too many futures leads to long blocking time and
   * memory issues.
   *
   * @returns a Future[Response], representing a reprojection task.  It should only modify memtable state
   *          if the task succeeds.
   */
  def newTask(memTable: MemTable, dataset: Types.TableName, version: Int): Future[Response] = {
    import Column.ColumnType._

    val setup = memTable.getIngestionSetup(dataset, version).getOrElse(
                  throw new IllegalArgumentException(s"Could not find $dataset/$version"))
    setup.schema(setup.sortColumnNum).columnType match {
      case LongColumn    => reproject[Long](memTable, setup.dataset, setup.schema, version)
      case other: Column.ColumnType => throw new RuntimeException("Illegal sort key type $other")
    }
  }

  // The inner, typed reprojection task launcher that must be implemented.
  def reproject[K](memTable: MemTable, dataset: Dataset, schema: Seq[Column], version: Int): Future[Response]
}