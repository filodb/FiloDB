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
   * delete rows from the memtable until there are none left.
   *
   * @returns a Future[Response], representing a reprojection task.  It should only modify memtable state
   *          if the task succeeds.
   */
  def newTask[K](memTable: MemTable, dataset: Types.TableName, version: Int): Future[Response]
}