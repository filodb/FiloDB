package filodb.core.reprojector

import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.Dataset

/**
 * Holds the current state of a dataset reprojector, especially outstanding ColumnStore tasks.
 */
trait ReprojectorState[K] {
  def dataset: Dataset
  def outstandingTasks: Map[KeyRange[K], Future[Response]]
}

/**
 * The Reprojector flushes rows out of the MemTable and writes out Segments to the ColumnStore.
 * Most of the work done by this actual implementation should just be figuring out which rows/keyranges/
 * partitions/segments to flush.  Most of the work should be done asynchronously.
 */
trait Reprojector {
  /**
   * Does reprojection (columnar flushes from memtable) for a single dataset.  Returns an updated copy of
   * the ReprojectorState.  Side effects:
   *  - ColumnStore I/O
   *  - May delete rows from MemTable that have been acked successfully as flushed by ColumnStore.
   *
   * @returns an updated ReprojectorState with current outstanding I/O requests. Stale ones may be cleaned up.
   */
  def reproject[K](state: ReprojectorState[K]): ReprojectorState[K]

  /**
   * Cleans up tasks that have been done from the ReprojectorState.
   */
  def cleanup[K](state: ReprojectorState[K]): ReprojectorState[K]
}