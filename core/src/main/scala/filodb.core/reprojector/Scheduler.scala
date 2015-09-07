package filodb.core.reprojector

/**
 * The Scheduler is responsible for scheduling reprojection tasks.
 */
trait Scheduler {
  val memTable: MemTable
  val flushPolicy: FlushPolicy

  /**
   * Called periodically to determine if a MemTable flush needs to occur, and if so, what dataset
   * needs to be flushed.  The dataset's Reprojector is then called.
   */
  def runOnce(): Unit
}