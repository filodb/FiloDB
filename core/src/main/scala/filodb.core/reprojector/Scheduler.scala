package filodb.core.reprojector

import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.concurrent.Future
import scala.util.{Failure, Success}

import filodb.core._
import filodb.core.Types._
import filodb.core.metadata.MetaStore

object Scheduler {
  val DefaultMaxTasks = 16
}

/**
 * The Scheduler is a stateful class for scheduling reprojection tasks.
 * - Checks the FlushPolicy to see if new flush cycles need to be started
 * - Maintains existing Flush tasks, keeps them going
 *
 * One reprojection task per (dataset, version) is scheduled at a time.
 *
 * IMPORTANT: Not meant to be called concurrently, since all the work is done offline in futures.
 * This should be instantiated as a singleton.
 */
class Scheduler(memTable: MemTable,
                reprojector: Reprojector,
                flushPolicy: FlushPolicy,
                maxTasks: Int = Scheduler.DefaultMaxTasks) extends StrictLogging {
  import Scheduler._
  logger.info(s"Starting Scheduler with memTable $memTable, reprojector $reprojector, and $flushPolicy")

  // Keeps track of active reprojection tasks, one per dataset/version
  var tasks = Map.empty[(TableName, Int), Future[Response]]

  /**
   * Call this periodically to maintain reprojection tasks.
   * - Checks if previous tasks have finished
   * - Checks existing flush cycles from the memtable and schedules new projection tasks for flushes
   * - Sees if new flush cycles should be started, per FlushPolicy recommendations
   *
   * IMPORTANT: this is supposed to be called either in a single thread or wrapped in an Actor, not called
   * concurrently.
   */
  def runOnce(): Unit = {
    // Always clean up tasks even if no flush, keep list of tasks current
    tasks = cleanupTasks(tasks)

    // Do we already have flushes in progress?
    // Kick off more reprojection tasks to keep flushes going until Locked tables are empty.
    val moreToFlush = memTable.flushingDatasets.map(_._1).toSet
    val flushingNeedTask = moreToFlush -- tasks.keySet
    logger.debug(s"Flushes in progress: $moreToFlush   needing a task: $flushingNeedTask")
    val tasksLeft = maxTasks - tasks.size
    logger.debug(s"Room for $tasksLeft tasks, starting them...")
    flushingNeedTask.take(tasksLeft).foreach { case (dataset, ver) =>
      addNewTask(dataset, ver)
    }

    // At this point, every dataset with pending flushes should have a task, unless
    // there are not enough slots (maxTasks).
    // If there is room for more tasks, see if new flushes can be started.
    if (tasks.size < maxTasks) {
      flushPolicy.nextFlush(memTable).foreach { case (nextDataset, version) =>
        if (memTable.flipBuffers(nextDataset, version) != MemTable.Flipped) {
          logger.warn("FlushPolicy $flushPolicy nextFlush returned already flushing dataset " +
                      s"($nextDataset/$version)")
          logger.warn("This should not happen, unless Scheduler is running concurrently!")
          return
        }
        logger.info(s"Starting new flush cycle for ($nextDataset/$version)...")
        addNewTask(nextDataset, version)
      }
    } else {
      logger.debug(s"Task table full (${tasks.size} tasks), not starting more flushes...")
    }
  }

  private def addNewTask(dataset: TableName, version: Int): Unit = {
    val newTaskFuture = reprojector.newTask(memTable, dataset, version)
    logger.debug(s"Starting new reprojection task for ($dataset/$version)...")
    tasks = tasks + ((dataset -> version) -> newTaskFuture)
  }

  private def cleanupTasks(tasks: Map[(TableName, Int), Future[Response]]):
      Map[(TableName, Int), Future[Response]] =
    tasks.filterNot { case (nameVer, taskFuture) => isComplete(nameVer, taskFuture) }

  private def isComplete(nameVer: (TableName, Int), taskFuture: Future[Response]): Boolean =
    taskFuture.value match {
      case None             => false
      case Some(Failure(t)) => logger.error(s"Reprojection task $nameVer failed", t)
                               true
      case Some(Success(r)) => logger.debug(s"Reprojection task $nameVer succeeded: $r")
                               true
    }
}