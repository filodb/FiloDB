package filodb.core.reprojector

import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import filodb.core._
import filodb.core.Types._
import filodb.core.metadata.MetaStore

object Scheduler {
  val DefaultMaxTasks = 16
  val DefaultMaxFailures = 32

  sealed trait FlushResponse
  case object Flushed extends FlushResponse
  case object NoAvailableTasks extends FlushResponse

  // Reports on scheduler and memtable stats, good for debugging
  case class SchedulerStats(activeTasks: Set[(TableName, Int)],
                            failedTasks: Seq[((TableName, Int), Throwable)],
                            activeRows: Seq[((TableName, Int), Long)],
                            flushingRows: Seq[((TableName, Int), Long)])
}

/**
 * The Scheduler is a stateful class for scheduling reprojection tasks.
 * - Checks the FlushPolicy to see if new flush cycles need to be started
 * - Maintains existing Flush tasks, keeps them going
 *
 * One reprojection task per (dataset, version) is scheduled at a time.
 *
 * IMPORTANT: Not meant to be called concurrently, since all the work is done offline in futures.
 * This should be instantiated as a singleton and invoked only from a single thread
 * or from a single Actor.
 */
class Scheduler(memTable: MemTable,
                reprojector: Reprojector,
                flushPolicy: FlushPolicy,
                maxTasks: Int = Scheduler.DefaultMaxTasks,
                maxFailures: Int = Scheduler.DefaultMaxFailures) extends StrictLogging {
  import Scheduler._
  logger.info(s"Starting Scheduler with memTable $memTable, reprojector $reprojector, and $flushPolicy")

  // Keeps track of active reprojection tasks, one per dataset/version
  type TaskMap = Map[(TableName, Int), Future[Seq[String]]]
  val EmptyTaskMap = Map.empty[(TableName, Int), Future[Seq[String]]]
  var tasks = EmptyTaskMap

  // A list of failed Tasks with the most recent failure in front. Capped.
  var failedTasks = List.empty[((TableName, Int), Throwable)]

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
    val tasksLeft = maxTasks - tasks.size
    if (flushingNeedTask.nonEmpty) {
      logger.debug(s"Flushes in progress: $moreToFlush   needing a task: $flushingNeedTask")
      logger.debug(s"Room for $tasksLeft tasks, starting them...")
      flushingNeedTask.take(tasksLeft).foreach { case (dataset, ver) =>
        addNewTask(dataset, ver)
      }
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

  /**
   * Initiates a flush cycle manually for a given dataset and version.
   * This might be called by an ingestion source when it is done with the ingestion, for example.
   * @return Flushed, or NoAvailableTasks if there are no slots available to start a flush.
   */
  def flush(dataset: TableName, version: Int): FlushResponse = {
    if (tasks contains (dataset -> version)) return Flushed
    if (tasks.size < maxTasks) {
      // NOTE: doens't matter if the memtable could be flipped.  If it cannot, that means existing
      // flush in progress, but no active task, so we can initiate a task.
      memTable.flipBuffers(dataset, version)
      logger.info(s"Starting new flush cycle for ($dataset/$version)...")
      addNewTask(dataset, version)
      Flushed
    } else {
      NoAvailableTasks
    }
  }

  def stats: SchedulerStats =
    SchedulerStats(tasks.keySet,
                   failedTasks,
                   memTable.allNumRows(MemTable.Active, true),
                   memTable.flushingDatasets)

  /**
   * Returns a Future for any outstanding reprojection tasks for a given dataset
   * @return Future[Nil] if there are no reprojection tasks for given dataset/version
   *          Future[Nil] for a reprojection task that errors out
   *          Future[Seq[String]] for a reprojection task that finishes normally
   */
  def waitForReprojection(dataset: TableName, version: Int)
                         (implicit ec: ExecutionContext): Future[Seq[String]] = {
    tasks.get((dataset, version))
         .map { taskFuture =>
           taskFuture.recoverWith {
             case t: Throwable => Future.successful(Nil)
           }
         }.getOrElse(Future.successful(Nil))
  }

  /**
   * Same as waitForReprojection but acts on all versions of a given dataset
   * @return a Seq of (version, Seq[String]) pairs
   */
  def waitForReprojection(dataset: TableName)
                         (implicit ec: ExecutionContext): Future[Seq[(Int, Seq[String])]] = {
    val matchingVers = tasks.keys.filter { case (ds, _) => ds == dataset }.toSeq
    Future.sequence(matchingVers.map { case (_, ver) =>
                      waitForReprojection(dataset, ver).map { details => (ver, details) }
                    })
  }

  // Should be used mostly for tests
  def reset(): Unit = {
    tasks = EmptyTaskMap
    failedTasks = List.empty[((TableName, Int), Throwable)]
  }

  // TODO: limit retries of failed tasks?  What to do with rows in memtable?
  private def addNewTask(dataset: TableName, version: Int): Unit = {
    val newTaskFuture = reprojector.newTask(memTable, dataset, version)
    logger.debug(s"Starting new reprojection task for ($dataset/$version)...")
    tasks = tasks + ((dataset -> version) -> newTaskFuture)
  }

  private def cleanupTasks(tasks: TaskMap): TaskMap =
    tasks.filterNot { case (nameVer, taskFuture) => isComplete(nameVer, taskFuture) }

  private def isComplete(nameVer: (TableName, Int), taskFuture: Future[Seq[String]]): Boolean =
    taskFuture.value match {
      case None             => false
      case Some(Failure(t)) => logger.error(s"Reprojection task $nameVer failed", t)
                               failedTasks = ((nameVer, t) :: failedTasks).take(maxFailures)
                               true
      case Some(Success(r)) => logger.info(s"Reprojection task $nameVer succeeded: ${r.toList}")
                               true
    }
}