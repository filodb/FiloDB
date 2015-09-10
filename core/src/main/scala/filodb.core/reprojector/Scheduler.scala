package filodb.core.reprojector

import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.concurrent.Future
import scala.util.{Failure, Success}

import filodb.core._
import filodb.core.Types._
import filodb.core.metadata.MetaStore

object Scheduler {
  val DefaultMaxTasks = 16

  trait SchedulerResponse
  case object NoFlushNeeded extends SchedulerResponse
  case object TooManyTasks extends SchedulerResponse
  case class TaskScheduled(dataset: TableName) extends SchedulerResponse

  case class SchedulerTask(dataset: TableName, version: Int, task: Future[Response])
}

/**
 * The Scheduler is a stateful class for scheduling reprojection tasks.
 * It keeps track of outstanding reprojection tasks and balances against resources.
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

  var tasks = Seq.empty[SchedulerTask]
  var currentFlush: Option[(TableName, Int)] = None

  /**
   * Called periodically to determine if a MemTable flush needs to occur, and if so, what dataset
   * needs to be flushed.  The dataset's Reprojector is then called.
   *
   * The reprojector returns Futures, and the scheduler is responsible for limiting the number of
   * outstanding Futures.   It is up to the reprojector to decide how much work to do.
   *
   * IMPORTANT: this is supposed to be called either in a single thread or wrapped in an Actor, not called
   * concurrently.
   */
  def runOnce(): SchedulerResponse = {
    // Always clean up tasks even if no flush, keep list of tasks current
    cleanupTasks()

    // Do we already have a flush in progress?  Keep it going.  Otherwise, start a new one.
    if (currentFlush.isDefined || flushPolicy.shouldFlush(memTable)) {
      if (isTooManyTasks) return TooManyTasks

      val (nextDataset, version) = currentFlush.getOrElse {
        logger.debug(s"Starting new flush cycle...")
        flushPolicy.nextFlush(memTable)
      }

      val newTaskFuture = reprojector.newTask(memTable, nextDataset, version)
      logger.debug(s"Starting new reprojection task for ($nextDataset/$version)...")
      tasks = tasks :+ SchedulerTask(nextDataset, version, newTaskFuture)
      currentFlush = Some((nextDataset, version))
      TaskScheduled(nextDataset)
    } else {
      NoFlushNeeded
    }
  }

  private def isTooManyTasks: Boolean = {
    if (tasks.size >= maxTasks) {
      logger.info(s"TooManyTasks: ${tasks.size} outstanding tasks, but $maxTasks allowed")
      true
    } else {
      false
    }
  }

  private def cleanupTasks(): Unit = {
    def cleanupRest(tasks: Seq[SchedulerTask]): Seq[SchedulerTask] = {
      tasks match {
        case Nil          => Nil
        case task :: tail => if (isComplete(task)) { cleanupRest(tail) }
                             else { Vector(task) ++ cleanupRest(tail) }
      }
    }

    tasks = cleanupRest(tasks)
  }

  private def isComplete(task: SchedulerTask): Boolean = task match {
    case SchedulerTask(dataset, version, taskFuture) => taskFuture.value match {
      case None             => false
      case Some(Failure(t)) => logger.error("Reprojection task ($dataset/$version) failed", t)
                               true
      case Some(Success(r)) => logger.debug("Reprojection task ($dataset/$version) succeeded: $r")
                               true
    }
  }
}