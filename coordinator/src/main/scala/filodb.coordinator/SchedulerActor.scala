package filodb.coordinator

import akka.actor.{Actor, ActorRef, Props}

import filodb.core.reprojector.Scheduler

object SchedulerActor {
  case object RunOnce
  case object ReportStats

  case object Flushed
  case object NoSlotsAvailable

  def props(scheduler: Scheduler): Props =
    Props(classOf[SchedulerActor], scheduler)
}

/**
 * A really simple Actor wrapper around the FiloDB Scheduler.
 * Basically allows use of Akka Scheduler to schedule regular heartbeats.
 * Should be a singleton (TODO: how to enforce singleton?)
 * Having this be a separate actor is good because scheduling operations
 * can take up some CPU cycles (for chunking and scheduling I/O)
 */
class SchedulerActor(scheduler: Scheduler) extends BaseActor {
  import SchedulerActor._

  def receive: Receive = {
    case RunOnce =>
      scheduler.runOnce()

    case ReportStats =>
      val stats = scheduler.stats
      logger.info(s"Scheduler tasks: ${stats.activeTasks}")
      logger.info(s"Failed tasks: ${stats.failedTasks}")
      logger.info(s"MemTable active table rows: ${stats.activeRows}")
      logger.info(s"MemTable flushing tables: ${stats.flushingRows}")
      // TODO: Check for any failures, and report them ... perhaps on EventBus?

    case NodeCoordinatorActor.Flush(dataset, version) =>
      scheduler.flush(dataset, version) match {
        case Scheduler.Flushed          => sender ! Flushed
        case Scheduler.NoAvailableTasks => sender ! NoSlotsAvailable
      }
      // Arguably there is no point to retry, because if there are no slots,
      // eventually the dataset will get flushed in subsequent scheduler runs, depending
      // on the FlushPolicy.
  }
}
