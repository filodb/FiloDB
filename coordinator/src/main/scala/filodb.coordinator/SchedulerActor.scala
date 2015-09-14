package filodb.coordinator

import akka.actor.{Actor, ActorRef, Props}

import filodb.core.reprojector.Scheduler

object SchedulerActor {
  case object RunOnce

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
      // TODO: Check for any failures, and report them ... perhaps on EventBus?
    case CoordinatorActor.Flush(dataset, version) =>
      scheduler.flush(dataset, version)
      // TODO: check response.
      // Though, arguably there is no point to retry, because if there are no slots,
      // eventually the dataset will get flushed in subsequent scheduler runs, depending
      // on the FlushPolicy.
  }
}
