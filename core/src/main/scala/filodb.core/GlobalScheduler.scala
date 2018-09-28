package filodb.core

import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Scheduler, UncaughtExceptionReporter}


object GlobalScheduler extends StrictLogging {

  /**
    * Should not use Scala or Monix's default Global Implicit since it does
    * not have a configurable uncaught exception handler.
    */
  implicit lazy val globalImplicitScheduler = Scheduler.computation(
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in GlobalScheduler", _)))
}
