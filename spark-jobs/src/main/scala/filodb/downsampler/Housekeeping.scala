package filodb.downsampler

import com.datastax.driver.core.Session
import com.typesafe.config.Config
import com.typesafe.scalalogging.{Logger, StrictLogging}
import monix.execution.Scheduler

import filodb.core.concurrentCache

object Housekeeping extends StrictLogging {
  lazy protected[downsampler] val dsLogger: Logger = logger
  lazy protected[downsampler] val sessionMap = concurrentCache[Config, Session](2)

  lazy protected[downsampler] val readSched = Scheduler.io("cass-read-sched")
  lazy protected[downsampler] val writeSched = Scheduler.io("cass-write-sched")


}
