package filodb.memory.data

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

object Shutdown extends StrictLogging {

  val forcedShutdowns = Kamon.counter("forced-shutdowns").withoutTags()
  def haltAndCatchFire(e: Exception, unitTest: Boolean = false): Unit = {
    forcedShutdowns.increment()
    if (unitTest) throw e
    logger.error(s"Shutting down process since it may be in an unstable/corrupt state", e)
    Runtime.getRuntime.halt(189)
  }

}
