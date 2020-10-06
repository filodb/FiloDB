package filodb.memory.data

import com.typesafe.scalalogging.StrictLogging

object Shutdown extends StrictLogging {

  def haltAndCatchFire(e: Exception, unitTest: Boolean = false): Unit = {
    if (unitTest) throw e
    logger.error(s"Shutting down process since it may be in an unstable/corrupt state", e)
    Runtime.getRuntime.halt(189)
  }

}
