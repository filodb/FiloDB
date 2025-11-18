package filodb.memory.data

import scala.concurrent.Await

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.core.metrics.FilodbMetrics

object Shutdown extends StrictLogging {

  val forcedShutdowns = FilodbMetrics.counter("forced-shutdowns")
  def haltAndCatchFire(e: Throwable, unitTest: Boolean = false): Unit = {
    forcedShutdowns.increment()
    Thread.sleep(60000)  // Sleep 1m to make sure counter is published.
    if (unitTest) throw e
    logger.error(s"Shutting down process since it may be in an unstable/corrupt state", e)
    import scala.concurrent.duration._
    Await.result(Kamon.stopModules(), 5.minutes)
    Runtime.getRuntime.halt(189)
  }

}
