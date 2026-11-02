package filodb.memory.data

import scala.concurrent.Await

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.core.metrics.FilodbMetrics

object Shutdown extends StrictLogging {

  val forcedShutdowns = FilodbMetrics.counter("forced-shutdowns")

  // Set via -Dfilodb.unit-test=true in forked test JVMs. Without this, a deliberately injected failure in one
  // test would halt the whole shared test JVM (exit code 189) a minute later, killing unrelated tests.
  private val unitTestMode = java.lang.Boolean.getBoolean("filodb.unit-test")

  def haltAndCatchFire(e: Throwable, unitTest: Boolean = false): Unit = {
    forcedShutdowns.increment()
    if (unitTestMode && !unitTest) {
      logger.error("Unit test mode: skipping process halt that would otherwise follow this error", e)
    } else {
      Thread.sleep(60000)  // Sleep 1m to make sure counter is published.
      if (unitTest) throw e
      logger.error(s"Shutting down process since it may be in an unstable/corrupt state", e)
      import scala.concurrent.duration._
      Await.result(Kamon.stopModules(), 5.minutes)
      Runtime.getRuntime.halt(189)
    }
  }

}
