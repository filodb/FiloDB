package filodb.core.query

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

object QueryConfig {
  val DefaultVectorsLimit = 150
}

class QueryConfig(@transient queryConfig: Config) {
  val askTimeout = queryConfig.as[FiniteDuration]("ask-timeout")
  val staleSampleAfterMs = queryConfig.getDuration("stale-sample-after").toMillis
  val minStepMs = queryConfig.getDuration("min-step").toMillis
  val fastReduceMaxWindows = queryConfig.getInt("fastreduce-max-windows")
  val routingConfig = queryConfig.getConfig("routing")
  val parser = queryConfig.as[String]("parser")
  val translatePromToFilodbHistogram = queryConfig.getBoolean("translate-prom-to-filodb-histogram")
  val fasterRateEnabled = queryConfig.as[Option[Boolean]]("faster-rate").getOrElse(false)
}

/**
 * IMPORTANT: Use this for testing only, using this for anything other than testing may yield undesired behavior
 */
object EmptyQueryConfig extends QueryConfig(queryConfig = ConfigFactory.parseString(
  """
    |    ask-timeout = 10 seconds
    |    stale-sample-after = 5 minutes
    |    sample-limit = 1000000
    |    min-step = 1 ms
    |    faster-rate = true
    |    fastreduce-max-windows = 50
    |    translate-prom-to-filodb-histogram = true
    |    parser = "antlr"
    |    routing {
    |      # not currently used
    |    }
    |""".stripMargin))
