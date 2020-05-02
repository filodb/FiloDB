package filodb.core.query

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

class QueryConfig(queryConfig: Config) {
  lazy val askTimeout = queryConfig.as[FiniteDuration]("ask-timeout")
  lazy val staleSampleAfterMs = queryConfig.getDuration("stale-sample-after").toMillis
  lazy val minStepMs = queryConfig.getDuration("min-step").toMillis
  lazy val fastReduceMaxWindows = queryConfig.getInt("fastreduce-max-windows")
  lazy val routingConfig = queryConfig.getConfig("routing")

  /**
   * Feature flag test: returns true if the config has an entry with "true", "t" etc
   */
  def has(feature: String): Boolean = queryConfig.as[Option[Boolean]](feature).getOrElse(false)
}

object QueryConfig {
  val DefaultVectorsLimit = 150
}

object EmptyQueryConfig extends QueryConfig(queryConfig = ConfigFactory.empty())
