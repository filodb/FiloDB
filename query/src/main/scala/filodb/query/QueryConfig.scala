package filodb.query

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

object QueryConfig {
  val DefaultVectorsLimit = 150
}

class QueryConfig(queryConfig: Config) {
  lazy val askTimeout = queryConfig.as[FiniteDuration]("ask-timeout")
  lazy val staleSampleAfterMs = queryConfig.getDuration("stale-sample-after").toMillis
  lazy val minStepMs = queryConfig.getDuration("min-step").toMillis
  lazy val fastReduceMaxWindows = queryConfig.getInt("fastreduce-max-windows")

  /**
   * Feature flag test: returns true if the config has an entry with "true", "t" etc
   */
  def has(feature: String): Boolean = queryConfig.as[Option[Boolean]](feature).getOrElse(false)
}

object EmptyQueryConfig extends QueryConfig(queryConfig = ConfigFactory.empty())
