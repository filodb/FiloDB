package filodb.core.query

import scala.concurrent.duration.FiniteDuration
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

object QueryConfig {
  val DefaultVectorsLimit = 150
}

class QueryConfig(private val queryConfig: Config) {
  lazy val askTimeout = queryConfig.as[FiniteDuration]("ask-timeout")
  lazy val staleSampleAfterMs = queryConfig.getDuration("stale-sample-after").toMillis
  lazy val minStepMs = queryConfig.getDuration("min-step").toMillis
  lazy val fastReduceMaxWindows = queryConfig.getInt("fastreduce-max-windows")
  lazy val routingConfig = queryConfig.getConfig("routing")
  lazy val parser = queryConfig.as[String]("parser")
  lazy val translatePromToFilodbHistogram= queryConfig.getBoolean("translate-prom-to-filodb-histogram")

  /**
   * Feature flag test: returns true if the config has an entry with "true", "t" etc
   */
  def has(feature: String): Boolean = queryConfig.as[Option[Boolean]](feature).getOrElse(false)

  override def hashCode(): Int = {
    super.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[QueryConfig] && obj.asInstanceOf[QueryConfig].queryConfig == queryConfig
  }

  override def toString: String = {
    s"QueryContext(configHash=${queryConfig.hashCode()})"
  }
}

object EmptyQueryConfig extends QueryConfig(queryConfig = ConfigFactory.empty())
