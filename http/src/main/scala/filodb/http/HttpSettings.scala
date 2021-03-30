package filodb.http

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.FilodbSettings

class HttpSettings(config: Config, val filoSettings: FilodbSettings) {
  lazy val httpServerBindHost = config.getString("filodb.http.bind-host")
  lazy val httpServerBindPort = config.getInt("filodb.http.bind-port")
  lazy val httpServerStartTimeout = config.getDuration("filodb.http.start-timeout")
  lazy val httpRuntimeApiRoutes = config.getStringList("filodb.http.runtime-routes")

  lazy val queryDefaultSpread = config.getInt("filodb.spread-default")
  lazy val querySampleLimit = config.getInt("filodb.query.sample-limit")
  lazy val queryAskTimeout = config.as[FiniteDuration]("filodb.query.ask-timeout")
  lazy val queryBinaryJoinCardLimit = config.getInt("filodb.query.join-cardinality-limit")
  lazy val queryGroupByCardLimit = config.getInt("filodb.query.group-by-cardinality-limit")
}
