package filodb.http

import com.typesafe.config.Config

class HttpSettings(config: Config) {
  lazy val httpServerBindHost = config.getString("filodb.http.bind-host")
  lazy val httpServerBindPort = config.getInt("filodb.http.bind-port")
  lazy val httpServerStartTimeout = config.getDuration("filodb.http.start-timeout")

  lazy val queryDefaultSpread = config.getInt("filodb.spread.default")
  lazy val querySampleLimit = config.getInt("filodb.query.sample-limit")
}
