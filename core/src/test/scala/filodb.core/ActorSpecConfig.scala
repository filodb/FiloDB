package filodb.core

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

trait ActorSpecConfig {
  val defaultConfig = """
                      | akka.log-dead-letters = 0
                      """.stripMargin
  // Making this lazy is needed for overrides to work successfully
  lazy val configString = defaultConfig
  // Allow Java system properties to set config options like akka.test.timefactor
  lazy val config = ConfigFactory.parseString(configString).withFallback(ConfigFactory.defaultOverrides())
  def getNewSystem = ActorSystem("test", config)
}