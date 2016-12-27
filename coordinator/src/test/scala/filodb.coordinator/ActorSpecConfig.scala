package filodb.core

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpecLike, Matchers, BeforeAndAfter, BeforeAndAfterAll}

trait ActorSpecConfig {
  val defaultConfig = """
                      | akka.log-dead-letters = 0
                      | akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
                      | akka.loggers = ["akka.testkit.TestEventListener"]
                      """.stripMargin
  // Making this lazy is needed for overrides to work successfully
  lazy val configString = defaultConfig
  // Allow Java system properties to set config options like akka.test.timefactor
  lazy val config = ConfigFactory.parseString(configString)
                                 .withFallback(ConfigFactory.load("application_test.conf"))
  def getNewSystem = ActorSystem("test", config)
}

abstract class ActorTest(system: ActorSystem) extends TestKit(system)
with FunSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfter with ImplicitSender {
  override def afterAll() : Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
