package filodb.coordinator

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}

import filodb.core.AbstractSpec
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object ActorSpecConfig {
  // Use a random available port for each test to avoid port conflicts
  def getFreePort: Int = {
    val socket = new java.net.ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  def getNewSystem(name: String, config: Config): ActorSystem = {
    ActorSystem(name, config)
  }
}

trait ActorSpecConfig {
  val defaultConfig = """
                      |akka.debug.receive = on
                      |akka.log-dead-letters = 0
                      |akka.log-dead-letters-during-shutdown = off
                      |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
                      |akka.loggers = ["akka.testkit.TestEventListener"]
                      |akka.debug.unhandled = on
                      """.stripMargin
  // Making this lazy is needed for overrides to work successfully
  lazy val configString = defaultConfig
  // Allow Java system properties to set config options like akka.test.timefactor
  lazy val config = ConfigFactory.parseString(configString)
    .withFallback(ConfigFactory.parseResources("application_test.conf"))
    .withFallback(ConfigFactory.load("filodb-defaults.conf")).resolve()

  def getNewSystem = {
    FilodbSettings.reset()  // Reset to ensure fresh initialization
    FilodbSettings.initialize(config)
    ActorSpecConfig.getNewSystem("test", config)
  }
}

trait SeedNodeConfig {
  val host = InetAddress.getLocalHost.getHostAddress
  // Use a random port within a range to avoid conflicts between tests
  val port = 25520 + util.Random.nextInt(1000)
}

abstract class AbstractTestKit(system: ActorSystem) extends TestKit(system)
  with Suite with Matchers
  with BeforeAndAfterAll with BeforeAndAfter
  with ImplicitSender {

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}

abstract class ActorTest(system: ActorSystem) extends AbstractTestKit(system) with AnyFunSpecLike

object AkkaSpec {
  val host = InetAddress.getLocalHost.getHostAddress

  def getFreePort: Int = 25520 + util.Random.nextInt(10000)

  def userConfig(port: Int) = ConfigFactory.parseString(
    s"""
      |filodb {
      |  seed-nodes = ["akka://filo-standalone@$host:$port"]
      |  dataset-definitions {
      |    prometheus {
      |      string-columns = []
      |      double-columns = ["value"]
      |      long-columns   = ["timestamp"]
      |      int-columns    = []
      |      map-columns    = ["tags"]
      |      partition-keys = ["tags"]
      |      row-keys       = ["timestamp"]
      |    }
      |  }
      |}
    """.stripMargin)

  val logAkkaToConsole = sys.env.get("LOG_AKKA_TO_CONSOLE")
                                .map(x => s"""\nakka.loggers = ["akka.testkit.TestEventListener"]""")
                                .getOrElse("")

  def serverConfig(port: Int) = ConfigFactory.parseString(
   s"""akka.remote.artery.canonical.port = $port
      |akka.remote.artery.canonical.host = $host
      |akka.log-received-messages = on
      |akka.log-sent-messages = on
      |akka.debug.lifecycle = on
      |akka.jvm-exit-on-fatal-error = off
    """.stripMargin + logAkkaToConsole)
    .withFallback(ConfigFactory.load("application_test.conf")).resolve()

  def settings(port: Int) = new FilodbSettings(userConfig(port).withFallback(serverConfig(port)))

  // Keep a default settings for backward compatibility
  lazy val defaultPort: Int = getFreePort
  lazy val settings: FilodbSettings = settings(defaultPort)

  def getNewSystem(c: Option[Config] = None): ActorSystem = {
    ActorSpecConfig.getNewSystem("test", c.map(_.withFallback(settings.allConfig)) getOrElse settings.allConfig)
  }
}

abstract class AkkaSpec(system: ActorSystem) extends AbstractTestKit(system)
  with SeedNodeConfig
  with AnyWordSpecLike
  with Eventually
  with IntegrationPatience
  with Matchers
  with ScalaFutures {

  // Use a fresh port for each test instance to avoid conflicts
  def this() = this({
    val port = AkkaSpec.getFreePort
    ActorSpecConfig.getNewSystem("akka-test", AkkaSpec.settings(port).allConfig)
  })
  def this(config: Config) = this({
    val port = AkkaSpec.getFreePort
    ActorSpecConfig.getNewSystem("akka-test", config.withFallback(AkkaSpec.settings(port).allConfig))
  })

}

trait RunnableSpec extends AbstractSpec with SeedNodeConfig {

  System.setProperty("filodb.seed-nodes", s"akka://filo-standalone@$host:$port")

  override def afterAll(): Unit = {
    super.afterAll()
    System.clearProperty("filodb.seed-nodes")
  }
}
