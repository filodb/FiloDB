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
  def getNewSystem(name: String, config: Config): ActorSystem = {
    // Delay between each test, to provide some allowance for the port binding to succeed.
    // Ideally the port binding should automatically retry, but this is a simpler fix.
    Thread.sleep(5000)
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
    .withFallback(ConfigFactory.load("filodb-defaults.conf"))

  def getNewSystem = {
    FilodbSettings.initialize(config)
    ActorSpecConfig.getNewSystem("test", config)
  }
}

trait SeedNodeConfig {
  val host = InetAddress.getLocalHost.getHostAddress
  val port = 2552
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

object AkkaSpec extends SeedNodeConfig {

  val userConfig = ConfigFactory.parseString(
    s"""
      |filodb {
      |  seed-nodes = ["akka.tcp://filo-standalone@$host:$port"]
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

  val serverConfig = ConfigFactory.parseString(
   s"""akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.host = $host
      |akka.log-received-messages = on
      |akka.log-sent-messages = on
      |akka.debug.lifecycle = on
      |akka.jvm-exit-on-fatal-error = off
    """.stripMargin + logAkkaToConsole)
    .withFallback(ConfigFactory.load("application_test.conf"))

  val settings = new FilodbSettings(userConfig.withFallback(serverConfig))

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

  def this() = this(ActorSpecConfig.getNewSystem("akka-test", AkkaSpec.settings.allConfig))
  def this(config: Config) =
    this(ActorSpecConfig.getNewSystem("akka-test", config.withFallback(AkkaSpec.settings.allConfig)))

}

trait RunnableSpec extends AbstractSpec with SeedNodeConfig {

  System.setProperty("filodb.seed-nodes", s"akka.tcp://filo-standalone@$host:$port")

  override def afterAll(): Unit = {
    super.afterAll()
    System.clearProperty("filodb.seed-nodes")
  }
}
