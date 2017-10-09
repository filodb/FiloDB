package filodb.akkabootstrapper

import akka.actor.{ActorSystem, AddressFromURIString}
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._

class SeedNodeHeadDiscoverySpec extends BaseSeedNodeDiscoverySpec(AbstractTestKit.head) {
  "WhitelistSeedDiscovery" must {
    "discover if selfNode is head of list" in {
      seeds.headOption shouldEqual Some(selfAddress)
      val discovery = new WhitelistAkkaClusterSeedDiscovery(cluster, settings)
      // remove self node unless it is in the head of the sorted list.
      discovery.discoverAkkaClusterSeeds.contains(cluster.selfAddress) shouldBe true
      discovery.discoverAkkaClusterSeeds.size shouldEqual settings.seedsWhitelist.size
    }
  }
}

class SeedNodeLastDiscoverySpec extends BaseSeedNodeDiscoverySpec(AbstractTestKit.last) {
  "WhitelistSeedDiscovery" must {
    "discover if selfNode is last of list" in {
      seeds.last shouldEqual selfAddress
      val discovery = new WhitelistAkkaClusterSeedDiscovery(cluster, settings)
      // remove self node unless it is in the head of the sorted list.
      discovery.discoverAkkaClusterSeeds.contains(cluster.selfAddress) shouldBe false
      discovery.discoverAkkaClusterSeeds.size shouldEqual settings.seedsWhitelist.size - 1
    }
  }
}

class AkkaBootstrapperSettingsSpec
  extends AbstractTestKit(ConfigFactory.parseString(
    s"""
       |akka-bootstrapper{
       |  whitelist.seeds = [
       |  "akka.tcp://test@127.0.0.1:0",
       |  "akka://test:127.0.0.1:0" ]
       |}
      """.stripMargin).withFallback(AbstractTestKit.rootConfig))
    with WordSpecLike {

  "AkkaBootstrapperSettings" must {

    val selfAddress = Cluster(system).selfAddress
    val settings = new AkkaBootstrapperSettings(system.settings.config)

    "validate configured seeds" in {
      val seeds = settings.seedsWhitelist
      (settings.seeds._1 ++ settings.seeds._2).size shouldEqual seeds.size
    }
    "catch malformed addresses for user code to decide handling of invalids" in {
      settings.invalidSeeds.size shouldEqual 1
      val valid = settings.seeds._1.collect { case Right(address) => address }
      valid.contains(selfAddress) shouldBe false
    }
  }
}

abstract class BaseSeedNodeDiscoverySpec(config: Config)
  extends AbstractTestKit(config) with WordSpecLike {

  protected val cluster = Cluster(system)
  protected val selfAddress = cluster.selfAddress

  protected val settings = new AkkaBootstrapperSettings(system.settings.config)
  protected val seeds = settings.seedsWhitelist.map(AddressFromURIString(_))

  "WhitelistSeedDiscovery" must {
    "include the self node in seeds if first not malformed and first is self" in {
      seeds.contains(selfAddress) shouldBe true
    }
  }
}

object AbstractTestKit {

  val name = "seed-test"
  val host = "127.0.0.1"
  val port = 2552

  val rootConfig: Config =
    ConfigFactory.parseString(
      s"""
         |akka-bootstrapper {
         |  seed-discovery.timeout = 1 minute
         |  seed-discovery.class = "filodb.akkabootstrapper.WhitelistAkkaClusterSeedDiscovery"
         |  http-seeds.base-url = "http://$host:8080/"
         |}
         |akka.remote.netty.tcp.port = $port
         |akka.remote.netty.tcp.hostname = $host
         |akka.jvm-exit-on-fatal-error = off
         |akka.loggers = ["akka.testkit.TestEventListener"]
         |akka.actor.provider = "cluster"
      """
        .stripMargin)
      .withFallback(ConfigFactory.load("application_test.conf"))

  def head: Config =
    ConfigFactory.parseString(
      s"""
         |akka-bootstrapper{
         |  whitelist.seeds = [
         |   "akka.tcp://$name@$host:$port",
         |   "akka.tcp://$name@$host:2553",
         |   "akka.tcp://$name@$host:2554"
         |  ]
         |}
      """.stripMargin).withFallback(rootConfig)

  def last: Config =
    ConfigFactory.parseString(
      s"""
         |akka-bootstrapper{
         |  whitelist.seeds = [
         |  "akka.tcp://$name@$host:2553",
         |  "akka.tcp://$name@$host:2554",
         |  "akka.tcp://$name@$host:$port" ]
         |}
      """.stripMargin).withFallback(rootConfig)

}

abstract class AbstractTestKit(config: Config)
  extends TestKit(ActorSystem(AbstractTestKit.name, config))
  with Suite with Matchers
  with BeforeAndAfterAll with BeforeAndAfter
  with ImplicitSender {

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
