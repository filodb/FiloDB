package filodb.akkabootstrapper

import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpecLike

class ValidSeedValidatorSpec extends BaseSeedNodeDiscoverySpec(AbstractTestKit.head) {
  "Valid WhitelistSeedValidator" must {
    "return expected valid seed nodes for valid configuration" in {
      val strategy = new WhitelistClusterSeedDiscovery(cluster, settings)
      strategy.invalidSeedNodes.isEmpty shouldBe true
      strategy.validSeedNodes.size shouldEqual settings.seedsWhitelist.size
      strategy.discoverClusterSeeds.size shouldEqual strategy.validSeedNodes.size
    }
  }
}

class InvalidSeedValidatorSpec extends AbstractTestKit(
  ConfigFactory.parseString(
    s"""
       |akka-bootstrapper.whitelist.seeds = [
       |  "akka.tcp://test@127.0.0.1:0", "akka://test:127.0.0.1:0", "akka.tcp://test@localhost" ]
      """.stripMargin).withFallback(AbstractTestKit.rootConfig))
  with WordSpecLike {

  "Invalid WhitelistSeedValidator" must {
    "return expected invalid seed nodes for invalid configuration" in {
      val settings = new AkkaBootstrapperSettings(system.settings.config)
      val strategy = new WhitelistClusterSeedDiscovery(Cluster(system), settings)
      strategy.invalidSeedNodes.size shouldEqual settings.seedsWhitelist.size - 1
      strategy.validSeedNodes.size shouldEqual settings.seedsWhitelist.size - 2
      strategy.validSeedNodes.contains(strategy.cluster.selfAddress) shouldBe false
      intercept[java.net.MalformedURLException](strategy.discoverClusterSeeds)
    }
  }
}
