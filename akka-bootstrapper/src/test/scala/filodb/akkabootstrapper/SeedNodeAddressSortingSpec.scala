package filodb.akkabootstrapper

import scala.collection.immutable.SortedSet
import scala.util.Random

import akka.actor.Address
import akka.cluster.{Cluster, Member}
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object SeedNodeSortingFixture {

  val expected = IndexedSeq(
    Address("akka.tcp", "sys1", "host1", 2552),
    Address("akka.tcp", "sys1", "host2", 2552),
    Address("akka.tcp", "sys1", "host3", 8000),
    Address("akka.tcp", "sys1", "host4", 9000),
    Address("akka.tcp", "sys1", "host5", 10000))

  val config = {
    val seeds = expected.map(_.toString).mkString("\"", "\",\"", "\"")
    ConfigFactory
      .parseString(s"""akka-bootstrapper.explicit-list.seeds = [ $seeds ] """)
      .withFallback(AbstractTestKit.rootConfig)
  }
}

class SeedNodeAddressSortingSpec
  extends AbstractTestKit(SeedNodeSortingFixture.config)
    with AnyWordSpecLike {

  "An Ordering[Address]" must {
    "be sorted by address correctly" in {
      import Member.addressOrdering
      val m1 = Address("akka.tcp", "sys1", "host1", 9000)
      val m2 = Address("akka.tcp", "sys1", "host1", 10000)
      val m3 = Address("akka.tcp", "sys1", "host2", 8000)
      val m4 = Address("akka.tcp", "sys1", "host2", 9000)
      val m5 = Address("akka.tcp", "sys1", "host2", 10000)

      val expected = IndexedSeq(m1, m2, m3, m4, m5)
      val shuffled = Random.shuffle(expected)
      shuffled.sorted shouldEqual expected
      (SortedSet.empty[Address] ++ shuffled).toIndexedSeq shouldEqual expected
    }
  }
  "ExplicitListClusterSeedDiscovery.discoverClusterSeeds" must {
    val expected = SeedNodeSortingFixture.expected

    "be sorted by address correctly from config" in {
      val settings = new AkkaBootstrapperSettings(system.settings.config)
      val strategy = new ExplicitListClusterSeedDiscovery(Cluster(system), settings)
      strategy.discoverClusterSeeds shouldEqual expected
    }
    "be sorted by address correctly from shuffled seeds" in {
      val shuffledConfig = {
        val shuffled = Random.shuffle(expected)
        val seeds = shuffled.map(_.toString).mkString("\"", "\",\"", "\"")
        ConfigFactory
          .parseString(s"""akka-bootstrapper.explicit-list.seeds = [ $seeds ] """)
          .withFallback(AbstractTestKit.rootConfig)
      }

      val settings = new AkkaBootstrapperSettings(shuffledConfig)
      val strategy = new ExplicitListClusterSeedDiscovery(Cluster(system), settings)
      strategy.discoverClusterSeeds shouldEqual expected
    }
  }
}