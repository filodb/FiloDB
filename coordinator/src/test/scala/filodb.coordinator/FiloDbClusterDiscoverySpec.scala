package filodb.coordinator

import com.typesafe.config.ConfigFactory

import filodb.coordinator.v2.FiloDbClusterDiscovery

// scalastyle:off null
class FiloDbClusterDiscoverySpec extends AkkaSpec {

  "Should allocate 4 shards to each coordinator" in {

    val config = ConfigFactory.parseString(
      """
        |filodb.cluster-discovery.num-nodes = 4
        |""".stripMargin)

    val settings = new FilodbSettings(config)
    val disc = new FiloDbClusterDiscovery(settings, null, null)(null)

    disc.shardsForOrdinal(0, 16) shouldEqual Seq(0, 1, 2, 3)
    disc.shardsForOrdinal(1, 16) shouldEqual Seq(4, 5, 6, 7)
    disc.shardsForOrdinal(2, 16) shouldEqual Seq(8, 9, 10, 11)
    disc.shardsForOrdinal(3, 16) shouldEqual Seq(12, 13, 14, 15)
    intercept[IllegalArgumentException] {
      disc.shardsForOrdinal(4, 8)
    }
  }

  "Should allocate the extra n shards to first n nodes" in {
    val config = ConfigFactory.parseString(
      """
        |filodb.cluster-discovery.num-nodes = 5
        |""".stripMargin)

    val settings = new FilodbSettings(config)
    val disc = new FiloDbClusterDiscovery(settings, null, null)(null)

    disc.shardsForOrdinal(0, 8) shouldEqual Seq(0, 1)
    disc.shardsForOrdinal(1, 8) shouldEqual Seq(2, 3)
    disc.shardsForOrdinal(2, 8) shouldEqual Seq(4, 5)
    disc.shardsForOrdinal(3, 8) shouldEqual Seq(6)
    disc.shardsForOrdinal(4, 8) shouldEqual Seq(7)

    intercept[IllegalArgumentException] {
      disc.shardsForOrdinal(5, 8)
    }
  }

}
