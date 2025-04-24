package filodb.coordinator

import akka.actor.ActorRef
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.coordinator.v2.FiloDbClusterDiscovery
import filodb.core.GdeltTestData.dataset6

// scalastyle:off null
class FiloDbClusterDiscoverySpec extends AkkaSpec {

  private val ref = dataset6.ref

  "Should allocate 4 shards to each coordinator" in {

    val config = ConfigFactory.parseString(
      """
        |filodb.min-num-nodes-in-cluster = 4
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
        |filodb.min-num-nodes-in-cluster = 5
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

  "Should merge shard mapper correctly when ShardStatusError is present" in {
    // FiloDB Cluster Layout - Num Nodes = 2 - Num Shards = 4
    val config = ConfigFactory.parseString(
      """
        |filodb.min-num-nodes-in-cluster = 2
        |""".stripMargin)

    val settings = new FilodbSettings(config)
    val disc = new FiloDbClusterDiscovery(settings, null, null)(null)

    // Creating node 1 shard mapper
    val shardMapperNode1 = new ShardMapper(4) // node 1 owns shard 0 and 1
    shardMapperNode1.updateFromEvent(IngestionStarted.apply(ref, 0, ActorRef.noSender))
    shardMapperNode1.updateFromEvent(IngestionError.apply(ref, 1, new Exception("invalid data")))

    // Creating node 2 shard mapper
    val shardMapperNode2 = new ShardMapper(4) // node 2 owns shard 2 and 3
    shardMapperNode2.updateFromEvent(RecoveryInProgress.apply(ref, 3, ActorRef.noSender, 40))
    shardMapperNode2.updateFromEvent(IngestionError.apply(ref, 2, new Exception("invalid data")))

    val node1Snapshot = CurrentShardSnapshot.apply(ref, shardMapperNode1)
    val node2Snapshot = CurrentShardSnapshot.apply(ref, shardMapperNode2)

    implicit val scheduler: Scheduler = Scheduler.global
    for {
      acc <- disc.mergeSnapshotResponses(ref, 4, Observable.apply(node1Snapshot, node2Snapshot))
    } {
      acc.activeShards() shouldEqual Seq(0)
      acc.notActiveShards() shouldEqual Seq(1, 2, 3)
      acc.statusForShard(1).isInstanceOf[ShardStatusError] shouldEqual true
      acc.statusForShard(2).isInstanceOf[ShardStatusError] shouldEqual true
      acc.statusForShard(3).isInstanceOf[ShardStatusRecovery] shouldEqual true
    }
  }
}
