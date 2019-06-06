package filodb.coordinator

import scala.concurrent.duration._

import akka.actor.{ActorRef, Address}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import filodb.core.{DatasetRef, TestData}
import filodb.core.downsample.DownsampleConfig
import filodb.core.metadata.Dataset
import filodb.core.store.IngestionConfig

class ShardManagerSpec extends AkkaSpec {
  import NodeClusterActor.{DatasetVerified, IngestionSource}

  protected val dataset1 = DatasetRef("one")
  protected val datasetObj1 = Dataset(dataset1.dataset, Seq("seg:int"), Seq("timestamp:long"))
  protected val dataset2 = DatasetRef("two")
  protected val datasetObj2 = Dataset(dataset2.dataset, Seq("tags:map"), Seq("timestamp:long"))

  protected val resources1 = ConfigFactory.parseString("""num-shards=8
                                               min-num-nodes=3""")
  protected val resources2 = ConfigFactory.parseString("""num-shards=16
                                               min-num-nodes=2""")

  val settings = new FilodbSettings(ConfigFactory.load("application_test.conf"))
  protected val shardManager = new ShardManager(settings, DefaultShardAssignmentStrategy)

  private def makeTestProbe(name: String): TestProbe = {
    val tp = TestProbe(name)
    // Uncomment to ignore messages of a specific type.
    //tp.ignoreMsg({case m: Any => m.isInstanceOf[...]})
    tp
  }

  val coord1 = makeTestProbe("coordinator1")
  val coord1Address = uniqueAddress(coord1.ref)

  val coord2 = makeTestProbe("coordinator2")
  val coord2Address = uniqueAddress(coord2.ref)

  val coord3 = makeTestProbe("coordinator3")
  val coord3Address = uniqueAddress(coord3.ref)

  val coord4 = makeTestProbe("coordinator4")
  val coord4Address = uniqueAddress(coord4.ref)

  val subscriber = makeTestProbe("subscriber")

  val noOpSource1 = IngestionSource(classOf[NoOpStreamFactory].getName)

  val ingestionConfig1 = IngestionConfig(dataset1, resources1, noOpSource1.streamFactoryClass,
    ConfigFactory.empty, TestData.storeConf, DownsampleConfig.disabled)

  val noOpSource2 = IngestionSource(classOf[NoOpStreamFactory].getName)
  val ingestionConfig2 = IngestionConfig(dataset2, resources2, noOpSource2.streamFactoryClass,
    ConfigFactory.empty, TestData.storeConf, DownsampleConfig.disabled)

  private def expectNoMessage(coord: TestProbe): Unit = {
    coord.expectNoMessage(100.milliseconds)
  }

  def uniqueAddress(probe: ActorRef): Address =
    probe.path.address.copy(system = s"${probe.path.address.system}-${probe.path.name}")

  /* Please read/run this spec from top to bottom. Later tests depend on initial tests to run */
  "ShardManager" must {

    "allow subscription of self for shard events on all datasets" in {
      shardManager.subscribeAll(subscriber.ref)
      subscriber.expectMsg(ShardSubscriptions(Set.empty, Set(subscriber.ref)))
      expectNoMessage(subscriber) // should not get a CurrentShardSnapshot since there isnt a dataset yet
    }

    "change state for addition of first coordinator without datasets" in {
      shardManager.addMember(coord1Address, coord1.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref)
      shardManager.datasetInfo.size shouldBe 0
      expectNoMessage(coord1) // since there are no datasets, there should be no assignments
    }

    "change state for addition of second coordinator without datasets" in {
      shardManager.addMember(coord2Address, coord2.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord2.ref)
      shardManager.datasetInfo.size shouldBe 0
      expectNoMessage(coord2) // since there are no datasets, there should be no assignments
    }

    "change state for addition of third coordinator without datasets" in {
      shardManager.addMember(coord3Address, coord3.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord2.ref, coord3.ref)
      shardManager.datasetInfo.size shouldBe 0
      expectNoMessage(coord3) // since there are no datasets, there should be no assignments
    }

    "change state for removal of coordinator without datasets" in {
      shardManager.removeMember(coord2Address)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord3.ref)
      shardManager.datasetInfo.size shouldBe 0
      expectNoMessage(coord2) // since there are no datasets, there should be no assignments
    }

    "change state for addition of new dataset" in {
      val assignments = shardManager.addDataset(datasetObj1, ingestionConfig1, noOpSource1, Some(self))
      shardManager.datasetInfo.size shouldBe 1
      assignments shouldEqual Map(coord3.ref -> Seq(0,1,2), coord1.ref -> Seq(3,4,5))

      expectMsg(DatasetVerified)

      for (coord <- Seq(coord1, coord3)) {
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Seq(3, 4, 5)
          s.map.shardsForCoord(coord2.ref) shouldEqual Nil
          s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        }
        expectNoMessage(coord)
      }

      // NOTE: because subscriptions do not kick in right away, we don't get new snapshots until
      // after ShardSubscriptions message
    }

    "send shard subscribers updates on shard events as a result of dataset addition" in {
      subscriber.expectMsg(ShardSubscriptions(Set(
        ShardSubscription(dataset1, Set(subscriber.ref))), Set(subscriber.ref)))
      for (i <- 1 to 2) {
        // First is the initial set, the second is generated along with the state.
        subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Seq(3, 4, 5)
          s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        }
      }
      expectNoMessage(subscriber)
    }

    "change state for addition of coordinator when there are datasets" in {
      shardManager.addMember(coord2Address, coord2.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord3.ref, coord2.ref)

      // Should receive new snapshot with shards 6,7 for coord2
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(3, 4, 5)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
      }

      // All should see the changes.
      for (coord <- Seq(coord1, coord2, coord3)) {
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Seq(3, 4, 5)
          s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
          s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        }
        expectNoMessage(coord)
      }
    }

    "change state for addition of spare coordinator" in {
      shardManager.addMember(coord4Address, coord4.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord3.ref, coord2.ref, coord4.ref)
      shardManager.datasetInfo.size shouldBe 1
      expectNoMessage(coord4) // since this is a spare node, there should be no assignments
      expectNoMessage(subscriber)

      for (coord <- Seq(coord1, coord2, coord3, coord3)) {
        expectNoMessage(coord)
      }
    }

    "change state for removal of coordinator when there are datasets and spare nodes" in {
      shardManager.removeMember(coord1Address)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord2.ref, coord4.ref)
      shardManager.datasetInfo.size shouldBe 1

      // All coordinators should see a state message, even the removed one.
      for (coord <- Seq(coord2, coord3, coord4)) {
        //expectDataset(coord, datasetObj1).expectMsgPF() { case s: ShardIngestionState =>
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Nil          // coord1 is gone
          s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
          s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
          s.map.shardsForCoord(coord4.ref) shouldEqual Seq(3, 4, 5) // coord4 takes over
        }
        expectNoMessage(coord)
      }

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Nil
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(3, 4, 5)
      }

      expectNoMessage(subscriber)
    }

    "reassign shards where additional room available on removal of coordinator when there are no spare nodes" in {
      shardManager.removeMember(coord4Address)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord2.ref)
      shardManager.datasetInfo.size shouldBe 1

      // Ingestion should be stopped on downed node, and one shard reassigned.
      for (coord <- Seq(coord2, coord3)) {
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Nil
          s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
          s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
          s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        }
        expectNoMessage(coord)
      }

      for (coord <- Seq(coord1, coord2, coord3, coord3)) {
        expectNoMessage(coord)
      }

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Nil
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        s.map.numAssignedShards shouldEqual 6
      }

      expectNoMessage(subscriber)
    }

    "reassign remaining unassigned shards when a replacement node comes back" in {
      shardManager.addMember(coord4.ref.path.address, coord4.ref)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord2.ref, coord4.ref)

      for (coord <- Seq(coord2, coord3, coord4)) {
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Nil
          s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
          s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
          s.map.shardsForCoord(coord4.ref) shouldEqual Seq(4, 5)
        }
        expectNoMessage(coord)
      }

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(4, 5)
        s.map.unassignedShards shouldEqual Nil
      }
      expectNoMessage(subscriber)
    }

    "ingestion error on a shard should reassign shard to another node" in {
      shardManager.coordinators shouldEqual Seq(coord3.ref, coord2.ref, coord4.ref)
      shardManager.updateFromExternalShardEvent(subscriber.ref,
                                                IngestionError(dataset1, 0, new IllegalStateException("simulated")))

      for (coord <- Seq(coord2, coord3, coord4)) {
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Nil
          s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
          s.map.shardsForCoord(coord3.ref) shouldEqual Seq(1, 2)
          s.map.shardsForCoord(coord4.ref) shouldEqual Seq(0, 4, 5)
        }
        expectNoMessage(coord)
      }

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(1, 2)
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(0, 4, 5)
        s.map.unassignedShards shouldEqual Nil
      }
      expectNoMessage(subscriber)
    }

    "continual ingestion error on a shard should not reassign shard to another node" in {
      shardManager.coordinators shouldEqual Seq(coord3.ref, coord2.ref, coord4.ref)
      shardManager.updateFromExternalShardEvent(subscriber.ref,
                                                IngestionError(dataset1, 0, new IllegalStateException("simulated")))

      // Shard 0 is "bad" and so it's unassigned now.

      for (coord <- Seq(coord2, coord3, coord4)) {
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Nil
          s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
          s.map.shardsForCoord(coord3.ref) shouldEqual Seq(1, 2)
          s.map.shardsForCoord(coord4.ref) shouldEqual Seq(4, 5)
        }
        expectNoMessage(coord)
      }

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(1, 2)
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(4, 5)
        s.map.unassignedShards shouldEqual Seq(0)
      }
      expectNoMessage(subscriber)
    }

    "change state for removal of dataset" in {
      shardManager.removeDataset(dataset1)
      shardManager.datasetInfo.size shouldBe 0

      for (coord <- Seq(coord2, coord3, coord4)) {
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Nil
          s.map.shardsForCoord(coord2.ref) shouldEqual Nil
          s.map.shardsForCoord(coord3.ref) shouldEqual Nil
          s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        }
        expectNoMessage(coord)
      }

      shardManager.subscriptions.subscriptions.size shouldBe 0

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.unassignedShards shouldEqual (0 to 7)
      }
      expectNoMessage(subscriber)
    }

    "change state for addition of multiple datasets" in {
      // add back coord1
      shardManager.addMember(coord1Address, coord1.ref)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord2.ref, coord4.ref, coord1.ref)

      val assignments1 = shardManager.addDataset(datasetObj1, ingestionConfig1, noOpSource1, Some(self))
      shardManager.datasetInfo.size shouldBe 1
      assignments1 shouldEqual Map(coord1.ref -> Seq(0, 1, 2),
        coord2.ref -> Seq(6, 7),
        coord3.ref -> Seq(),
        coord4.ref -> Seq(3, 4, 5))

      for (coord <- Seq(coord1, coord2, coord3, coord4)) {
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset1
          s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 1, 2)
          s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
          s.map.shardsForCoord(coord3.ref) shouldEqual Nil
          s.map.shardsForCoord(coord4.ref) shouldEqual Seq(3, 4, 5)
        }
        expectNoMessage(coord)
      }

      // addition of dataset results in snapshot/subscriptions broadcast
      subscriber.expectMsg(
        ShardSubscriptions(Set(ShardSubscription(dataset1, Set(subscriber.ref))), Set(subscriber.ref)))
      for (i <- 1 to 2) {
        // First is the initial set, the second is generated along with the state.
        subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
          s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 1, 2)
          s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
          s.map.shardsForCoord(coord4.ref) shouldEqual Seq(3, 4, 5)
          s.map.unassignedShards shouldEqual Nil
        }
      }
      expectNoMessage(subscriber)

      // Assignments first go to the most recently deployed node,
      // coord2 and coord3 are spare nodes for dataset2.

      val assignments2 = shardManager.addDataset(datasetObj2, ingestionConfig2, noOpSource2, Some(self))
      shardManager.datasetInfo.size shouldBe 2
      assignments2 shouldEqual Map(
        coord1.ref -> Range(0, 8),
        coord4.ref -> Range(8, 16),
        coord2.ref -> Seq.empty,
        coord3.ref -> Seq.empty)

      for (coord <- Seq(coord1, coord2, coord3, coord4)) {
        coord.expectMsgPF() { case s: ShardIngestionState =>
          s.ref shouldEqual dataset2
          s.map.shardsForCoord(coord1.ref) shouldEqual Range(0, 8)
          s.map.shardsForCoord(coord2.ref) shouldEqual Nil
          s.map.shardsForCoord(coord3.ref) shouldEqual Nil
          s.map.shardsForCoord(coord4.ref) shouldEqual Range(8, 16)
        }
        expectNoMessage(coord)
      }

      // shard subscriptions should work with multiple datasets
      subscriber.expectMsg(
        ShardSubscriptions(Set(ShardSubscription(dataset1,Set(subscriber.ref)),
          ShardSubscription(dataset2,Set(subscriber.ref))),
          Set(subscriber.ref)))

      for (i <- 1 to 2) {
        // First is the initial set, the second is generated along with the state.
        subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset2 =>
          s.map.shardsForCoord(coord1.ref) shouldEqual Range(0, 8)
          s.map.shardsForCoord(coord2.ref) shouldEqual Seq.empty
          s.map.shardsForCoord(coord3.ref) shouldEqual Seq.empty
          s.map.shardsForCoord(coord4.ref) shouldEqual Range(8, 16)
        }
      }
      expectNoMessage(subscriber)
    }

    "recover state on a failed over node " in {
      val shardManager2 = new ShardManager(settings, DefaultShardAssignmentStrategy)

      // node cluster actor should trigger setup dataset for each registered dataset on recovery
      shardManager2.addDataset(datasetObj1, ingestionConfig1, noOpSource1, Some(self)) shouldEqual Map.empty
      shardManager2.addDataset(datasetObj2, ingestionConfig2, noOpSource2, Some(self)) shouldEqual Map.empty

      // node cluster actor should help with recovery of shard mapping
      for {
        (dataset, map) <- shardManager.shardMappers
      } shardManager2.recoverShards(dataset, map)

      // node cluster actor should help with recovery of subscriptions
      shardManager2.recoverSubscriptions(shardManager.subscriptions)

      // akka membership events will trigger addMember calls from node cluster actor
      shardManager.coordinators.size shouldEqual 4
      for {
        c <- shardManager.coordinators
      } shardManager2.addMember(uniqueAddress(c), c)

      // now test continuance of business after failover to new shardManager by downing a node and verifying effects
      shardManager2.removeMember(coord4Address)
      shardManager2.coordinators shouldBe Seq(coord3.ref, coord2.ref, coord1.ref)
      shardManager2.datasetInfo.size shouldBe 2

      // dataset2: reassign shards 8-16 to coord2.
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset2 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Range(0, 8)
        s.map.shardsForCoord(coord2.ref) shouldEqual (8 until 16)
        s.map.shardsForCoord(coord3.ref) shouldEqual Nil
        s.map.shardsForCoord(coord4.ref) shouldEqual Nil
      }

      // dataset1: reassign shards 3,4,5 to coord2 and coord3
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(4, 5)
        s.map.shardsForCoord(coord4.ref) shouldEqual Nil
      }

      expectNoMessage(subscriber)

      // ingestion should be stopped on downed node for 8 + 3 shards

      { // coord1
        coord1.expectMsgPF() {
          case s: ShardIngestionState =>
            s.ref shouldEqual dataset2
            s.map.shardsForCoord(coord1.ref) shouldEqual Range(0, 8)
            s.map.shardsForCoord(coord2.ref) shouldEqual (8 until 16)
            s.map.shardsForCoord(coord3.ref) shouldEqual Nil
            s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        }

        coord1.expectMsgPF() {
          case s: ShardIngestionState =>
            s.ref shouldEqual dataset1
            s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 1, 2)
            s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
            s.map.shardsForCoord(coord3.ref) shouldEqual Seq(4, 5)
            s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        }

        expectNoMessage(coord1)
      }

      { // coord2
        coord2.expectMsgPF() {
          case s: ShardIngestionState =>
            s.ref shouldEqual dataset2
            s.map.shardsForCoord(coord1.ref) shouldEqual Range(0, 8)
            s.map.shardsForCoord(coord2.ref) shouldEqual (8 until 16)
            s.map.shardsForCoord(coord3.ref) shouldEqual Nil
            s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        }

        coord2.expectMsgPF() {
          case s: ShardIngestionState =>
            s.ref shouldEqual dataset1
            s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 1, 2)
            s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
            s.map.shardsForCoord(coord3.ref) shouldEqual Seq(4, 5)
            s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        }

        expectNoMessage(coord2)
      }

      { // coord3
        coord3.expectMsgPF() {
          case s: ShardIngestionState =>
            s.ref shouldEqual dataset2
            s.map.shardsForCoord(coord1.ref) shouldEqual Range(0, 8)
            s.map.shardsForCoord(coord2.ref) shouldEqual (8 until 16)
            s.map.shardsForCoord(coord3.ref) shouldEqual Nil
            s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        }

        coord3.expectMsgPF() {
          case s: ShardIngestionState =>
            s.ref shouldEqual dataset1
            s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 1, 2)
            s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
            s.map.shardsForCoord(coord3.ref) shouldEqual Seq(4, 5)
            s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        }

        expectNoMessage(coord3)
      }

      expectNoMessage(coord4)
    }
  }
}



