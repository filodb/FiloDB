package filodb.coordinator

import akka.actor.{ActorRef, Address}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import filodb.core.{DatasetRef, TestData}
import filodb.core.metadata.Dataset

class ShardManagerSpec extends AkkaSpec {
  import NodeClusterActor.{DatasetResourceSpec, DatasetVerified, IngestionSource, SetupDataset}
  import client.IngestionCommands.DatasetSetup

  protected val dataset1 = DatasetRef("one")
  protected val datasetObj1 = Dataset(dataset1.dataset, Seq("seg:int"), Seq("timestamp:long"))
  protected val dataset2 = DatasetRef("two")
  protected val datasetObj2 = Dataset(dataset2.dataset, Seq("tags:map"), Seq("timestamp:long"))

  protected val resources1 = DatasetResourceSpec(8, 3)
  protected val resources2 = DatasetResourceSpec(16, 2)

  val settings = new FilodbSettings(ConfigFactory.load("application_test.conf"))
  protected val shardManager = new ShardManager(settings, DefaultShardAssignmentStrategy)

  val coord1 = TestProbe("coordinator1")
  val coord1Address = uniqueAddress(coord1.ref)

  val coord2 = TestProbe("coordinator2")
  val coord2Address = uniqueAddress(coord2.ref)

  val coord3 = TestProbe("coordinator3")
  val coord3Address = uniqueAddress(coord3.ref)

  val coord4 = TestProbe("coordinator4")
  val coord4Address = uniqueAddress(coord4.ref)

  val subscriber = TestProbe("subscriber")

  val noOpSource1 = IngestionSource(classOf[NoOpStreamFactory].getName)
  val setupDs1 = SetupDataset(dataset1, resources1, noOpSource1, TestData.storeConf)

  val noOpSource2 = IngestionSource(classOf[NoOpStreamFactory].getName)
  val setupDs2 = SetupDataset(dataset2, resources2, noOpSource2, TestData.storeConf)


  def uniqueAddress(probe: ActorRef): Address =
    probe.path.address.copy(system = s"${probe.path.address.system}-${probe.path.name}")

  /* Please read/run this spec from top to bottom. Later tests depend on initial tests to run */
  "ShardManager" must {

    "allow subscription of self for shard events on all datasets" in {
      shardManager.subscribeAll(subscriber.ref)
      subscriber.expectMsg(ShardSubscriptions(Set.empty, Set(subscriber.ref)))
      subscriber.expectNoMessage() // should not get a CurrentShardSnapshot since there isnt a dataset yet
    }

    "change state for addition of first coordinator without datasets" in {
      shardManager.addMember(coord1Address, coord1.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref)
      shardManager.datasetInfo.size shouldBe 0
      coord1.expectNoMessage() // since there are no datasets, there should be no assignments
    }

    "change state for addition of second coordinator without datasets" in {
      shardManager.addMember(coord2Address, coord2.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord2.ref)
      shardManager.datasetInfo.size shouldBe 0
      coord2.expectNoMessage() // since there are no datasets, there should be no assignments
    }

    "change state for addition of third coordinator without datasets" in {
      shardManager.addMember(coord3Address, coord3.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord2.ref, coord3.ref)
      shardManager.datasetInfo.size shouldBe 0
      coord3.expectNoMessage() // since there are no datasets, there should be no assignments
    }

    "change state for removal of coordinator without datasets" in {
      shardManager.removeMember(coord2Address)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord3.ref)
      shardManager.datasetInfo.size shouldBe 0
      coord2.expectNoMessage() // since there are no datasets, there should be no assignments
    }

    "change state for addition of new dataset" in {
      val assignments = shardManager.addDataset(setupDs1, datasetObj1, self)
      shardManager.datasetInfo.size shouldBe 1
      assignments shouldEqual Map(coord3.ref -> Seq(0,1,2), coord1.ref -> Seq(3,4,5))

      expectMsg(DatasetVerified)
      coord3.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      // assignments first go to the most recently deployed node
      coord3.expectMsgAllOf(
        StartShardIngestion(dataset1, 0, None),
        StartShardIngestion(dataset1, 1, None),
        StartShardIngestion(dataset1, 2, None))

      coord1.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord1.expectMsgAllOf(
        StartShardIngestion(dataset1, 3, None),
        StartShardIngestion(dataset1, 4, None),
        StartShardIngestion(dataset1, 5, None))

      // NOTE: because subscriptions do not kick in right away, we don't get new snapshots unitl after
      // ShardSubscriptions message
    }

    "send shard subscribers updates on shard events as a result of dataset addition" in {
      subscriber.expectMsg(ShardSubscriptions(Set(
        ShardSubscription(dataset1, Set(subscriber.ref))), Set(subscriber.ref)))
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.ref shouldEqual dataset1
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(3, 4, 5)
      }
      subscriber.expectNoMessage()
    }

    "change state for addition of coordinator when there are datasets" in {
      shardManager.addMember(coord2Address, coord2.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord3.ref, coord2.ref)

      coord2.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord2.expectMsgAllOf(
        StartShardIngestion(dataset1, 6, None),
        StartShardIngestion(dataset1, 7, None))

      // Should receive new snapshot with shards 6,7 for coord2
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(3, 4, 5)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
      }
    }

    "change state for addition of spare coordinator" in {
      shardManager.addMember(coord4Address, coord4.ref)
      shardManager.coordinators shouldBe Seq(coord1.ref, coord3.ref, coord2.ref, coord4.ref)
      shardManager.datasetInfo.size shouldBe 1
      coord4.expectNoMessage() // since this is a spare node, there should be no assignments
      subscriber.expectNoMessage()
    }

    "change state for removal of coordinator when there are datasets and spare nodes" in {
      shardManager.removeMember(coord1Address)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord2.ref, coord4.ref)
      shardManager.datasetInfo.size shouldBe 1

      // first ingestion should be stopped on downed node
      coord1.expectMsgAllOf(
        StopShardIngestion(dataset1, 3),
        StopShardIngestion(dataset1, 4),
        StopShardIngestion(dataset1, 5))

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord1.ref) shouldEqual Nil
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
      }

      // spare coord4 should take over the shards
      coord4.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord4.expectMsgAllOf(
        StartShardIngestion(dataset1, 3, None),
        StartShardIngestion(dataset1, 4, None),
        StartShardIngestion(dataset1, 5, None))

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(3, 4, 5)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
      }
      subscriber.expectNoMessage()
    }

    "reassign shards where additional room available on removal of coordinator when there are no spare nodes" in {
      shardManager.removeMember(coord4Address)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord2.ref)
      shardManager.datasetInfo.size shouldBe 1

      // ingestion should be stopped on downed node
      coord4.expectMsgAllOf(
        StopShardIngestion(dataset1, 3),
        StopShardIngestion(dataset1, 4),
        StopShardIngestion(dataset1, 5))


      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        // s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
        // NOTE: you would expect shard2 to have 6 & 7 only in the first snapshot which is after removing coord4.
        // The problem is that ShardMapper is mutable, and since this is a local transfer, the actor message
        // points to the ShardMapper object which by this point already has the shard 3 added.  :(
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        s.map.numAssignedShards shouldEqual 6
      }

      // coord2 has room for one more
      coord2.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord2.expectMsgAllOf(
        StartShardIngestion(dataset1, 3, None))

      // since there are no spare nodes now, other coords (which are "down") should not get any message
      coord1.expectNoMessage()
      coord3.expectNoMessage()

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.unassignedShards shouldEqual Seq(4, 5)
        s.map.numAssignedShards shouldEqual 6
      }
      subscriber.expectNoMessage()
    }

    "reassign remaining unassigned shards when a replacement node comes back" in {
      shardManager.addMember(coord4.ref.path.address, coord4.ref)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord2.ref, coord4.ref)

      coord4.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord4.expectMsgAllOf(
        StartShardIngestion(dataset1, 4, None),
        StartShardIngestion(dataset1, 5, None))

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(4, 5)
        s.map.unassignedShards shouldEqual Nil
      }
      subscriber.expectNoMessage()
    }

    "ingestion error on a shard should reassign shard to another node" in {
      shardManager.coordinators shouldEqual Seq(coord3.ref, coord2.ref, coord4.ref)
      shardManager.updateFromExternalShardEvent(subscriber.ref,
                                                IngestionError(dataset1, 0, new IllegalStateException("simulated")))

      coord4.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord4.expectMsgAllOf(
        StartShardIngestion(dataset1, 0, None))

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(1, 2)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(0, 4, 5)
        s.map.unassignedShards shouldEqual Nil
      }
      subscriber.expectNoMessage()
    }

    "continual ingestion error on a shard should not reassign shard to another node" in {
      shardManager.coordinators shouldEqual Seq(coord3.ref, coord2.ref, coord4.ref)
      shardManager.updateFromExternalShardEvent(subscriber.ref,
                                                IngestionError(dataset1, 0, new IllegalStateException("simulated")))
      coord3.expectNoMessage()
      coord2.expectNoMessage()
      coord4.expectNoMessage()
      subscriber.expectNoMessage()
    }

    "change state for removal of dataset" in {
      shardManager.removeDataset(dataset1)
      shardManager.datasetInfo.size shouldBe 0

      coord3.expectMsgAllOf(
        StopShardIngestion(dataset1, 1),
        StopShardIngestion(dataset1, 2))

      coord4.expectMsgAllOf(
        StopShardIngestion(dataset1, 4), // shard 0 is in error state
        StopShardIngestion(dataset1, 5))

      coord2.expectMsgAllOf(
        StopShardIngestion(dataset1, 3),
        StopShardIngestion(dataset1, 6),
        StopShardIngestion(dataset1, 7))

      shardManager.subscriptions.subscriptions.size shouldBe 0

      // 3 snapshots one for each coord
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.unassignedShards shouldEqual (0 to 7)
      }
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.unassignedShards shouldEqual (0 to 7)
      }
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.unassignedShards shouldEqual (0 to 7)
      }
      subscriber.expectNoMessage()
    }

    "change state for addition of multiple datasets" in {
      // add back coord1
      shardManager.addMember(coord1Address, coord1.ref)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord2.ref, coord4.ref, coord1.ref)

      val assignments1 = shardManager.addDataset(setupDs1, datasetObj1, self)
      shardManager.datasetInfo.size shouldBe 1
      assignments1 shouldEqual Map(coord1.ref -> Seq(0, 1, 2),
        coord4.ref -> Seq(3, 4, 5),
        coord2.ref -> Seq(6, 7),
        coord3.ref -> Seq())

      coord1.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      // assignments first go to the most recently deployed node
      coord1.expectMsgAllOf(
        StartShardIngestion(dataset1, 0, None),
        StartShardIngestion(dataset1, 1, None),
        StartShardIngestion(dataset1, 2, None))

      // No CurrentShardSnapshot yet.  We have to get subscription first.

      coord4.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord4.expectMsgAllOf(
        StartShardIngestion(dataset1, 3, None),
        StartShardIngestion(dataset1, 4, None),
        StartShardIngestion(dataset1, 5, None))

      coord2.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord2.expectMsgAllOf(
        StartShardIngestion(dataset1, 6, None),
        StartShardIngestion(dataset1, 7, None))

      coord3.expectNoMessage() // coord3 is spare node for dataset1

      // addition of dataset results in snapshot/subscriptions broadcast
      subscriber.expectMsg(
        ShardSubscriptions(Set(ShardSubscription(dataset1, Set(subscriber.ref))), Set(subscriber.ref)))
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(3, 4, 5)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
        s.map.unassignedShards shouldEqual Nil
      }

      val assignments2 = shardManager.addDataset(setupDs2, datasetObj2, self)
      shardManager.datasetInfo.size shouldBe 2
      assignments2 shouldEqual Map(
        coord1.ref -> Range(0, 8),
        coord4.ref -> Range(8, 16),
        coord2.ref -> Seq.empty,
        coord3.ref -> Seq.empty)

      coord1.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj2.asCompactString
        ds.source shouldEqual noOpSource2
      }
      // assignments first go to the most recently deployed node
      val msgs1 = coord1.receiveWhile(messages = 8) { case m: StartShardIngestion if m.ref == dataset2 => m }
      msgs1 should have length (8)

      coord4.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj2.asCompactString
        ds.source shouldEqual noOpSource2
      }
      val msgs2 = coord4.receiveWhile(messages = 8) { case m: StartShardIngestion if m.ref == dataset2 => m }
      msgs2 should have length (8)

      // coord2 and coord3 are spare nodes for dataset2
      coord2.expectNoMessage()
      coord3.expectNoMessage()

      // shard subscriptions should work with multiple datasets
      subscriber.expectMsg(
        ShardSubscriptions(Set(ShardSubscription(dataset1,Set(subscriber.ref)),
          ShardSubscription(dataset2,Set(subscriber.ref))),
          Set(subscriber.ref)))

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset2 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Range(0, 8)
        s.map.shardsForCoord(coord4.ref) shouldEqual Range(8, 16)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq.empty
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq.empty
      }

      subscriber.expectNoMessage()
    }

    "recover state on a failed over node " in {
      val shardManager2 = new ShardManager(settings, DefaultShardAssignmentStrategy)

      // node cluster actor should trigger setup dataset for each registered dataset on recovery
      shardManager2.addDataset(setupDs1, datasetObj1, self) shouldEqual Map.empty
      shardManager2.addDataset(setupDs2, datasetObj2, self) shouldEqual Map.empty

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

      // dataset2: reassign shards 8-16 to coord2.  Will get two snapshots during move
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset2 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Range(0, 8)
        s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        s.map.shardsForCoord(coord2.ref) shouldEqual (8 until 16)
        s.map.shardsForCoord(coord3.ref) shouldEqual Nil
      }

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset2 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Range(0, 8)
        s.map.shardsForCoord(coord4.ref) shouldEqual Nil
        s.map.shardsForCoord(coord2.ref) shouldEqual (8 until 16)
        s.map.shardsForCoord(coord3.ref) shouldEqual Nil
      }

      // dataset1: reassign shards 3,4,5 to coord2 and coord3
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(4, 5)
        s.map.shardsForCoord(coord4.ref) shouldEqual Nil
      }

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(3, 6, 7)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(4, 5)
        s.map.shardsForCoord(coord4.ref) shouldEqual Nil
      }

      // ingestion should be stopped on downed node for 8 + 3 shards
      coord4.receiveWhile(messages = 11) { case m: StopShardIngestion => m } should have length (11)

      // ingestion should failover to coord2 for dataset2 first, and get 8 StartShardIngestion messages
      coord2.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj2.asCompactString
        ds.source shouldEqual noOpSource2
      }
      coord2.receiveWhile(messages = 8) { case m: StartShardIngestion => m } should have length (8)

      // then failover to coord2 for dataset1, with 1 shard
      coord2.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord2.expectMsg(StartShardIngestion(dataset1, 3, None))

      // ingestion should failover to coord3 for dataset1
      coord3.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }

      // 3 shards for dataset1
      coord3.expectMsgAllOf(
        StartShardIngestion(dataset1, 4, None),
        StartShardIngestion(dataset1, 5, None))
    }
  }
}



