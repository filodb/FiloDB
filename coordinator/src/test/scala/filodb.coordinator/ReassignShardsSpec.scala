package filodb.coordinator

import akka.actor.{ActorRef, Address}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import filodb.coordinator.NodeClusterActor._
import filodb.coordinator.client.IngestionCommands.DatasetSetup
import filodb.core.{DatasetRef, Success, TestData}
import filodb.core.metadata.Dataset
import filodb.core.store.{AssignShardConfig, UnassignShardConfig}

class ReassignShardsSpec extends AkkaSpec {
  import NodeClusterActor.{DatasetResourceSpec, IngestionSource, SetupDataset}

  protected val dataset1 = DatasetRef("one")
  protected val datasetObj1 = Dataset(dataset1.dataset, Seq("seg:int"), Seq("timestamp:long"))
  protected val dataset2 = DatasetRef("two")
  protected val datasetObj2 = Dataset(dataset2.dataset, Seq("tags:map"), Seq("timestamp:long"))

  protected val resources1 = DatasetResourceSpec(8, 3)

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

  val coordInvalid = TestProbe("coordinatorInvalid")
  val coordInvalidAddress = uniqueAddress(coordInvalid.ref)

  val subscriber = TestProbe("subscriber")

  val noOpSource1 = IngestionSource(classOf[NoOpStreamFactory].getName)
  val setupDs1 = SetupDataset(dataset1, resources1, noOpSource1, TestData.storeConf)

  def uniqueAddress(probe: ActorRef): Address =
    probe.path.address.copy(system = s"${probe.path.address.system}-${probe.path.name}")

  /* Please read/run this spec from top to bottom. Later tests depend on initial tests to run */
  "ShardManager reassign shard sequential operations" must {

    "fail with no datasets" in {
      shardManager.subscribeAll(subscriber.ref)
      subscriber.expectMsg(ShardSubscriptions(Set.empty, Set(subscriber.ref)))
      subscriber.expectNoMessage() // should not get a CurrentShardSnapshot since there isnt a dataset yet

      shardManager.addMember(coord3Address, coord3.ref)
      shardManager.coordinators shouldBe Seq(coord3.ref)
      shardManager.datasetInfo.size shouldBe 0
      coord3.expectNoMessage() // since there are no datasets, there should be no assignments

      shardManager.addMember(coord4Address, coord4.ref)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord4.ref)
      shardManager.datasetInfo.size shouldBe 0
      coord4.expectNoMessage() // since there are no more shards left to assign

      val shardAssign1 = AssignShardConfig(coord1Address.toString, Seq(0,1))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsg(DatasetUnknown(dataset1)) // since there are no datasets

      val shardAssign2 = AssignShardConfig(coordInvalidAddress.toString, Seq(0,1))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign2, dataset2), self)
      expectMsg(DatasetUnknown(dataset2))

    }

    "fail when minnumnodes not available" in {
      val assignments = shardManager.addDataset(setupDs1, datasetObj1, self)

      shardManager.datasetInfo.size shouldBe 1
      assignments shouldEqual Map(coord4.ref -> Seq(0, 1, 2), coord3.ref -> Seq(3, 4, 5))
      expectMsg(DatasetVerified)
      coord4.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      // assignments first go to the most recently deployed node
      coord4.expectMsgAllOf(
        StartShardIngestion(dataset1, 0, None),
        StartShardIngestion(dataset1, 1, None),
        StartShardIngestion(dataset1, 2, None))

      coord3.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      // assignments first go to the most recently deployed node
      coord3.expectMsgAllOf(
        StartShardIngestion(dataset1, 3, None),
        StartShardIngestion(dataset1, 4, None),
        StartShardIngestion(dataset1, 5, None))

      // NOTE: because subscriptions do not kick in right away, we don't get new snapshots unitl after
      // ShardSubscriptions message
      subscriber.expectMsg(ShardSubscriptions(Set(
        ShardSubscription(dataset1, Set(subscriber.ref))), Set(subscriber.ref)))
      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.ref shouldEqual dataset1
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(3, 4, 5)
//        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(6, 7)
//        s.map.shardsForCoord(coord1.ref) shouldEqual Seq()
      }
      subscriber.expectNoMessage()

      val shardAssign1 = AssignShardConfig(coord4Address.toString, Seq(5))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsgPF() { case s: BadSchema =>
        s.message should startWith(s"Can not start")
      }
      subscriber.expectNoMessage()

      val shardAssign2 = AssignShardConfig(coord2Address.toString, Seq(0))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign2, dataset1), self)
      expectMsg(BadData(s"${coord2Address.toString} not found"))
      subscriber.expectNoMessage()
    }

    "fail with invalid node" in {
      val shardAssign1 = AssignShardConfig(coordInvalidAddress.toString, Seq(0))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsg(BadData(s"${coordInvalidAddress.toString} not found"))
    }

    "succeed when minnumnodes are available" in {

      shardManager.addMember(coord2Address, coord2.ref)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord4.ref, coord2.ref)
      shardManager.datasetInfo.size shouldBe 1

      coord2.expectMsgPF() { case ds: DatasetSetup =>
        ds.compactDatasetStr shouldEqual datasetObj1.asCompactString
        ds.source shouldEqual noOpSource1
      }
      coord2.expectMsgAllOf(
        StartShardIngestion(dataset1, 6, None),
        StartShardIngestion(dataset1, 7, None))

      val assignments = shardManager.shardMappers(dataset1).shardValues
      assignments shouldEqual Array((coord4.ref, ShardStatusAssigned), (coord4.ref, ShardStatusAssigned),
        (coord4.ref, ShardStatusAssigned), (coord3.ref, ShardStatusAssigned), (coord3.ref, ShardStatusAssigned),
        (coord3.ref, ShardStatusAssigned), (coord2.ref, ShardStatusAssigned), (coord2.ref, ShardStatusAssigned))

      val shardAssign1 = AssignShardConfig(coord2Address.toString, Seq(5))
      shardManager.stopShards(NodeClusterActor.StopShards(UnassignShardConfig(shardAssign1.shardList), dataset1), self)
      expectMsg(Success)
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsg(Success)

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(3, 4)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(5, 6, 7)
      }
    }

    "not change after adding spare node" in {

      shardManager.addMember(coord1Address, coord1.ref)
      shardManager.coordinators shouldBe Seq(coord3.ref, coord4.ref, coord2.ref, coord1.ref)
      shardManager.datasetInfo.size shouldBe 1
      coord1.expectNoMessage() // since there are no datasets, there should be no assignments

      val assignments = shardManager.shardMappers(dataset1).shardValues
      assignments shouldEqual Array((coord4.ref, ShardStatusAssigned), (coord4.ref, ShardStatusAssigned),
        (coord4.ref, ShardStatusAssigned), (coord3.ref, ShardStatusAssigned), (coord3.ref, ShardStatusAssigned),
        (coord2.ref, ShardStatusAssigned), (coord2.ref, ShardStatusAssigned), (coord2.ref, ShardStatusAssigned))

    }

    "fail with invalid datasets" in {
      val shardAssign = AssignShardConfig(coord1Address.toString, Seq(0,1))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign, dataset2), self)
      expectMsg(DatasetUnknown(dataset2))

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(3, 4)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(5, 6, 7)
      }
    }

    "fail with invalid shardNum" in {

      val shardAssign1 = AssignShardConfig(coord1Address.toString, Seq(8))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsg(BadSchema(s"Invalid shards found List(8). Valid shards are List()"))

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(0, 1, 2)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(3, 4)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(5, 6, 7)
      }

    }

    "fail when assigned to same node" in {
      val shardAssign1 = AssignShardConfig(coord4Address.toString, Seq(0))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsgPF() { case s: BadSchema =>
        s.message should startWith (s"Can not reassign shards to same node")
      }
    }

    "fail when coord has no capacity" in {
      val shardAssign1 = AssignShardConfig(coord4Address.toString, Seq(4))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsgPF() { case s: BadSchema =>
        s.message should startWith (s"Can not start List")
      }
    }

    "succeed with single node" in {

      val shardAssign1 = AssignShardConfig(coord1Address.toString, Seq(2))
      shardManager.stopShards(NodeClusterActor.StopShards(UnassignShardConfig(shardAssign1.shardList), dataset1), self)
      expectMsg(Success)
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsg(Success)

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(0, 1)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(3, 4)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(5, 6, 7)
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(2)
      }

      val shardAssign2 = AssignShardConfig(coord3Address.toString, Seq(1))
      shardManager.stopShards(NodeClusterActor.StopShards(UnassignShardConfig(shardAssign2.shardList), dataset1), self)
      expectMsg(Success)
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign2, dataset1), self)
      expectMsg(Success)

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(0)
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(1, 3, 4)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(5, 6, 7)
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(2)
      }
    }

    "succeed with multiple shards" in {

      val shardAssign2 = AssignShardConfig(coord1Address.toString, Seq(0, 7))
      shardManager.stopShards(NodeClusterActor.StopShards(UnassignShardConfig(shardAssign2.shardList), dataset1), self)
      expectMsg(Success)
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign2, dataset1), self)
      expectMsg(Success)

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq()
        s.map.shardsForCoord(coord3.ref) shouldEqual Seq(1, 3, 4)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(5, 6)
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 2, 7)
      }
    }

    "succeed with remove member after reassignment" in {

      shardManager.removeMember(coord3Address)
      shardManager.coordinators shouldBe Seq(coord4.ref, coord2.ref, coord1.ref)
      shardManager.datasetInfo.size shouldBe 1

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot if s.ref == dataset1 =>
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(3, 4)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(1, 5, 6)
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 2, 7)
      }

      val shardAssign1 = AssignShardConfig(coord3Address.toString, Seq(0))
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsg(BadData(s"${coord3Address.toString} not found"))

      val shardAssign2 = AssignShardConfig(coord4Address.toString, Seq(2))
      shardManager.stopShards(NodeClusterActor.StopShards(UnassignShardConfig(shardAssign2.shardList), dataset1), self)
      expectMsg(Success)
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign2, dataset1), self)
      expectMsg(Success)

      subscriber.expectMsgPF() { case s: CurrentShardSnapshot =>
        s.map.shardsForCoord(coord4.ref) shouldEqual Seq(2, 3, 4)
        s.map.shardsForCoord(coord2.ref) shouldEqual Seq(1, 5, 6)
        s.map.shardsForCoord(coord1.ref) shouldEqual Seq(0, 7)
      }
    }

    "fail after remove dataset" in {

      shardManager.removeDataset(dataset1)
      shardManager.datasetInfo.size shouldBe 0

      val shardAssign1 = AssignShardConfig(coord1Address.toString, Seq(0,1))
      shardManager.stopShards(NodeClusterActor.StopShards(UnassignShardConfig(shardAssign1.shardList), dataset1), self)
      expectMsg(DatasetUnknown(dataset1)) // since there are no datasets

    }

    "succeed after adding dataset back" in {

      val assignments = shardManager.addDataset(setupDs1, datasetObj1, self)

      shardManager.datasetInfo.size shouldBe 1
      assignments shouldEqual Map(coord1.ref -> Seq(0, 1, 2), coord2.ref -> Seq(3, 4, 5), coord4.ref -> Seq(6, 7))
      expectMsg(DatasetVerified)

      val shardAssign1 = AssignShardConfig(coord4Address.toString, Seq(5))
      shardManager.stopShards(NodeClusterActor.StopShards(UnassignShardConfig(shardAssign1.shardList), dataset1), self)
      expectMsg(Success)
      shardManager.startShards(NodeClusterActor.StartShards(shardAssign1, dataset1), self)
      expectMsg(Success)

      val assignments2 = shardManager.shardMappers(dataset1).shardValues
      assignments2 shouldEqual Array((coord1.ref, ShardStatusAssigned), (coord1.ref, ShardStatusAssigned),
        (coord1.ref, ShardStatusAssigned), (coord2.ref, ShardStatusAssigned), (coord2.ref, ShardStatusAssigned),
        (coord4.ref, ShardStatusAssigned), (coord4.ref, ShardStatusAssigned), (coord4.ref, ShardStatusAssigned))

    }
  }
}