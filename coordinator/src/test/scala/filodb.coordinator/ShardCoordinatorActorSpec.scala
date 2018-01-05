package filodb.coordinator

import akka.actor.{Actor, ActorRef, AddressFromURIString, Props}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach

import filodb.coordinator.NodeClusterActor.{DatasetResourceSpec, IngestionSource, SetupDataset}
import filodb.core.DatasetRef
import filodb.core.metadata.Dataset

class ShardCoordinatorCumulativeStateSpec extends ShardCoordinatorSpec {

  import ShardAssignmentStrategy.DatasetShards
  import ShardSubscriptions._

  "ShardCoordinatorActor" must {
    "add the first node coordinator, no datasets added by clients yet, all shards" in {
      shardActor ! AddMember(localCoordinator, localAddress)
      expectMsgPF() {
        case CoordinatorAdded(coordinator, shards, _) =>
          coordinator.compareTo(localCoordinator) shouldEqual 0
          shards shouldEqual Seq.empty
      }
    }
    "on AddDataset, assign shards correctly when nodes added after dataset shard add" in {
      val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)
      val sd = SetupDataset(dataset1, resources, noOpSource)

      shardActor ! AddDataset(sd, datasetObj, Set(localCoordinator), self)
      expectMsgPF() {
        case DatasetAdded(dataset, source, coordShards, ackTo) =>
          dataset shouldEqual datasetObj
          dataset.name shouldEqual dataset1.dataset
          coordShards shouldEqual Map(localCoordinator -> initialShards)
      }
    }
    "have no coordinators as subscribers for a dataset" in {
      shardActor ! GetSubscribers(dataset1)
      expectMsgPF() { case Subscribers(subs, _) => subs shouldEqual Set.empty }
    }
    "update new dataset's shardmapper and move newly-assigned shard status from unassigned to assigned" in {
      shardActor ! GetSnapshot(dataset1)
      expectMsgPF() {
        case CurrentShardSnapshot(ds, map) =>
          map.unassignedShards.size shouldEqual resources.numShards / resources.minNumNodes
          map.assignedShards shouldEqual initialShards
          map.shardValues.size shouldEqual resources.numShards
          map.shardValues shouldEqual Seq(
            (localCoordinator, ShardStatusAssigned), (localCoordinator, ShardStatusAssigned),
            (ActorRef.noSender, ShardStatusUnassigned), (ActorRef.noSender, ShardStatusUnassigned))

          initialShards.forall { shard =>
            map.coordForShard(shard) == localCoordinator && !map.unassigned(shard)
          } shouldEqual true
      }
    }
    "not subscribe to an invalid dataset and ack DatasetUnknown to subscriber" in {
      val invalid = "invalid"
      shardActor ! Subscribe(self, DatasetRef(invalid))
      expectMsg(NodeClusterActor.DatasetUnknown(DatasetRef(invalid)))
      shardActor ! Unsubscribe(self)
    }
    "subscribe subscribers to a new subscription dataset and ack the CurrentShardSnapshot" in {
      subscribers foreach { probe =>
        shardActor ! Subscribe(probe.ref, dataset1)
        probe.expectMsgPF() {
          case CurrentShardSnapshot(ds, map) =>
            ds shouldEqual dataset1
            map.unassignedShards.size shouldEqual resources.numShards / resources.minNumNodes
            map.assignedShards shouldEqual initialShards
            map.shardValues shouldEqual Seq(
              (localCoordinator, ShardStatusAssigned), (localCoordinator, ShardStatusAssigned),
              (ActorRef.noSender, ShardStatusUnassigned), (ActorRef.noSender, ShardStatusUnassigned))

            initialShards.forall { shard =>
              map.statusForShard(shard) == ShardStatusAssigned &&
                map.coordForShard(shard) == localCoordinator &&
                map.assignedShards == initialShards &&
                !map.unassigned(shard)
            } shouldEqual true
        }
      }
    }
    "on IngestionStarted, update the shardmapper, and publish event to subscribers" in {
      initialShards foreach { shard =>
        val event = IngestionStarted(dataset1, shard, localCoordinator)
        shardActor ! event
        subscribers.forall(_.expectMsgPF() { case e: IngestionStarted => e == event })
      }

      shardActor ! GetSnapshot(dataset1)
      expectMsgPF() {
        case CurrentShardSnapshot(ds, map) =>
          map.shardValues shouldEqual Seq(
            (localCoordinator, ShardStatusActive), (localCoordinator, ShardStatusActive),
            (ActorRef.noSender, ShardStatusUnassigned), (ActorRef.noSender, ShardStatusUnassigned))

          initialShards.forall { shard =>
            map.statusForShard(shard) == ShardStatusActive &&
              map.coordForShard(shard) == localCoordinator &&
              map.assignedShards == initialShards &&
              !map.unassigned(shard)
          } shouldEqual true
      }
    }
    "not overwrite already-assigned shards to a 'seen' coordinator during AddMember" in {
      // there are two times ShardAssignmentStrategy.addShards is called:
      // on AddMember and AddDataset by NodeClusterActor
      shardActor ! AddMember(localCoordinator, localAddress)
      expectMsgPF() {
        case CoordinatorAdded(coordinator, Nil, _) =>
          coordinator shouldEqual localCoordinator
      }

      shardActor ! GetSnapshot(dataset1)
      expectMsgPF() {
        case CurrentShardSnapshot(ds, map) =>
          initialShards.forall { shard =>
            map.statusForShard(shard) == ShardStatusActive &&
              map.coordForShard(shard) == localCoordinator &&
              map.assignedShards == initialShards &&
              !map.unassigned(shard)
          } shouldEqual true
      }
    }
    "not reassign already-assigned shard to a new coordinator during AddMember" in {
      shardActor ! AddMember(downingCoordinator, downingAddress)
      val dshards = expectMsgPF() {
        case CoordinatorAdded(coordinator, dss, _) =>
          dss.size shouldEqual 1
          coordinator.compareTo(downingCoordinator) shouldEqual 0
          dss
      }

      dshards.foreach { dss =>
        nextShards foreach { shard =>
          val assigned = ShardAssignmentStarted(dataset1, shard, downingCoordinator)
          subscribers.forall(_.expectMsg(assigned) == assigned) shouldEqual true
        }
      }

      dshards.forall { case DatasetShards(ref, map, shards) =>
        ref == dataset1 &&
          shards == nextShards &&
          map.assignedShards == initialShards ++ nextShards &&
          initialShards.forall { shard =>
            !map.unassigned(shard) &&
              map.coordForShard(shard) == localCoordinator &&
              map.statusForShard(shard) == ShardStatusActive
          } &&
          nextShards.forall { shard =>
            !map.unassigned(shard) &&
              map.coordForShard(shard) == downingCoordinator &&
              map.statusForShard(shard) == ShardStatusAssigned
          }
      } shouldEqual true

      dshards.foreach { dss =>
        nextShards foreach { shard =>
          val started = IngestionStarted(dataset1, shard, downingCoordinator)
          shardActor ! started
          subscribers.forall(_.expectMsg(started) == started) shouldEqual true
        }
      }
    }
    "with 2 nodes ingesting, have the expected shardmapper node values of coordinator -> to ShardStatus" in {
      shardActor ! GetSnapshot(dataset1)
      expectMsgPF() {
        case CurrentShardSnapshot(ds, map) =>
          map.shardValues shouldEqual Seq(
            (localCoordinator, ShardStatusActive), (localCoordinator, ShardStatusActive),
            (downingCoordinator, ShardStatusActive), (downingCoordinator, ShardStatusActive))
      }
    }
    "not reassign already-assigned shards when adding a new dataset during AddDataset" in {
      val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)
      val sd = SetupDataset(dataset1, resources, noOpSource)
      shardActor ! AddDataset(sd, datasetObj, Set(localCoordinator), self)
      expectMsg(NodeClusterActor.DatasetExists(dataset1))
    }
    "unassign shards on MemberRemoved, remove coord from shardmapper, update status" in {
      system stop downingCoordinator

      shardActor ! RemoveMember(downingCoordinator)
      expectMsgPF() {
        case CoordinatorRemoved(coordinator, dshards) =>
          coordinator.compareTo(downingCoordinator) shouldEqual 0

          dshards.forall { dss =>
            dss.shards == nextShards &&
              dss.mapper.unassignedShards == nextShards &&
              dss.mapper.shardsForAddress(downingCoordinator.path.address).isEmpty &&
              dss.shards.forall { shard =>
                dss.mapper.statusForShard(shard) == ShardStatusDown &&
                  Option(dss.mapper.coordForShard(shard)).isEmpty &&
                  dss.mapper.unassigned(shard)
              }} shouldEqual true
      }

      shardActor ! GetSubscriptions
      expectMsgPF() {
        case ShardSubscriptions(subscriptions, watchers) =>
          subscriptions.headOption.forall(_.dataset == dataset1) shouldEqual true
          watchers.isEmpty shouldEqual true
      }

      shardActor ! GetSnapshot(dataset1)
      expectMsgPF() {
        case CurrentShardSnapshot(ds, mapper) =>
          ds shouldEqual dataset1
      }
    }
    "update subscribers on MemberRemoved with ShardMemberRemoved => ShardStatusUnassigned" in {
      nextShards forall { shard =>
        val unassignment = ShardDown(dataset1, shard, downingCoordinator)
        subscribers forall { _.expectMsgPF() {
          case e: ShardDown => e === unassignment
        }}
      } shouldEqual true
    }
    "on third MemberAdded, expect 2 nodes - second was downed and removed" in {
      shardActor ! AddMember(thirdCoordinator, thirdAddress)
      expectMsgPF() {
        case CoordinatorAdded(coord, dshards, _) =>
          dshards.size shouldEqual 1
          (coord compareTo thirdCoordinator) shouldEqual 0
          dshards.flatMap(_.shards) shouldEqual nextShards

          nextShards foreach { shard =>
            val assigned = ShardAssignmentStarted(dataset1, shard, thirdCoordinator)
            subscribers.forall(_.expectMsg(assigned) == assigned) shouldEqual true
          }
      }

      shardActor ! GetSubscriptions
      expectMsgPF() {
        case e@ShardSubscriptions(subscriptions, watchers) =>
          val subscriberRefs = subscribers.map(_.ref)
          subscriptions shouldEqual Set(ShardSubscription(dataset1, subscriberRefs))
          e.subscribers(dataset1) shouldEqual subscriberRefs
          watchers.isEmpty shouldEqual true
      }

      nextShards foreach { shard =>
        val started = IngestionStarted(dataset1, shard, thirdCoordinator)
        shardActor ! started
        subscribers.forall(_.expectMsg(started) == started) shouldEqual true
      }
    }
    "on DatasetAdded, second dataset, have the expected shard assignments with 2 nodes" in {
      val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)
      val sd = SetupDataset(dataset2, resources, noOpSource)
      val dataset = datasetObj.copy(name = dataset2.dataset)
      val coordinators = Set(localCoordinator, thirdCoordinator)

      shardActor ! AddDataset(sd, dataset, coordinators, self)
      expectMsgPF() {
        case DatasetAdded(dset, _, nodeToShards, _) =>
          dset shouldEqual dataset
          nodeToShards.keySet shouldEqual coordinators

          // Ensure that every shard is uniquely assigned
          val uniqueShards = nodeToShards.values.reduce(_ ++ _).toSet
          uniqueShards shouldEqual (0 until resources.numShards).toSet
          dataset.name shouldEqual dataset2.dataset

          subscribers foreach { probe =>
            shardActor ! Subscribe(probe.ref, dataset2)
            probe.expectMsgPF() {
              case CurrentShardSnapshot(ds, mapper) =>
                ds shouldEqual dataset2
                mapper.numShards shouldEqual resources.numShards
                mapper.unassignedShards.size shouldEqual 0
                mapper.assignedShards.size shouldEqual resources.numShards
                mapper.shardValues.size shouldEqual resources.numShards
                mapper.allNodes.size shouldEqual coordinators.size
                mapper.assignedShards shouldEqual (0 until resources.numShards)
            }
          }

          for {(node, shards) <- nodeToShards; shard <- shards} {
            val event = IngestionStarted(dataset2, shard, node)
            shardActor ! event
            subscribers.forall(_.expectMsg(event) == event) shouldEqual true
          }

          shardActor ! GetSnapshot(dataset2)
          expectMsgPF() {
            case CurrentShardSnapshot(ds, map) =>
              map.assignedShards shouldEqual initialShards ++ nextShards
              map.assignedShards forall { shard =>
                map.activeShard(shard) &&
                  map.statusForShard(shard) == ShardStatusActive
              } shouldEqual true

              map.shardValues.sortBy(_._1) shouldEqual Seq(
                (localCoordinator, ShardStatusActive), (localCoordinator, ShardStatusActive),
                (thirdCoordinator, ShardStatusActive), (thirdCoordinator, ShardStatusActive))
          }
      }
    }
    "get a set of all ShardSubscriptions with the expected state" in {
      shardActor ! GetSubscriptions
      expectMsgPF() {
        case e: ShardSubscriptions =>
          e.subscriptions.size shouldEqual 2
          Set(dataset1, dataset2).forall(assertions(e, _))
      }

      def assertions(e: ShardSubscriptions, ds: DatasetRef): Boolean =
        e.subscription(ds).forall { subscription =>
          subscription.subscribers == Set(subscriber1.ref, subscriber2.ref) &&
            e.subscribers(ds) == subscription.subscribers
        }
    }
    "unsubscribe a subscriber" in {
      val probe = TestProbe()
      shardActor ! Subscribe(probe.ref, dataset2)
      probe.expectMsgPF() {
        case CurrentShardSnapshot(ds, mapper) =>
          shardActor ! Unsubscribe(probe.ref)
      }

      shardActor ! GetSubscribers(dataset2)
      expectMsgPF() {
        case Subscribers(subs, ds) =>
          subs.contains(probe.ref) shouldBe false
      }
    }
    "remove a subscription" in {
      shardActor ! GetSubscriptions
      val subscriptions = expectMsgPF() {
        case pre: ShardSubscriptions =>
          pre.subscriptions.size shouldEqual 2
          pre.subscriptions
      }
      shardActor ! RemoveDataset(dataset2)
      shardActor ! GetSubscriptions
      expectMsgPF() {
        case post: ShardSubscriptions =>
          post.subscription(dataset2).isEmpty shouldBe true
          post.subscriptions.size shouldEqual 1
      }
      shardActor ! GetSubscribers(dataset2)
      expectMsgPF() {
        case Subscribers(subs, ds) => subs shouldEqual Set.empty
      }
    }
  }
}

class ShardCoordinatorActorSpec extends ShardCoordinatorSpec with BeforeAndAfterEach {
  import ShardSubscriptions._

  override def beforeEach(): Unit = {
    shardActor ! NodeProtocol.ResetState
    expectMsg(NodeProtocol.StateReset)
  }

  "ShardActor" must {
    "assign shards correctly when nodes added after dataset shard add" in {
      // Add a dataset with no nodes
      val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)
      val sd = SetupDataset(dataset1, resources, noOpSource)
      val dataset = datasetObj
      shardActor ! AddDataset(sd, dataset, Set.empty, self)
      expectMsgPF() {
        case DatasetAdded(ds, source, nodeShards, ackTo) =>
          ds.name shouldEqual dataset1.dataset
          nodeShards.size shouldEqual 0
      }

      shardActor ! Subscribe(subscriber1.ref, dataset1)
      subscriber1.expectMsgPF() {
        case CurrentShardSnapshot(ds, mapper) =>
          mapper.numAssignedShards shouldEqual 0
      }

      // Now subscribe/add a single coordinator.  Check shard assignments
      shardActor ! AddMember(localCoordinator, localAddress)
      expectMsgPF() {
        case CoordinatorAdded(coord, updates, selfAddress) =>
          updates should have length 1
          updates.head.ref shouldEqual dataset1
          updates.head.shards shouldEqual Seq(0, 1)
          coord shouldEqual localCoordinator
      }

      // TODO: test that shardActor sends out commands?  otherwise how does the added shards
      // start ingesting?

      shardActor ! GetSnapshot(dataset1)
      expectMsgPF() {
        case CurrentShardSnapshot(ds, mapper) =>
          mapper.numAssignedShards shouldEqual 2
          mapper.unassignedShards.size shouldEqual 2
      }

      // Now subscribe another coordinator.  Check assignments again
      shardActor ! AddMember(thirdCoordinator, thirdAddress)
      expectMsgPF() {
        case CoordinatorAdded(coord, updates, selfAddress) =>
          updates should have length 1
          updates.head.ref shouldEqual dataset1
          updates.head.shards shouldEqual Seq(2, 3)
          coord shouldEqual thirdCoordinator
      }

      shardActor ! GetSnapshot(dataset1)
      expectMsgPF() {
        case CurrentShardSnapshot(ds, mapper) =>
          mapper.numAssignedShards shouldEqual 4
          mapper.unassignedShards.size shouldEqual 0
      }
    }
  }
}

class TestCoordinator extends BaseActor {
  override def receive: Actor.Receive = {
    case e => logger.debug(s"${self.path.toSerializationFormat} received $e")
  }
}

trait ShardCoordinatorSpec extends AkkaSpec {

  import ActorName._

  protected val dataset1 = DatasetRef("one")
  protected val dataset2 = DatasetRef("two")
  protected val datasetObj = Dataset(dataset1.dataset, Seq("seg:int"), Seq("timestamp:long"))

  protected val resources = DatasetResourceSpec(4, 2)
  protected val initialShards = Seq(0, 1)
  protected val nextShards = Seq(2, 3)

  protected val shardActor = system.actorOf(Props(
    new ShardCoordinatorActor(new DefaultShardAssignmentStrategy)))

  protected val conf = ConfigFactory
    .parseString("akka.remote.netty.tcp.port=0")
    .withFallback(AkkaSpec.serverConfig)

  protected lazy val node1 = AkkaSpec.getNewSystem(Some(conf))
  protected lazy val node2 = AkkaSpec.getNewSystem(Some(conf))

  protected val localCoordinator = system.actorOf(Props[TestCoordinator], CoordinatorName)
  protected val localAddress = AddressFromURIString(s"akka.tcp://${system.name}@$host:2552")

  protected lazy val downingCoordinator = node1.actorOf(Props[TestCoordinator], CoordinatorName)
  protected val downingAddress = AddressFromURIString(s"akka.tcp://${system.name}@$host:2552")

  protected val thirdCoordinator = node2.actorOf(Props[TestCoordinator], CoordinatorName)
  protected val thirdAddress = AddressFromURIString(s"akka.tcp://${system.name}@$host:2552")

  protected val subscriber1 = TestProbe()
  protected lazy val subscriber2 = TestProbe()
  protected lazy val subscribers = Set(subscriber1, subscriber2)

  override def afterAll(): Unit = {
    node1.terminate()
    node2.terminate()
    super.afterAll()
  }
}