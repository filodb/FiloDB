package filodb.coordinator

import akka.actor.{Actor, ActorRef, Props}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import filodb.coordinator.NodeClusterActor.{DatasetResourceSpec, IngestionSource, SetupDataset}
import filodb.core.DatasetRef
import filodb.core.metadata.Dataset

class ShardCoordinatorActorSpec extends AkkaSpec {

  import ShardSubscriptions._, ActorName._

  private val dataset1 = DatasetRef("one")
  private val dataset2 = DatasetRef("two")

  private val resources = DatasetResourceSpec(4, 2)

  private val subscriber1 = TestProbe()
  private val subscriber2 = TestProbe()

  private lazy val conf = ConfigFactory.parseString("akka.remote.netty.tcp.port=0").withFallback(AkkaSpec.serverConfig)
  private lazy val node1 = AkkaSpec.getNewSystem(Some(conf))
  private lazy val node2 = AkkaSpec.getNewSystem(Some(conf))

  val localCoordinator = system.actorOf(Props(new TestCoordinator(self)), CoordinatorName)
  val downingCoordinator = node1.actorOf(Props(new TestCoordinator(self)), CoordinatorName)
  val thirdCoordinator = node2.actorOf(Props(new TestCoordinator(self)), CoordinatorName)

  override def afterAll(): Unit = {
    node1.terminate()
    node2.terminate()
    super.afterAll()
  }

  "ShardActor" must {
    val strategy = new DefaultShardAssignmentStrategy
    val shardActor = system.actorOf(Props(new ShardCoordinatorActor(strategy)))

    "subscribe self-node coordinator, no datasets created yet" in {
      shardActor ! SubscribeCoordinator(localCoordinator)
      expectMsgPF() {
        case CoordinatorSubscribed(coord, updates) =>
          updates.size shouldEqual 0 // no datasets added yet
          coord shouldEqual localCoordinator
      }
    }
    "add a subscription for dataset shard updates" in {
      val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)
      val sd = SetupDataset(dataset1, Seq.empty, resources, noOpSource)
      val dataset = Dataset(dataset1.dataset, Seq.empty, Seq.empty)
      shardActor ! AddDataset(sd, dataset, Seq.empty, Set(localCoordinator), self)
      expectMsgPF() {
        case DatasetAdded(dataset, columns, source, nodeShards, ackTo) =>
          dataset.name shouldEqual dataset1.dataset

          nodeShards foreach { case (node, shards) =>
            shards.toSet shouldEqual Set(0, 1)
            shards.foreach { shard =>
              val event = IngestionStarted(dataset1, shard, node)
              shardActor ! event
              expectMsgPF() { case e: IngestionStarted => e shouldEqual event }
            }
          }
      }
      Set(subscriber1, subscriber2) foreach { probe =>
        shardActor ! Subscribe(probe.ref, dataset1)
        probe.expectMsgPF() {
          case e: CurrentShardSnapshot =>
            e.ref shouldEqual dataset1
            e.map.unassignedShards.size shouldEqual resources.numShards / resources.minNumNodes
            e.map.assignedShards.size shouldEqual resources.numShards / resources.minNumNodes
            e.map.shardValues.size shouldEqual resources.numShards
        }
      }
    }
    "not subscribe to an invalid dataset, ack DatasetUnknown to subscriber" in {
      shardActor ! Subscribe(subscriber1.ref, DatasetRef("invalid"))
      expectMsgPF() {
        case SubscriptionUnknown(ds, sub) => ds.dataset shouldEqual "invalid"
      }
    }
    "subscribe second coordinator, ack to parent" in {
      shardActor ! SubscribeCoordinator(downingCoordinator)
      expectMsgPF() {
        case CoordinatorSubscribed(coord, datasets) =>
          datasets.size shouldEqual 1
          coord shouldEqual downingCoordinator
      }
    }
    "update subscribers on second coordinator terminated / node removed" in {
      shardActor ! GetSubscribers(dataset1)
      expectMsgPF() {
        case Subscribers(subscribers, dataset) =>
          dataset shouldEqual dataset1
          subscribers.size shouldEqual 4
          subscribers.count(_.path.name == CoordinatorName) shouldEqual 2
          subscribers.count(a => a == subscriber1.ref || a == subscriber2.ref) shouldEqual 2

          system stop downingCoordinator

          val downed = Set(ShardDown(dataset1, 2), ShardDown(dataset1, 3))
          downed foreach { event =>
            shardActor ! event
            expectMsg(event)
            Set(subscriber1, subscriber2) foreach (_.expectMsg(event))
          }
      }
    }
    "on node removed, the downed coordinator should no longer be in the subscribers" in {
      shardActor ! GetSubscribers(dataset1)
      expectMsgPF() {
        case Subscribers(subscribers, _) =>
          subscribers.size shouldEqual 3
          subscribers.exists(_.compareTo(downingCoordinator) == 0) should be(false)
      }
    }
    "subscribe a third node, expect 2 nodes - second was downed" in {
      shardActor ! SubscribeCoordinator(thirdCoordinator)
      expectMsgPF() {
        case CoordinatorSubscribed(coord, datsets) =>
          datsets.size shouldEqual 1
          (coord compareTo thirdCoordinator) == 0 shouldBe true
      }

      shardActor ! GetSubscriptions
      expectMsgPF() {
        case e@ShardSubscriptions(subscriptions) =>
          subscriptions.forall(_.subscribers.contains(localCoordinator)) shouldBe true
          subscriptions.forall(_.subscribers.contains(thirdCoordinator)) shouldBe true

          subscriptions.size shouldEqual 1
          val subscribers = e.subscribers(dataset1)
          subscribers.size shouldEqual 4
          subscribers.count(_.path.name == CoordinatorName) shouldEqual 2
          subscribers.count(a => a == subscriber1.ref || a == subscriber2.ref) shouldEqual 2
      }
    }
    "have the expected shard assignments when adding a second dataset with 2 nodes" in {
      val coordinators = Set(localCoordinator, thirdCoordinator)
      val regularSubscribers = Set(subscriber1, subscriber2)
      val subscribers = coordinators ++ regularSubscribers.map(_.ref)

      val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)
      val sd = SetupDataset(dataset2, Seq.empty, resources, noOpSource)
      val dataset = Dataset(dataset2.dataset, Seq.empty, Seq.empty)
      shardActor ! AddDataset(sd, dataset, Seq.empty, coordinators, self)

      expectMsgPF() {
        case DatasetAdded(dataset, columns, source, nodeToShards, ackTo) =>
          nodeToShards.size shouldEqual coordinators.size
          coordinators.forall(nodeToShards.keySet.contains) shouldBe true
          dataset.name shouldEqual dataset2.dataset

          for {(node, shards) <- nodeToShards; shard <- shards} {
            val event = IngestionStarted(dataset2, shard, node)
            shardActor ! event
            coordinators foreach (c => expectMsg(event))
          }
      }

      regularSubscribers foreach { probe =>
        shardActor ! Subscribe(probe.ref, dataset2)
        probe.expectMsgPF() {
          case CurrentShardSnapshot(ds, mapper) =>
            ds shouldEqual dataset2
            mapper.numShards shouldEqual 4
            mapper.unassignedShards.size shouldEqual resources.numShards / resources.minNumNodes
            mapper.assignedShards.size shouldEqual resources.numShards / resources.minNumNodes
            mapper.shardValues.size shouldEqual resources.numShards

            // TODO was false: mapper.allNodes.size shouldEqual coordinators.size
            mapper.assignedShards shouldEqual Seq(0, 1)
            mapper.assignedShards foreach { shard =>
              mapper.activeShard(shard) shouldBe true
              mapper.statusForShard(shard) shouldBe ShardStatusNormal
            }
        }
      }
    }
    "receive DatasetExists if AddDataset has existing dataset" in {
      val coordinators = Set(localCoordinator, thirdCoordinator)
      val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)
      val dataset = Dataset(dataset2.dataset, Seq.empty, Seq.empty)
      val sd = SetupDataset(dataset2, Seq.empty, resources, noOpSource)
      shardActor ! AddDataset(sd, dataset, Seq.empty, coordinators, self)

      expectMsg(NodeClusterActor.DatasetExists(dataset2))
    }
    "get a set of all ShardSubscriptions with the expected state" in {
      val coordinators = Set(localCoordinator, thirdCoordinator)

      shardActor ! GetSubscriptions
      expectMsgPF() {
        case e: ShardSubscriptions =>
          e.subscriptions.size shouldEqual 2
          Set(dataset1, dataset2) foreach (ds => assertions(e, ds))
      }

      def assertions(e: ShardSubscriptions, ds: DatasetRef): Unit = {
        val subscriptionOpt = e.subscription(ds)
        subscriptionOpt.isDefined shouldBe true
        val subscription = subscriptionOpt.get

        val subscribers = subscription.subscribers
        e.subscribers(ds) shouldEqual subscribers
        subscribers.size shouldEqual 4
        e.subscribers(ds).size shouldEqual 4
        val dsCoords = subscribers.filter(_.path.name == CoordinatorName)
        dsCoords.size shouldEqual 2

        coordinators foreach (c => dsCoords contains c shouldBe true)
        Set(subscriber1.ref, subscriber2.ref) foreach { sub =>
          e.subscribers(ds) contains sub shouldBe true
        }
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
       case Subscribers(subscribers, ds) =>
         subscribers.contains(probe.ref) shouldBe false
     }
   }
   "remove a subscription" in {
     shardActor ! GetSubscriptions
     expectMsgPF() {
       case pre: ShardSubscriptions =>
         pre.subscriptions.size shouldEqual 2
     }
     shardActor ! RemoveSubscription(dataset2)
     shardActor ! GetSubscriptions
     expectMsgPF() {
       case post: ShardSubscriptions =>
         post.subscription(dataset2).isEmpty shouldBe true
         post.subscriptions.size shouldEqual 1
     }
     shardActor ! GetSubscribers(dataset2)
     expectMsgPF() {
       case Subscribers(subscribers, ds) =>
         subscribers shouldEqual Set.empty
     }
   }
  }
}

class TestCoordinator(listener: ActorRef) extends BaseActor {
  override def receive: Actor.Receive = {
    case e =>
      logger.debug(s"${self.path.toSerializationFormat} received $e")
      listener forward e
  }
}