package filodb.coordinator

import akka.actor.ActorRef
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory

import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.{DatasetRef, NamesTestData}

object NodeClusterSpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  val first = role("first")
  val second = role("second")

  // this configuration will be used for all nodes
  // Uses our common Akka test config from application_test.conf
  val globalConfig = ConfigFactory.load("application_test.conf")
  commonConfig(globalConfig)
}

abstract class NodeClusterSpec extends MultiNodeSpec(NodeClusterSpecConfig)
  with FunSpecLike with Matchers with BeforeAndAfterAll
  with CoordinatorSetupWithFactory
  with ImplicitSender
  with ScalaFutures {

  import NodeClusterSpecConfig._
  import NodeClusterActor._
  import NamesTestData._

  override def initialParticipants = roles.size

  override def beforeAll() = {
    metaStore.clearAllData().futureValue
    multiNodeSpecBeforeAll()
  }

  override def afterAll() = multiNodeSpecAfterAll()

  val config = globalConfig.getConfig("filodb")

  val address1 = node(first).address
  val address2 = node(second).address

  var clusterActor: ActorRef = _

  it("should start NodeClusterActor, CoordActors and join one node") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor

    runOn(first) {
      // Initialize dataset
      coordinatorActor ! DatasetCommands.CreateDataset(dataset, schema)
      expectMsg(DatasetCommands.DatasetCreated)

      clusterActor = system.actorOf(
                       NodeClusterActor.props(cluster, "executor", metaStore, assignmentStrategy))
      clusterActor ! GetRefs("first")
      expectMsg(NoSuchRole)
    }
    enterBarrier("cluster-actor-started")

    // Join first node only.  Get refs and map back.
    runOn(first) {
      clusterActor ! SubscribeShardUpdates(datasetRef)
      expectMsg(UnknownDataset(datasetRef))

      cluster join address1
      awaitCond(cluster.state.members.size == 1)
    }

    enterBarrier("first-node-joined")
  }

  it("should get roles back") {
    runOn(first) {
      clusterActor ! GetRefs("first")
      expectMsg(Set(coordinatorActor))
    }
  }

  val spec = DatasetResourceSpec(4, 2)   // 4 shards, 2 nodes, 2 shards per node

  it("should return UnknownDataset when dataset missing or no columns defined") {
    runOn(first) {
      clusterActor ! SetupDataset(DatasetRef("noColumns"), Seq("first"), spec, noOpSource)
      expectMsg(UnknownDataset(DatasetRef("noColumns")))
    }
  }

  it("should return UndefinedColumns if trying to setup with undefined columns") {
    runOn(first) {
      clusterActor ! SetupDataset(datasetRef, Seq("first", "country"), spec, noOpSource)
      expectMsg(UndefinedColumns(Set("country")))
    }
  }

  // NOTE: getting a BadSchema should never happen.  Creating a dataset with either CLI or Spark,
  // all of the columns are checked.

  it("should setup dataset on all nodes for valid dataset and get ShardMap updates") {
    runOn(first) {
      clusterActor ! SetupDataset(datasetRef, Seq("first", "last", "age"), spec, noOpSource)
      expectMsg(DatasetVerified)

      clusterActor ! SubscribeShardUpdates(datasetRef)
      val shardMap = expectMsgClass(classOf[ShardMapper])
      shardMap.numAssignedShards should equal (2)
      shardMap.allNodes should equal (Set(coordinatorActor))

      // Calling SetupDataset again should return DatasetAlreadySetup
      clusterActor ! SetupDataset(datasetRef, Seq("first", "age"), spec, noOpSource)
      expectMsg(DatasetAlreadySetup(datasetRef))

      coordinatorActor ! MiscCommands.GetClusterActor
      expectMsg(Some(clusterActor))
    }

    enterBarrier("dataset-setup")
  }

  it("should get ShardMap updates and have shards assigned when new node joins") {
    runOn(second) {
      cluster join address1
      awaitCond(cluster.state.members.size == 2)
    }

    enterBarrier("second-node-joined")

    runOn(first) {
      val shardMap = expectMsgClass(classOf[ShardMapper])
      shardMap.numAssignedShards should equal (4)
      shardMap.allNodes.size should equal (2)
    }

    enterBarrier("second-node-update-received")
  }

  it("should update PartitionMap if node goes down") (pending)
}

class NodeClusterSpecMultiJvmNode1 extends NodeClusterSpec
class NodeClusterSpecMultiJvmNode2 extends NodeClusterSpec