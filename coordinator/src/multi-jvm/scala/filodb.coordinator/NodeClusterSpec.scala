package filodb.coordinator

import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

import filodb.core._

object NodeClusterSpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  val first = role("first")
  val second = role("second")

  // this configuration will be used for all nodes
  // Uses our common Akka test config from application_test.conf
  val globalConfig = ConfigFactory.load("application_test.conf")
  commonConfig(globalConfig)
}

/**
 * Tests the NodeClusterActor cluster singleton, dataset setup error responses,
 * and shard assignment changes when nodes join and leave
 */
abstract class NodeClusterSpec extends ClusterSpec(NodeClusterSpecConfig) {

  import akka.testkit._
  import NodeClusterSpecConfig._
  import NodeClusterActor._
  import GdeltTestData._

  private lazy val metaStore = cluster.metaStore

  private lazy val coordinatorActor = cluster.coordinatorActor

  private val ref = projection6.datasetRef
  private val spec = DatasetResourceSpec(4, 2)   // 4 shards, 2 nodes, 2 shards per node

  override def initialParticipants = roles.size

  override def beforeAll(): Unit = {
    metaStore.clearAllData().futureValue
    multiNodeSpecBeforeAll()

    // Initialize dataset
    metaStore.newDataset(projection6.dataset).futureValue shouldEqual Success
    schema.foreach { col => metaStore.newColumn(col, ref).futureValue shouldEqual Success }
  }

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  val config = globalConfig.getConfig("filodb")

  val address1 = node(first).address
  val address2 = node(second).address

  var clusterActor: ActorRef = _

  it("should start NodeClusterActor, CoordActors and join one node") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor

    runOn(first) {
      cluster join address1
      awaitCond(cluster.state.members.size == 1)

      clusterActor = cluster.clusterSingletonProxy("worker", withManager = true)
    }
    enterBarrier("first-node-joined-cluster-actor-started")
  }

  it("should get roles back or NoSuchRole") {
    runOn(first) {
      clusterActor ! GetRefs("first")
      expectMsg(30.seconds, NoSuchRole)

      clusterActor ! GetRefs("worker")
      expectMsg(Set(coordinatorActor))
    }
  }

  it("should return UnknownDataset when dataset missing or no columns defined") {
    runOn(first) {
      clusterActor ! SubscribeShardUpdates(ref)
      expectMsg(DatasetUnknown(ref))

      clusterActor ! SetupDataset(DatasetRef("noColumns"), Seq("first"), spec, noOpSource)
      expectMsg(DatasetUnknown(DatasetRef("noColumns")))
    }
  }

  it("should return UndefinedColumns if trying to setup with undefined columns") {
    runOn(first) {
      clusterActor ! SetupDataset(ref, Seq("first", "country"), spec, noOpSource)
      expectMsg(UndefinedColumns(Set("first", "country")))
    }
    enterBarrier("end-of-undefined-columns")
  }

  // NOTE: getting a BadSchema should never happen.  Creating a dataset with either CLI or Spark,
  // all of the columns are checked.

  it("should setup dataset on all nodes for valid dataset and get ShardMap updates") {
    runOn(first) {
      val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)
      val columns = schema.map(_.name)
      val command = SetupDataset(projection6.datasetRef, columns, spec, noOpSource)

      clusterActor ! command
      expectMsg(DatasetVerified)

      val subscriber1 = TestProbe()
      val subscriber2 = TestProbe()

      subscriber1.send(clusterActor, SubscribeShardUpdates(ref))
      subscriber1.expectMsgPF(3.seconds.dilated) {
        case CurrentShardSnapshot(ds, newMap) =>
          0 until spec.numShards forall (shard => newMap.statusForShard(shard) == ShardUnassigned) shouldBe true
          newMap.allNodes.isEmpty shouldBe true // ingestion not started
          newMap.numShards shouldEqual spec.numShards
      }

      subscriber1.receiveWhile(messages = spec.numShards) {
        case e: IngestionStarted =>
          e.shard shouldEqual (0 +- 1)
          e.node shouldEqual coordinatorActor
      }

      subscriber2.send(clusterActor, SubscribeShardUpdates(ref))
      subscriber2.expectMsgPF(3.seconds.dilated) {
        case CurrentShardSnapshot(ds, newMap) =>
          newMap.numAssignedShards shouldEqual 2
          newMap.allNodes shouldEqual Set(coordinatorActor) // only one

          for {
            shard <- 0 until newMap.numAssignedShards
          } newMap.statusForShard(shard) shouldEqual ShardStatusNormal

      }

      clusterActor ! command
      expectMsg(DatasetExists(ref))
    }

    enterBarrier("dataset-setup")
  }

 it("should get ShardMapper updates and have shards assigned when new node joins") {
    runOn(second) {
      cluster join address1
      awaitCond(cluster.state.members.size == 2)
      clusterActor = cluster.clusterSingletonProxy("worker", withManager = true)
    }

    enterBarrier("second-node-joined")

    runOn(first) {

      clusterActor ! SubscribeShardUpdates(ref)
      expectMsgPF(3.seconds.dilated) {
        case CurrentShardSnapshot(ref, map) =>
          map.numAssignedShards shouldEqual 2 // TODO not 4
          map.allNodes.size shouldEqual 1 // only one coord created in entire multi test
      }
    }

    enterBarrier("second-node-update-received")
  }

  it("should update PartitionMap if node goes down") (pending)
}

class NodeClusterSpecMultiJvmNode1 extends NodeClusterSpec
class NodeClusterSpecMultiJvmNode2 extends NodeClusterSpec