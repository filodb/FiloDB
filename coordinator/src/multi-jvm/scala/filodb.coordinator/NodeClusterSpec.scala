package filodb.coordinator

import akka.actor.ActorRef
import akka.cluster.Cluster
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import filodb.core.store.{InMemoryColumnStore, InMemoryMetaStore}

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
  with CoordinatorSetup
  with ImplicitSender {

  import NodeClusterSpecConfig._
  import NodeClusterActor._

  override def initialParticipants = roles.size

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()

  val config = globalConfig.getConfig("filodb")

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global
  lazy val columnStore = new InMemoryColumnStore(context)
  lazy val metaStore = new InMemoryMetaStore

  val address1 = node(first).address
  val address2 = node(second).address

  var clusterActor: ActorRef = _

  it("should start NodeClusterActor, CoordActors and wait for all nodes to enter barrier") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor

    runOn(first) {
      clusterActor = system.actorOf(NodeClusterActor.props(cluster, "executor"))
      clusterActor ! GetRefs("first")
      expectMsg(NoSuchRole)
    }
    enterBarrier("cluster-actor-started")
  }

  it("should register new nodes in PartitionMapper and send updates") {
    // Join first node only.  Get refs and map back.
    runOn(first) {
      clusterActor ! SubscribePartitionUpdates
      val mapper1 = expectMsgClass(classOf[PartitionMapUpdate])
      mapper1.map.isEmpty should be (true)

      cluster join address1
      awaitCond(cluster.state.members.size == 1)

      // Should now get a partitionMap update since one node (us) joined
      val mapper2 = expectMsgClass(classOf[PartitionMapUpdate])
      mapper2.map.isEmpty should be (false)
      mapper2.map.numNodes should be (8)
    }

    enterBarrier("first-node-joined")

    runOn(second) {
      cluster join address1
      awaitCond(cluster.state.members.size == 2)
    }

    enterBarrier("second-node-joined")

    runOn(first) {
      val mapper = expectMsgClass(classOf[PartitionMapUpdate])
      mapper.map.numNodes should be (16)
    }

    enterBarrier("second-node-update-received")
  }

  it("should update PartitionMap if node goes down") (pending)
}

class NodeClusterSpecMultiJvmNode1 extends NodeClusterSpec
class NodeClusterSpecMultiJvmNode2 extends NodeClusterSpec