package filodb.coordinator

import akka.remote.testkit._
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import filodb.core._

class ShardAssignmentClusterSingletonSpecMultiJvmNode1 extends ShardAssignmentClusterSingletonSpec
class ShardAssignmentClusterSingletonSpecMultiJvmNode2 extends ShardAssignmentClusterSingletonSpec

abstract class ShardAssignmentClusterSingletonSpec
  extends MultiNodeSpec(ShardAssignmentSpecMultiNodeConfig)
    with MultiNodeClusterBehavior {

  import NodeClusterActor._
  import ShardAssignmentSpecMultiNodeConfig._
  import GdeltTestData._

  private val ref = dataset6.ref
  private val spec = DatasetResourceSpec(4, 2)

  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore.newDataset(dataset6).futureValue shouldEqual Success
  }

  "Cluster node shard assignment" must {
    s"start and initialize nodes for $roles and assert MemberUp, MemberStatus.Up on all nodes" in {
      awaitClusterUp(first, second)

      runOn(first, second) {
        within(defaultTimeout) {
          awaitAssert(cluster.state.members.count(m => m.status == akka.cluster.MemberStatus.Up) shouldEqual roles.size)
          cluster.state.members.size shouldEqual roles.size
          info(s"Cluster members up: ${cluster.state.members}")
        }
      }
      enterBarrier("roles-up")
    }

    s"on DatasetAdded assign shards to ${roles.size} nodes and have expected state updated to subscribers on nodes" in {
      val roleAddresses = Seq(address(first), address(second))

      def assert(receiver: Option[TestProbe]): Boolean = {
        val expect = (map: ShardMapper) => {
          map.allNodes.size === roles.size &&
            map.assignedShards === (0 until spec.numShards).toList &&
            map.assignedShards.forall(s => map.statusForShard(s) == ShardStatusAssigned) &&
            map.numAssignedShards == spec.numShards &&
            map.unassignedShards.isEmpty
        }

        receiver
          .map( _.expectMsgPF(defaultTimeout) { case CurrentShardSnapshot(ds, map) => expect(map) })
          .getOrElse(expectMsgPF(defaultTimeout) { case CurrentShardSnapshot(ds, map) => expect(map) })
      }

      runOn(first) {
        clusterSingleton ! SetupDataset(ref, spec, noOpSource)
        expectMsg(DatasetVerified)
      }
      enterBarrier(s"${dataset6.name}-added-on-first")

      runOn(first, second) {
        clusterSingleton ! GetShardMap(ref)
        assert(None) shouldEqual true
        enterBarrier("dataset-verified-on-all-nodes")
      }

      runOn(first, second) {
        val subscribers = createSubscribers(2)
        subscribers foreach (_.send(clusterSingleton, SubscribeShardUpdates(ref)))
        subscribers forall (s => assert(Some(s))) shouldEqual true
        subscribers foreach (s => clusterSingleton ! ShardSubscriptions.Unsubscribe(s.ref))
      }
      enterBarrier("subscriber-updates-verified")
    }

    "have expected datasets registered on all nodes then verify cluster singleton restart on new member node" in {
      import akka.cluster.MemberStatus
      import akka.cluster.ClusterEvent._

      runOn(first, second) {
        clusterSingleton ! ListRegisteredDatasets
        expectMsg(Seq(ref))
      }
      enterBarrier("singleton-dataset-check-before-member-leave")

      awaitOnClusterLeave(second, first, classOf[MemberRemoved], MemberStatus.Removed) {
        watcher.expectMsgPF(defaultTimeout) {
          case NodeProtocol.PreStart(identity) =>
            identity.name shouldEqual ActorName.ClusterSingletonName
            info(s"$identity PreStart")
        }
      }
    }
  }
}

object ShardAssignmentSpecMultiNodeConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  val globalConfig = ConfigFactory.load("application_test.conf")

  commonConfig(debugConfig(on = false)
    .withFallback(ConfigFactory.parseString("akka.actor.provider = cluster"))
    .withFallback(globalConfig))

}
