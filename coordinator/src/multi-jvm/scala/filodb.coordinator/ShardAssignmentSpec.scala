package filodb.coordinator
import java.util.UUID

import akka.remote.testkit._
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}

import filodb.core._

class ShardAssignmentClusterSingletonSpecMultiJvmNode1 extends ShardAssignmentClusterSingletonSpec
class ShardAssignmentClusterSingletonSpecMultiJvmNode2 extends ShardAssignmentClusterSingletonSpec
//class ShardAssignmentClusterSingletonSpecMultiJvmNode3 extends ShardAssignmentSpec
//class ShardAssignmentClusterSingletonSpecMultiJvmNode4 extends ShardAssignmentSpec

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

      awaitOnClusterLeave(second, first, classOf[MemberRemoved], MemberStatus.Removed)
    }
    "dataset on second after first MemberRemoved unassign shards and update survivor nodes and subscribers" is pending
    /* Not implemented yet in the restart, part of another ticket.
      runOn(second) {
        info("Starting to test cluster singleton handover state to second")
        import akka.pattern.ask
        implicit val t: Timeout = defaultTimeout * 2

        def registered: Seq[DatasetRef] = (clusterSingleton ? ListRegisteredDatasets).mapTo[Seq[DatasetRef]].futureValue

        awaitAssert(registered shouldEqual Seq(ref), max = t.duration)
        enterBarrier("registered-on-second-handover-singleton")

        clusterSingleton ! GetShardMap(ref)
        expectMsgPF(defaultTimeout) {
          case CurrentShardSnapshot(ds, map) =>
            map.allNodes shouldEqual Seq(address(second))
            map.numAssignedShards shouldEqual 2
            map.assignedShards shouldEqual Seq(2, 3)
            map.unassignedShards shouldEqual Seq(0, 1)
          //Seq(0, 1).forall(s => map.statusForShard(s) == ShardStatusUnassigned) shouldEqual true
          //Seq(2, 3).forall(s => map.statusForShard(s) == ShardStatusNormal) shouldEqual true
        }
      }

      enterBarrier("finished")
      */
  }
}

object ShardAssignmentSpecMultiNodeConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  //val third = role("third")
  //val fourth = role("fourth")

  val globalConfig = ConfigFactory.load("application_test.conf")

  commonConfig(debugConfig(on = false)
    .withFallback(clusterConfig)
    .withFallback(globalConfig))

  // in addition to application_test.conf:
  def clusterConfig: Config = ConfigFactory.parseString(
    s"""
    akka.actor.provider = cluster
    akka.cluster {
      jmx.enabled                         = off
      gossip-interval                     = 200 ms
      leader-actions-interval             = 200 ms
      unreachable-nodes-reaper-interval   = 200 ms
      periodic-tasks-initial-delay        = 300 ms
      publish-stats-interval              = 0 s # always, when it happens
      #failure-detector.heartbeat-interval = 200 ms
      #singleton-proxy {
      #  singleton-identification-interval = 1s
      #  buffer-size = 1000
      #}
    }
    akka.loglevel = INFO
    akka.log-dead-letters = off
    akka.log-dead-letters-during-shutdown = off
    akka.remote {
      log-remote-lifecycle-events = off
      artery.advanced.flight-recorder {
        enabled=off
        destination=target/flight-recorder-${UUID.randomUUID().toString}.afr
      }
    }
    akka.loggers = ["akka.testkit.TestEventListener"]
    akka.test {
      single-expect-default = 5 s
    }
    """)
}
