package filodb.coordinator

import scala.concurrent.Await

import akka.actor.ActorRef

class FilodbClusterSpec extends AkkaSpec {

  "FilodbCluster" must {
    val cluster = FilodbCluster(system)
    import cluster.settings._

    "have the expected start state"  in {
      cluster.isInitialized should be (false)
      cluster.isJoined should be (false)
      cluster.isTerminated should be (false)
      cluster.state.members.size shouldEqual 0
    }
    "load and setup basic components successfully" in {
      val tracer = cluster.kamonInit(ClusterRole.Server)
      tracer.path.name should be (ActorName.TraceLoggerName)

      cluster.coordinatorActor.path.name should be (ActorName.CoordinatorName)
      Await.result(cluster.metaStore.initialize(), InitializationTimeout)
      cluster.join()
      awaitCond(cluster.isJoined, max = cluster.settings.DefaultTaskTimeout)
      cluster.isTerminated should be (false)
    }
    "provide CurrentClusterState" in {
      cluster.state.leader.isDefined should be (true)
      cluster.state.members.size should be (1)
    }
    "setup cluster singleton and manager successfully" in {
      import ActorName._

      def pathElementsExist(a: ActorRef): Boolean =
        Set("user", NodeGuardianName, NodeClusterProxyName)
          .forall(v => a.path.elements.exists(_ == v))

      // both operations (they differ slightly) should return the same final actor
      val clusterActor1 = cluster.clusterSingletonProxy("worker", withManager = true)
      pathElementsExist(clusterActor1) should be (true)
      val clusterActor2 = cluster.clusterSingletonProxy("worker", withManager = false)
      pathElementsExist(clusterActor2) should be (true)
      clusterActor1 shouldEqual clusterActor2
    }
    "shutdown cleanly" in {
      cluster.shutdown()
      awaitCond(cluster.isTerminated, GracefulStopTimeout)
    }
  }
}
