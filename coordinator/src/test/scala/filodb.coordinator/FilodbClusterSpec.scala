package filodb.coordinator

import scala.concurrent.Await

import akka.actor.ActorRef

class FilodbClusterSpec extends AkkaSpec {

  "FilodbCluster" must {
    val cluster = FilodbCluster(system)

    "have the expected start state"  in {
      cluster.isInitialized should be (false)
      cluster.isJoined should be (false)
      cluster.isTerminated should be (false)
    }
    "load and setup basic components successfully" in {
      val tracer = cluster.kamonInit(ClusterRole.Server)
      tracer.path.name should be (ActorName.TraceLoggerName)

      cluster.coordinatorActor.path.name should be (ActorName.CoordinatorName)
      Await.result(cluster.metaStore.initialize(), cluster.settings.InitializationTimeout)
      cluster.join()
      awaitCond(cluster.isJoined, max = cluster.settings.DefaultTaskTimeout)
      cluster.isTerminated should be (false)
    }
    "setup cluster singleton and manager successfully" in {
      import ActorName._

      def pathElementsExist(a: ActorRef): Boolean =
        Set("user", NodeGuardianName, ClusterSingletonProxyName)
          .forall(v => a.path.elements.exists(_ == v))

      // both operations (they differ slightly) should return the same final actor
      val clusterActor1 = cluster.clusterSingleton("worker", withManager = true)
      pathElementsExist(clusterActor1) should be (true)
      val clusterActor2 = cluster.clusterSingleton("worker", withManager = false)
      pathElementsExist(clusterActor2) should be (true)
      clusterActor1 shouldEqual clusterActor2
    }
    "shutdown cleanly" in {
      cluster.shutdown()
      awaitCond(cluster.isTerminated, cluster.settings.GracefulStopTimeout)
    }
  }
}