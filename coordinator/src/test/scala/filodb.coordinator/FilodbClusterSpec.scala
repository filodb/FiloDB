package filodb.coordinator

import akka.actor.ActorRef

class FilodbClusterSpec extends AkkaSpec {
  "FilodbCluster" must {
    val cluster = FilodbCluster(system)

    "have the expected start state"  in {
      cluster.isInitialized shouldEqual false
      cluster.isJoined shouldEqual false
      cluster.isTerminated shouldEqual false
    }
    "load and setup basic components successfully and reach expected 'start' state" in {
      cluster.coordinatorActor.path.name should be (ActorName.CoordinatorName)
      awaitCond(cluster.metaStore.initialize().isCompleted, cluster.settings.InitializationTimeout)
      cluster.join()
      awaitCond(cluster.isJoined, max = cluster.settings.DefaultTaskTimeout)
    }
    "setup cluster singleton and manager successfully" in {
      import ActorName._

      def pathElementsExist(a: ActorRef): Boolean =
        Set("user", NodeGuardianName, ClusterSingletonProxyName)
          .forall(v => a.path.elements.exists(_ == v))

      val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)
      cluster.isInitialized shouldEqual true
      cluster.clusterActor.isDefined shouldEqual true
      pathElementsExist(clusterActor) should be (true)
    }
  }
}
