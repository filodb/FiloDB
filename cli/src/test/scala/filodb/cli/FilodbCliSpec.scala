package filodb.cli

import filodb.coordinator.{ActorName, ClusterRole, RunnableSpec}

class FilodbCliSpec extends RunnableSpec {
  "A Filodb Cli" must {
    "initialize" in {
      eventually(CliMain.cluster.isInitialized)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      CliMain.role shouldEqual ClusterRole.Cli
      CliMain.system.name shouldEqual ClusterRole.Cli.systemName
      val coordinatorActor = CliMain.coordinatorActor
      coordinatorActor.path.name shouldEqual ActorName.CoordinatorName
    }
    "shutdown cleanly" in {
      CliMain.cluster.clusterActor.isEmpty shouldEqual true
      CliMain.shutdown()
      CliMain.cluster.clusterActor.isEmpty shouldEqual true
      eventually(CliMain.cluster.isTerminated)
    }
  }
}
