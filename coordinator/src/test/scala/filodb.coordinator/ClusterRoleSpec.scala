package filodb.coordinator

import filodb.core.AbstractSpec

class ClusterRoleSpec extends AbstractSpec {
  "ClusterRole" must {
    "have the expected roleName and systemName for ClusterRole.Cli" in {
      ClusterRole.Cli.roleName shouldEqual "worker"
      ClusterRole.Cli.systemName shouldEqual "filo-cli"
    }
    "have the expected roleName and systemName for ClusterRole.Server" in {
      ClusterRole.Server.roleName shouldEqual "worker"
      ClusterRole.Server.systemName shouldEqual "filo-standalone"
    }
    "have the expected roleName and systemName for ClusterRole.Driver" in {
      ClusterRole.Driver.roleName shouldEqual "driver"
      ClusterRole.Driver.systemName shouldEqual "filo-spark"
    }
    "have the expected roleName and systemName for ClusterRole.Executor" in {
      ClusterRole.Executor.roleName shouldEqual "executor"
      ClusterRole.Executor.systemName shouldEqual "filo-spark"
    }
  }
}
