package filodb.coordinator

import akka.japi.Util.immutableSeq

import filodb.core.AbstractSpec

trait ClusterNodeConfigurationSpec extends AbstractSpec with NodeRoleAwareConfiguration {

  def roles: Seq[String] = immutableSeq(systemConfig.getStringList("akka.cluster.roles"))

}

class ClusterNodeConfigurationServerSpec extends ClusterNodeConfigurationSpec {
  override val role = ClusterRole.Cli

  "ClusterNodeConfiguration" must {
    "have the expected config for ClusterRole.Cli" in {
      roles.size shouldEqual 1
      roles.forall(_ == "worker")
      systemConfig shouldEqual GlobalConfig.systemConfig
    }
  }
}
class ClusterNodeConfigurationCliSpec extends ClusterNodeConfigurationSpec {
  override val role = ClusterRole.Server

  "ClusterNodeConfiguration" must {
    "have the expected config for ClusterRole.Server" in {
      roles.size shouldEqual 1
      roles.forall(_ == "worker")
    }
  }
}
class ClusterNodeConfigurationDriverSpec extends ClusterNodeConfigurationSpec {
  override val role = ClusterRole.Driver

  "ClusterNodeConfiguration" must {
    "have the expected config for ClusterRole.Driver" in {
      roles.size shouldEqual 1
      roles.forall(_ == "driver")
    }
  }
}
class ClusterNodeConfigurationExecutorSpec extends ClusterNodeConfigurationSpec {
  override val role = ClusterRole.Executor

  "ClusterNodeConfiguration" must {
    "have the expected config for ClusterRole.Executor" in {
      roles.size shouldEqual 1
      roles.forall(_ == "executor")
    }
  }
}
