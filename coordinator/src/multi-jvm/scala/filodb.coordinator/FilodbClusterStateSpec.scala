package filodb.coordinator

import java.util.UUID

import akka.remote.testkit._
import com.typesafe.config.{Config, ConfigFactory}

class FilodbClusterStateSpecMultiJvmNode1 extends FilodbClusterStateSpec
class FilodbClusterStateSpecMultiJvmNode2 extends FilodbClusterStateSpec
class FilodbClusterStateSpecMultiJvmNode3 extends FilodbClusterStateSpec
class FilodbClusterStateSpecMultiJvmNode4 extends FilodbClusterStateSpec

abstract class FilodbClusterStateSpec
  extends MultiNodeSpec(FilodbClusterStateSpecMultiNodeConfig)
    with MultiNodeClusterBehavior {

  import FilodbClusterStateSpecMultiNodeConfig._

  "FilodbCluster" must {
    "initialize and join the cluster" in {
      awaitClusterUp(roles: _*)

      runOn(roles: _*) {
        within(defaultTimeout) {
          filodbCluster.isInitialized
          filodbCluster.isJoined
        }
      }
      enterBarrier("roles-up")
    }
    "have expected state to one node leaving" in {
      runOn(second) {
        within(defaultTimeout) {
          info(s"leaving on $myself")
          cluster leave myAddress
          awaitCond(!filodbCluster.isJoined, defaultTimeout)
          info(s"removed on $myself")
          awaitCond(cluster.isTerminated, defaultTimeout)
          info(s"cluster terminated on $myself")
        }
      }

      runOn(roles.filterNot(_ == second): _*) {
        awaitCond(!filodbCluster.state.members.exists(_.address == address(second)), defaultTimeout)
        awaitCond(filodbCluster.state.members.size == 3, defaultTimeout)
      }
    }
    "have expected state on one node downing" in {
      val victim = fourth
      val downing = address(victim)

      runOn(victim) {
        info(s"downing $myself")
        cluster down downing
        awaitCond(!filodbCluster.isJoined, defaultTimeout)
        info(s"removed on $myself")
        awaitCond(cluster.isTerminated, defaultTimeout)
        info(s"cluster terminated on $myself")
      }

      runOn(first, third) {
        awaitCond(filodbCluster.state.members.size == 2, defaultTimeout)
      }
      enterBarrier("finished")
    }
  }
}

object FilodbClusterStateSpecMultiNodeConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(clusterConfig
    .withFallback(debugConfig(on = false)
    .withFallback(ConfigFactory.load("application_test.conf"))))

  def clusterConfig: Config = ConfigFactory.parseString(
    s"""
       |akka.actor.provider = cluster
       |akka.actor.warn-about-java-serializer-usage = off
       |akka.cluster {
       |      jmx.enabled                         = off
       |      gossip-interval                     = 200 ms
       |      leader-actions-interval             = 200 ms
       |      unreachable-nodes-reaper-interval   = 500 ms
       |      periodic-tasks-initial-delay        = 300 ms
       |      publish-stats-interval              = 0 s # always, when it happens
       |      failure-detector.heartbeat-interval = 500 ms
       |}
       |akka.loglevel = INFO
       |akka.log-dead-letters = off
       |akka.log-dead-letters-during-shutdown = off
       |akka.remote {
       |  log-remote-lifecycle-events = off
       |  artery.advanced.flight-recorder {
       |    enabled=on
       |    destination=target/flight-recorder-${UUID.randomUUID().toString}.afr
       |  }
       |}
       |akka.test {
       |  single-expect-default = 5 s
       |}
    """.stripMargin)

  testTransport(on = true)
}