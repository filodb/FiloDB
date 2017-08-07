package filodb.coordinator

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging

import filodb.core.store.MetaStore

/** Mixin used for nodes and tests creating an `ActorSystem`. */
trait NodeRoleAwareConfiguration extends NodeConfiguration {

  /** The FiloDB role, which is related to the Akka Cluster role. */
  def role: ClusterRole

  /** Sets the `akka.cluster.roles` config from the defined `filodb.coordinator.Role`. */
  def roleName: String = role match {
    case ClusterRole.Server   => "worker" // the default role in filodb-defaults.conf
    case ClusterRole.Cli      => "worker" // the default role in filodb-defaults.conf
    case ClusterRole.Driver   => "driver"
    case ClusterRole.Executor => "executor"
  }

  /** The ActorSystem name based on role. */
  protected def systemName: String = role match {
    case ClusterRole.Server => "filo-standalone"
    case ClusterRole.Cli    => "filo-cli"
    case _                  => "filo-spark"
  }
}

/** Mixin for easy usage of the FiloDBCluster Extension and startup.
  * Used by nodes starting an ActorSystem and the FiloDB Cluster.
  */
private[filodb] trait FilodbClusterNode extends NodeRoleAwareConfiguration with StrictLogging {

  /** For FiloDB cluster role `Server` or `CLI` the Akka Cluster role
    * uses the default "worker" hard-coded in filodb-defaults.conf
    * read into the config from `GlobalConfig.systemConfig`.
    *
    * For FiloDB cluster role `Driver` or `Executor` the Akka Cluster
    * role is either "driver" or "executor".
    */
  def role: ClusterRole

  /** The `ActorSystem` used to create the FilodbCluster Akka Extension. */
  def system: ActorSystem

  def cluster: FilodbCluster

  implicit lazy val ec = cluster.ec

  lazy val metaStore: MetaStore = cluster.metaStore

  lazy val coordinatorActor = cluster.coordinatorActor

  def shutdown(): Unit = cluster.shutdown()

}
