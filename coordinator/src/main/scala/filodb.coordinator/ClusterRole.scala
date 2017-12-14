package filodb.coordinator

/** This is a FiloDB cluster role. Each role has its own lifecycle behavior.
  * Lifecycle-aware components use the role type to execute the appropriate
  * behavior for that role, as well as some Akka Cluster related settings.
  */
sealed trait ClusterRole {

  /** Sets the `akka.cluster.roles` config from the defined `filodb.coordinator.Role`. */
  def roleName: String = this match {
    case ClusterRole.Server   => "worker" // the default role in filodb-defaults.conf
    case ClusterRole.Cli      => "worker" // the default role in filodb-defaults.conf
    case ClusterRole.Driver   => "driver"
    case ClusterRole.Executor => "executor"
  }

  /** The ActorSystem name based on role. */
  def systemName: String = this match {
    case ClusterRole.Server => "filo-standalone"
    case ClusterRole.Cli    => "filo-cli"
    case _                  => "filo-spark"
  }
}

object ClusterRole {
  case object Server extends ClusterRole
  case object Driver extends ClusterRole
  case object Executor extends ClusterRole
  case object Cli extends ClusterRole
}
