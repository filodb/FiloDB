package filodb.coordinator

/** This is a FiloDB cluster role. Each role has its own lifecycle behavior.
  * Lifecycle-aware components use the role type to execute the appropriate
  * behavior for that role, as well as some Akka Cluster related settings.
  */
sealed trait ClusterRole
object ClusterRole {
  case object Server extends ClusterRole
  case object Driver extends ClusterRole
  case object Executor extends ClusterRole
  case object Cli extends ClusterRole
}
