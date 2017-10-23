package filodb.akkabootstrapper

import akka.actor.Address
import akka.cluster.Cluster

/** Collects invalid and valid seed nodes from configuration.
  * Allows the user to decide error handling if any invalid are found.
  *
  * Can be extended for further validation behavior.
  */
trait SeedValidator {
  self: ClusterSeedDiscovery =>

  def cluster: Cluster

  def settings: AkkaBootstrapperSettings

  /** Collects invalid seed nodes. */
  def invalidSeedNodes: List[String]

  /** Collects valid seed nodes. */
  def validSeedNodes: List[Address]

}