package filodb.akkabootstrapper

import java.net.MalformedURLException

import scala.collection.immutable

import akka.actor.{Address, AddressFromURIString}
import akka.cluster.{Cluster, Member}

/**
  * This implementation of discovery allows clients to whitelist nodes that form the
  * cluster seeds. Essentially, this is just an adapter that allows for the simple
  * implementation of `akka.cluster.Cluster.joinSeedNodes`.
  *
  * Collects invalid and valid seed nodes from configuration.
  * Allows the user to decide error handling if any invalid are found.
  */
class WhitelistClusterSeedDiscovery(cluster: Cluster, settings: AkkaBootstrapperSettings)
  extends ClusterSeedDiscovery(cluster, settings)
    with SeedValidator { self: ClusterSeedDiscovery =>

  /** Attempts to create addresses from a whitelist seed config. */
  private val validate = (s: String) =>
    try AddressFromURIString(s) catch { case e: MalformedURLException =>
      logger.error("MalformedURLException: invalid cluster seed node [{}]", s)
      s
    }

  private val validated = settings.seedsWhitelist.map(validate)

  /** Collects invalid seed nodes. */
  override val invalidSeedNodes: List[String] =
    validated collect { case a: String => a }

  /** Collects valid seed nodes. */
  override val validSeedNodes: List[Address] =
    validated collect { case a: Address => a }

  /** Removes cluster self node unless it is in the head of the sorted list.
    * First logs all invalid seeds first during validation, for full auditing,
    * in `filodb.akkabootstrapper.WhitelistSeedValidator.validate`.
    * Then raises exception to fail fast.
    */
  override protected lazy val discoverPeersForNewCluster: immutable.Seq[Address] = {

    if (invalidSeedNodes.nonEmpty) throw new MalformedURLException(
      s"Detected ${invalidSeedNodes.size} invalid 'whitelist' seed node configurations.")

    import Member.addressOrdering

    val headOpt = validSeedNodes.headOption
    val selfAddress = cluster.selfAddress
    validSeedNodes
      .filter(address => address != selfAddress || headOpt.contains(address))
      .sorted
  }
}
