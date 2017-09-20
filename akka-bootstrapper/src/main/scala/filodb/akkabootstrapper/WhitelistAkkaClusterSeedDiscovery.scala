package filodb.akkabootstrapper

import akka.actor.{Address, AddressFromURIString}
import akka.cluster.Cluster
import scala.collection.immutable.Seq

/**
  * This implementation of discovery allows clients to whitelist nodes that form the cluster seeds.
  * Essentially, this is just an adapter that allows for the simple implementation of cluster.joinSeedNodes(knownSeeds)
  */
class WhitelistAkkaClusterSeedDiscovery(cluster: Cluster,
                                        settings: AkkaBootstrapperSettings)
  extends AkkaClusterSeedDiscovery(cluster, settings) {

  override protected def discoverPeersForNewAkkaCluster: Seq[Address] = {

    val seedsWhitelist = settings.seedsWhitelist
    val selfAddress = cluster.selfAddress

    // remove self node unless it is in the head of the sorted list.
    seedsWhitelist.filter(address => address != selfAddress || address == seedsWhitelist.head)
      .map(AddressFromURIString.apply)
  }

}