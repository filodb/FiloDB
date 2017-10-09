package filodb.akkabootstrapper

import scala.collection.immutable

import akka.actor.Address
import akka.cluster.Cluster

/**
  * This implementation of discovery allows clients to whitelist nodes that form the cluster seeds.
  * Essentially, this is just an adapter that allows for the simple implementation of cluster.joinSeedNodes(knownSeeds).
  *
  * Logs all the invalid entries versus just the first and failing on that.
  */
class WhitelistAkkaClusterSeedDiscovery(cluster: Cluster,
                                        settings: AkkaBootstrapperSettings
                                       ) extends AkkaClusterSeedDiscovery(cluster, settings) {

  /** Removes cluster self node unless it is in the head of the sorted list. */
  override protected lazy val discoverPeersForNewAkkaCluster: immutable.Seq[Address] = {
    val valid = settings.seeds._1.collect { case Right(address) => address }
    val headOpt = valid.headOption
    val selfAddress = cluster.selfAddress
    valid.filter(address => address != selfAddress || headOpt.contains(address))
  }
}
