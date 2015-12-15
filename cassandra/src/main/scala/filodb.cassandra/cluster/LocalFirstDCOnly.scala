package filodb.cassandra.cluster

import java.net.{InetAddress, NetworkInterface}
import java.util

import com.datastax.driver.core._
import com.datastax.driver.core.policies.LoadBalancingPolicy

import scala.collection.JavaConversions._
import scala.util.Random

class LocalFirstDCOnly(contactPoints: Set[InetAddress],
                       useDC: Option[String]=None) extends LoadBalancingPolicy {

  private var nodes = Set.empty[Host]
  private var clusterMetadata: Metadata = _
  private val random = new Random
  private var dcToUse = ""

  override def newQueryPlan(keyspace: String,
                            statement: Statement): util.Iterator[Host] =
    sortNodesByStatusAndProximity(nodes).iterator

  override def distance(host: Host): HostDistance =
    if (host.getDatacenter == dcToUse) {
      if (isLocalHost(host)) HostDistance.LOCAL else HostDistance.REMOTE
    } else {
      HostDistance.IGNORED
    }

  override def init(cluster: Cluster, hosts: util.Collection[Host]): Unit = {
    nodes = hosts.toSet
    clusterMetadata = cluster.getMetadata
    dcToUse = useDC.getOrElse(localDC(hosts.toSet))
  }


  override def onAdd(host: Host): Unit = {
    nodes -= host
    nodes += host
  }

  override def onRemove(host: Host): Unit = {
    nodes -= host
  }

  override def onUp(host: Host): Unit = {}

  override def onDown(host: Host): Unit = {}

  override def close(): Unit = {}


  private val localAddresses =
    NetworkInterface.getNetworkInterfaces.flatMap(_.getInetAddresses).toSet

  /** Returns true if given host is local host */
  private def isLocalHost(host: Host): Boolean = {
    val hostAddress = host.getAddress
    hostAddress.isLoopbackAddress || localAddresses.contains(hostAddress)
  }

  // scalastyle:off

  /** Finds the DCs of the contact points and returns hosts in those DC(s) from `allHosts`.
    * It guarantees to return at least the hosts pointed by `contactPoints`, even if their
    * DC information is missing. Other hosts with missing DC information are not considered. */
  private def nodesInTheSameDC(allHosts: Set[Host]): Set[Host] = {
    val contactNodes = allHosts.filter(h => contactPoints.contains(h.getAddress))
    val contactDCs = contactNodes.map(_.getDatacenter).filter(_ != null)
    contactNodes ++ allHosts.filter(h => contactDCs.contains(h.getDatacenter))
  }

  private def dcs(hosts: Set[Host]) =
    hosts.filter(_.getDatacenter != null).map(_.getDatacenter)

  // scalastyle:on

  /** Sorts nodes in the following order:
    * 1. live nodes in the same DC as `contactPoints` starting with localhost if up
    * 2. down nodes in the same DC as `contactPoints`
    *
    * Nodes within a group are ordered randomly.
    * Nodes from other DCs are not included. */
  private def sortNodesByStatusAndProximity(hostsToSort: Set[Host]): Seq[Host] = {
    val nodesInLocalDC = nodesInTheSameDC(hostsToSort)
    val (allUpHosts, downHosts) = nodesInLocalDC.partition(_.isUp)
    val (localHost, upHosts) = allUpHosts.partition(isLocalHost)
    localHost.toSeq ++ random.shuffle(upHosts.toSeq) ++ random.shuffle(downHosts.toSeq)
  }

  private def localDC(hosts: Set[Host]) = dcs(nodesInTheSameDC(hosts)).head


}
