package filodb.akkabootstrapper

import scala.annotation.tailrec
import scala.collection.immutable.Seq

import akka.actor.{Address, ExtendedActorSystem}
import akka.cluster.Cluster
import com.typesafe.scalalogging.StrictLogging
import org.xbill.DNS._

abstract class DnsSrvAkkaClusterSeedDiscovery(override val cluster: Cluster,
                                              override val settings: AkkaBootstrapperSettings)
  extends AkkaClusterSeedDiscovery(cluster, settings) {

  val srvLookup = new Lookup(settings.serviceName, Type.SRV, DClass.IN)
  val selfAddress = cluster.selfAddress
  val simpleResolver = settings.resolverHost.map(r => new SimpleResolver(r))
  simpleResolver.foreach { _.setPort(settings.resolverPort) }
  simpleResolver.foreach { r => srvLookup.setResolver(r) }

  override protected def discoverPeersForNewAkkaCluster: Seq[Address] = {
    logger.info("Discovering cluster peers by looking up SRV records from DNS for {}", settings.serviceName)
    val startTime = System.currentTimeMillis()
    discover(startTime)
  }

  @tailrec
  private def discover(startTime: Long): Seq[Address] = {
    val currentTime = System.currentTimeMillis()
    if (currentTime - startTime > settings.seedsDiscoveryTimeout) {
      throw DiscoveryTimeoutException(s"Could not discover seeds within ${settings.seedsDiscoveryTimeout} ms")
    }

    // Lookup the SRV records for the current application's serviceName from DNS
    val recordsNullable = srvLookup.run()
    val srvRecords = Option(recordsNullable).getOrElse(Array())
    if (srvLookup.getResult != Lookup.SUCCESSFUL || srvRecords.isEmpty) {
      logger.info("This application is not yet registered with DNS. Could not find SRV records for {}. Waiting ...",
        settings.serviceName)
      Thread.sleep(settings.srvPollInterval)
      discover(startTime)
    } else if (srvRecords.length == settings.seedNodeCount) {
      // Look up the A record for each SRV record result
      val peers = srvRecords.asInstanceOf[Array[Record]].map { r =>
        val srvRecord = r.asInstanceOf[SRVRecord]
        val ipAddress = lookupARecord(srvRecord.getTarget.toString)
        val port = srvRecord.getPort
        Address(cluster.selfAddress.protocol, cluster.system.name, ipAddress, port)
      }.sortBy(a => a.toString)

      val headOpt = peers.headOption
      val selfAddress = cluster.selfAddress
      peers.filter(address => address != selfAddress || headOpt.contains(address)).toList
    } else if (srvRecords.length > settings.seedNodeCount) {
      throw new IllegalStateException(
        s"Should not have found more nodes than ${settings.seedNodeCount}. This is unexpected. Check your " +
          s"seedNodeCount configuration.")
    } else {
      logger.info("Found {} nodes in the cluster. Waiting for {} quorum nodes to start",
        srvRecords.length.toString, settings.seedNodeCount.toString)
      Thread.sleep(settings.srvPollInterval)
      discover(startTime)
    }
  }

  private  def lookupARecord(host: String): String = {
    val dnsALookup = new Lookup(host, Type.A, DClass.IN)
    simpleResolver.foreach { r => dnsALookup.setResolver(r) }
    val recordsNullable = dnsALookup.run()
    val dnsARecords = Option(recordsNullable).getOrElse(Array())
    if (dnsALookup.getResult != Lookup.SUCCESSFUL || dnsARecords.isEmpty) {
      throw new IllegalStateException(s"No A record found! for $host")
    } else {
      dnsARecords.head.asInstanceOf[ARecord].getAddress.getHostAddress
    }
  }

}


/**
  * This concrete implementation assumes that registration and de-registration with DNS is not required.
  * And it is done automatically by the deployment environment, for example Mesos DNS.
  */
final class SimpleDnsSrvAkkaClusterSeedDiscovery(cluster: Cluster,
                                                 settings: AkkaBootstrapperSettings)
  extends DnsSrvAkkaClusterSeedDiscovery(cluster, settings)

/**
  * This concrete implementation is used for local development. It does a registration
  * and de-registration with Consul DNS.
  */
final class ConsulAkkaClusterSeedDiscovery(cluster: Cluster,
                                           settings: AkkaBootstrapperSettings)
  extends DnsSrvAkkaClusterSeedDiscovery(cluster, settings) with StrictLogging {

  private val defaultAddress = cluster.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
  val port = defaultAddress.port.get
  val host = defaultAddress.host.get

  val registrationServiceName: String = settings.registrationServiceName
  val serviceId = s"$registrationServiceName-$host-$port"
  val consulClient = new ConsulClient(settings)
  consulClient.register(serviceId, registrationServiceName, host, port)
  logger.info(s"Registered with consul $host:$port as $registrationServiceName ")

  cluster.system.registerOnTermination {
    consulClient.deregister(serviceId)
    logger.info(s"Deregistered $serviceId with consul")
  }

}

