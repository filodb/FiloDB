package filodb.akkabootstrapper

import akka.actor.{Address, AddressFromURIString}
import akka.cluster.Cluster
import com.typesafe.scalalogging.StrictLogging
import spray.json._

import scala.collection.immutable.Seq
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scalaj.http.Http

abstract class AkkaClusterSeedDiscovery(val cluster: Cluster, val settings: AkkaBootstrapperSettings)
  extends StrictLogging with ClusterMembershipJsonSuppport {

  @throws(classOf[DiscoveryTimeoutException])
  def discoverAkkaClusterSeeds: Seq[Address] = {
    discoverExistingAkkaCluster match {
      case Seq() => discoverPeersForNewAkkaCluster
      case nonEmpty: Seq[Address] => nonEmpty
    }
  }

  @throws(classOf[DiscoveryTimeoutException])
  protected def discoverPeersForNewAkkaCluster: Seq[Address]

  protected def discoverExistingAkkaCluster: Seq[Address] = {

    val seedsEndpoint = settings.seedsBaseUrl + settings.seedsPath

    logger.info("Checking seeds endpoint {} to see if cluster already exists", seedsEndpoint)

    try {

      val response = Http(seedsEndpoint).timeout(1000, 1000).asString
      if (!response.is2xx) {
        logger.info("Seeds endpoint returned a {}. Assuming cluster does not exist. Response body was {}",
          response.code.toString, response.body)
        Seq.empty[Address]
      } else {
        val membersResponse = response.body.parseJson.convertTo[ClusterMembershipHttpResponse]
        logger.info("Cluster exists. Response: {}", membersResponse)
        membersResponse.members.sorted.map(a => AddressFromURIString.parse(a))
      }
    } catch {
      case NonFatal(e) =>
        logger.info("Seeds endpoint {} did not return the seeds. Cluster does not exist.", seedsEndpoint)
        Seq.empty[Address]
    }
  }
}


object AkkaClusterSeedDiscovery {

  def apply(cluster: Cluster, settings: AkkaBootstrapperSettings): AkkaClusterSeedDiscovery = {
    val className = settings.seedDiscoveryClass
    val args: Seq[(Class[_], AnyRef)] = Seq((cluster.getClass, cluster), (settings.getClass, settings))
    cluster.system.dynamicAccess.createInstanceFor[AkkaClusterSeedDiscovery](className, args) match {
      case Failure(e) =>
        throw new IllegalArgumentException(s"Could not instantiate seed discovery class " +
          s"${settings.seedDiscoveryClass}. Please check your configuration", e)
      case Success(clazz) => clazz
    }
  }

}
