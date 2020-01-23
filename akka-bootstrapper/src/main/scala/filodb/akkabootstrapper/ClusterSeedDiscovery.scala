package filodb.akkabootstrapper

import scala.collection.immutable.Seq
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import akka.actor.{Address, AddressFromURIString}
import akka.cluster.Cluster
import com.typesafe.scalalogging.StrictLogging
import scalaj.http.{Http, HttpResponse}

/** Seed node strategy. Some implementations discover, some simply read from immutable config. */
abstract class ClusterSeedDiscovery(val cluster: Cluster,
                                    val settings: AkkaBootstrapperSettings) extends StrictLogging {
  import io.circe.parser.decode
  import io.circe.generic.auto._

  @throws(classOf[DiscoveryTimeoutException])
  def discoverClusterSeeds: Seq[Address] = {
    discoverExistingCluster match {
      case Seq() => discoverPeersForNewCluster
      case nonEmpty: Seq[Address] => nonEmpty
    }
  }

  @throws(classOf[DiscoveryTimeoutException])
  //scalastyle:off null
  protected def discoverPeersForNewCluster: Seq[Address]

  protected def discoverExistingCluster: Seq[Address] = {

    val seedsEndpoint = settings.seedsBaseUrl + settings.seedsPath
    var response: HttpResponse[String] = null
    var retriesRemaining = settings.seedsHttpRetries
    do {
      try {
        logger.info(s"Trying to fetch seeds from $seedsEndpoint ... $retriesRemaining retries remaining.")
        response = Http(seedsEndpoint).timeout(2000, 2000).asString
        logger.info(s"Seeds endpoint returned a ${response.code}. Response body was ${response.body}")
      } catch {
        case NonFatal(e) => {
          if (e.isInstanceOf[java.net.ConnectException]) {
            // Don't bother logging the full the trace for something which is expected.
            e.setStackTrace(new Array[StackTraceElement](0))
          }
          logger.info(s"Seeds endpoint $seedsEndpoint failed. This is expected on cluster bootstrap", e)
        }
      }
      retriesRemaining -= 1
      if (retriesRemaining > 0) Thread.sleep(settings.seedsHttpSleepBetweenRetries.toMillis)
    } while ((response == null || !response.is2xx) && retriesRemaining > 0)

    if (response == null || !response.is2xx) {
      logger.info(s"Giving up on discovering seeds after ${settings.seedsHttpRetries} retries. " +
        s"Assuming cluster does not exist. ")
      Seq.empty[Address]
    } else {
      decode[ClusterMembershipHttpResponse](response.body) match {
        case Right(membersResponse) =>
          logger.info("Cluster exists. Response: {}", membersResponse)
          membersResponse.members.sorted.map(a => AddressFromURIString.parse(a))
        case Left(ex) =>
          logger.error(s"Exception parsing JSON response ${response.body}, returning empty seeds", ex)
          Seq.empty[Address]
      }
    }
  }
}

object ClusterSeedDiscovery extends StrictLogging {
  /** Seed node strategy. Some implementations discover, some simply read them. */
  def apply(cluster: Cluster, settings: AkkaBootstrapperSettings): ClusterSeedDiscovery = {
    import settings.{seedDiscoveryClass => fqcn}

    logger.info(s"Using $fqcn strategy to discover cluster seeds")
    cluster.system.dynamicAccess.createInstanceFor[ClusterSeedDiscovery](
      fqcn, Seq((cluster.getClass, cluster), (settings.getClass, settings))) match {
        case Failure(e) =>
          throw new IllegalArgumentException(
            s"Could not instantiate seed discovery class $fqcn. Please check your configuration", e)
        case Success(clazz) => clazz
      }
  }
}
