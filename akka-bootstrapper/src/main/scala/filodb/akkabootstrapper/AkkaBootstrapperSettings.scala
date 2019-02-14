package filodb.akkabootstrapper

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.Try

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

final class AkkaBootstrapperSettings(val config: Config) extends StrictLogging {

  private val bootstrapper: Config = config.getConfig("akka-bootstrapper")

  if (bootstrapper.getBoolean("log-config-debug")) {
    logger.debug("Loaded the following akka-bootstrapper config: \n: {}",
      bootstrapper.root().render())
    logger.debug("Loaded the following akka.remote.netty.tcp config: \n: {}",
      config.getConfig("akka.remote.netty.tcp").root().render())
  }

  val seedDiscoveryClass = bootstrapper.getString("seed-discovery.class")
  val seedsDiscoveryTimeout = bootstrapper.getDuration("seed-discovery.timeout", TimeUnit.MILLISECONDS)

  val seedsBaseUrl = bootstrapper.getString("http-seeds.base-url")
  val seedsPath = bootstrapper.getString("http-seeds.path")
  val seedsHttpRetries = bootstrapper.getInt("http-seeds.retries")
  val seedsHttpSleepBetweenRetries = bootstrapper.getDuration("http-seeds.sleep-between-retries")

  // used by simple dns srv and consul
  lazy val seedNodeCount: Integer = bootstrapper.getInt("dns-srv.seed-node-count")
  lazy val srvPollInterval = bootstrapper.getDuration("dns-srv.poll-interval", TimeUnit.MILLISECONDS)
  lazy val serviceName: String = bootstrapper.getString("dns-srv.service-name")
  lazy val resolverHost: Option[String] = Try(bootstrapper.getString("dns-srv.resolver-host")).toOption
  lazy val resolverPort: Int = bootstrapper.getInt("dns-srv.resolver-port")

  // used by consul discovery
  lazy val consulApiHost: String = bootstrapper.getString("consul.api-host")
  lazy val consulApiPort: Int = bootstrapper.getInt("consul.api-port")
  lazy val registrationServiceName: String = bootstrapper.getString("consul.registration-service-name")

  lazy val seedsWhitelist: List[String] = bootstrapper.getStringList("whitelist.seeds").asScala.toList

}
