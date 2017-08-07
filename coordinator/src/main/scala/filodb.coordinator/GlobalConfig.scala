package filodb.coordinator

import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.{Scheduler => MonixScheduler}

/**
  * Loads the overall configuration in a specific order:
  * - System properties
  * - Config file in location specified by filodb.config.file
  * - filodb-defaults.conf (resource / in jar)
  * - cluster-reference.conf
  * - all other reference.conf's
  */
object GlobalConfig {

  val ioPool = MonixScheduler.io()

  val systemConfig: Config = {
    ConfigFactory.invalidateCaches()

    val customConfig = sys.props.get("filodb.config.file").orElse(sys.props.get("config.file"))
                                .map { path => ConfigFactory.parseFile(new java.io.File(path)) }
                                .getOrElse(ConfigFactory.empty)
    // ConfigFactory.parseResources() does NOT work in Spark 1.4.1 executors
    // and only the below works.
    // filodb-defaults.conf sets cluster.roles=["worker"] as the default
    val defaultsFromUrl = ConfigFactory.parseURL(getClass.getResource("/filodb-defaults.conf"))
    val clusterFromUrl = ConfigFactory.parseURL(getClass.getResource("/cluster-reference.conf"))
    ConfigFactory.defaultOverrides.withFallback(customConfig) // spark overrides cluster.roles, cli doesn't
                 .withFallback(defaultsFromUrl)
                 .withFallback(clusterFromUrl)
                 .withFallback(ConfigFactory.defaultReference())
                 .resolve()
  }
}

/** Mixin used for nodes and tests. */
trait NodeConfiguration {

  /** The global Filo configuration. */
  val systemConfig: Config = GlobalConfig.systemConfig

}

/**
 * This is an optional method for Spark and other FiloDB apps to reuse the same config loading
 * mechanism provided by FiloDB, instead of using the default Typesafe config loading.  To use this
 * pass -Dkamon.config-provider=filodb.coordinator.KamonConfigProvider
 */
class KamonConfigProvider extends kamon.ConfigProvider {
  def config: Config = GlobalConfig.systemConfig
}
