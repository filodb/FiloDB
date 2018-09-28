package filodb.coordinator

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Scheduler => MonixScheduler, UncaughtExceptionReporter}

/**
  * Loads the overall configuration in a specific order:
  * - System properties
  * - Config file in location specified by filodb.config.file
  * - filodb-defaults.conf (resource / in jar)
  * - cluster-reference.conf
  * - all other reference.conf's
  */
object GlobalConfig extends StrictLogging {

  val ioPool = MonixScheduler.io(
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in GlobalConfig.ioPool", _)))

  val systemConfig: Config = {
    ConfigFactory.invalidateCaches()

    val customConfig = sys.props.get("filodb.config.file").orElse(sys.props.get("config.file"))
                                .map { path => ConfigFactory.parseFile(new java.io.File(path)) }
                                .getOrElse(ConfigFactory.empty)
    // ConfigFactory.parseResources() does NOT work in Spark 1.4.1 executors
    // and only the below works.
    // filodb-defaults.conf sets cluster.roles=["worker"] as the default
    val defaultsFromUrl = ConfigFactory.load("filodb-defaults")
    ConfigFactory.defaultOverrides.withFallback(customConfig) // spark overrides cluster.roles, cli doesn't
                 .withFallback(defaultsFromUrl)
                 .withFallback(ConfigFactory.defaultReference())
                 .resolve()
  }
}

/** Mixin used for nodes and tests. */
trait NodeConfiguration {

  /** The global Filo configuration. */
  val systemConfig: Config = GlobalConfig.systemConfig

}


