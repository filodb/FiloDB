package filodb.core

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import scala.jdk.CollectionConverters._

import filodb.core.metadata.DatasetOptions

/**
  * Loads the overall configuration in a specific order:
  * - System properties
  * - Config file in location specified by filodb.config.file
  * - filodb-defaults.conf (resource / in jar)
  * - cluster-reference.conf
  * - all other reference.conf's
  */
object GlobalConfig extends StrictLogging {
  val defaultsFromUrl = ConfigFactory.load("filodb-defaults")
  val defaultFiloConfig = defaultsFromUrl.getConfig("filodb")

  val unresolvedSystemConfig: Config = {
    ConfigFactory.invalidateCaches()

    val customConfig = sys.props.get("filodb.config.file").orElse(sys.props.get("config.file"))
                                .map { path => ConfigFactory.parseFile(new java.io.File(path)) }
                                .getOrElse(ConfigFactory.empty)
    // ConfigFactory.parseResources() does NOT work in Spark 1.4.1 executors
    // and only the below works.
    // filodb-defaults.conf sets cluster.roles=["worker"] as the default
    ConfigFactory.defaultOverrides.withFallback(customConfig) // spark overrides cluster.roles, cli doesn't
                 .withFallback(defaultsFromUrl)
                 .withFallback(ConfigFactory.defaultReference())
  }

  val systemConfig: Config = {
    unresolvedSystemConfig.resolve()
  }

  val configToDisableAkkaCluster = ConfigFactory.parseString(
    """
      |akka {
      |  extensions = []
      |  actor.provider = "remote"
      |}
      |""".stripMargin)

  // Workspaces which are disabled for using min/max columns for otel histograms
  val workspacesDisabledForMaxMin : Option[Set[String]] =
    systemConfig.hasPath("filodb.query.workspaces-disabled-max-min") match {
      case false => None
      case true => Some(systemConfig.getStringList("filodb.query.workspaces-disabled-max-min").asScala.toSet)
  }

  // Column filter used to check the enabling/disabling the use of max-min columns
  val maxMinTenantColumnFilter = systemConfig.getString("filodb.query.max-min-tenant-column-filter")

  // default dataset-options config
  val datasetOptions: Option[DatasetOptions] =
    systemConfig.hasPath("filodb.partition-schema.options") match {
      case false => None
      case true => {
        val datasetOptionsConfig = systemConfig.getConfig("filodb.partition-schema.options")
        Some(DatasetOptions.fromConfig(datasetOptionsConfig))
      }
    }

  val PromMetricLabel = "__name__"

  val hierarchicalConfig: Option[Config] = {
    systemConfig.hasPath("filodb.query.hierarchical") match {
      case false => None
      case true => Some(systemConfig.getConfig("filodb.query.hierarchical"))
    }
  }

  val indexRecoveryRecursiveOffsetMaxDepth: Option[Int] = {
    systemConfig.hasPath("filodb.index-recovery.recursive-offset-max-depth") match {
      case false => None
      case true => Some(systemConfig.getInt("filodb.index-recovery.recursive-offset-max-depth"))
    }
  }
}
