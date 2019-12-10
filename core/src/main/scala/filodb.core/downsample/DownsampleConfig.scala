package filodb.core.downsample

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

final case class DownsampleConfig(config: Config) {
  val enabled = config.hasPath("enabled") && config.getBoolean("enabled")

  /**
    * Resolutions to downsample at
    */
  val resolutions = if (config.hasPath ("resolutions")) config.as[Seq[FiniteDuration]]("resolutions")
                    else Seq.empty

  /**
    * TTL for downsampled data for the resolutions in same order
    */
  val ttls = if (config.hasPath ("ttls")) config.as[Seq[FiniteDuration]]("ttls").map(_.toSeconds.toInt)
             else Seq.empty
  require(resolutions.length == ttls.length)

  /**
    * Schemas to downsample
    */
  val schemas = if (config.hasPath ("raw-schema-names")) config.as[Seq[String]]("raw-schema-names")
                else Seq.empty

  def makePublisher(): DownsamplePublisher = {
    if (!enabled) {
      NoOpDownsamplePublisher
    } else {
      val publisherClass = config.getString("publisher-class")
      val pub = Class.forName(publisherClass).getDeclaredConstructor(classOf[Config])
        .newInstance(config).asInstanceOf[DownsamplePublisher]
      pub
    }
  }
}

object DownsampleConfig {
  val disabled = DownsampleConfig(ConfigFactory.empty)
  def downsampleConfigFromSource(sourceConfig: Config): DownsampleConfig = {
    if (sourceConfig.hasPath("downsample")) DownsampleConfig(sourceConfig.getConfig("downsample"))
    else disabled
  }
}

