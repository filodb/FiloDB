package filodb.core.downsample

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

final case class DownsampleConfig(config: Config) {
  val enabled = config.hasPath("enabled") && config.getBoolean("enabled")
  val resolutions = if (enabled) config.as[Seq[Int]]("resolutions-ms") else Seq.empty
  val retentions = if (enabled) config.as

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

