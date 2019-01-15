package filodb.core.downsample

import scala.collection.JavaConverters._

import com.typesafe.config.{Config, ConfigFactory}

final case class DownsampleConfig(config: Config) {
  val enabled = config.hasPath("enabled") && config.getBoolean("enabled")
  val resolutions = if (enabled) config.getIntList("resolutions-ms").asScala.map(_.intValue()) else Seq.empty

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

