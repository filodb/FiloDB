package filodb.core.downsample

import scala.collection.JavaConverters._

import com.typesafe.config.{Config, ConfigFactory}

final case class DownsampleConfig(downsampleConfig: Config) {
  val enabled = downsampleConfig.hasPath("enabled") && downsampleConfig.getBoolean("enabled")
  val resolutions = if (enabled) downsampleConfig.getIntList("resolutions-ms").asScala.map(_.intValue()) else Seq.empty

  def makePublisher(): DownsamplePublisher = {
    if (!enabled) {
      NoOpPublisher
    } else {
      val publisherClass = downsampleConfig.getString("publisher-class")
      val pub = Class.forName(publisherClass).getDeclaredConstructor(classOf[Config])
        .newInstance(downsampleConfig).asInstanceOf[DownsamplePublisher]
      pub
    }
  }
}

object DownsampleConfig {
  val disabled = DownsampleConfig(ConfigFactory.empty)
}

