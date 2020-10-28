package filodb.core.downsample

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import filodb.core.DatasetRef

final case class DownsampleConfig(config: Config) {
  /**
    * Resolutions to downsample at
    */
  val resolutions = if (config.hasPath ("resolutions")) config.as[Seq[FiniteDuration]]("resolutions")
                    else Seq.empty

  /**
    * TTL for downsampled data for the resolutions in same order
    */
  val ttls = if (config.hasPath ("ttls")) config.as[Seq[FiniteDuration]]("ttls")
             else Seq.empty
  require(resolutions.length == ttls.length, "Downsample TTLs and resolutions are not same length")
  require(ttls.sorted == ttls, "Downsample TTLs are not sorted")
  require(resolutions.sorted == resolutions, "Downsample resolutions are not sorted")

  /**
    * Schemas to downsample
    */
  val schemas = if (config.hasPath ("raw-schema-names")) config.as[Seq[String]]("raw-schema-names")
                else Seq.empty

  def downsampleDatasetRefs(rawDataset: String): Seq[DatasetRef] = {
    resolutions.map { res =>
      DatasetRef(s"${rawDataset}_ds_${res.toMinutes}")
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

