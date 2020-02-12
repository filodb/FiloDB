package filodb.downsampler.index

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.FilodbSettings
import filodb.downsampler.DownsamplerSettings
import filodb.downsampler.DownsamplerSettings.downsampleStoreConfig

/**
  * DownsamplerSettings is always used in the context of an object so that it need not be serialized to a spark executor
  * from the spark application driver.
  */
object DSIndexJobSettings extends StrictLogging {

  val filodbSettings = new FilodbSettings(ConfigFactory.empty)

  val filodbConfig = filodbSettings.config

  val dsIndexJobConfig = filodbConfig.getConfig("ds-index-job")
  logger.info(s"Loaded following downsampler config: ${dsIndexJobConfig.root().render()}" )

  val batchSize = dsIndexJobConfig.getInt("cass-write-batch-size")

  val splitsPerNode = dsIndexJobConfig.getInt("splits-per-node")

  val cassWriteTimeout = dsIndexJobConfig.as[FiniteDuration]("cassandra-write-timeout")

  // Longer lookback-time is needed to account for failures in the job runs.
  // As the updates need to be applied incrementally, this batching gives us the buffer
  // required to fix previous run failures.
  val batchLookbackInHours = dsIndexJobConfig.as[Option[Long]]("batch-lookback-in-hours")
                                .getOrElse(downsampleStoreConfig.flushInterval.toHours)

  val numShards = filodbSettings.streamConfigs
    .find(_.getString("dataset") == DownsamplerSettings.rawDatasetName)
    .headOption.getOrElse(ConfigFactory.empty())
    .as[Option[Int]]("num-shards").getOrElse(0)

  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60
}

