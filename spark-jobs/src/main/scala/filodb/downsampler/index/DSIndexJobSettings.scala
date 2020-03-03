package filodb.downsampler.index

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._

import filodb.downsampler.{DownsamplerLogger, DownsamplerSettings}

/**
  * DownsamplerSettings is always used in the context of an object so that it need not be serialized to a spark executor
  * from the spark application driver.
  */
class DSIndexJobSettings(settings: DownsamplerSettings) extends Serializable {

  @transient lazy val filodbSettings = settings.filodbSettings

  @transient lazy val filodbConfig = filodbSettings.config

  @transient lazy val dsIndexJobConfig = {
    val conf = filodbConfig.getConfig("ds-index-job")
    DownsamplerLogger.dsLogger.info(s"Loaded following downsampler config: ${conf.root().render()}" )
    conf
  }

  @transient lazy val batchSize = dsIndexJobConfig.getInt("cass-write-batch-size")

  @transient lazy val splitsPerNode = dsIndexJobConfig.getInt("splits-per-node")

  @transient lazy val cassWriteTimeout = dsIndexJobConfig.as[FiniteDuration]("cassandra-write-timeout")

  @transient lazy val migrateRawIndex = dsIndexJobConfig.getBoolean("migrate-full-index")

  // Longer lookback-time is needed to account for failures in the job runs.
  // As the updates need to be applied incrementally, migration needs to happen from the failed batch until the
  // latest hour. This is to ensure that subsequent mutations were not overwritten.
  @transient lazy val batchLookbackInHours = dsIndexJobConfig.as[Option[Long]]("batch-lookback-in-hours")
    .getOrElse(settings.downsampleStoreConfig.flushInterval.toHours)

  @transient lazy val numShards = filodbSettings.streamConfigs
    .find(_.getString("dataset") == settings.rawDatasetName)
    .getOrElse(ConfigFactory.empty())
    .as[Option[Int]]("num-shards").getOrElse(0)

  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60
}
