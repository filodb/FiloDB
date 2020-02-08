package filodb.downsampler

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.{FilodbSettings, NodeClusterActor}
import filodb.core.store.{IngestionConfig, StoreConfig}

/**
  * DownsamplerSettings is always used in the context of an object so that it need not be serialized to a spark executor
  * from the spark application driver.
  */
object DownsamplerSettings extends StrictLogging {

  val filodbSettings = new FilodbSettings(ConfigFactory.empty)

  val filodbConfig = filodbSettings.allConfig.getConfig("filodb")

  val downsamplerConfig = filodbConfig.getConfig("downsampler")
  logger.info(s"Loaded following downsampler config: ${downsamplerConfig.root().render()}" )

  val sessionProvider = downsamplerConfig.as[Option[String]]("cass-session-provider-fqcn")

  val cassandraConfig = filodbConfig.getConfig("cassandra")

  val rawDatasetName = downsamplerConfig.getString("raw-dataset-name")

  logger.info(s"Parsing dataset configs at ${filodbSettings.datasetConfPaths}")

  val rawDatasetIngestionConfig = filodbSettings.streamConfigs.map { config =>
        IngestionConfig(config, NodeClusterActor.noOpSource.streamFactoryClass).get
      }.find(_.ref.toString == rawDatasetName).get

  logger.info(s"DatasetConfig for dataset $rawDatasetName was $rawDatasetIngestionConfig")

  val rawSchemaNames = rawDatasetIngestionConfig.downsampleConfig.schemas

  val downsampleResolutions = rawDatasetIngestionConfig.downsampleConfig.resolutions

  val downsampleTtls = rawDatasetIngestionConfig.downsampleConfig.ttls.map(_.toSeconds.toInt)

  val downsampledDatasetRefs = rawDatasetIngestionConfig.downsampleConfig.downsampleDatasetRefs(rawDatasetName)

  val downsampleStoreConfig = StoreConfig(downsamplerConfig.getConfig("downsample-store-config"))

  val ttlByResolution = downsampleResolutions.zip(downsampleTtls).toMap

  val batchSize = downsamplerConfig.getInt("cass-write-batch-size")

  val batchTime = downsamplerConfig.as[FiniteDuration]("cass-write-batch-time")

  val splitsPerNode = downsamplerConfig.getInt("splits-per-node")

  val cassWriteTimeout = downsamplerConfig.as[FiniteDuration]("cassandra-write-timeout")

  val widenIngestionTimeRangeBy = downsamplerConfig.as[FiniteDuration]("widen-ingestion-time-range-by")

  val downsampleChunkDuration = downsampleStoreConfig.flushInterval.toMillis

  val whitelist = downsamplerConfig.as[Seq[Map[String, String]]]("whitelist-filters").map(_.toSeq)

  val blacklist = downsamplerConfig.as[Seq[Map[String, String]]]("blacklist-filters").map(_.toSeq)

  Kamon.loadReportersFromConfig() // register metric reporters

}

