package filodb.downsampler

import scala.concurrent.duration._

import com.typesafe.config.{ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.{FilodbSettings, NodeClusterActor}
import filodb.core.store.{IngestionConfig, StoreConfig}

/**
  * DownsamplerSettings is always used in the context of an object so that it need not be serialized to a spark executor
  * from the spark application driver.
  */
case class DownsamplerSettings(confAsString: String = "") extends StrictLogging {

  Kamon.init()

  @transient lazy val conf = ConfigFactory.parseString(confAsString)

  @transient lazy val filodbSettings = new FilodbSettings(conf)

  @transient lazy val filodbConfig = filodbSettings.allConfig.getConfig("filodb")

  @transient lazy val downsamplerConfig = {
    val conf = filodbConfig.getConfig("downsampler")
    logger.info(s"Loaded following downsampler config: ${conf.root().render()}" )
    conf
  }

  @transient lazy val cassandraConfig = filodbConfig.getConfig("cassandra")

  @transient lazy val rawDatasetName = downsamplerConfig.getString("raw-dataset-name")

  @transient lazy val rawDatasetIngestionConfig = {
    logger.info(s"Parsing dataset configs at ${filodbSettings.datasetConfPaths}")
    val ingConf = filodbSettings.streamConfigs.map { config =>
      IngestionConfig(config, NodeClusterActor.noOpSource.streamFactoryClass).get
    }.find(_.ref.toString == rawDatasetName).get
    logger.info(s"DatasetConfig for dataset $rawDatasetName was $ingConf")
    ingConf
  }

  @transient lazy val rawSchemaNames = rawDatasetIngestionConfig.downsampleConfig.schemas

  @transient lazy val downsampleResolutions = rawDatasetIngestionConfig.downsampleConfig.resolutions

  @transient lazy val downsampleTtls = rawDatasetIngestionConfig.downsampleConfig.ttls.map(_.toSeconds.toInt)

  @transient lazy val downsampledDatasetRefs =
    rawDatasetIngestionConfig.downsampleConfig.downsampleDatasetRefs(rawDatasetName)

  @transient lazy val downsampleStoreConfig = StoreConfig(downsamplerConfig.getConfig("downsample-store-config"))

  @transient lazy val ttlByResolution = downsampleResolutions.zip(downsampleTtls).toMap

  @transient lazy val batchSize = downsamplerConfig.getInt("cass-write-batch-size")

  @transient lazy val batchTime = downsamplerConfig.as[FiniteDuration]("cass-write-batch-time")

  @transient lazy val splitsPerNode = downsamplerConfig.getInt("splits-per-node")

  @transient lazy val cassWriteTimeout = downsamplerConfig.as[FiniteDuration]("cassandra-write-timeout")

  @transient lazy val widenIngestionTimeRangeBy = downsamplerConfig.as[FiniteDuration]("widen-ingestion-time-range-by")

  @transient lazy val downsampleChunkDuration = downsampleStoreConfig.flushInterval.toMillis

  @transient lazy val whitelist = downsamplerConfig.as[Seq[Map[String, String]]]("whitelist-filters").map(_.toSeq)

  @transient lazy val blacklist = downsamplerConfig.as[Seq[Map[String, String]]]("blacklist-filters").map(_.toSeq)

}

