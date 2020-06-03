package filodb.downsampler.chunk

import scala.concurrent.duration._

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.{FilodbSettings, NodeClusterActor}
import filodb.core.store.{IngestionConfig, StoreConfig}
import filodb.downsampler.DownsamplerContext


/**
  * DownsamplerSettings is always used in the context of an object so that it need not be serialized to a spark executor
  * from the spark application driver.
  */
class DownsamplerSettings(conf: Config = ConfigFactory.empty()) extends Serializable {

  @transient lazy val filodbSettings = new FilodbSettings(conf)

  @transient lazy val filodbConfig = filodbSettings.allConfig.getConfig("filodb")

  @transient lazy val downsamplerConfig = {
    val conf = filodbConfig.getConfig("downsampler")
    DownsamplerContext.dsLogger.info(s"Loaded following downsampler config: ${conf.root().render()}" )
    conf
  }

  @transient lazy val cassandraConfig = filodbConfig.getConfig("cassandra")

  @transient lazy val rawDatasetName = downsamplerConfig.getString("raw-dataset-name")

  @transient lazy val rawDatasetIngestionConfig = {
    DownsamplerContext.dsLogger.info(s"Parsing dataset configs at ${filodbSettings.datasetConfPaths}")
    val ingConf = filodbSettings.streamConfigs.map { config =>
      IngestionConfig(config, NodeClusterActor.noOpSource.streamFactoryClass).get
    }.find(_.ref.toString == rawDatasetName).get
    DownsamplerContext.dsLogger.info(s"DatasetConfig for dataset $rawDatasetName was $ingConf")
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

  @transient lazy val trace = downsamplerConfig.as[Seq[Map[String, String]]]("trace-filters").map(_.toSeq)

  /**
    * Two conditions should satisfy for eligibility:
    * (a) If whitelist is nonEmpty partKey should match a filter in the whitelist.
    * (b) It should not match any filter in blacklist
    */
  def isEligibleForDownsample(pkPairs: Seq[(String, String)]): Boolean = {
    if (whitelist.nonEmpty && !whitelist.exists(w => w.forall(pkPairs.contains))) {
      false
    } else {
      blacklist.forall(w => !w.forall(pkPairs.contains))
    }
  }

  def shouldTrace(pkPairs: Seq[(String, String)]): Boolean = {
    trace.exists(w => w.forall(pkPairs.contains))
  }

}

