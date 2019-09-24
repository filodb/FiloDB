package filodb.downsampler

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.{FilodbSettings}
import filodb.core.store.StoreConfig

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

  val rawSchemaNames = downsamplerConfig.as[Seq[String]]("raw-schema-names")

  val downsampleResolutions = downsamplerConfig.as[Array[FiniteDuration]]("resolutions")

  val downsampleTtls = downsamplerConfig.as[Array[FiniteDuration]]("ttls").map(_.toSeconds.toInt)
  require(downsampleResolutions.length == downsampleTtls.length)

  val downsampleStoreConfig = StoreConfig(downsamplerConfig.getConfig("downsample-store-config"))

  val ttlByResolution = downsampleResolutions.zip(downsampleTtls).toMap

  val batchSize = downsamplerConfig.getInt("num-partitions-per-cass-write")

  val blockMemorySize = downsamplerConfig.getMemorySize("off-heap-block-memory-size").toBytes

  val nativeMemManagerSize = downsamplerConfig.getMemorySize("off-heap-native-memory-size").toBytes

  val cassWriteTimeout = downsamplerConfig.as[FiniteDuration]("cassandra-write-timeout")

  val widenIngestionTimeRangeBy = downsamplerConfig.as[FiniteDuration]("widen-ingestion-time-range-by")

  val chunkDuration = downsampleStoreConfig.flushInterval.toMillis

}

