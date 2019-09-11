package filodb.downsampler

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._

import filodb.core.store.StoreConfig

object DownsamplerSettings {

  val filodbConfig = ConfigFactory.load().getConfig("filodb")

  val downsamplerConfig = filodbConfig.getConfig("downsampler")

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
  val cassWriteTimeout = 10.minutes

  val now = downsamplerConfig.as[Option[Long]]("downsampler-execution-time").getOrElse(System.currentTimeMillis())
  val chunkDuration = downsampleStoreConfig.flushInterval.toMillis
  val userTimeEnd = (now / chunkDuration) * chunkDuration

  val userTimeStart = userTimeEnd - chunkDuration
  val ingestionTimeStart = userTimeStart - 2.hours.toMillis
  val ingestionTimeEnd = userTimeEnd + 2.hours.toMillis

}
