package filodb.repair

import java.io.File
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration.FiniteDuration

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, GlobalConfig}
import filodb.core.metadata.Schemas
import filodb.core.store.{PartKeyRecord, ScanSplit}
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils

class PartitionKeysCopier(conf: SparkConf) {

  // Get filo config from spark conf or file.
  private def getFiloConfig(valueConf: String, filePathConf: String) = {
    val configString = conf.get(valueConf, "")
    val config = if (!configString.isBlank) {
      ConfigFactory.parseString(configString).resolve()
    } else {
      ConfigFactory.parseFile(new File(conf.get(filePathConf)))
        .withFallback(GlobalConfig.systemConfig)
        .resolve()
    }
    config
  }

  def datasetConfig(mainConfig: Config, datasetName: String): Config = {
    val sourceConfigPaths = mainConfig.getConfigList("inline-dataset-configs")
    sourceConfigPaths.stream()
      .filter(new util.function.Predicate[Config] {
        override def test(conf: Config): Boolean = conf.getString("dataset").equals(datasetName)
      })
      .findFirst()
      .orElseThrow()
  }

  // Examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
  private def parseDateTime(str: String) = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str))

  // Both "source" and "target" refer to file paths which define config files that have a
  // top-level "filodb" section and a "cassandra" subsection.
  private val rawSourceConfig = getFiloConfig(
    "spark.filodb.partitionkeys.copier.source.config.value",
    "spark.filodb.partitionkeys.copier.source.config.file"
  )
  private val rawTargetConfig = getFiloConfig(
    "spark.filodb.partitionkeys.copier.target.config.value",
    "spark.filodb.partitionkeys.copier.target.config.file"
  )
  private val sourceConfig = rawSourceConfig.getConfig("filodb")
  private val targetConfig = rawTargetConfig.getConfig("filodb")
  private val sourceCassConfig = sourceConfig.getConfig("cassandra")
  private val targetCassConfig = targetConfig.getConfig("cassandra")
  private val datasetName = conf.get("spark.filodb.partitionkeys.copier.dataset")
  private val sourceDatasetConfig = datasetConfig(sourceConfig, datasetName)
  private val targetDatasetConfig = datasetConfig(targetConfig, datasetName)
  private val sourceSession = FiloSessionProvider.openSession(sourceCassConfig)
  private val targetSession = FiloSessionProvider.openSession(targetCassConfig)

  val schemas = Schemas.fromConfig(sourceConfig).get
  private[repair] def partKeyHashFn = (partKey: PartKeyRecord) =>
    Option(schemas.part.binSchema.partitionHash(partKey.partKey, UnsafeUtils.arayOffset))

  val numOfShards: Int = sourceDatasetConfig.getInt("num-shards")
  private val isDownsampleCopy = conf.getBoolean("spark.filodb.partitionkeys.copier.is.downsample.copy", false)
  private val copyStartTime = parseDateTime(conf.get("spark.filodb.partitionkeys.copier.start.time"))
  private val copyEndTime = parseDateTime(conf.get("spark.filodb.partitionkeys.copier.end.time"))

  private val dsSettings = new DownsamplerSettings(rawSourceConfig)
  private val highestDSResolution = dsSettings.rawDatasetIngestionConfig.downsampleConfig.resolutions.last
  private val diskTimeToLiveSeconds = if (isDownsampleCopy) {
    dsSettings.ttlByResolution(highestDSResolution)
  } else {
    targetDatasetConfig.getConfig("sourceconfig.store")
      .as[FiniteDuration]("disk-time-to-live").toSeconds.toInt
  }
  private val datasetRef = if (isDownsampleCopy) {
    DatasetRef(s"${datasetName}_ds_${highestDSResolution.toMinutes}")
  } else {
    DatasetRef.fromDotString(datasetName)
  }

  private val readSched = Scheduler.io("cass-read-sched")
  private val writeSched = Scheduler.io("cass-write-sched")

  val sourceCassandraColStore = new CassandraColumnStore(
    sourceConfig, readSched, sourceSession, isDownsampleCopy
  )(writeSched)
  val targetCassandraColStore = new CassandraColumnStore(
    targetConfig, readSched, targetSession, isDownsampleCopy
  )(writeSched)

  // Disable the copy phase either for fully deleting with no replacement, or for no-op testing.
  private[repair] val noCopy = conf.getBoolean("spark.filodb.partitionkeys.copier.noCopy", false)
  private[repair] val numSplitsForScans = sourceCassConfig.getInt("num-token-range-splits-for-scans")

  private[repair] def getSourceScanSplits = sourceCassandraColStore.getScanSplits(datasetRef, numSplitsForScans)
  private[repair] def getTargetScanSplits = targetCassandraColStore.getScanSplits(datasetRef, numSplitsForScans)

  def copySourceToTarget(splitIter: Iterator[ScanSplit]): Unit = {
    sourceCassandraColStore.copyPartitionKeysByTimeRange(
      datasetRef,
      numOfShards,
      splitIter,
      copyStartTime.toEpochMilli(),
      copyEndTime.toEpochMilli(),
      targetCassandraColStore,
      diskTimeToLiveSeconds)
  }

  def shutdown(): Unit = {
    sourceCassandraColStore.shutdown()
    targetCassandraColStore.shutdown()
  }
}

object PartitionKeysCopier {

  class ByteComparator extends java.util.Comparator[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = java.util.Arrays.compareUnsigned(a, b)
  }

  val cache = new java.util.TreeMap[Array[Byte], PartitionKeysCopier](new ByteComparator)

  // scalastyle: off null
  def lookup(conf: SparkConf): PartitionKeysCopier = synchronized {
    // SparkConf cannot be used as a key, so serialize it instead.
    val bout = new java.io.ByteArrayOutputStream()
    val oout = new java.io.ObjectOutputStream(bout)
    oout.writeObject(conf)
    oout.close()
    val key = bout.toByteArray()

    var copier = cache.get(key)
    if (copier == null) {
      copier = new PartitionKeysCopier(conf)
      cache.put(key, copier)
    }
    copier
  }

  // scalastyle: on
}

/**
 * For launching the Spark job.
 */
object PartitionKeysCopierMain extends App with StrictLogging {
  run(new SparkConf(loadDefaults = true))

  def run(conf: SparkConf): SparkSession = {
    logger.info(s"PartitionKeysCopier Spark Job Properties: ${conf.toDebugString}")
    val copier = PartitionKeysCopier.lookup(conf)
    val spark = SparkSession.builder()
      .appName("FiloDBPartitionKeysCopier")
      .config(conf)
      .getOrCreate()

    if (copier.noCopy) {
      logger.info("PartitionKeysCopier copy phase disabled")
    } else {
      val splits = copier.getSourceScanSplits
      logger.info(s"Copy phase cassandra split size: ${splits.size}. We will have this many spark partitions. " +
        s"Tune num-token-range-splits-for-scans which was ${copier.numSplitsForScans}, if parallelism is low")
      spark
        .sparkContext
        .makeRDD(splits)
        .foreachPartition(splitIter => PartitionKeysCopier.lookup(conf).copySourceToTarget(splitIter))
    }
    logger.info(s"PartitionKeysCopier Driver completed successfully")
    copier.shutdown()
    spark
  }
}