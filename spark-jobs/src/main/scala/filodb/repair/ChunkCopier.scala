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
import filodb.core.store.ScanSplit
import filodb.downsampler.chunk.DownsamplerSettings


/**
  * Contains all the objects necessary for performing worker tasks. Is constructed from SparkConf
  * and is cached.
  */
class ChunkCopier(conf: SparkConf) {

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

  val rawSourceConfig = getFiloConfig(
    "spark.filodb.chunks.copier.source.config.value",
    "spark.filodb.chunks.copier.source.config.file"
  )
  val rawTargetConfig = getFiloConfig(
    "spark.filodb.chunks.copier.target.config.value",
    "spark.filodb.chunks.copier.target.config.file"
  )
  val sourceConfig = rawSourceConfig.getConfig("filodb")
  val targetConfig = rawTargetConfig.getConfig("filodb")
  val sourceCassConfig = sourceConfig.getConfig("cassandra")
  val targetCassConfig = targetConfig.getConfig("cassandra")

  val datasetName = conf.get("spark.filodb.chunks.copier.dataset")
  val datasetRef = DatasetRef.fromDotString(datasetName)
  val targetDatasetConfig = datasetConfig(targetConfig, datasetName)

  // Examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
  private def parseDateTime(str: String) = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str))

  val copyStartTime = parseDateTime(conf.get("spark.filodb.chunks.copier.start.time"))
  val copyEndTime = parseDateTime(conf.get("spark.filodb.chunks.copier.end.time"))

  // Destructively deletes everything in the target before updating anthing. Is used when chunks aren't aligned.
  val deleteFirst = conf.getBoolean("spark.filodb.chunks.copier.delete.first", false)

  // Disable the copy phase either for fully deleting with no replacement, or for no-op testing.
  val noCopy = conf.getBoolean("spark.filodb.chunks.copier.noCopy", false)

  val isDownsampleRepair = conf.getBoolean("spark.filodb.chunks.copier.is.downsample.copy", false)
  val diskTimeToLiveSeconds = if (isDownsampleRepair) {
    val dsSettings = new DownsamplerSettings(rawSourceConfig)
    val highestDSResolution = dsSettings.rawDatasetIngestionConfig.downsampleConfig.resolutions.last
    dsSettings.ttlByResolution(highestDSResolution)
  } else {
    targetDatasetConfig.getConfig("sourceconfig.store")
      .as[FiniteDuration]("disk-time-to-live").toSeconds.toInt
  }

  val numSplitsForScans = sourceCassConfig.getInt("num-token-range-splits-for-scans")
  val batchSize = conf.getInt("spark.filodb.chunks.copier.batchSize", 10000)

  val readSched = Scheduler.io("cass-read-sched")
  val writeSched = Scheduler.io("cass-write-sched")

  val sourceSession = FiloSessionProvider.openSession(sourceCassConfig)
  val targetSession = FiloSessionProvider.openSession(targetCassConfig)

  val sourceCassandraColStore = new CassandraColumnStore(
    sourceConfig, readSched, sourceSession, isDownsampleRepair)(writeSched)
  val targetCassandraColStore = new CassandraColumnStore(
    targetConfig, readSched, targetSession, isDownsampleRepair)(writeSched)

  private[repair] def getSourceScanSplits = sourceCassandraColStore.getScanSplits(datasetRef, numSplitsForScans)

  private[repair] def getTargetScanSplits = targetCassandraColStore.getScanSplits(datasetRef, numSplitsForScans)

  def copySourceToTarget(splitIter: Iterator[ScanSplit]): Unit = {
    sourceCassandraColStore.copyOrDeleteChunksByIngestionTimeRange(
      datasetRef,
      splitIter,
      copyStartTime.toEpochMilli(),
      copyEndTime.toEpochMilli(),
      batchSize,
      targetCassandraColStore,
      diskTimeToLiveSeconds)
  }

  def deleteFromTarget(splitIter: Iterator[ScanSplit]): Unit = {
    targetCassandraColStore.copyOrDeleteChunksByIngestionTimeRange(
      datasetRef,
      splitIter,
      copyStartTime.toEpochMilli(),
      copyEndTime.toEpochMilli(),
      batchSize,
      targetCassandraColStore,
      0) // ttl 0 is interpreted as delete
  }

  def shutdown(): Unit = {
    sourceCassandraColStore.shutdown()
    targetCassandraColStore.shutdown()
  }
}

object ChunkCopier {
  class ByteComparator extends java.util.Comparator[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = java.util.Arrays.compareUnsigned(a, b)
  }

  val cache = new java.util.TreeMap[Array[Byte], ChunkCopier](new ByteComparator)

  // scalastyle: off null
  def lookup(conf: SparkConf): ChunkCopier = synchronized {
    // SparkConf cannot be used as a key, so serialize it instead.
    val bout = new java.io.ByteArrayOutputStream()
    val oout = new java.io.ObjectOutputStream(bout)
    oout.writeObject(conf)
    oout.close()
    val key = bout.toByteArray()

    var copier = cache.get(key)
    if (copier == null) {
      copier = new ChunkCopier(conf)
      cache.put(key, copier)
    }
    copier
  }
  // scalastyle: on
}

/**
  * For launching the Spark job.
  */
object ChunkCopierMain extends App with StrictLogging {
  run(new SparkConf(loadDefaults = true))

  def run(conf: SparkConf): SparkSession = {
    logger.info(s"ChunkCopier Spark Job Properties: ${conf.toDebugString}")

    val copier = ChunkCopier.lookup(conf)

    val spark = SparkSession.builder()
      .appName("FiloDBChunkCopier")
      .config(conf)
      .getOrCreate()

    if (copier.deleteFirst) {
      logger.info("ChunkCopier deleting from target first")

      val splits = copier.getTargetScanSplits

      logger.info(s"Delete phase cassandra split size: ${splits.size}. We will have this many spark partitions. " +
        s"Tune splitsPerNode which was ${copier.numSplitsForScans} if parallelism is low")

      spark.sparkContext
        .makeRDD(splits)
        .foreachPartition(splitIter => ChunkCopier.lookup(conf).deleteFromTarget(splitIter))
    }

    if (copier.noCopy) {
      logger.info("ChunkCopier copy phase disabled")
    } else {
      val splits = copier.getSourceScanSplits

      logger.info(s"Copy phase cassandra split size: ${splits.size}. We will have this many spark partitions. " +
        s"Tune splitsPerNode which was ${copier.numSplitsForScans} if parallelism is low")

      spark.sparkContext
        .makeRDD(splits)
        .foreachPartition(splitIter => ChunkCopier.lookup(conf).copySourceToTarget(splitIter))
    }

    logger.info(s"ChunkCopier Driver completed successfully")

    copier.shutdown()
    spark
  }
}
