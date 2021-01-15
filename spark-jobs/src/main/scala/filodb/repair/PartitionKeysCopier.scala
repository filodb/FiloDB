package filodb.repair

import java.io.File
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.{lang, util}

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.metadata.Schemas
import filodb.core.store.{PartKeyRecord, ScanSplit}
import filodb.core.{DatasetRef, GlobalConfig}
import filodb.memory.format.UnsafeUtils
import monix.execution.Scheduler
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class PartitionKeysCopier(conf: SparkConf) {

  private def openConfig(str: String) = {
    val sysConfig = GlobalConfig.systemConfig.getConfig("filodb")
    ConfigFactory.parseFile(new File(conf.get(str))).getConfig("filodb").withFallback(sysConfig)
  }

  def getShardNum: Int = {
    def getConfig(path: lang.String): Config = {
      ConfigFactory.parseFile(new File(path))
    }

    val sourceConfigPaths: util.List[lang.String] = sourceConfig.getStringList("dataset-configs")
    val datasetConfig: Config = sourceConfigPaths.stream()
      .map[Config](new util.function.Function[lang.String, Config]() {
        override def apply(path: lang.String): Config = getConfig(path)
      })
      .filter(new util.function.Predicate[Config] {
        override def test(conf: Config): Boolean = conf.getString("dataset").equals(sourceDataset)
      })
      .findFirst()
      .orElseThrow()

    val numShards = datasetConfig.getInt("num-shards")
    numShards
  }

  // Examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
  private def parseDateTime(str: String) = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str))

  // Both "source" and "target" refer to file paths which define config files that have a
  // top-level "filodb" section and a "cassandra" subsection.
  private val sourceConfig = openConfig("spark.filodb.partitionkeys.copier.source.configFile")
  private val targetConfig = openConfig("spark.filodb.partitionkeys.copier.target.configFile")
  private val sourceCassConfig = sourceConfig.getConfig("cassandra")
  private val targetCassConfig = targetConfig.getConfig("cassandra")
  private val sourceDataset = conf.get("spark.filodb.partitionkeys.copier.source.dataset")
  private val sourceDatasetRef = DatasetRef.fromDotString(sourceDataset)
  private val targetDatasetRef = DatasetRef.fromDotString(conf.get("spark.filodb.partitionkeys.copier.target.dataset"))
  private val sourceSession = FiloSessionProvider.openSession(sourceCassConfig)
  private val targetSession = FiloSessionProvider.openSession(targetCassConfig)

  val schemas = Schemas.fromConfig(sourceConfig).get
  private[repair] def partKeyHashFn = (partKey: PartKeyRecord) =>
    Option(schemas.part.binSchema.partitionHash(partKey, UnsafeUtils.arayOffset))

  private val numOfShards: Int = getShardNum
  private val repairStartTime = parseDateTime(conf.get("spark.filodb.partitionkeys.copier.repairStartTime"))
  private val repairEndTime = parseDateTime(conf.get("spark.filodb.partitionkeys.copier.repairEndTime"))
  private val diskTimeToLiveSeconds = conf.getTimeAsSeconds("spark.filodb.partitionkeys.copier.diskTimeToLive")
  private val readSched = Scheduler.io("cass-read-sched")
  private val writeSched = Scheduler.io("cass-write-sched")

  val sourceCassandraColStore = new CassandraColumnStore(sourceConfig, readSched, sourceSession)(writeSched)
  val targetCassandraColStore = new CassandraColumnStore(targetConfig, readSched, targetSession)(writeSched)

  // Disable the copy phase either for fully deleting with no replacement, or for no-op testing.
  private[repair] val noCopy = conf.getBoolean("spark.filodb.partitionkeys.copier.noCopy", false)
  private[repair] val numSplitsForScans = sourceCassConfig.getInt("num-token-range-splits-for-scans")

  private[repair] def getSourceScanSplits = sourceCassandraColStore.getScanSplits(sourceDatasetRef, numSplitsForScans)
  private[repair] def getTargetScanSplits = targetCassandraColStore.getScanSplits(targetDatasetRef, numSplitsForScans)

  def copySourceToTarget(splitIter: Iterator[ScanSplit]): Unit = {
    sourceCassandraColStore.copyPartitionKeysByTimeRange(
      sourceDatasetRef,
      numOfShards,
      splitIter,
      repairStartTime.toEpochMilli(),
      repairEndTime.toEpochMilli(),
      targetCassandraColStore,
      targetDatasetRef,
      partKeyHashFn,
      diskTimeToLiveSeconds.toInt)
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