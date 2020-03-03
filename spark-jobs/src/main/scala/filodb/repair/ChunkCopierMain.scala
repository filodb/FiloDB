package filodb.repair

import java.io.File
import java.time.Instant
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, GlobalConfig}
import filodb.core.store.ScanSplit

/**
  * Contains all the objects necessary for performing worker tasks. Is constructed from SparkConf
  * and is cached.
  */
class ChunkCopier(conf: SparkConf) {
  def openConfig(str: String) = {
    val sysConfig = GlobalConfig.systemConfig.getConfig("filodb")
    ConfigFactory.parseFile(new File(conf.get(str))).getConfig("filodb").withFallback(sysConfig)
  }

  // Both "source" and "target" refer to file paths which define config files that have a
  // top-level "filodb" section and a "cassandra" subsection.
  val sourceConfig = openConfig("spark.filodb.chunkcopier.source.configFile")
  val targetConfig = openConfig("spark.filodb.chunkcopier.target.configFile")

  val sourceCassConfig = sourceConfig.getConfig("cassandra")
  val targetCassConfig = targetConfig.getConfig("cassandra")

  val sourceDatasetRef = DatasetRef.fromDotString(conf.get("spark.filodb.chunkcopier.source.dataset"))
  val targetDatasetRef = DatasetRef.fromDotString(conf.get("spark.filodb.chunkcopier.target.dataset"))

  // Examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
  def parseDateTime(str: String) = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str))

  val ingestionTimeStart = parseDateTime(conf.get("spark.filodb.chunkcopier.ingestionTimeStart"))
  val ingestionTimeEnd = parseDateTime(conf.get("spark.filodb.chunkcopier.ingestionTimeEnd"))

  val diskTimeToLiveSeconds = conf.getTimeAsSeconds("spark.filodb.chunkcopier.diskTimeToLive")
  val splitsPerNode = conf.getInt("spark.filodb.chunkcopier.splitsPerNode", 1)
  val batchSize = conf.getInt("spark.filodb.chunkcopier.batchSize", 10000)

  val readSched = Scheduler.io("cass-read-sched")
  val writeSched = Scheduler.io("cass-write-sched")

  val sourceSession = FiloSessionProvider.openSession(sourceCassConfig)
  val targetSession = FiloSessionProvider.openSession(targetCassConfig)

  val sourceCassandraColStore = new CassandraColumnStore(sourceConfig, readSched, sourceSession)(writeSched)
  val targetCassandraColStore = new CassandraColumnStore(targetConfig, readSched, targetSession)(writeSched)

  def getScanSplits = sourceCassandraColStore.getScanSplits(sourceDatasetRef, splitsPerNode)

  def run(splitIter: Iterator[ScanSplit]): Unit = {
    sourceCassandraColStore.copyChunksByIngestionTimeRange(
      sourceDatasetRef,
      splitIter,
      ingestionTimeStart.toEpochMilli(),
      ingestionTimeEnd.toEpochMilli(),
      batchSize,
      targetCassandraColStore,
      targetDatasetRef,
      diskTimeToLiveSeconds.toInt)
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

object ChunkCopierMain extends App with StrictLogging {
  run(new SparkConf(loadDefaults = true))

  // scalastyle:off method.length
  def run(conf: SparkConf): Unit = {
    logger.info(s"ChunkCopier Spark Job Properties: ${conf.toDebugString}")

    val copier = ChunkCopier.lookup(conf)

    val spark = SparkSession.builder()
      .appName("FiloDBChunkCopier")
      .config(conf)
      .getOrCreate()

    val splits = copier.getScanSplits

    logger.info(s"Cassandra split size: ${splits.size}. We will have this many spark partitions. " +
      s"Tune splitsPerNode which was $copier.splitsPerNode if parallelism is low")

    spark.sparkContext
      .makeRDD(splits)
      .foreachPartition(splitIter => ChunkCopier.lookup(conf).run(splitIter))

    logger.info(s"ChunkCopier Driver completed successfully")

    copier.shutdown()
  }
  // scalastyle:on
}
