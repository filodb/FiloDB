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

object ChunkCopierMain extends App {
  new ChunkCopier().run(new SparkConf(loadDefaults = true))

  // Define here so they don't get serialized. This is really nasty by the way, relying on
  // global state.
  var sourceCassandraColStore: CassandraColumnStore = _
  var targetCassandraColStore: CassandraColumnStore = _
}

class ChunkCopier extends StrictLogging {

  // scalastyle:off method.length
  def run(conf: SparkConf): Unit = {
    import ChunkCopierMain._

    val spark = SparkSession.builder()
      .appName("FiloDBChunkCopier")
      .config(conf)
      .getOrCreate()

    logger.info(s"ChunkCopier Spark Job Properties: ${conf.toDebugString}")

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

    sourceCassandraColStore = new CassandraColumnStore(sourceConfig, readSched, sourceSession)(writeSched)
    targetCassandraColStore = new CassandraColumnStore(targetConfig, readSched, targetSession)(writeSched)

    // Need this here because function will be serialized, and Instant class isn't Java 8
    // compatible, and Spark is stuck on Java 8.
    val startMillis = ingestionTimeStart.toEpochMilli()
    val endMillis = ingestionTimeEnd.toEpochMilli()

    val splits = sourceCassandraColStore.getScanSplits(sourceDatasetRef, splitsPerNode)

    logger.info(s"Cassandra split size: ${splits.size}. We will have this many spark partitions. " +
      s"Tune splitsPerNode which was $splitsPerNode if parallelism is low")

    spark.sparkContext
      .makeRDD(splits)
      .foreachPartition{ splitIter =>
        sourceCassandraColStore.copyChunksByIngestionTimeRange(
          sourceDatasetRef,
          splitIter,
          startMillis,
          endMillis,
          batchSize,
          targetCassandraColStore,
          targetDatasetRef,
          diskTimeToLiveSeconds.toInt)
      }

    logger.info(s"ChunkCopier Driver completed successfully")

    sourceCassandraColStore.shutdown()
    targetCassandraColStore.shutdown()
  }
  // scalastyle:on
}
