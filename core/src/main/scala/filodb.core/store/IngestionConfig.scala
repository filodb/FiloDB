package filodb.core.store

import scala.concurrent.duration._
import scala.util.Try

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import filodb.core.{DatasetRef, IngestionKeys}
import filodb.core.downsample.DownsampleConfig

final case class StoreConfig(flushInterval: FiniteDuration,
                             timeAlignedChunksEnabled: Boolean,
                             diskTTLSeconds: Long,
                             maxChunksSize: Int,
                             // Max write buffer size for Histograms, UTF8Strings, other blobs
                             maxBlobBufferSize: Int,
                             // Number of bytes to allocate to chunk storage in each shard
                             shardMemSize: Long,
                             shardMemPercent: Double,
                             maxBufferPoolSize: Int,
                             groupsPerShard: Int,
                             numPagesPerBlock: Int,
                             failureRetries: Int,
                             maxChunkTime: FiniteDuration,
                             retryDelay: FiniteDuration,
                             partIndexFlushMaxDelaySeconds: Int,
                             partIndexFlushMinDelaySeconds: Int,
                             // Use a MultiPartitionScan (instead of single partition at a time) for on-demand paging
                             multiPartitionODP: Boolean,
                             demandPagingParallelism: Int,
                             demandPagingEnabled: Boolean,
                             evictedPkBfCapacity: Int,
                             // filters on ingested records to log in detail
                             traceFilters: Map[String, String],
                             meteringEnabled: Boolean,
                             acceptDuplicateSamples: Boolean,
                             // approx data resolution, used for estimating the size of data to be scanned for
                             // answering queries, specified in milliseconds
                             estimatedIngestResolutionMillis: Int) {
  import scala.jdk.CollectionConverters._
  def toConfig: Config =
    ConfigFactory.parseMap(Map[String, Any]("flush-interval" -> s"${flushInterval.toSeconds}s",
                               "time-aligned-chunks-enabled" -> timeAlignedChunksEnabled,
                               "disk-time-to-live" -> s"${diskTTLSeconds}s",
                               "max-chunks-size" -> maxChunksSize,
                               "max-blob-buffer-size" -> maxBlobBufferSize,
                               "shard-mem-size" -> shardMemSize,
                               "shard-mem-percent" -> shardMemPercent,
                               "max-buffer-pool-size" -> maxBufferPoolSize,
                               "groups-per-shard" -> groupsPerShard,
                               "max-chunk-time" -> s"${maxChunkTime.toSeconds}s",
                               "num-block-pages" -> numPagesPerBlock,
                               "failure-retries" -> failureRetries,
                               "retry-delay" -> s"${retryDelay.toSeconds}s",
                               "part-index-flush-max-delay" -> s"${partIndexFlushMaxDelaySeconds}s",
                               "part-index-flush-min-delay" -> s"${partIndexFlushMinDelaySeconds}s",
                               "multi-partition-odp" -> multiPartitionODP,
                               "demand-paging-parallelism" -> demandPagingParallelism,
                               "demand-paging-enabled" -> demandPagingEnabled,
                               "evicted-pk-bloom-filter-capacity" -> evictedPkBfCapacity,
                               "metering-enabled" -> meteringEnabled,
                               "accept-duplicate-samples" -> acceptDuplicateSamples,
                               "ingest-resolution-millis" -> estimatedIngestResolutionMillis).asJava)
}

final case class AssignShardConfig(address: String, shardList: Seq[Int])

final case class UnassignShardConfig(shardList: Seq[Int])

object StoreConfig {
  // NOTE: there are no defaults for flush interval and shard memory, those should be explicitly calculated
  // default max-data-per-shard-query was calculated as follows:
  // 750k TsPartitions * 48 chunksets queried * 2kb per chunkset / 256 shards = 280MB

  // The num-block-pages setting when multiplied by the page size (4KB) defines the
  // BlockManager block size. When num-block-pages is 100, the effective block size is 400KB.

  val defaults = ConfigFactory.parseString("""
                                           |disk-time-to-live = 3 days
                                           |max-chunks-size = 400
                                           |max-data-per-shard-query = 300 MB
                                           |max-blob-buffer-size = 15000
                                           |max-buffer-pool-size = 10000
                                           |groups-per-shard = 60
                                           |shard-mem-percent = 100 # assume only one dataset per node by default
                                           |num-block-pages = 100
                                           |failure-retries = 3
                                           |retry-delay = 15 seconds
                                           |// less than 1 min to reduce possibility of double purge of time series
                                           |part-index-flush-max-delay = 55 seconds
                                           |part-index-flush-min-delay = 30 seconds
                                           |multi-partition-odp = false
                                           |demand-paging-parallelism = 10
                                           |demand-paging-enabled = true
                                           |evicted-pk-bloom-filter-capacity = 5000000
                                           |trace-filters = {}
                                           |metering-enabled = true
                                           |accept-duplicate-samples = false
                                           |time-aligned-chunks-enabled = false
                                           |ingest-resolution-millis = 60000
                                           |""".stripMargin)
  /** Pass in the config inside the store {}  */
  def apply(storeConfig: Config): StoreConfig = {
    val config = storeConfig.withFallback(defaults)
    val flushInterval = config.as[FiniteDuration]("flush-interval")

    // switch buffers and create chunk when current sample's timestamp crosses flush boundary.
    // e.g. for a flush-interval of 1hour, if new sample falls in different hour than last sample, then switch buffers
    // and create chunk. This helps in aligning chunks across Active/Active HA clusters and facilitates chunk migration
    // between the clusters during disaster recovery.
    // Note: Enabling this might result into creation of smaller suboptimal chunks.
    val timeAlignedChunksEnabled = config.getBoolean("time-aligned-chunks-enabled")

    // maxChunkTime should atleast be length of flush interval to accommodate all data within one chunk.
    // better to be slightly greater so if more samples arrive within that flush period, two chunks are not created.
    val fallbackMaxChunkTime = (flushInterval.toMillis * 1.1).toLong.millis
    val maxChunkTime = config.as[Option[FiniteDuration]]("max-chunk-time").getOrElse(fallbackMaxChunkTime)
    StoreConfig(flushInterval,
                timeAlignedChunksEnabled,
                config.as[FiniteDuration]("disk-time-to-live").toSeconds,
                config.getInt("max-chunks-size"),
                config.getInt("max-blob-buffer-size"),
                config.getMemorySize("shard-mem-size").toBytes,
                config.getDouble("shard-mem-percent"),
                config.getInt("max-buffer-pool-size"),
                config.getInt("groups-per-shard"),
                config.getInt("num-block-pages"),
                config.getInt("failure-retries"),
                maxChunkTime,
                config.as[FiniteDuration]("retry-delay"),
                config.as[FiniteDuration]("part-index-flush-max-delay").toSeconds.toInt,
                config.as[FiniteDuration]("part-index-flush-min-delay").toSeconds.toInt,
                config.getBoolean("multi-partition-odp"),
                config.getInt("demand-paging-parallelism"),
                config.getBoolean("demand-paging-enabled"),
                config.getInt("evicted-pk-bloom-filter-capacity"),
                config.as[Map[String, String]]("trace-filters"),
                config.getBoolean("metering-enabled"),
                config.getBoolean("accept-duplicate-samples"),
                config.getInt("ingest-resolution-millis"))
  }
}

/**
 * Contains all the config needed to recreate `NodeClusterActor.SetupDataset`, set up a dataset for streaming
 * ingestion on FiloDB nodes.   Note: the resources Config needs to be translated by an upper layer.
 */
final case class IngestionConfig(ref: DatasetRef,
                                 resources: Config,
                                 streamFactoryClass: String,
                                 sourceConfig: Config,
                                 storeConfig: StoreConfig,
                                 downsampleConfig: DownsampleConfig = DownsampleConfig.disabled) {

  // called by NodeClusterActor, by this point, validation and failure if
  // config parse issue or not available are raised from Cli / HTTP
  def numShards: Int = IngestionConfig.numShards(resources).get
  def minNumNodes: Int = IngestionConfig.minNumNodes(resources).get
  def sourceStoreConfig: Config = storeConfig.toConfig.atPath("store").withFallback(sourceConfig)
}

object IngestionConfig {
  import IngestionKeys.{Dataset => DatasetRefKey, _}

  /* These two are not called until NodeClusterActor creates
     DatasetResourceSpec for SetupData, but they are not specifically written/read via C*,
     only as string. Why not parse early to fail fast and store specifically like 'dataset'. */
  def numShards(c: Config): Try[Int] = c.intT(IngestionKeys.NumShards)
  def minNumNodes(c: Config): Try[Int] = c.intT(IngestionKeys.MinNumNodes)

  /** Creates an IngestionConfig from a "source config" file - see conf/timeseries-dev-source.conf.
    * Allows the caller to decide what to do with configuration parsing errors and when.
    * Fails if no dataset is provided by the config submitter.
    */
  private[core] def apply(topLevelConfig: Config): Try[IngestionConfig] = {
    for {
      resolved      <- topLevelConfig.resolveT
      dataset       <- resolved.stringT(DatasetRefKey) // fail fast if missing
      factory       <- resolved.stringT(SourceFactory) // fail fast if missing
      numShards     <- numShards(resolved) // fail fast if missing
      minNodes      <- minNumNodes(resolved) // fail fast if missing
      sourceConfig   = resolved.as[Option[Config]](IngestionKeys.SourceConfig).getOrElse(ConfigFactory.empty)
      downsampleConf = DownsampleConfig.downsampleConfigFromSource(sourceConfig)
      ref            = DatasetRef.fromDotString(dataset)
      storeConf     <- sourceConfig.configT("store").map(StoreConfig.apply)
    } yield IngestionConfig(ref, resolved, factory, sourceConfig, storeConf, downsampleConf)
  }

  def apply(sourceConfig: Config, backupSourceFactory: String): Try[IngestionConfig] = {
    val backup = ConfigFactory.parseString(s"$SourceFactory = $backupSourceFactory")
    apply(sourceConfig.withFallback(backup))
  }

  def apply(sourceStr: String, backupSourceFactory: String): Try[IngestionConfig] =
    Try(ConfigFactory.parseString(sourceStr))
      .flatMap(apply(_, backupSourceFactory))

  /** Creates an IngestionConfig from `ingestionconfig` Cassandra table. */
  def apply(ref: DatasetRef, factoryclass: String, resources: String, sourceconfig: String): IngestionConfig = {
    val sourceConf = ConfigFactory.parseString(sourceconfig)
    val downsampleConf = DownsampleConfig.downsampleConfigFromSource(sourceConf)
    IngestionConfig(
      ref,
      ConfigFactory.parseString(resources),
      factoryclass,
      sourceConf,
      StoreConfig(sourceConf.getConfig("store")),
      downsampleConf)
  }
}

