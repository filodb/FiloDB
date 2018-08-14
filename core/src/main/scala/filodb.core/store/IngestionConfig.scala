package filodb.core.store

import scala.concurrent.duration._
import scala.util.Try

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import filodb.core.{DatasetRef, IngestionKeys}

final case class StoreConfig(flushInterval: FiniteDuration,
                             diskTTLSeconds: Int,
                             demandPagedRetentionPeriod: FiniteDuration,
                             maxChunksSize: Int,
                             // Number of bytes to allocate to chunk storage in each shard
                             shardMemSize: Long,
                             // Number of bytes to allocate to ingestion write buffers per shard
                             ingestionBufferMemSize: Long,
                             allocStepSize: Int,
                             numToEvict: Int,
                             groupsPerShard: Int,
                             numPagesPerBlock: Int,
                             partIndexFlushMaxDelaySeconds: Int,
                             partIndexFlushMinDelaySeconds: Int,
                             demandPagingEnabled: Boolean) {
  import collection.JavaConverters._
  def toConfig: Config =
    ConfigFactory.parseMap(Map("flush-interval" -> (flushInterval.toSeconds + "s"),
                               "disk-time-to-live" -> (diskTTLSeconds + "s"),
                               "demand-paged-chunk-retention-period" -> (demandPagedRetentionPeriod.toSeconds + "s"),
                               "max-chunks-size" -> maxChunksSize,
                               "shard-mem-size" -> shardMemSize,
                               "ingestion-buffer-mem-size" -> ingestionBufferMemSize,
                               "buffer-alloc-step-size" -> allocStepSize,
                               "num-partitions-to-evict" -> numToEvict,
                               "groups-per-shard" -> groupsPerShard,
                               "num-block-pages" -> numPagesPerBlock,
                               "part-index-flush-max-delay" -> (partIndexFlushMaxDelaySeconds + "s"),
                               "part-index-flush-min-delay" -> (partIndexFlushMinDelaySeconds + "s"),
                               "demand-paging-enabled" -> demandPagingEnabled).asJava)
}

object StoreConfig {
  // NOTE: there are no defaults for flush interval and shard memory, those should be explicitly calculated
  val defaults = ConfigFactory.parseString("""
                                           |disk-time-to-live = 3 days
                                           |demand-paged-chunk-retention-period = 72 hours
                                           |max-chunks-size = 500
                                           |ingestion-buffer-mem-size = 10M
                                           |buffer-alloc-step-size = 1000
                                           |num-partitions-to-evict = 1000
                                           |groups-per-shard = 60
                                           |num-block-pages = 1000
                                           |part-index-flush-max-delay = 60 seconds
                                           |part-index-flush-min-delay = 30 seconds
                                           |demand-paging-enabled = true
                                           |""".stripMargin)
  /** Pass in the config inside the store {}  */
  def apply(storeConfig: Config): StoreConfig = {
    val config = storeConfig.withFallback(defaults)
    StoreConfig(config.as[FiniteDuration]("flush-interval"),
                config.as[FiniteDuration]("disk-time-to-live").toSeconds.toInt,
                config.as[FiniteDuration]("demand-paged-chunk-retention-period"),
                config.getInt("max-chunks-size"),
                config.getMemorySize("shard-mem-size").toBytes,
                config.getMemorySize("ingestion-buffer-mem-size").toBytes,
                config.getInt("buffer-alloc-step-size"),
                config.getInt("num-partitions-to-evict"),
                config.getInt("groups-per-shard"),
                config.getInt("num-block-pages"),
                config.as[FiniteDuration]("part-index-flush-max-delay").toSeconds.toInt,
                config.as[FiniteDuration]("part-index-flush-min-delay").toSeconds.toInt,
                config.getBoolean("demand-paging-enabled"))
  }
}

/**
 * Contains all the config needed to recreate `NodeClusterActor.SetupDataset`, set up a dataset for streaming
 * ingestion on FiloDB nodes.   Note: the resources Config needs to be translated by an upper layer.
 */
final case class IngestionConfig(ref: DatasetRef,
                                 resources: Config,
                                 streamFactoryClass: String,
                                 streamConfig: Config,
                                 storeConfig: StoreConfig) {

  // called by NodeClusterActor, by this point, validation and failure if
  // config parse issue or not available are raised from Cli / HTTP
  def numShards: Int = IngestionConfig.numShards(resources).get
  def minNumNodes: Int = IngestionConfig.minNumNodes(resources).get

  def streamStoreConfig: Config = storeConfig.toConfig.atPath("store").withFallback(streamConfig)
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
  private[core] def apply(sourceConfig: Config): Try[IngestionConfig] =
    for {
      resolved  <- sourceConfig.resolveT
      dataset   <- resolved.stringT(DatasetRefKey) // fail fast if missing
      factory   <- resolved.stringT(SourceFactory) // fail fast if missing
      numShards <- numShards(resolved)             // fail fast if missing
      minNodes  <- minNumNodes(resolved)           // fail fast if missing
      streamConfig = resolved.as[Option[Config]](IngestionKeys.SourceConfig).getOrElse(ConfigFactory.empty)
      ref          = DatasetRef.fromDotString(dataset)
      storeConf <- streamConfig.configT("store").map(StoreConfig.apply)
    } yield IngestionConfig(ref, resolved, factory, streamConfig, storeConf)

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
    IngestionConfig(
      ref,
      ConfigFactory.parseString(resources),
      factoryclass,
      sourceConf,
      StoreConfig(sourceConf.getConfig("store")))
  }
}

