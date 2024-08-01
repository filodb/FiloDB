package filodb.core.memstore

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.StampedLock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Random, Try}

import bloomfilter.CanGenerateHashFrom
import bloomfilter.mutable.BloomFilter
import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import debox.{Buffer, Map => DMap}
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler, UncaughtExceptionReporter}
import monix.execution.atomic.AtomicBoolean
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong
import spire.syntax.cfor._

import filodb.core.{ErrorResponse, _}
import filodb.core.binaryrecord2._
import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityTracker, QuotaSource, RocksDbCardinalityStore}
import filodb.core.metadata.{Schema, Schemas}
import filodb.core.query.{ColumnFilter, Filter, QuerySession}
import filodb.core.store._
import filodb.memory._
import filodb.memory.data.Shutdown
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.ZeroCopyUTF8String._

class TimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

  val shardTotalRecoveryTime = Kamon.gauge("memstore-total-shard-recovery-time",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))
  val rowsIngested = Kamon.counter("memstore-rows-ingested").withTags(TagSet.from(tags))
  val partitionsCreated = Kamon.counter("memstore-partitions-created").withTags(TagSet.from(tags))
  val dataDropped = Kamon.counter("memstore-data-dropped").withTags(TagSet.from(tags))
  val unknownSchemaDropped = Kamon.counter("memstore-unknown-schema-dropped").withTags(TagSet.from(tags))
  val oldContainers = Kamon.counter("memstore-incompatible-containers").withTags(TagSet.from(tags))
  val offsetsNotRecovered = Kamon.counter("memstore-offsets-not-recovered").withTags(TagSet.from(tags))
  val outOfOrderDropped = Kamon.counter("memstore-out-of-order-samples").withTags(TagSet.from(tags))
  val rowsSkipped  = Kamon.counter("recovery-row-skipped").withTags(TagSet.from(tags))
  val rowsPerContainer = Kamon.histogram("num-samples-per-container").withoutTags()
  val numSamplesEncoded = Kamon.counter("memstore-samples-encoded").withTags(TagSet.from(tags))
  val encodedBytes  = Kamon.counter("memstore-encoded-bytes-allocated", MeasurementUnit.information.bytes)
    .withTags(TagSet.from(tags))
  val encodedHistBytes = Kamon.counter("memstore-hist-encoded-bytes", MeasurementUnit.information.bytes)
    .withTags(TagSet.from(tags))
  val flushesSuccessful = Kamon.counter("memstore-flushes-success").withTags(TagSet.from(tags))
  val flushesFailedChunkWrite = Kamon.counter("memstore-flushes-failed-chunk").withTags(TagSet.from(tags))
  val flushesFailedOther = Kamon.counter("memstore-flushes-failed-other").withTags(TagSet.from(tags))

  val numDirtyPartKeysFlushed = Kamon.counter("memstore-index-num-dirty-keys-flushed").withTags(TagSet.from(tags))
  val indexRecoveryNumRecordsProcessed = Kamon.counter("memstore-index-recovery-partkeys-processed").
    withTags(TagSet.from(tags))
  val indexPartkeyLookups = Kamon.counter("memstore-index-partkey-lookups").withTags(TagSet.from(tags))
  val partkeyLabelScans = Kamon.counter("memstore-labels-partkeys-scanned").withTags(TagSet.from(tags))
  val downsampleRecordsCreated = Kamon.counter("memstore-downsample-records-created").withTags(TagSet.from(tags))

  /**
    * These gauges are intended to be combined with one of the latest offset of Kafka partitions so we can produce
    * stats on message lag:
    *   kafka_ingestion_lag = kafka_latest_offset - offsetLatestInMem
    *   memstore_ingested_to_persisted_lag = offsetLatestInMem - offsetLatestFlushed
    *   etc.
    *
    * NOTE: only positive offsets will be recorded.  Kafka does not give negative offsets, but Kamon cannot record
    * negative numbers either.
    * The "latest" vs "earliest" flushed reflects that there are really n offsets, one per flush group.
    */
  val offsetLatestInMem = Kamon.gauge("shard-offset-latest-inmemory").withTags(TagSet.from(tags))
  val offsetLatestFlushed = Kamon.gauge("shard-offset-flushed-latest").withTags(TagSet.from(tags))
  val offsetEarliestFlushed = Kamon.gauge("shard-offset-flushed-earliest").withTags(TagSet.from(tags))
  val numPartitions = Kamon.gauge("num-partitions").withTags(TagSet.from(tags))
  val numActivelyIngestingParts = Kamon.gauge("num-ingesting-partitions").withTags(TagSet.from(tags))

  val numChunksPagedIn = Kamon.counter("chunks-paged-in").withTags(TagSet.from(tags))
  val partitionsPagedFromColStore = Kamon.counter("memstore-partitions-paged-in").withTags(TagSet.from(tags))
  val partitionsQueried = Kamon.counter("memstore-partitions-queried").withTags(TagSet.from(tags))
  val purgedPartitions = Kamon.counter("memstore-partitions-purged").withTags(TagSet.from(tags))
  val purgedPartitionsFromIndex = Kamon.counter("memstore-partitions-purged-index").withTags(TagSet.from(tags))
  val purgePartitionTimeMs = Kamon.counter("memstore-partitions-purge-time-ms", MeasurementUnit.time.milliseconds)
                                              .withTags(TagSet.from(tags))
  val partitionsRestored = Kamon.counter("memstore-partitions-paged-restored").withTags(TagSet.from(tags))
  val chunkIdsEvicted = Kamon.counter("memstore-chunkids-evicted").withTags(TagSet.from(tags))
  val partitionsEvicted = Kamon.counter("memstore-partitions-evicted").withTags(TagSet.from(tags))
  val queryTimeRangeMins = Kamon.histogram("query-time-range-minutes").withTags(TagSet.from(tags))
  val queriesBySchema = Kamon.counter("leaf-queries-by-schema").withTags(TagSet.from(tags))
  val memoryStats = new MemoryStats(tags)

  val bufferPoolSize = Kamon.gauge("memstore-writebuffer-pool-size").withTags(TagSet.from(tags))
  val indexEntries = Kamon.gauge("memstore-index-entries").withTags(TagSet.from(tags))
  val indexBytes   = Kamon.gauge("memstore-index-ram-bytes").withTags(TagSet.from(tags))

  val evictedPartKeyBloomFilterQueries = Kamon.counter("evicted-pk-bloom-filter-queries").withTags(TagSet.from(tags))
  val evictedPartKeyBloomFilterFalsePositives = Kamon.counter("evicted-pk-bloom-filter-fp").withTags(TagSet.from(tags))
  val evictedPkBloomFilterSize = Kamon.gauge("evicted-pk-bloom-filter-approx-size").withTags(TagSet.from(tags))
  val evictedPartIdLookupMultiMatch = Kamon.counter("evicted-partId-lookup-multi-match").withTags(TagSet.from(tags))

  /**
    * Difference between the local clock and the received ingestion timestamps, in milliseconds.
    * If this gauge is negative, then the received timestamps are ahead, and it will stay this
    * way for a bit, due to the monotonic adjustment. When the gauge value is positive (which is
    * expected), then the delay reflects the delay between the generation of the samples and
    * receiving them, assuming that the clocks are in sync.
    */
  val ingestionClockDelay = Kamon.gauge("ingestion-clock-delay",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))

  /**
   *  This measures the time from Message Queue to when the data is stored.
   */
  val ingestionPipelineLatency = Kamon.histogram("ingestion-pipeline-latency",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))

  val chunkFlushTaskLatency = Kamon.histogram("chunk-flush-task-latency-after-retries",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))

  /**
   * How much time a thread was potentially stalled while attempting to ensure
   * free space. Unit is nanoseconds.
   */
  val memstoreEvictionStall = Kamon.counter("memstore-eviction-stall",
                           MeasurementUnit.time.nanoseconds).withTags(TagSet.from(tags))
  val evictablePartKeysSize = Kamon.gauge("memstore-num-evictable-partkeys").withTags(TagSet.from(tags))
  val missedEviction = Kamon.counter("memstore-missed-eviction").withTags(TagSet.from(tags))

}

object TimeSeriesShard {
  /**
    * Writes metadata for TSPartition where every vector is written
    */
  def writeMeta(addr: Long, partitionID: Int, info: ChunkSetInfo, vectors: Array[BinaryVectorPtr]): Unit = {
    UnsafeUtils.setInt(UnsafeUtils.ZeroPointer, addr, partitionID)
    ChunkSetInfo.copy(info, addr + 4)
    cforRange { 0 until vectors.size } { i =>
      ChunkSetInfo.setVectorPtr(addr + 4, i, vectors(i))
    }
  }

  /**
    * Copies serialized ChunkSetInfo bytes from persistent storage / on-demand paging.
    */
  def writeMeta(addr: Long, partitionID: Int, bytes: Array[Byte], vectors: ArrayBuffer[BinaryVectorPtr]): Unit = {
    UnsafeUtils.setInt(UnsafeUtils.ZeroPointer, addr, partitionID)
    ChunkSetInfo.copy(bytes, addr + 4)
    cforRange { 0 until vectors.size } { i =>
      ChunkSetInfo.setVectorPtr(addr + 4, i, vectors(i))
    }
  }

  /**
    * Copies serialized ChunkSetInfo bytes from persistent storage / on-demand paging.
    */
  def writeMetaWithoutPartId(addr: Long, bytes: Array[Byte], vectors: Array[BinaryVectorPtr]): Unit = {
    ChunkSetInfo.copy(bytes, addr)
    cforRange { 0 until vectors.size } { i =>
      ChunkSetInfo.setVectorPtr(addr, i, vectors(i))
    }
  }

  // Initial size of partitionSet and partition map structures.  Make large enough to avoid too many resizes.
  val InitialNumPartitions = 128 * 1024

  // Not a real partition, just a special marker for "out of memory"
  val OutOfMemPartition = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeriesPartition]

  val EmptyBitmap = new EWAHCompressedBitmap()

  /**
    * Calculates the flush group of an ingest record or partition key.  Be sure to use the right RecordSchema -
    * dataset.ingestionSchema or dataset.partKeySchema.l
    */
  def partKeyGroup(schema: RecordSchema, partKeyBase: Any, partKeyOffset: Long, numGroups: Int): Int = {
    Math.abs(schema.partitionHash(partKeyBase, partKeyOffset) % numGroups)
  }

  private[memstore] final val CREATE_NEW_PARTID = -1
}

private[core] final case class PartKey(base: Any, offset: Long)
private[core] final case class PartKeyWithTimes(base: Any, offset: Long, startTime: Long, endTime: Long)

trait PartitionIterator extends Iterator[TimeSeriesPartition] {
  def skippedPartIDs: Buffer[Int]
}

object PartitionIterator {
  def fromPartIt(baseIt: Iterator[TimeSeriesPartition]): PartitionIterator = new PartitionIterator {
    val skippedPartIDs = Buffer.empty[Int]
    final def hasNext: Boolean = baseIt.hasNext
    final def next: TimeSeriesPartition = baseIt.next
  }
}

/**
  * TSPartition lookup from filters result, usually step 1 of querying.
  *
  * @param partsInMemory iterates through the in-Memory partitions, some of which may not need ODP.
  *                          Caller needs to filter further
  * @param firstSchemaId if defined, the first Schema ID found. If not defined, probably there's no data.
  * @param partIdsMemTimeGap contains partIDs in memory but with potential time gaps in data. Their
  *                          startTimes from Lucene are mapped from the ID.
  * @param partIdsNotInMemory is a collection of partIds fully not in memory
  */
case class PartLookupResult(shard: Int,
                            chunkMethod: ChunkScanMethod,
                            partsInMemory: debox.Buffer[Int],
                            firstSchemaId: Option[Int] = None,
                            partIdsMemTimeGap: debox.Map[Int, Long] = debox.Map.empty,
                            partIdsNotInMemory: debox.Buffer[Int] = debox.Buffer.empty,
                            pkRecords: Seq[PartKeyLuceneIndexRecord] = Seq.empty,
                            dataBytesScannedCtr: AtomicLong)

final case class SchemaMismatch(expected: String, found: String, clazz: String) extends
  Exception(s"Multiple schemas found, please filter. Expected schema $expected, found schema $found in $clazz")

object SchemaMismatch {
  def apply(expected: Schema, found: Schema, clazz: String): SchemaMismatch =
    SchemaMismatch(expected.name, found.name, clazz)
}

case class TimeSeriesShardInfo(shardNum: Int,
                               stats: TimeSeriesShardStats,
                               bufferPools: debox.Map[Int, WriteBufferPool],
                               nativeMemoryManager: NativeMemoryManager)

// scalastyle:off number.of.methods
// scalastyle:off file.size.limit
/**
  * Contains all of the data for a SINGLE shard of a time series oriented dataset.
  *
  * Each partition has an integer ID which is used for bitmap indexing using PartitionKeyIndex.
  * Within a shard, the partitions are grouped into a fixed number of groups to facilitate persistence and recovery:
  * - groups spread out the persistence/flushing load across time
  * - having smaller batches of flushes shortens the window of recovery and enables skipping of records/less CPU
  *
  * Each incoming time series is hashed into a group.  Each group has its own watermark.  The watermark indicates,
  * for that group, up to what offset incoming records for that group has been persisted.  At recovery time, records
  * that fall below the watermark for that group will be skipped (since they can be recovered from disk).
  *
  * @param bufferMemoryManager Unencoded/unoptimized ingested data is stored in buffers that are allocated from this
  *                            memory pool. This pool is also used to store partition keys.
  * @param storeConfig the store portion of the sourceconfig, not the global FiloDB application config
  */
class TimeSeriesShard(val ref: DatasetRef,
                      val schemas: Schemas,
                      val storeConfig: StoreConfig,
                      numShards: Int,
                      quotaSource: QuotaSource,
                      val shardNum: Int,
                      val bufferMemoryManager: NativeMemoryManager,
                      colStore: ColumnStore,
                      metastore: MetaStore,
                      evictionPolicy: PartitionEvictionPolicy,
                      filodbConfig: Config)
                     (implicit val ioPool: ExecutionContext) extends StrictLogging {
  import collection.JavaConverters._

  import FiloSchedulers._
  import TimeSeriesShard._

  /////// START CONFIGURATION FIELDS ///////////////////

  private val clusterType = filodbConfig.getString("cluster-type")
  private val deploymentPartitionName = filodbConfig.getString("deployment-partition-name")
  private val targetMaxPartitions = filodbConfig.getInt("memstore.max-partitions-on-heap-per-shard")
  private val ensureTspHeadroomPercent = filodbConfig.getDouble("memstore.ensure-tsp-count-headroom-percent")
  private val ensureBlockHeadroomPercent = filodbConfig.getDouble("memstore.ensure-block-memory-headroom-percent")
  private val ensureNativeMemHeadroomPercent = filodbConfig.getDouble("memstore.ensure-native-memory-headroom-percent")
  private val indexFacetingEnabledShardKeyLabels =
                           filodbConfig.getBoolean("memstore.index-faceting-enabled-shard-key-labels")
  private val indexFacetingEnabledAllLabels = filodbConfig.getBoolean("memstore.index-faceting-enabled-for-all-labels")
  private val numParallelFlushes = filodbConfig.getInt("memstore.flush-task-parallelism")
  private val disableIndexCaching = filodbConfig.getBoolean("memstore.disable-index-caching")
  private val partKeyIndexType = filodbConfig.getString("memstore.part-key-index-type")


  /////// END CONFIGURATION FIELDS ///////////////////

  /////// START MEMBER STATE FIELDS ///////////////////

  val shardStats = new TimeSeriesShardStats(ref, shardNum)
  @volatile var isReadyForQuery = false

  private[memstore] val creationTime = System.currentTimeMillis()

  /**
    * Map of all partitions in the shard stored in memory, indexed by partition ID
    */
  private[memstore] val partitions = new NonBlockingHashMapLong[TimeSeriesPartition](InitialNumPartitions, false)

  /**
    * next partition ID number
    */
  private var nextPartitionID = 0

  /**
    * This index helps identify which partitions have any given column-value.
    * Used to answer queries not involving the full partition key.
    * Maintained using a high-performance bitmap index.
    */
  private[memstore] final val partKeyIndex: PartKeyIndexRaw = partKeyIndexType match {
    case "lucene" => new PartKeyLuceneIndex(ref, schemas.part,
      indexFacetingEnabledAllLabels, indexFacetingEnabledShardKeyLabels, shardNum,
      storeConfig.diskTTLSeconds * 1000, disableIndexCaching = disableIndexCaching)
    case "tantivy" => new PartKeyTantivyIndex(ref, schemas.part,
      shardNum, storeConfig.diskTTLSeconds * 1000)
    case x => sys.error(s"Unsupported part key index type: '$x'")
  }

  private val cardTracker: CardinalityTracker = initCardTracker()

  /**
    * Keeps track of count of rows ingested into memstore, not necessarily flushed.
    * This is generally used to report status and metrics.
    */
  private final var ingested = 0L

  private val trackQueriesHoldingEvictionLock = filodbConfig.getBoolean("memstore.track-queries-holding-eviction-lock")
  /**
   * Lock that protects chunks and TSPs from being reclaimed from Memstore.
   * This is needed to prevent races between ODP queries and reclaims and ensure that
   * TSPs and chunks dont get evicted when queries are being served.
   */
  private[memstore] final val evictionLock = new EvictionLock(trackQueriesHoldingEvictionLock,
    s"shard=$shardNum dataset=$ref")

  /**
   * Queue of partIds that are eligible for eviction since they have stopped ingesting.
   * Caller needs to double check ingesting status since they may have started to re-ingest
   * since partId was added to this queue.
   *
   * It is a set since intermittent time series can cause duplicates in evictablePartIds.
   * Max size multiplied by 1.04 to minimize array resizes.
   */
  protected[memstore] final val evictablePartIds =
      new EvictablePartIdQueueSet(math.ceil(targetMaxPartitions * 1.04).toInt)
  protected[memstore] final val evictableOdpPartIds = new EvictablePartIdQueueSet(8192)

  /**
    * Keeps track of last offset ingested into memory (not necessarily flushed).
    * This value is used to keep track of the checkpoint to be written for next flush for any group.
    */
  private final var _offset = Long.MinValue

  /**
   * The maximum blockMetaSize amongst all the schemas this Dataset could ingest
   */
  private[memstore] val maxMetaSize = schemas.schemas.values.map(_.data.blockMetaSize).max

  require (storeConfig.maxChunkTime > storeConfig.flushInterval, "MaxChunkTime should be greater than FlushInterval")
  private[memstore] val maxChunkTime = storeConfig.maxChunkTime.toMillis

  private val acceptDuplicateSamples = storeConfig.acceptDuplicateSamples

  // Called to remove chunks from ChunkMap of a given partition, when an offheap block is reclaimed
  private val reclaimListener = initReclaimListener()

  // Create a single-threaded scheduler just for ingestion.  Name the thread for ease of debugging
  // NOTE: to control intermixing of different Observables/Tasks in this thread, customize ExecutionModel param
  val ingestSched = Scheduler.singleThread(s"$IngestSchedName-$ref-$shardNum",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in TimeSeriesShard.ingestSched", _)))

  private[memstore] val blockMemorySize = {
    val size = if (filodbConfig.getBoolean("memstore.memory-alloc.automatic-alloc-enabled")) {
      val numNodes = filodbConfig.getInt("min-num-nodes-in-cluster")
      val availableMemoryBytes: Long = Utils.calculateAvailableOffHeapMemory(filodbConfig)
      val blockMemoryManagerPercent = filodbConfig.getDouble("memstore.memory-alloc.block-memory-manager-percent")
      val blockMemForDatasetPercent = storeConfig.shardMemPercent // fraction of block memory for this dataset
      val numShardsPerNode = Math.ceil(numShards / numNodes.toDouble)
      logger.info(s"Calculating Block memory size with automatic allocation strategy. " +
        s"Dataset dataset=$ref has blockMemForDatasetPercent=$blockMemForDatasetPercent " +
        s"numShardsPerNode=$numShardsPerNode")
      (availableMemoryBytes * blockMemoryManagerPercent *
        blockMemForDatasetPercent / 100 / 100 / numShardsPerNode).toLong
    } else {
      storeConfig.shardMemSize
    }
    logger.info(s"Block Memory for dataset=$ref shard=$shardNum bytesAllocated=$size")
    size
  }

  protected val numGroups = storeConfig.groupsPerShard
  private val chunkRetentionHours = (storeConfig.diskTTLSeconds / 3600).toInt
  private[memstore] val pagingEnabled = storeConfig.demandPagingEnabled

  /**
    * PartitionSet - access TSPartition using ingest record partition key in O(1) time.
    */
  private[memstore] final val partSet = PartitionSet.ofSize(InitialNumPartitions)
  // Use a StampedLock because it supports optimistic read locking. This means that no blocking
  // occurs in the common case, when there isn't any contention reading from partSet.
  private[memstore] final val partSetLock = new StampedLock

  // The off-heap block store used for encoded chunks
  private val shardTags = Map("dataset" -> ref.dataset, "shard" -> shardNum.toString)
  private val blockStore = new PageAlignedBlockManager(blockMemorySize, shardStats.memoryStats, reclaimListener,
    storeConfig.numPagesPerBlock, evictionLock)
  private[core] val blockFactoryPool = new BlockMemFactoryPool(blockStore, maxMetaSize, shardTags)

  // Requires blockStore.
  private val headroomTask = startHeadroomTask(ingestSched)

  val partitionMaker = new DemandPagedChunkStore(this, blockStore)

  private val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, reuseOneContainer = true)
  private val partKeyArray = partKeyBuilder.allContainers.head.base.asInstanceOf[Array[Byte]]
  private[memstore] val bufferPools = {
    val pools = schemas.schemas.values.map { sch =>
      sch.schemaHash -> new WriteBufferPool(bufferMemoryManager, sch.data, storeConfig)
    }
    DMap(pools.toSeq: _*)
  }

  private val shardInfo = TimeSeriesShardInfo(shardNum, shardStats, bufferPools, bufferMemoryManager)

  private final val partitionGroups = Array.fill(numGroups)(new EWAHCompressedBitmap)

  /**
    * Bitmap to track actively ingesting partitions.
    * This bitmap is maintained in addition to the ingesting flag per partition.
    * TSP.ingesting is MUCH faster than bit.get(i) but we need the bitmap for faster operations
    * for all partitions of shard (like ingesting cardinality counting, rollover of time buckets etc).
    */
  private[memstore] final val activelyIngesting = debox.Set.empty[Int]

  private val numFlushIntervalsDuringRetention = Math.ceil(chunkRetentionHours.hours / storeConfig.flushInterval).toInt

  // Use 1/4 of flush intervals within retention period for initial ChunkMap size
  private val initInfoMapSize = Math.max((numFlushIntervalsDuringRetention / 4) + 4, 20)

  /**
    * Dirty partitions whose start/end times have not been updated to cassandra.
    *
    * IMPORTANT. Only modify this var in IngestScheduler
    */
  private[memstore] final var dirtyPartitionsForIndexFlush = debox.Buffer.empty[Int]

  /**
    * This is the group during which this shard will flush dirty part keys. Randomized to
    * ensure we dont flush time buckets across shards at same time
    */
  private final val dirtyPartKeysFlushGroup = Random.nextInt(numGroups)
  logger.info(s"Dirty Part Keys for shard=$shardNum will flush in group $dirtyPartKeysFlushGroup")

  /**
    * The offset up to and including the last record in this group to be successfully persisted.
    * Also used during recovery to figure out what incoming records to skip (since it's persisted)
    */
  private final val groupWatermark = Array.fill(numGroups)(Long.MinValue)

  /**
    * Highest ingestion timestamp observed.
    */
  private[memstore] var lastIngestionTime = Long.MinValue

  // Flush groups when ingestion time is observed to cross a time boundary (typically an hour),
  // plus a group-specific offset. This simplifies disaster recovery -- chunks can be copied
  // without concern that they may overlap in time.
  private val flushBoundaryMillis = Option(storeConfig.flushInterval.toMillis)

  // Defines the group-specific flush offset, to distribute the flushes around such they don't
  // all flush at the same time. With an hourly boundary and 60 flush groups, flushes are
  // scheduled once a minute.
  private val flushOffsetMillis = flushBoundaryMillis.get / numGroups

  private[memstore] val evictedPartKeys =
    BloomFilter[PartKey](storeConfig.evictedPkBfCapacity, falsePositiveRate = 0.01)(new CanGenerateHashFrom[PartKey] {
      override def generateHash(from: PartKey): Long = {
        schemas.part.binSchema.partitionHash(from.base, from.offset)
      }
    })
  private var evictedPartKeysDisposed = false

  private val brRowReader = new MultiSchemaBRRowReader()

  /**
    * Detailed filtered ingestion record logging.  See "trace-filters" StoreConfig setting.  Warning: may blow up
    * logs, use at your own risk.
    */
  private val tracedPartFilters = storeConfig.traceFilters

  private[memstore] val ingestConsumer = new IngestConsumer()

  private[memstore] val addPartitionsDisabled = AtomicBoolean(false)

  /////// END MEMBER STATE FIELDS ///////////////////

  /////// START INNER CLASS DEFINITIONS ///////////////////

  /**
    * Iterate TimeSeriesPartition objects relevant to given partIds.
    */
  case class InMemPartitionIterator(intIt: IntIterator) extends PartitionIterator {
    var nextPart = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeriesPartition]
    val skippedPartIDs = debox.Buffer.empty[Int]
    private def findNext(): Unit = {
      while (intIt.hasNext && nextPart == UnsafeUtils.ZeroPointer) {
        val nextPartID = intIt.next
        nextPart = partitions.get(nextPartID)
        if (nextPart == UnsafeUtils.ZeroPointer) skippedPartIDs += nextPartID
      }
    }

    findNext()

    final def hasNext: Boolean = nextPart != UnsafeUtils.ZeroPointer
    final def next: TimeSeriesPartition = {
      val toReturn = nextPart
      nextPart = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeriesPartition] // reset so that we can keep going
      findNext()
      toReturn
    }
  }

  /**
    * Iterate TimeSeriesPartition objects relevant to given partIds.
    */
  case class InMemPartitionIterator2(partIds: debox.Buffer[Int]) extends PartitionIterator {
    var nextPart = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeriesPartition]
    val skippedPartIDs = debox.Buffer.empty[Int]
    var nextPartId = -1
    findNext()

    private def findNext(): Unit = {
      while (nextPartId + 1 < partIds.length && nextPart == UnsafeUtils.ZeroPointer) {
        nextPartId += 1
        nextPart = partitions.get(partIds(nextPartId))
        if (nextPart == UnsafeUtils.ZeroPointer) skippedPartIDs += partIds(nextPartId)
      }
    }

    final def hasNext: Boolean = nextPart != UnsafeUtils.ZeroPointer

    final def next: TimeSeriesPartition = {
      val toReturn = nextPart
      nextPart = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeriesPartition] // reset so that we can keep going
      findNext()
      toReturn
    }
  }

  // RECOVERY: Check the watermark for the group that this record is part of.  If the ingestOffset is < watermark,
  // then do not bother with the expensive partition key comparison and ingestion.  Just skip it
  class IngestConsumer(var ingestionTime: Long = 0,
                       var numActuallyIngested: Int = 0,
                       var ingestOffset: Long = -1L) extends BinaryRegionConsumer {
    // Receives a new ingestion BinaryRecord
    final def onNext(recBase: Any, recOffset: Long): Unit = {
      val schemaId = RecordSchema.schemaID(recBase, recOffset)
      val schema = schemas(schemaId)
      if (schema != Schemas.UnknownSchema) {
        val group = partKeyGroup(schema.ingestionSchema, recBase, recOffset, numGroups)
        if (ingestOffset < groupWatermark(group)) {
          shardStats.rowsSkipped.increment()
          try {
            // Need to add partition to update index with new partitions added during recovery with correct startTime.
            // This is important to do since the group designated for dirty part key persistence can
            // lag behind group the partition belongs to. Hence during recovery, we skip
            // ingesting the sample, but create the partition and mark it as dirty.
            // TODO:
            // explore aligning index time buckets with chunks, and we can then
            // remove this partition existence check per sample.
            val part: FiloPartition = getOrAddPartitionForIngestion(recBase, recOffset, group, schema)
            if (part == OutOfMemPartition) { disableAddPartitions() }
          } catch {
            case e: OutOfOffheapMemoryException => disableAddPartitions()
            case e: Exception                   => logger.error(s"Unexpected ingestion err", e); disableAddPartitions()
          }
        } else {
          getOrAddPartitionAndIngest(ingestionTime, recBase, recOffset, group, schema)
          numActuallyIngested += 1
        }
      } else {
        logger.debug(s"Unknown schema ID $schemaId will be ignored during ingestion")
        shardStats.unknownSchemaDropped.increment()
      }
    }
  }

  /**
   * Iterator for traversal of partIds, value for the given label will be extracted from the PartitionKey.
   * This is specifically implemented to avoid building a Map for single label.
   * this implementation maps partIds to label/values eagerly, this is done inorder to dedup the results.
   */
  case class SingleLabelValuesResultIterator(partIds: debox.Buffer[Int], label: String,
                                             querySession: QuerySession, statsGroup: Seq[String], limit: Int)
    extends Iterator[ZeroCopyUTF8String] {
    private val rows = labels
    override def size: Int = labels.size

    def labels: Iterator[ZeroCopyUTF8String] = {
      var partLoopIndx = 0
      val rows = new mutable.HashSet[ZeroCopyUTF8String]()
      val colIndex = schemas.part.binSchema.colNames.indexOf(label)
      while(partLoopIndx < partIds.length && rows.size < limit) {
        val partId = partIds(partLoopIndx)
        //retrieve PartKey either from In-memory map or from PartKeyIndex
        val nextPart = partKeyFromPartId(partId)
        if (colIndex > -1) //column value lookup (e.g metric), no need for map-consumer
          rows.add(schemas.part.binSchema.asZCUTF8Str(nextPart.base, nextPart.offset, colIndex))
        else
          schemas.part.binSchema.singleColValues(nextPart.base, nextPart.offset, label, rows)
        partLoopIndx += 1
      }
      shardStats.partkeyLabelScans.increment(partLoopIndx)
      querySession.queryStats.getTimeSeriesScannedCounter(statsGroup).addAndGet(partLoopIndx)
      rows.toIterator
    }

    def hasNext: Boolean = rows.hasNext

    def next(): ZeroCopyUTF8String = rows.next
  }

  /**
   * Iterator for traversal of partIds, value for the given labels will be extracted from the PartitionKey.
   * this implementation maps partIds to label/values eagerly, this is done inorder to dedup the results.
   */
  case class LabelValueResultIterator(partIds: debox.Buffer[Int], labelNames: Seq[String],
                                      querySession: QuerySession, statsGroup: Seq[String], limit: Int)
    extends Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] {
    private lazy val rows = labelValues
    override def size: Int = rows.size

    def labelValues: Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
      var partLoopIndx = 0
      val rows = new mutable.HashSet[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]()
      while(partLoopIndx < partIds.length && rows.size < limit) {
        val partId = partIds(partLoopIndx)

        //retrieve PartKey either from In-memory map or from PartKeyIndex
        val nextPart = partKeyFromPartId(partId)

        // FIXME This is non-performant and temporary fix for fetching label values based on filter criteria.
        // Other strategies needs to be evaluated for making this performant - create facets for predefined fields or
        // have a centralized service/store for serving metadata

        val currVal = schemas.part.binSchema.colValues(nextPart.base, nextPart.offset, labelNames).
          zipWithIndex.filter(_._1 != null).map{case(value, ind) => labelNames(ind).utf8 -> value.utf8}.toMap

        if (currVal.nonEmpty) rows.add(currVal)
        partLoopIndx += 1
      }
      querySession.queryStats.getTimeSeriesScannedCounter(statsGroup).addAndGet(partLoopIndx)
      rows.toIterator
    }

    override def hasNext: Boolean = rows.hasNext

    override def next(): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = rows.next
  }

  /////// END INNER CLASS DEFINITIONS ///////////////////

  /////// START INIT METHODS ///////////////////

  private def initCardTracker() = {
    if (storeConfig.meteringEnabled) {
      // FIXME switch this to some local-disk based store when we graduate out of POC mode
      val cardStore = new RocksDbCardinalityStore(ref, shardNum)

      val defaultQuota = quotaSource.getDefaults(ref)
      val tracker = new CardinalityTracker(ref, shardNum, schemas.part.options.shardKeyColumns.length,
        defaultQuota, cardStore)
      quotaSource.getQuotas(ref).foreach { q =>
        tracker.setQuota(q.shardKeyPrefix, q.quota)
      }
      tracker
    } else UnsafeUtils.ZeroPointer.asInstanceOf[CardinalityTracker]
  }

  private def initReclaimListener() = {
    new ReclaimListener {
      def onReclaim(metaAddr: BinaryVectorPtr, numBytes: Int): Unit = {
        val partID = UnsafeUtils.getInt(metaAddr)
        val partition = partitions.get(partID)
        if (partition != UnsafeUtils.ZeroPointer) {
          // The number of bytes passed in is the metadata size which depends on schema.  It should match the
          // TSPartition's blockMetaSize; if it doesn't that is a flag for possible corruption, and we should halt
          // the process to be safe and log details for further debugging.
          val chunkID = UnsafeUtils.getLong(metaAddr + 4)
          if (numBytes != partition.schema.data.blockMetaSize) {
            Shutdown.haltAndCatchFire(new RuntimeException(f"POSSIBLE CORRUPTION DURING onReclaim(" +
              f"metaAddr=0x$metaAddr%08x, numBytes=$numBytes)" +
              s"Expected meta size: ${partition.schema.data.blockMetaSize} for schema=${partition.schema}" +
              s"  Reclaiming chunk chunkID=$chunkID from shard=$shardNum " +
              s"partID=$partID ${partition.stringPartition}"))
          }
          partition.removeChunksAt(chunkID)
          logger.debug(s"Reclaiming chunk chunkID=$chunkID from shard=$shardNum " +
            s"partID=$partID ${partition.stringPartition}")
        }
      }
    }
  }
  /////// END INIT METHODS ///////////////////

  /////// START SHARD RECOVERY METHODS ///////////////////

  def recoverIndex(): Future[Long] = {
    val indexBootstrapper = new RawIndexBootstrapper(colStore)
    indexBootstrapper.bootstrapIndexRaw(partKeyIndex, shardNum, ref)(bootstrapPartKey)
      .executeOn(ingestSched) // to make sure bootstrapIndex task is run on ingestion thread
      .map { count =>
        startFlushingIndex()
        logger.info(s"Bootstrapped index for dataset=$ref shard=$shardNum with $count records")
        count
      }.runToFuture(ingestSched)
  }

  def startFlushingIndex(): Unit =
    partKeyIndex.startFlushThread(storeConfig.partIndexFlushMinDelaySeconds, storeConfig.partIndexFlushMaxDelaySeconds)

  /**
   * Handles actions to be performed for the shard upon bootstrapping
   * a partition key from index store
   * @param pk partKey
   * @return partId assigned to key
   */
  // scalastyle:off method.length
  private[memstore] def bootstrapPartKey(pk: PartKeyRecord): Int = {
    assertThreadName(IngestSchedName)
    val schemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
    val schema = schemas(schemaId)
    val partId = if (pk.endTime == Long.MaxValue) {
      // this is an actively ingesting partition
      val group = partKeyGroup(schemas.part.binSchema, pk.partKey, UnsafeUtils.arayOffset, numGroups)
      if (schema != Schemas.UnknownSchema) {
        val part = createNewPartition(pk.partKey, UnsafeUtils.arayOffset, group, CREATE_NEW_PARTID, schema, false, 4)
        // In theory, we should not get an OutOfMemPartition here since
        // it should have occurred before node failed too, and with data stopped,
        // index would not be updated. But if for some reason we see it, drop data
        if (part == OutOfMemPartition) {
          logger.error("Could not accommodate partKey while recovering index. " +
            "WriteBuffer size may not be configured correctly")
          -1
        } else {
          val stamp = partSetLock.writeLock()
          try {
            partSet.add(part) // createNewPartition doesn't add part to partSet
            part.ingesting = true
            part.partID
          } finally {
            partSetLock.unlockWrite(stamp)
          }
        }
      } else {
        logger.info(s"Ignoring part key with unknown schema ID $schemaId")
        shardStats.unknownSchemaDropped.increment()
        -1
      }
    } else {
      // partition assign a new partId to non-ingesting partition,
      // but no need to create a new TSPartition heap object
      // instead add the partition to evictedPArtKeys bloom filter so that it can be found if necessary
      evictedPartKeys.synchronized {
        require(!evictedPartKeysDisposed)
        evictedPartKeys.add(PartKey(pk.partKey, UnsafeUtils.arayOffset))
      }
      createPartitionID()
    }

    activelyIngesting.synchronized {
      if (pk.endTime == Long.MaxValue) activelyIngesting += partId
      else activelyIngesting -= partId
    }
    shardStats.indexRecoveryNumRecordsProcessed.increment()
    if (schema != Schemas.UnknownSchema) {
      if (storeConfig.meteringEnabled) {
        val shardKey = schema.partKeySchema.colValues(pk.partKey, UnsafeUtils.arayOffset,
          schema.options.shardKeyColumns)
        modifyCardinalityCountNoThrow(shardKey, 1, if (pk.endTime == Long.MaxValue) 1 else 0)
      }
    }
    partId
  }

  /**
   * WARNING: Not performant. Use only in tests, or during initial bootstrap.
   */
  def refreshPartKeyIndexBlocking(): Unit = partKeyIndex.refreshReadersBlocking()

  /**
   * Sets the watermark for each subgroup.  If an ingested record offset is below this watermark then it will be
   * assumed to already have been persisted, and the record will be discarded.  Use only for recovery.
   * @param watermarks a Map from group number to watermark
   */
  def setGroupWatermarks(watermarks: Map[Int, Long]): Unit =
    watermarks.foreach { case (group, mark) => groupWatermark(group) = mark }

  def createDataRecoveryObservable(dataset: DatasetRef, shardNum: Int,
                                   dataStream: Observable[SomeData],
                                   startOffset: Long,
                                   endOffset: Long,
                                   checkpoints: Map[Int, Long],
                                   reportingInterval: Long,
                                   timeout: FiniteDuration): Observable[Long] = {
    setGroupWatermarks(checkpoints)
    if (endOffset < startOffset) Observable.empty
    else {
      var targetOffset = startOffset + reportingInterval
      var startOffsetValidated = false
      dataStream.map { r =>
        if (!startOffsetValidated) {
          if (r.offset > startOffset) {
            val offsetsNotRecovered = r.offset - startOffset
            logger.error(s"Could not recover dataset=$dataset shard=$shardNum from check pointed offset possibly " +
              s"because of retention issues. recoveryStartOffset=$startOffset " +
              s"firstRecordOffset=${r.offset} offsetsNotRecovered=$offsetsNotRecovered")
            shardStats.offsetsNotRecovered.increment(offsetsNotRecovered)
          }
          startOffsetValidated = true
        }
        ingest(r)
      }
        // Nothing to read from source, blocking indefinitely. In such cases, 60sec timeout causes it
        // to return endOffset, making recovery complete and to start normal ingestion.
        .timeoutOnSlowUpstreamTo(timeout, Observable.now(endOffset))
        .collect {
          case offset: Long if offset >= endOffset => // last offset reached
            offset
          case offset: Long if offset > targetOffset => // reporting interval reached
            targetOffset += reportingInterval
            offset
        }
    }
  }

  /////// END SHARD RECOVERY METHODS ///////////////////

  /////// START SHARD INGESTION METHODS ///////////////////

  /**
   * Returns a new non-negative partition ID which isn't used by any existing parition. A negative
   * partition ID wouldn't work with bitmaps.
   */
  private def createPartitionID(): Int = {
    assertThreadName(IngestSchedName)
    val id = nextPartitionID

    // It's unlikely that partition IDs will wrap around, and it's unlikely that collisions
    // will be encountered. In case either of these conditions occur, keep incrementing the id
    // until no collision is detected. A given shard is expected to support up to 1M actively
    // ingesting partitions, and so in the worst case, the loop might run for up to ~100ms.
    // Afterwards, a complete wraparound is required for collisions to be detected again.

    do {
      nextPartitionID += 1
      if (nextPartitionID < 0) {
        nextPartitionID = 0
        logger.info(s"dataset=$ref shard=$shardNum nextPartitionID has wrapped around to 0 again")
      }
    } while (partitions.containsKey(nextPartitionID))

    id
  }

  /**
   * Ingest new BinaryRecords in a RecordContainer to this shard.
   * Skips rows if the offset is below the group watermark for that record's group.
   * Adds new partitions if needed.
   */
  def ingest(container: RecordContainer, offset: Long): Long = {
    assertThreadName(IngestSchedName)
    if (container.isCurrentVersion) {
      if (!container.isEmpty) {
        ingestConsumer.ingestionTime = container.timestamp
        ingestConsumer.numActuallyIngested = 0
        ingestConsumer.ingestOffset = offset
        brRowReader.recordBase = container.base
        container.consumeRecords(ingestConsumer)
        shardStats.rowsIngested.increment(ingestConsumer.numActuallyIngested)
        shardStats.rowsPerContainer.record(ingestConsumer.numActuallyIngested)
        ingested += ingestConsumer.numActuallyIngested
        _offset = offset
      }
      else {
        // Adding this log line to debug the shard stuck in recovery scenario(s)
        logger.error(s"[Container Empty] record-offset: ${offset} last-ingested-offset: ${_offset}")
      }
    } else {
      shardStats.oldContainers.increment()
    }
    _offset
  }

  def ingest(data: SomeData): Long = ingest(data.records, data.offset)

  def startIngestion(dataStream: Observable[SomeData],
                     cancelTask: Task[Unit],
                     flushSched: Scheduler): CancelableFuture[Unit] = {
    dataStream.flatMap {
      case d: SomeData =>
        // The write buffers for all partitions in a group are switched here, in line with ingestion
        // stream.  This avoids concurrency issues and ensures that buffers for a group are switched
        // at the same offset/watermark
        val tasks = createFlushTasks(d.records)
        ingest(d)
        Observable.fromIterable(tasks)
    }
      .mapParallelUnordered(numParallelFlushes) {
        // asyncBoundary so subsequent computations in pipeline happen in default threadpool
        task => task.executeOn(flushSched).asyncBoundary
      }
      .completedL
      .doOnCancel(cancelTask)
      .runToFuture(ingestSched)
  }

  private[memstore] def updatePartEndTimeInIndex(p: TimeSeriesPartition, endTime: Long): Unit =
    partKeyIndex.updatePartKeyWithEndTime(p.partKeyBytes, p.partID, endTime)()

  private def updateIndexWithEndTime(p: TimeSeriesPartition,
                                     partFlushChunks: Iterator[ChunkSet],
                                     dirtyParts: debox.Buffer[Int]): Unit = {
    // TODO re-enable following assertion. Am noticing that monix uses TrampolineExecutionContext
    // causing the iterator to be consumed synchronously in some cases. It doesnt
    // seem to be consistent environment to environment.
    //assertThreadName(IOSchedName)

    // Below is coded to work concurrently with logic in getOrAddPartitionAndIngest
    // where we try to activate an inactive time series
    activelyIngesting.synchronized {
      if (partFlushChunks.isEmpty && p.ingesting) {
        var endTime = p.timestampOfLatestSample
        if (endTime == -1) endTime = System.currentTimeMillis() // this can happen if no sample after reboot
        updatePartEndTimeInIndex(p, endTime)
        dirtyParts += p.partID
        val oldActivelyIngestingSize = activelyIngesting.size
        activelyIngesting -= p.partID

        markPartAsNotIngesting(p, odp = false)
        if (storeConfig.meteringEnabled) {
          val shardKey = p.schema.partKeySchema.colValues(p.partKeyBase, p.partKeyOffset,
                                                          p.schema.options.shardKeyColumns)
          cardTracker.modifyCount(shardKey, 0, -1)
        }
      }
    }
  }

  protected[memstore] def markPartAsNotIngesting(p: TimeSeriesPartition, odp: Boolean): Unit = {
    p.ingesting = false
    if (odp) {
      evictableOdpPartIds.put(p.partID)
    } else {
      evictablePartIds.put(p.partID)
    }
    shardStats.evictablePartKeysSize.increment()
  }

  private def getOrAddPartitionForIngestion(recordBase: Any, recordOff: Long,
                                            group: Int, schema: Schema) = {
    var part = partSet.getWithIngestBR(recordBase, recordOff, schema)
    if (part == null) {
      part = addPartitionForIngestion(recordBase, recordOff, schema, group)
    }
    part
  }

  /**
   * Looks up the previously assigned partId of a possibly evicted partition.
   * @return partId >=0 if one is found, CREATE_NEW_PARTID (-1) if not found.
   */
  private def lookupPreviouslyAssignedPartId(partKeyBase: Array[Byte], partKeyOffset: Long): Int = {
    assertThreadName(IngestSchedName)
    shardStats.evictedPartKeyBloomFilterQueries.increment()

    val mightContain = evictedPartKeys.synchronized {
      if (!evictedPartKeysDisposed) {
        evictedPartKeys.mightContain(PartKey(partKeyBase, partKeyOffset))
      } else {
        false
      }
    }

    if (mightContain) {
      partKeyIndex.partIdFromPartKeySlow(partKeyBase, partKeyOffset)
        .getOrElse {
          shardStats.evictedPartKeyBloomFilterFalsePositives.increment()
          CREATE_NEW_PARTID
        }
    } else CREATE_NEW_PARTID
  }

  /**
   * Adds new partition with appropriate partId. If it is a newly seen partKey, then new partId is assigned.
   * If it is a previously seen partKey that is already in index, it reassigns same partId so that indexes
   * are still valid.
   *
   * This method also updates lucene index and dirty part keys properly.
   */
  private def addPartitionForIngestion(recordBase: Any, recordOff: Long, schema: Schema, group: Int) = {
    assertThreadName(IngestSchedName)
    // TODO: remove when no longer needed - or figure out how to log only for tracing partitions
    logger.trace(s"Adding ingestion record details: ${schema.ingestionSchema.debugString(recordBase, recordOff)}")
    val partKeyOffset = schema.comparator.buildPartKeyFromIngest(recordBase, recordOff, partKeyBuilder)
    val previousPartId = lookupPreviouslyAssignedPartId(partKeyArray, partKeyOffset)
    // TODO: remove when no longer needed
    logger.trace(s"Adding part key details: ${schema.partKeySchema.debugString(partKeyArray, partKeyOffset)}")
    val newPart = createNewPartition(partKeyArray, partKeyOffset, group, previousPartId, schema, false)
    if (newPart != OutOfMemPartition) {
      val partId = newPart.partID
      val startTime = schema.ingestionSchema.getLong(recordBase, recordOff, 0)
      val shardKey = schema.partKeySchema.colValues(newPart.partKeyBase, newPart.partKeyOffset,
        schema.options.shardKeyColumns)
      if (previousPartId == CREATE_NEW_PARTID) {
        // add new lucene entry if this partKey was never seen before
        // causes endTime to be set to Long.MaxValue
        partKeyIndex.addPartKey(newPart.partKeyBytes, partId, startTime)()
        if (storeConfig.meteringEnabled) {
          modifyCardinalityCountNoThrow(shardKey, 1, 1)
        }
      } else {
        // newly created partition is re-ingesting now, so update endTime
        updatePartEndTimeInIndex(newPart, Long.MaxValue)
        if (storeConfig.meteringEnabled) {
          modifyCardinalityCountNoThrow(shardKey, 0, 1)
          // TODO remove temporary debugging since we were seeing some negative counts when this increment was absent
          if (partId % 100 < 5) { // log for 5% of the cases
            logger.info(s"Increment activeDelta for ${newPart.stringPartition}")
          }
        }
      }
      dirtyPartitionsForIndexFlush += partId // marks this part as dirty so startTime is flushed
      activelyIngesting.synchronized {
        activelyIngesting += partId
        newPart.ingesting = true
      }
      val stamp = partSetLock.writeLock()
      try {
        partSet.add(newPart)
      } finally {
        partSetLock.unlockWrite(stamp)
      }
    }
    newPart
  }

  /**
   * Retrieves or creates a new TimeSeriesPartition, updating indices, then ingests the sample from record.
   * partition portion of ingest BinaryRecord is used to look up existing TSPartition.
   * Copies the partition portion of the ingest BinaryRecord to offheap write buffer memory.
   * NOTE: ingestion is skipped if there is an error allocating WriteBuffer space.
   * @param recordBase the base of the ingestion BinaryRecord
   * @param recordOff the offset of the ingestion BinaryRecord
   * @param group the group number, from abs(record.partitionHash % numGroups)
   */
  def getOrAddPartitionAndIngest(ingestionTime: Long,
                                 recordBase: Any, recordOff: Long,
                                 group: Int, schema: Schema): Unit = {
    assertThreadName(IngestSchedName)
    try {
      val part: FiloPartition = getOrAddPartitionForIngestion(recordBase, recordOff, group, schema)
      if (part == OutOfMemPartition) {
        disableAddPartitions()
      }
      else {
        val tsp = part.asInstanceOf[TimeSeriesPartition]
        brRowReader.schema = schema.ingestionSchema
        brRowReader.recordOffset = recordOff
        tsp.ingest(ingestionTime, brRowReader, blockFactoryPool.checkoutForOverflow(group),
          storeConfig.timeAlignedChunksEnabled, flushBoundaryMillis, acceptDuplicateSamples, maxChunkTime)
        // time series was inactive and has just started re-ingesting
        if (!tsp.ingesting) {
          // DO NOT use activelyIngesting to check above condition since it is slow and is called for every sample
          activelyIngesting.synchronized {
            if (!tsp.ingesting) {
              // This is coded to work concurrently with logic in updateIndexWithEndTime
              // where we try to de-activate an active time series.
              // Checking ts.ingesting second time needed since the time series may have ended
              // ingestion in updateIndexWithEndTime during the wait time of lock acquisition.
              // DO NOT remove the second tsp.ingesting check without understanding this fully.
              updatePartEndTimeInIndex(part.asInstanceOf[TimeSeriesPartition], Long.MaxValue)
              dirtyPartitionsForIndexFlush += part.partID
              activelyIngesting += part.partID
              tsp.ingesting = true
              val shardKey = tsp.schema.partKeySchema.colValues(tsp.partKeyBase, tsp.partKeyOffset,
                tsp.schema.options.shardKeyColumns)
              if (storeConfig.meteringEnabled) {
                modifyCardinalityCountNoThrow(shardKey, 0, 1)
              }
            }
          }
        }
      }
    } catch {
      case e: OutOfOffheapMemoryException => disableAddPartitions()
      case e: Exception =>
        shardStats.dataDropped.increment()
        logger.error(s"Unexpected ingestion err in dataset=$ref " +
          s"shard=$shardNum partition=${schema.ingestionSchema.debugString(recordBase, recordOff)}", e)
    }
  }

  private def shouldTrace(partKeyAddr: Long): Boolean = {
    tracedPartFilters.nonEmpty && {
      val partKeyPairs = schemas.part.binSchema.toStringPairs(UnsafeUtils.ZeroPointer, partKeyAddr)
      tracedPartFilters.forall(p => partKeyPairs.contains(p))
    }
  }

  /**
   * Creates new partition and adds them to the shard data structures. DOES NOT update
   * lucene index. It is the caller's responsibility to add or skip that step depending on the situation.
   *
   * @param usePartId pass CREATE_NEW_PARTID to force creation of new partId instead of using one that is passed in
   */
  protected def createNewPartition(partKeyBase: Array[Byte], partKeyOffset: Long,
                                   group: Int, usePartId: Int, schema: Schema,
                                   odp: Boolean,
                                   initMapSize: Int = initInfoMapSize): TimeSeriesPartition = {
    assertThreadName(IngestSchedName)
    if (partitions.size() >= targetMaxPartitions) {
      disableAddPartitions()
    }
    // Check and evict, if after eviction we still don't have enough memory, then don't proceed
    // We do not evict in ODP cases since we do not want eviction of ODP partitions that we already paged
    // in for same query. Calling this as ODP cannibalism. :-)
    if (addPartitionsDisabled() && !odp) evictForHeadroom()
    if (addPartitionsDisabled()) OutOfMemPartition
    else {
      // PartitionKey is copied to offheap bufferMemory and stays there until it is freed
      // NOTE: allocateAndCopy and allocNew below could fail if there isn't enough memory.  It is CRUCIAL
      // that min-write-buffers-free setting is large enough to accommodate the below use cases ALWAYS
      val (_, partKeyAddr, _) = BinaryRegionLarge.allocateAndCopy(partKeyBase, partKeyOffset, bufferMemoryManager)
      val partId = if (usePartId == CREATE_NEW_PARTID) createPartitionID() else usePartId
      val newPart = if (shouldTrace(partKeyAddr)) {
        logger.debug(s"Adding tracing TSPartition dataset=$ref shard=$shardNum group=$group partId=$partId")
        new TracingTimeSeriesPartition(partId, ref, schema, partKeyAddr, shardInfo, initMapSize)
      } else {
        new TimeSeriesPartition(partId, schema, partKeyAddr, shardInfo, initMapSize)
      }
      partitions.put(partId, newPart)
      shardStats.partitionsCreated.increment()
      partitionGroups(group).set(partId)
      newPart
    }
  }

  /**
   * Modifies a cardinality count and catches/logs any exceptions.
   * Should only be used where an exception would otherwise cause ingestion/boostrap to fail.
   */
  private def modifyCardinalityCountNoThrow(shardKey: Seq[String],
                                            totalDelta: Int,
                                            activeDelta: Int): Unit = {
    try {
      cardTracker.modifyCount(shardKey, totalDelta, activeDelta)
    } catch {
      case t: Throwable =>
          logger.error("exception while modifying cardinality tracker count; shardKey=" + shardKey, t)
    }
  }

  /////// END SHARD INGESTION METHODS ///////////////////

  /////// START SHARD FLUSH METHODS ///////////////////

  /**
   * Prepares the given group for flushing.  This MUST be done in the same thread/stream as
   * input records to avoid concurrency issues, and to ensure that all the partitions in a
   * group are switched at the same watermark. Also required because this method removes
   * entries from the partition data structures.
   */
  def prepareFlushGroup(groupNum: Int): FlushGroup = {
    assertThreadName(IngestSchedName)

    // Rapidly switch all of the input buffers for a particular group
    logger.debug(s"Switching write buffers for group $groupNum in dataset=$ref shard=$shardNum")
    InMemPartitionIterator(partitionGroups(groupNum).intIterator)
      .foreach(_.switchBuffers(blockFactoryPool.checkoutForOverflow(groupNum)))

    val dirtyPartKeys = if (groupNum == dirtyPartKeysFlushGroup) {
      purgeExpiredPartitions()
      ensureCapOnEvictablePartIds()
      logger.debug(s"Switching dirty part keys in dataset=$ref shard=$shardNum out for flush. ")
      val old = dirtyPartitionsForIndexFlush
      dirtyPartitionsForIndexFlush = debox.Buffer.empty[Int]
      old
    } else {
      debox.Buffer.ofSize[Int](0)
    }

    FlushGroup(shardNum, groupNum, latestOffset, dirtyPartKeys)
  }

  private def purgeExpiredPartitions(): Unit = ingestSched.executeTrampolined { () =>
    assertThreadName(IngestSchedName)
    // TODO Much of the purging work other of removing TSP from shard data structures can be done
    // asynchronously on another thread. No need to block ingestion thread for this.
    val start = System.currentTimeMillis()
    val partsToPurge = partKeyIndex.partIdsEndedBefore(start - storeConfig.diskTTLSeconds * 1000)
    val removedParts = debox.Buffer.empty[Int]
    val partIter = InMemPartitionIterator2(partsToPurge)
    partIter.foreach { p =>
      if (!p.ingesting) {
        logger.debug(s"Purging partition with partId=${p.partID}  ${p.stringPartition} from " +
          s"memory in dataset=$ref shard=$shardNum")
        val schema = p.schema
        val shardKey = schema.partKeySchema.colValues(p.partKeyBase, p.partKeyOffset, schema.options.shardKeyColumns)
        if (storeConfig.meteringEnabled) {
          try {
            cardTracker.decrementCount(shardKey)
          } catch { case e: Exception =>
            logger.error("Got exception when reducing cardinality in tracker", e)
          }
        }
        removePartition(p)
        removedParts += p.partID
      }
    }
    partIter.skippedPartIDs.foreach { pId =>
      partKeyIndex.partKeyFromPartId(pId).foreach { pk =>
        val unsafePkOffset = PartKeyIndexRaw.bytesRefToUnsafeOffset(pk.offset)
        val schema = schemas(RecordSchema.schemaID(pk.bytes, unsafePkOffset))
        val shardKey = schema.partKeySchema.colValues(pk.bytes, unsafePkOffset,
          schemas.part.options.shardKeyColumns)
        if (storeConfig.meteringEnabled) {
          try {
            cardTracker.decrementCount(shardKey)
          } catch { case e: Exception =>
            logger.error("Got exception when reducing cardinality in tracker", e)
          }
        }
      }
    }
    partKeyIndex.removePartKeys(partIter.skippedPartIDs)
    partKeyIndex.removePartKeys(removedParts)
    if (removedParts.length + partIter.skippedPartIDs.length > 0)
      logger.info(s"Purged ${removedParts.length} partitions from memory/index " +
        s"and ${partIter.skippedPartIDs.length} from index only from dataset=$ref shard=$shardNum")
    shardStats.purgedPartitions.increment(removedParts.length)
    shardStats.purgedPartitionsFromIndex.increment(removedParts.length + partIter.skippedPartIDs.length)
    shardStats.purgePartitionTimeMs.increment(System.currentTimeMillis() - start)
  }

  /**
   * Creates zero or more flush tasks (up to the number of flush groups) based on examination
   * of the record container's ingestion time. This should be called before ingesting the container.
   *
   * Note that the tasks returned by this method aren't executed yet. The caller decides how
   * to run the tasks, and by which threads.
   */
  def createFlushTasks(container: RecordContainer): Seq[Task[Response]] = {
    val tasks = new ArrayBuffer[Task[Response]]()

    var oldTimestamp = lastIngestionTime
    val ingestionTime = Math.max(oldTimestamp, container.timestamp) // monotonic clock
    var newTimestamp = ingestionTime

    if (newTimestamp > oldTimestamp && oldTimestamp != Long.MinValue) {
      cforRange ( 0 until numGroups ) { group =>
        /* Logically, the task creation filter is as follows:

           // Compute the time offset relative to the group number. 0 min, 1 min, 2 min, etc.
           val timeOffset = group * flushOffsetMillis

           // Adjust the timestamp relative to the offset such that the
           // division rounds correctly.
           val oldTimestampAdjusted = oldTimestamp - timeOffset
           val newTimestampAdjusted = newTimestamp - timeOffset

           if (oldTimstampAdjusted / flushBoundary != newTimestampAdjusted / flushBoundary) {
             ...

           As written the code the same thing but with fewer operations. It's also a bit
           shorter, but you also had to read this comment...
         */
        if (oldTimestamp / flushBoundaryMillis.get != newTimestamp / flushBoundaryMillis.get) {
          // Flush out the group before ingesting records for a new hour (by group offset).
          tasks += createFlushTask(prepareFlushGroup(group))
        }
        oldTimestamp -= flushOffsetMillis
        newTimestamp -= flushOffsetMillis
      }
    }

    val currentTime = System.currentTimeMillis()
    shardStats.ingestionPipelineLatency.record(currentTime - container.timestamp)
    // Only update stuff if no exception was thrown.

    if (ingestionTime != lastIngestionTime) {
      lastIngestionTime = ingestionTime
      shardStats.ingestionClockDelay.update(currentTime - ingestionTime)
    }

    tasks
  }

  private def createFlushTask(flushGroup: FlushGroup): Task[Response] = {
    assertThreadName(IngestSchedName)
    // clone the bitmap so that reads on the flush thread do not conflict with writes on ingestion thread
    val partitionIt = InMemPartitionIterator(partitionGroups(flushGroup.groupNum).clone().intIterator)
    doFlushSteps(flushGroup, partitionIt)
  }

  private def updateGauges(): Unit = {
    assertThreadName(IngestSchedName)
    shardStats.bufferPoolSize.update(bufferPools.valuesArray.map(_.poolSize).sum)
    shardStats.indexEntries.update(partKeyIndex.indexNumEntries)
    shardStats.indexBytes.update(partKeyIndex.indexRamBytes)
    shardStats.numPartitions.update(numActivePartitions)
    val numIngesting = activelyIngesting.synchronized { activelyIngesting.size }
    shardStats.numActivelyIngestingParts.update(numIngesting)

    // Also publish MemFactory stats. Instance is expected to be shared, but no harm in
    // publishing a little more often than necessary.
    bufferMemoryManager.updateStats()
  }

  private def toPartKeyRecord(p: TimeSeriesPartition): PartKeyRecord = {
    assertThreadName(IOSchedName)
    var startTime = partKeyIndex.startTimeFromPartId(p.partID)
    if (startTime == -1) startTime = p.earliestTime // can remotely happen since lucene reads are eventually consistent
    if (startTime == Long.MaxValue) startTime = 0 // if for any reason we cant find the startTime, use 0
    val endTime = if (p.ingesting) {
      Long.MaxValue
    } else {
      val et = p.timestampOfLatestSample  // -1 can be returned if no sample after reboot
      if (et == -1) System.currentTimeMillis() else et
    }
    PartKeyRecord(p.partKeyBytes, startTime, endTime, shardNum)
  }

  // scalastyle:off method.length
  private def doFlushSteps(flushGroup: FlushGroup,
                           partitionIt: Iterator[TimeSeriesPartition]): Task[Response] = {
    assertThreadName(IngestSchedName)
    val flushStart = System.currentTimeMillis()

    // Only allocate the blockHolder when we actually have chunks/partitions to flush
    val blockHolder = blockFactoryPool.checkoutForFlush(flushGroup.groupNum)

    val chunkSetIter = partitionIt.flatMap { p =>
      // TODO re-enable following assertion. Am noticing that monix uses TrampolineExecutionContext
      // causing the iterator to be consumed synchronously in some cases. It doesnt
      // seem to be consistent environment to environment.
      // assertThreadName(IOSchedName)

      /* Step 2: Make chunks to be flushed for each partition */
      val chunks = p.makeFlushChunks(blockHolder)

      /* VERY IMPORTANT: This block is lazy and is executed when chunkSetIter is consumed
         in writeChunksFuture below */

      /* Step 4: Update endTime of all partKeys that stopped ingesting in this flush period. */
      updateIndexWithEndTime(p, chunks, flushGroup.dirtyPartsToFlush)
      chunks
    }

    // Note that all cassandra writes below  will have included retries. Failures after retries will imply data loss
    // in order to keep the ingestion moving. It is important that we don't fall back far behind.

    /* Step 1: Kick off partition iteration to persist chunks to column store */
    val writeChunksFuture = writeChunks(flushGroup, chunkSetIter, partitionIt, blockHolder)

    /* Step 5.2: We flush dirty part keys in the one designated group for each shard.
     * We recover future since we want to proceed to write dirty part keys even if chunk flush failed.
     * This is done after writeChunksFuture because chunkSetIter is lazy. More partKeys could
     * be added during iteration due to endTime detection
     */
    val writeDirtyPartKeysFuture = writeChunksFuture.recover {case _ => Success}
      .flatMap( _=> writeDirtyPartKeys(flushGroup))

    /* Step 6: Checkpoint after dirty part keys and chunks are flushed */
    val result = Future.sequence(Seq(writeChunksFuture, writeDirtyPartKeysFuture)).map {
      _.find(_.isInstanceOf[ErrorResponse]).getOrElse(Success)
    }.flatMap {
      case Success           => commitCheckpoint(ref, shardNum, flushGroup)
      case er: ErrorResponse => Future.successful(er)
    }.recover { case e =>
      logger.error(s"Internal Error when persisting chunks in dataset=$ref shard=$shardNum - should " +
        s"have not reached this state", e)
      DataDropped
    }
    result.onComplete { resp =>
      assertThreadName(IngestSchedName)
      try {
        // COMMENTARY ON BUG FIX DONE: Mark used blocks as reclaimable even on failure. Even if cassandra write fails
        // or other errors occur, we cannot leave blocks as not reclaimable and also release the factory back into pool.
        // Earlier, we were not calling this with the hope that next use of the blockMemFactory will mark them
        // as reclaimable. But the factory could be used for a different flush group. Not the same one. It can
        // succeed, and the wrong blocks can be marked as reclaimable.
        // Can try out tracking unreclaimed blockMemFactories without releasing, but it needs to be separate PR.
        blockHolder.markFullBlocksReclaimable()
        blockFactoryPool.release(blockHolder)
        flushDoneTasks(flushGroup, resp)
        shardStats.chunkFlushTaskLatency.record(System.currentTimeMillis() - flushStart)
      } catch { case e: Throwable =>
        logger.error(s"Error when wrapping up doFlushSteps in dataset=$ref shard=$shardNum", e)
      }
    }(ingestSched)
    // Note: The data structures accessed by flushDoneTasks can only be safely accessed by the
    //       ingestion thread, hence the onComplete steps are run from that thread.
    Task.fromFuture(result)
  }

  private def commitCheckpoint(ref: DatasetRef, shardNum: Int, flushGroup: FlushGroup): Future[Response] = {
    assertThreadName(IOSchedName)
    // negative checkpoints are refused by Kafka, and also offsets should be positive
    if (flushGroup.flushWatermark > 0) {
      val fut = metastore.writeCheckpoint(ref, shardNum, flushGroup.groupNum, flushGroup.flushWatermark).map { r =>
        shardStats.flushesSuccessful.increment()
        r
      }.recover { case e =>
        logger.error(s"Critical! Checkpoint persistence skipped in dataset=$ref shard=$shardNum", e)
        shardStats.flushesFailedOther.increment()
        // skip the checkpoint write
        // Sorry - need to skip to keep the ingestion moving
        DataDropped
      }
      // Update stats
      if (_offset >= 0) shardStats.offsetLatestInMem.update(_offset)
      groupWatermark(flushGroup.groupNum) = flushGroup.flushWatermark
      val maxWatermark = groupWatermark.max
      val minWatermark = groupWatermark.min
      if (maxWatermark >= 0) shardStats.offsetLatestFlushed.update(maxWatermark)
      if (minWatermark >= 0) shardStats.offsetEarliestFlushed.update(minWatermark)
      fut
    } else {
      Future.successful(NotApplied)
    }
  }

  protected def flushDoneTasks(flushGroup: FlushGroup, resTry: Try[Response]): Unit = {
    assertThreadName(IngestSchedName)
    resTry.foreach { resp =>
      logger.info(s"Flush of dataset=$ref shard=$shardNum group=${flushGroup.groupNum} " +
        s"flushWatermark=${flushGroup.flushWatermark} response=$resp offset=${_offset}")
    }
    updateGauges()
  }

  // scalastyle:off method.length
  private def writeDirtyPartKeys(flushGroup: FlushGroup): Future[Response] = {
    assertThreadName(IOSchedName)
    val partKeyRecords = InMemPartitionIterator2(flushGroup.dirtyPartsToFlush).map(toPartKeyRecord)
    val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
    colStore.writePartKeys(ref, shardNum,
      Observable.fromIteratorUnsafe(partKeyRecords),
      storeConfig.diskTTLSeconds, updateHour).map { resp =>
      if (flushGroup.dirtyPartsToFlush.length > 0) {
        logger.info(s"Finished flush of partKeys numPartKeys=${flushGroup.dirtyPartsToFlush.length}" +
          s" resp=$resp for dataset=$ref shard=$shardNum")
        shardStats.numDirtyPartKeysFlushed.increment(flushGroup.dirtyPartsToFlush.length)
      }
      resp
    }.recover { case e =>
      logger.error(s"Internal Error when persisting part keys in dataset=$ref shard=$shardNum - " +
        "should have not reached this state", e)
      DataDropped
    }
  }
  // scalastyle:on method.length

  private def writeChunks(flushGroup: FlushGroup,
                          chunkSetIt: Iterator[ChunkSet],
                          partitionIt: Iterator[TimeSeriesPartition],
                          blockHolder: BlockMemFactory): Future[Response] = {
    assertThreadName(IngestSchedName)

    val chunkSetStream = Observable.fromIteratorUnsafe(chunkSetIt)
    logger.debug(s"Created flush ChunkSets stream for group ${flushGroup.groupNum} in " +
      s"dataset=$ref shard=$shardNum")

    colStore.write(ref, chunkSetStream, storeConfig.diskTTLSeconds).recover { case e =>
      logger.error(s"Critical! Chunk persistence failed after retries and skipped in dataset=$ref " +
        s"shard=$shardNum", e)
      shardStats.flushesFailedChunkWrite.increment()

      // Encode and free up the remainder of the WriteBuffers that have not been flushed yet.  Otherwise they will
      // never be freed.
      partitionIt.foreach(_.encodeAndReleaseBuffers(blockHolder))
      // If the above futures fail with ErrorResponse because of DB failures, skip the chunk.
      // Sorry - need to drop the data to keep the ingestion moving
      DataDropped
    }
  }

  /////// END SHARD FLUSH METHODS ///////////////////

  /////// START TIME SERIES EVICTION / REMOVAL METHODS ///////////////////

  private def startHeadroomTask(sched: Scheduler) = {
    sched.scheduleWithFixedDelay(1, 1, TimeUnit.MINUTES, new Runnable {
      def run() = {
        evictForHeadroom()
      }
    })
  }

  /**
   * Check and evict partitions to free up memory and heap space.  NOTE: This must be called in the ingestion
   * stream so that there won't be concurrent other modifications.  Ideally this is called when trying to add partitions
   *
   * Expected to be called from evictForHeadroom method, only after obtaining evictionLock
   * to prevent wrong query results.
   *
   * @return true if able to evict enough or there was already space, false if not able to evict and not enough mem
   */
  private[memstore] def makeSpaceForNewPartitions(forceEvict: Boolean): Boolean = {
    assertThreadName(IngestSchedName)
    val numPartsToEvict = if (forceEvict) (targetMaxPartitions * ensureTspHeadroomPercent / 100).toInt
                          else evictionPolicy.numPartitionsToEvictForHeadroom(partSet.size, targetMaxPartitions,
                                                                              bufferMemoryManager)
    if (numPartsToEvict > 0) {
      evictPartitions(numPartsToEvict)
    } else {
      logger.error(s"Negative or Zero numPartsToEvict when eviction is needed! Is the system overloaded?")
      false
    }
  }

  /**
   * When purge happens faster than eviction and when eviction method is never called,
   * the evictablePartIds list keeps growing without control. We need to ensure cap its
   * size by removing it just enough items which in fact may already have been purged.
   */
  private[memstore] def ensureCapOnEvictablePartIds(): Unit = {
    assertThreadName(IngestSchedName)
    val numPartitions = partitions.size()
    val evictablePartIdsSize = evictablePartIds.size
    if (evictablePartIdsSize > numPartitions) {
      evictPartitions(evictablePartIdsSize - numPartitions)
    }
  }

  // scalastyle:off method.length
  private[memstore] def evictPartitions(numPartsToEvict: Int): Boolean = {
    assertThreadName(IngestSchedName)
    val partIdsToEvict = partitionsToEvict(numPartsToEvict)
    if (partIdsToEvict.isEmpty) {
      logger.warn(s"dataset=$ref shard=$shardNum No partitions to evict but we are still low on space. " +
        s"DATA WILL BE DROPPED")
      return false
    }
    // Finally, prune partitions and keyMap data structures
    logger.info(s"Evicting partitions from dataset=$ref shard=$shardNum ...")
    val intIt = partIdsToEvict.iterator()
    var numPartsEvicted = 0
    var numPartsAlreadyEvicted = 0
    var numPartsIngestingNotEvictable = 0
    val successfullyEvictedParts = new EWAHCompressedBitmap()
    while (intIt.hasNext) {
      val partitionObj = partitions.get(intIt.next)
      if (partitionObj != UnsafeUtils.ZeroPointer) {
        if (!partitionObj.ingesting) { // could have started re-ingesting after it got into evictablePartIds queue
          logger.debug(s"Evicting partId=${partitionObj.partID} ${partitionObj.stringPartition} " +
            s"from dataset=$ref shard=$shardNum")
          // add the evicted partKey to a bloom filter so that we are able to quickly
          // find out if a partId has been assigned to an ingesting partKey before a more expensive lookup.
          evictedPartKeys.synchronized {
            if (!evictedPartKeysDisposed) {
              evictedPartKeys.add(PartKey(partitionObj.partKeyBase, partitionObj.partKeyOffset))
            }
          }
          // The previously created PartKey is just meant for bloom filter and will be GCed
          removePartition(partitionObj)
          successfullyEvictedParts.set(partitionObj.partID)
          numPartsEvicted += 1
        } else {
          numPartsIngestingNotEvictable += 1
        }
      } else {
        numPartsAlreadyEvicted += 1
      }
    }
    // Pruning group bitmaps.
    for { group <- 0 until numGroups } {
      partitionGroups(group) = partitionGroups(group).andNot(successfullyEvictedParts)
    }
    val elemCount = evictedPartKeys.synchronized {
      if (!evictedPartKeysDisposed) evictedPartKeys.approximateElementCount() else 0
    }
    shardStats.evictedPkBloomFilterSize.update(elemCount)
    logger.info(s"Eviction task complete on dataset=$ref shard=$shardNum numPartsEvicted=$numPartsEvicted " +
      s"numPartsAlreadyEvicted=$numPartsAlreadyEvicted numPartsIngestingNotEvictable=$numPartsIngestingNotEvictable")
    shardStats.partitionsEvicted.increment(numPartsEvicted)
    true
  }
  // scalastyle:on method.length

  private def partitionsToEvict(numPartsToEvict: Int): debox.Buffer[Int] = {
    val partIdsToEvict = debox.Buffer.ofSize[Int](numPartsToEvict)
    evictableOdpPartIds.removeInto(numPartsToEvict, partIdsToEvict)
    if (partIdsToEvict.length < numPartsToEvict) {
      evictablePartIds.removeInto(numPartsToEvict - partIdsToEvict.length, partIdsToEvict)
    }
    shardStats.evictablePartKeysSize.update(evictableOdpPartIds.size + evictablePartIds.size)
    partIdsToEvict
  }

  /**
   * Calculate lock timeout based on headroom space available.
   * Lower the space available, longer the timeout.
   * If low on free space (under 2%), we make the linear timeout increase steeper.
   */
  private def getHeadroomLockTimeout(currentFreePercent: Double, ensurePercent: Double): Int = {
    if (currentFreePercent > 2) { // 2% threshold is hardcoded for now; configure later if really needed
      // Ramp up the timeout as the current headroom shrinks. Max timeout per attempt is a little
      // over 2 seconds, and the total timeout can be double that, for a total of 4 seconds.
      ((1.0 - (currentFreePercent / ensurePercent)) * EvictionLock.maxTimeoutMillis).toInt
    } else {
      // Ramp up the timeout aggressively as the current headroom shrinks. Max timeout per attempt is a little
      // over 65 seconds, and the total timeout can be double that, for a total of 65*2 seconds.
      ((1.0 - (currentFreePercent / ensurePercent)) * EvictionLock.direCircumstanceMaxTimeoutMillis).toInt
    }
  }

  /**
   * This task is run every 1 minute to make headroom in memory. It is also called
   * from createNewPartition method (inline during ingestion) if we run out of space between
   * headroom task runs.
   * This method acquires eviction lock and reclaim block and TSP memory.
   * If addPartitions is disabled, we force eviction even if we cannot acquire eviction lock.
   * @return true if eviction was attempted
   */
    // scalastyle:off method.length
  private[memstore] def evictForHeadroom(): Boolean = {

    // measure how much headroom we have
    val blockStoreCurrentFreePercent = blockStore.currentFreePercent
    val tspCountFreePercent = (targetMaxPartitions - partitions.size.toDouble) * 100 / targetMaxPartitions
    val nativeMemFreePercent = bufferMemoryManager.numFreeBytes.toDouble * 100 /
      bufferMemoryManager.upperBoundSizeInBytes

    // calculate lock timeouts based on free percents and target headroom to maintain. Lesser the headroom,
    // higher the timeout. Choose highest among the three.
    val blockTimeoutMs = getHeadroomLockTimeout(blockStoreCurrentFreePercent, ensureBlockHeadroomPercent)
    val tspTimeoutMs: Int = getHeadroomLockTimeout(tspCountFreePercent, ensureTspHeadroomPercent)
    val nativeMemTimeoutMs = getHeadroomLockTimeout(nativeMemFreePercent, ensureNativeMemHeadroomPercent)
    val highestTimeoutMs = Math.max(Math.max(blockTimeoutMs, tspTimeoutMs), nativeMemTimeoutMs)

    // whether to force evict even if lock cannot be acquired, if situation is dire
    // We force evict since we cannot slow down ingestion since alerting queries are at stake.
    val forceEvict = addPartitionsDisabled.get || blockStoreCurrentFreePercent < 1d ||
      tspCountFreePercent < 1d || nativeMemFreePercent < 1d

    // do only if one of blocks or TSPs need eviction or if addition of partitions disabled
    if (highestTimeoutMs > 0 || forceEvict) {
      val start = System.nanoTime()
      val timeoutMs = if (forceEvict) EvictionLock.direCircumstanceMaxTimeoutMillis else highestTimeoutMs
      logger.info(s"Preparing to evictForHeadroom on dataset=$ref shard=$shardNum since " +
        s"blockStoreCurrentFreePercent=$blockStoreCurrentFreePercent tspCountFreePercent=$tspCountFreePercent " +
        s"nativeMemFreePercent=$nativeMemFreePercent forceEvict=$forceEvict timeoutMs=$timeoutMs")
      val acquired = evictionLock.tryExclusiveReclaimLock(timeoutMs)
      // if forceEvict is true, then proceed even if we dont have a lock
      val jobDone = if (forceEvict || acquired) {
        if (!acquired) logger.error(s"Since addPartitionsDisabled is true, proceeding with reclaim " +
          s"even though eviction lock couldn't be acquired with final timeout of $timeoutMs ms. Trading " +
          s"off possibly wrong query results for old timestamps (due to old inactive partitions that would" +
          s" be evicted and skipped) in order to unblock ingestion and stop data loss. EvictionLock: $evictionLock")
        try {
          if (blockTimeoutMs > 0) {
            blockStore.ensureHeadroom(ensureBlockHeadroomPercent)
          }
          if (tspTimeoutMs > 0 || nativeMemTimeoutMs > 0) {
            if (makeSpaceForNewPartitions(forceEvict)) addPartitionsDisabled := false
          }
        } finally {
          if (acquired) evictionLock.releaseExclusive()
        }
        true
      } else {
        // could not do eviction since lock was not obtained
        shardStats.missedEviction.increment()
        false
      }
      val stall = System.nanoTime() - start
      shardStats.memstoreEvictionStall.increment(stall)
      jobDone
    } else {
      true
    }
  }
  // scalastyle:on method.length

  /**
   * Removes the given partition from our in-memory data structures.
   * Also frees partition key if necessary.
   * It is called either when evicting the partition, or permanently purging the partition.
   * Note this does not remove the partition key from index.
   */
  private def removePartition(partitionObj: TimeSeriesPartition): Unit = {
    assertThreadName(IngestSchedName)
    val stamp = partSetLock.writeLock()
    try {
      partSet.remove(partitionObj)
    } finally {
      partSetLock.unlockWrite(stamp)
    }
    if (partitions.remove(partitionObj.partID, partitionObj)) {
      partitionObj.shutdown()
    }
  }

  /**
   * Called during ingestion if we run out of memory while creating TimeSeriesPartition, or
   * if we went above the maxPartitionsLimit without being able to evict. Can appens if
   * within 1 minute (time between headroom tasks) too many new time series was added and it
   * went beyond limit of max partitions (or ran out of native memory).
   *
   * When partition addition is disabled, headroom task begins to run inline during ingestion
   * with force-eviction enabled.
   */
  private def disableAddPartitions(): Unit = {
    assertThreadName(IngestSchedName)
    if (addPartitionsDisabled.compareAndSet(false, true))
      logger.warn(s"dataset=$ref shard=$shardNum: Out of native memory, or max partitions reached. " +
        s"Not able to evict enough; adding partitions disabled")
    shardStats.dataDropped.increment()
  }

  /////// END TIME SERIES EVICTION / REMOVAL METHODS ///////////////////

  /////// START SHARD QUERY METHODS ///////////////////

  def scanTsCardinalities(shardKeyPrefix: Seq[String], depth: Int): Seq[CardinalityRecord] = {
    if (storeConfig.meteringEnabled) {
      cardTracker.scan(shardKeyPrefix, depth)
    } else {
      throw new IllegalArgumentException("Metering is not enabled")
    }
  }

  def indexNames(limit: Int): Seq[String] = partKeyIndex.indexNames(limit)

  def labelValues(labelName: String, topK: Int): Seq[TermInfo] = partKeyIndex.indexValues(labelName, topK)

  /**
   * This method is to apply column filters and fetch matching time series partitions.
   *
   * @param filters column filter
   * @param labelNames labels to return in the response
   * @param endTime end time
   * @param startTime start time
   * @param limit series limit
   * @return returns an iterator of map of label key value pairs of each matching time series
   */
  def labelValuesWithFilters(filters: Seq[ColumnFilter],
                             labelNames: Seq[String],
                             endTime: Long,
                             startTime: Long,
                             querySession: QuerySession,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    // TODO methods using faceting could be used here, but punted until performance test is completed
    val metricShardKeys = schemas.part.options.shardKeyColumns
    val metricGroupBy = deploymentPartitionName +: clusterType +: shardKeyValuesFromFilter(metricShardKeys, filters)
    LabelValueResultIterator(partKeyIndex.partIdsFromFilters(filters, startTime, endTime),
      labelNames, querySession, metricGroupBy, limit)
  }

  /**
   * This method is to apply column filters and fetch values for a label.
   *
   * @param filters column filter
   * @param label label values to return in the response
   * @param endTime end time
   * @param startTime start time
   * @param limit series limit
   * @return returns an iterator of values of for a given label
   */
  def singleLabelValuesWithFilters(filters: Seq[ColumnFilter],
                                   label: String,
                                   endTime: Long,
                                   startTime: Long,
                                   querySession: QuerySession,
                                   limit: Int): Iterator[ZeroCopyUTF8String] = {
    val metricShardKeys = schemas.part.options.shardKeyColumns
    val metricGroupBy = deploymentPartitionName +: clusterType +: shardKeyValuesFromFilter(metricShardKeys, filters)
    val startNs = Utils.currentThreadCpuTimeNanos
    val res = if (indexFacetingEnabledAllLabels ||
      (indexFacetingEnabledShardKeyLabels && schemas.part.options.shardKeyColumns.contains(label))) {
      partKeyIndex.labelValuesEfficient(filters, startTime, endTime, label, limit).iterator.map(_.utf8)
    } else {
      SingleLabelValuesResultIterator(partKeyIndex.partIdsFromFilters(filters, startTime, endTime),
        label, querySession, metricGroupBy, limit)
    }
    querySession.queryStats.getCpuNanosCounter(metricGroupBy).addAndGet(startNs - Utils.currentThreadCpuTimeNanos)
    res
  }

  /**
   * This method is to apply column filters and fetch matching time series partitions.
   *
   * @param filter column filter
   * @param endTime end time
   * @param startTime start time
   * @return returns an iterator of map of label key value pairs of each matching time series
   */
  def labelNames(filter: Seq[ColumnFilter],
                 endTime: Long,
                 startTime: Long): Seq[String] =
    partKeyIndex.labelNamesEfficient(filter, startTime, endTime)

  /**
   * Iterator for traversal of partIds, value for the given label will be extracted from the ParitionKey.
   * this implementation maps partIds to label/values eagerly, this is done inorder to dedup the results.
   */
  private def labelNamesFromPartKeys(partId: Int): Seq[String] = {
    val results = new mutable.HashSet[String]
    if (PartKeyLuceneIndex.NOT_FOUND == partId) Seq.empty
    else {
      val partKeyWithTimes = partKeyFromPartId(partId)
      results ++= schemas.part.binSchema.colNames(partKeyWithTimes.base, partKeyWithTimes.offset)
      results.toSeq
    }
  }

  /**
   * This method is to apply column filters and fetch matching time series partition keys.
   */
  def partKeysWithFilters(filter: Seq[ColumnFilter],
                          fetchFirstLastSampleTimes: Boolean,
                          endTime: Long,
                          startTime: Long,
                          limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    if (fetchFirstLastSampleTimes) {
      partKeyIndex.partKeyRecordsFromFilters(filter, startTime, endTime, limit).iterator.map { pk =>
        val partKeyMap = convertPartKeyWithTimesToMap(
          PartKeyWithTimes(pk.partKey, UnsafeUtils.arayOffset, pk.startTime, pk.endTime))
        partKeyMap ++ Map(
          ("_firstSampleTime_".utf8, pk.startTime.toString.utf8),
          ("_lastSampleTime_".utf8, pk.endTime.toString.utf8))
      }
    } else {
      val partIds = partKeyIndex.partIdsFromFilters(filter, startTime, endTime, limit)
      val inMem = InMemPartitionIterator2(partIds)
      val inMemPartKeys = inMem.map { p =>
        convertPartKeyWithTimesToMap(PartKeyWithTimes(p.partKeyBase, p.partKeyOffset, -1, -1))}
      val skippedPartKeys = inMem.skippedPartIDs.iterator().map(partId => {
        convertPartKeyWithTimesToMap(partKeyFromPartId(partId))})
      (inMemPartKeys ++ skippedPartKeys)
    }
  }

  private def convertPartKeyWithTimesToMap(partKey: PartKeyWithTimes): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = {
    schemas.part.binSchema.toStringPairs(partKey.base, partKey.offset).map(pair => {
      pair._1.utf8 -> pair._2.utf8
    }).toMap ++
      Map("_type_".utf8 -> Schemas.global.schemaName(RecordSchema.schemaID(partKey.base, partKey.offset)).utf8)
  }

  /**
   * retrieve partKey for a given PartId
   */
  private def partKeyFromPartId(partId: Int): PartKeyWithTimes = {
    val nextPart = partitions.get(partId)
    if (nextPart != UnsafeUtils.ZeroPointer)
      PartKeyWithTimes(nextPart.partKeyBase, nextPart.partKeyOffset, -1, -1)
    else { //retrieving PartKey from lucene index
      shardStats.indexPartkeyLookups.increment()
      val partKeyByteBuf = partKeyIndex.partKeyFromPartId(partId)
      if (partKeyByteBuf.isDefined) PartKeyWithTimes(partKeyByteBuf.get.bytes, UnsafeUtils.arayOffset, -1, -1)
      else throw new IllegalStateException("This is not an expected behavior." +
        " PartId should always have a corresponding PartKey!")
    }
  }

  def numRowsIngested: Long = ingested

  def numActivePartitions: Int = partSet.size

  def latestOffset: Long = _offset

  private[core] def getPartition(partKey: Array[Byte]): Option[TimeSeriesPartition] = {
    var part: Option[FiloPartition] = None
    // Access the partition set optimistically. If nothing acquired the write lock, then
    // nothing changed in the set, and the partition object is the correct one.
    var stamp = partSetLock.tryOptimisticRead()
    if (stamp != 0) {
      part = partSet.getWithPartKeyBR(partKey, UnsafeUtils.arayOffset, schemas.part)
    }
    if (!partSetLock.validate(stamp)) {
      // Because the stamp changed, the write lock was acquired and the set likely changed.
      // Try again with a full read lock, which will block if necessary so as to not run
      // concurrently with any thread making changes to the set. This guarantees that
      // the correct partition is returned.
      stamp = partSetLock.readLock()
      try {
        part = partSet.getWithPartKeyBR(partKey, UnsafeUtils.arayOffset, schemas.part)
      } finally {
        partSetLock.unlockRead(stamp)
      }
    }
    part.map(_.asInstanceOf[TimeSeriesPartition])
  }

  protected def schemaIDFromPartID(partID: Int): Int = {
    partitions.get(partID) match {
      case TimeSeriesShard.OutOfMemPartition =>
        partKeyIndex.partKeyFromPartId(partID).map { pkBytesRef =>
          val unsafeKeyOffset = PartKeyIndexRaw.bytesRefToUnsafeOffset(pkBytesRef.offset)
          RecordSchema.schemaID(pkBytesRef.bytes, unsafeKeyOffset)
        }.getOrElse(-1)
      case p: TimeSeriesPartition => p.schema.schemaHash
    }
  }

  def analyzeAndLogCorruptPtr(cve: CorruptVectorException): Unit =
    logger.error(cve.getMessage + "\n" + BlockDetective.stringReport(cve.ptr, blockStore, blockFactoryPool))

  private def shardKeyValuesFromFilter(shardKeyColumns: Seq[String], filters: Seq[ColumnFilter]): Seq[String] = {
    shardKeyColumns.map { col =>
      filters.collectFirst {
        case ColumnFilter(c, Filter.Equals(filtVal: String)) if c == col => filtVal
        case ColumnFilter(c, Filter.EqualsRegex(filtVal: String)) if c == col => filtVal
      }.getOrElse("multiple")
    }.toList
  }

  /**
   * Looks up partitions and schema info from ScanMethods, usually by doing a Lucene search.
   * Also returns detailed information about what is in memory and not, and does schema discovery.
   */
  // scalastyle:off method.length
  def lookupPartitions(partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod,
                       querySession: QuerySession): PartLookupResult = {
    // At the end, MultiSchemaPartitionsExec.execute releases the lock when the task is complete
    partMethod match {
      case SinglePartitionScan(partition, _) =>
        val partIds = debox.Buffer.empty[Int]
        getPartition(partition).foreach(p => partIds += p.partID)
        PartLookupResult(shardNum, chunkMethod, partIds, Some(RecordSchema.schemaID(partition)),
          dataBytesScannedCtr = querySession.queryStats.getDataBytesScannedCounter())
      case MultiPartitionScan(partKeys, _)   =>
        val partIds = debox.Buffer.empty[Int]
        partKeys.flatMap(getPartition).foreach(p => partIds += p.partID)
        PartLookupResult(shardNum, chunkMethod, partIds, partKeys.headOption.map(RecordSchema.schemaID),
          dataBytesScannedCtr = querySession.queryStats.getDataBytesScannedCounter())
      case FilteredPartitionScan(_, filters) =>
        val metricShardKeys = schemas.part.options.shardKeyColumns
        val metricGroupBy = deploymentPartitionName +: clusterType +: shardKeyValuesFromFilter(metricShardKeys, filters)
        val chunksReadCounter = querySession.queryStats.getDataBytesScannedCounter(metricGroupBy)
        // No matter if there are filters or not, need to run things through Lucene so we can discover potential
        // TSPartitions to read back from disk
        val startNs = Utils.currentThreadCpuTimeNanos
        val matches = partKeyIndex.partIdsFromFilters(filters, chunkMethod.startTime, chunkMethod.endTime)
        shardStats.queryTimeRangeMins.record((chunkMethod.endTime - chunkMethod.startTime) / 60000 )
        querySession.queryStats.getTimeSeriesScannedCounter(metricGroupBy).addAndGet(matches.length)
        querySession.queryStats.getCpuNanosCounter(metricGroupBy).addAndGet(Utils.currentThreadCpuTimeNanos - startNs)
        Kamon.currentSpan().tag(s"num-partitions-from-index-$shardNum", matches.length)

        // first find out which partitions are being queried for data not in memory
        val firstPartId = if (matches.isEmpty) None else Some(matches(0))
        val _schema = firstPartId.map(schemaIDFromPartID)
        _schema.foreach { s =>
          shardStats.queriesBySchema.withTag("schema", schemas(s).name).increment()
        }
        val it1 = InMemPartitionIterator2(matches)
        val partIdsToPage = it1.filter(_.earliestTime > chunkMethod.startTime).map(_.partID)
        val partIdsNotInMem = it1.skippedPartIDs
        Kamon.currentSpan().tag(s"num-partitions-not-in-memory-$shardNum", partIdsNotInMem.length)
        val startTimes = if (partIdsToPage.nonEmpty) {
          val st = partKeyIndex.startTimeFromPartIds(partIdsToPage)
          logger.debug(s"Some partitions have earliestTime > queryStartTime(${chunkMethod.startTime}); " +
            s"startTime lookup for query in dataset=$ref shard=$shardNum " +
            s"resulted in startTimes=$st")
          st
        }
        else {
          logger.debug(s"StartTime lookup was not needed. All partition's data for query in dataset=$ref " +
            s"shard=$shardNum are in memory")
          debox.Map.empty[Int, Long]
        }
        // now provide an iterator that additionally supplies the startTimes for
        // those partitions that may need to be paged
        PartLookupResult(shardNum, chunkMethod, matches, _schema, startTimes, partIdsNotInMem,
          Nil, chunksReadCounter)
    }
  }
  // scalastyle:on method.length

  def scanPartitions(iterResult: PartLookupResult,
                     colIds: Seq[Types.ColumnId],
                     querySession: QuerySession): Observable[ReadablePartition] = {

    val partIter = new InMemPartitionIterator2(iterResult.partsInMemory)
    Observable.fromIteratorUnsafe(partIter.map { p =>
      shardStats.partitionsQueried.increment()
      p
    })
  }

  /////// END SHARD QUERY METHODS ///////////////////

  /////// START SHARD SHUTDOWN METHODS ///////////////////

  /**
    * Please use this for testing only - reclaims ALL used offheap blocks.  Maybe you are trying to test
    * on demand paging.
    */
  private[filodb] def reclaimAllBlocksTestOnly() = blockStore.reclaimAll()

  /**
    * Reset all state in this shard.  Memory is not released as once released, then this class
    * cannot be used anymore (except partition key/chunkmap state is removed.)
    */
  def reset(): Unit = {
    logger.info(s"Clearing all MemStore state for dataset=$ref shard=$shardNum")
    ingestSched.executeTrampolined { () =>
      partitions.values.asScala.foreach(removePartition)
    }
    partKeyIndex.reset()
    // TODO unable to reset/clear bloom filter
    ingested = 0L
    for { group <- 0 until numGroups } {
      partitionGroups(group) = new EWAHCompressedBitmap()
      groupWatermark(group) = Long.MinValue
    }
  }

  def shutdown(): Unit = {
    try {
      logger.info(s"Shutting down dataset=$ref shard=$shardNum")
      if (storeConfig.meteringEnabled) {
        cardTracker.close()
      }
      evictedPartKeys.synchronized {
        if (!evictedPartKeysDisposed) {
          evictedPartKeysDisposed = true
          evictedPartKeys.dispose()
        }
      }
      reset()   // Not really needed, but clear everything just to be consistent
      partKeyIndex.closeIndex()
        /* Don't explicitly free the memory just yet. These classes instead rely on a finalize
         method to ensure that no threads are accessing the memory before it's freed.
      blockStore.releaseBlocks()
      */
      headroomTask.cancel()
      ingestSched.shutdown()
    } catch { case e: Exception =>
      logger.error("Exception when shutting down shard", e)
    }
  }

  /////// END SHARD SHUTDOWN METHODS ///////////////////
}
