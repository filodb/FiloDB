package filodb.core.memstore

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.StampedLock

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Random, Try}

import bloomfilter.CanGenerateHashFrom
import bloomfilter.mutable.BloomFilter
import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.scalalogging.StrictLogging
import debox.{Buffer, Map => DMap}
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import monix.eval.Task
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import monix.execution.atomic.AtomicBoolean
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong
import spire.syntax.cfor._

import filodb.core.{ErrorResponse, _}
import filodb.core.binaryrecord2._
import filodb.core.metadata.{Schema, Schemas}
import filodb.core.query.{ColumnFilter, QuerySession}
import filodb.core.store._
import filodb.memory._
import filodb.memory.data.Shutdown
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.ZeroCopyUTF8String._

class TimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

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
  val flushesFailedPartWrite = Kamon.counter("memstore-flushes-failed-partition").withTags(TagSet.from(tags))
  val flushesFailedChunkWrite = Kamon.counter("memstore-flushes-failed-chunk").withTags(TagSet.from(tags))
  val flushesFailedOther = Kamon.counter("memstore-flushes-failed-other").withTags(TagSet.from(tags))

  val numDirtyPartKeysFlushed = Kamon.counter("memstore-index-num-dirty-keys-flushed").withTags(TagSet.from(tags))
  val indexRecoveryNumRecordsProcessed = Kamon.counter("memstore-index-recovery-partkeys-processed").
    withTags(TagSet.from(tags))
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
  val partitionsRestored = Kamon.counter("memstore-partitions-paged-restored").withTags(TagSet.from(tags))
  val chunkIdsEvicted = Kamon.counter("memstore-chunkids-evicted").withTags(TagSet.from(tags))
  val partitionsEvicted = Kamon.counter("memstore-partitions-evicted").withTags(TagSet.from(tags))
  val queryTimeRangeMins = Kamon.histogram("query-time-range-minutes").withTags(TagSet.from(tags))
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
  val ingestionClockDelay = Kamon.gauge("ingestion-clock-delay").withTags(TagSet.from(tags))
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
                            pkRecords: Seq[PartKeyLuceneIndexRecord] = Seq.empty)

final case class SchemaMismatch(expected: String, found: String) extends
Exception(s"Multiple schemas found, please filter. Expected schema $expected, found schema $found")

object SchemaMismatch {
  def apply(expected: Schema, found: Schema): SchemaMismatch = SchemaMismatch(expected.name, found.name)
}

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
                      val shardNum: Int,
                      val bufferMemoryManager: MemFactory,
                      colStore: ColumnStore,
                      metastore: MetaStore,
                      evictionPolicy: PartitionEvictionPolicy)
                     (implicit val ioPool: ExecutionContext) extends StrictLogging {
  import collection.JavaConverters._

  import FiloSchedulers._
  import TimeSeriesShard._

  val shardStats = new TimeSeriesShardStats(ref, shardNum)

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
  private[memstore] final val partKeyIndex = new PartKeyLuceneIndex(ref, schemas.part, shardNum,
    storeConfig.demandPagedRetentionPeriod)

  /**
    * Keeps track of count of rows ingested into memstore, not necessarily flushed.
    * This is generally used to report status and metrics.
    */
  private final var ingested = 0L

  /**
    * Keeps track of last offset ingested into memory (not necessarily flushed).
    * This value is used to keep track of the checkpoint to be written for next flush for any group.
    */
  private final var _offset = Long.MinValue

  /**
   * The maximum blockMetaSize amongst all the schemas this Dataset could ingest
   */
  val maxMetaSize = schemas.schemas.values.map(_.data.blockMetaSize).max

  require (storeConfig.maxChunkTime > storeConfig.flushInterval, "MaxChunkTime should be greater than FlushInterval")
  val maxChunkTime = storeConfig.maxChunkTime.toMillis

  // Called to remove chunks from ChunkMap of a given partition, when an offheap block is reclaimed
  private val reclaimListener = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {
      val partID = UnsafeUtils.getInt(metaAddr)
      val partition = partitions.get(partID)
      if (partition != UnsafeUtils.ZeroPointer) {
        // The number of bytes passed in is the metadata size which depends on schema.  It should match the
        // TSPartition's blockMetaSize; if it doesn't that is a flag for possible corruption, and we should halt
        // the process to be safe and log details for further debugging.
        val chunkID = UnsafeUtils.getLong(metaAddr + 4)
        if (numBytes != partition.schema.data.blockMetaSize) {
          logger.error(f"POSSIBLE CORRUPTION DURING onReclaim(metaAddr=0x$metaAddr%08x, numBytes=$numBytes)" +
                       s"Expected meta size: ${partition.schema.data.blockMetaSize} for schema=${partition.schema}" +
                       s"  Reclaiming chunk chunkID=$chunkID from shard=$shardNum " +
                       s"partID=$partID ${partition.stringPartition}")
          logger.warn("Halting FiloDB...")
          sys.exit(33)   // Special onReclaim corruption exit code
        }
        partition.removeChunksAt(chunkID)
        logger.debug(s"Reclaiming chunk chunkID=$chunkID from shard=$shardNum " +
          s"partID=$partID ${partition.stringPartition}")
      }
    }
  }

  // Create a single-threaded scheduler just for ingestion.  Name the thread for ease of debugging
  // NOTE: to control intermixing of different Observables/Tasks in this thread, customize ExecutionModel param
  val ingestSched = Scheduler.singleThread(s"$IngestSchedName-$ref-$shardNum",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in TimeSeriesShard.ingestSched", _)))

  private val blockMemorySize = storeConfig.shardMemSize
  protected val numGroups = storeConfig.groupsPerShard
  private val chunkRetentionHours = (storeConfig.demandPagedRetentionPeriod.toSeconds / 3600).toInt
  val pagingEnabled = storeConfig.demandPagingEnabled

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
    storeConfig.numPagesPerBlock)
  private val blockFactoryPool = new BlockMemFactoryPool(blockStore, maxMetaSize, shardTags)

  /**
    * Lock that protects chunks from being reclaimed from Memstore.
    * This is needed to prevent races between ODP queries and reclaims.
    */
  private[memstore] final val reclaimLock = blockStore.reclaimLock

  // Requires blockStore.
  private val headroomTask = startHeadroomTask(ingestSched)

  // Each shard has a single ingestion stream at a time.  This BlockMemFactory is used for buffer overflow encoding
  // strictly during ingest() and switchBuffers().
  private[core] val overflowBlockFactory = new BlockMemFactory(blockStore, None, maxMetaSize,
                                             shardTags ++ Map("overflow" -> "true"), true)
  val partitionMaker = new DemandPagedChunkStore(this, blockStore, chunkRetentionHours)

  private val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, reuseOneContainer = true)
  private val partKeyArray = partKeyBuilder.allContainers.head.base.asInstanceOf[Array[Byte]]
  private[memstore] val bufferPools = {
    val pools = schemas.schemas.values.map { sch =>
      sch.schemaHash -> new WriteBufferPool(bufferMemoryManager, sch.data, storeConfig)
    }
    DMap(pools.toSeq: _*)
  }

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
    * Timestamp to start searching for partitions to evict. Advances as more and more partitions are evicted.
    * Used to ensure we keep searching for newer and newer partitions to evict.
    */
  private[core] var evictionWatermark: Long = 0L

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
  private val flushBoundaryMillis = storeConfig.flushInterval.toMillis

  // Defines the group-specific flush offset, to distribute the flushes around such they don't
  // all flush at the same time. With an hourly boundary and 60 flush groups, flushes are
  // scheduled once a minute.
  private val flushOffsetMillis = flushBoundaryMillis / numGroups

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
  val tracedPartFilters = storeConfig.traceFilters

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
            // Needed to update index with new partitions added during recovery with correct startTime.
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

  private[memstore] val ingestConsumer = new IngestConsumer()

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
    } else {
      shardStats.oldContainers.increment()
    }
    _offset
  }

  def startFlushingIndex(): Unit =
    partKeyIndex.startFlushThread(storeConfig.partIndexFlushMinDelaySeconds, storeConfig.partIndexFlushMaxDelaySeconds)

  def ingest(data: SomeData): Long = ingest(data.records, data.offset)

  def recoverIndex(): Future[Unit] = {
    val indexBootstrapper = new IndexBootstrapper(colStore)
    indexBootstrapper.bootstrapIndex(partKeyIndex, shardNum, ref)(bootstrapPartKey)
                     .map { count =>
                        startFlushingIndex()
                       logger.info(s"Bootstrapped index for dataset=$ref shard=$shardNum with $count records")
                     }.runAsync(ingestSched)
  }

  /**
    * Handles actions to be performed for the shard upon bootstrapping
    * a partition key from index store
    * @param pk partKey
    * @return partId assigned to key
    */
  // scalastyle:off method.length
  private[memstore] def bootstrapPartKey(pk: PartKeyRecord): Int = {
    assertThreadName(IngestSchedName)
    val partId = if (pk.endTime == Long.MaxValue) {
      // this is an actively ingesting partition
      val group = partKeyGroup(schemas.part.binSchema, pk.partKey, UnsafeUtils.arayOffset, numGroups)
      val schemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
      val schema = schemas(schemaId)
      if (schema != Schemas.UnknownSchema) {
        val part = createNewPartition(pk.partKey, UnsafeUtils.arayOffset, group, CREATE_NEW_PARTID, schema, 4)
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
    partId
  }

  def indexNames(limit: Int): Seq[String] = partKeyIndex.indexNames(limit)

  def labelValues(labelName: String, topK: Int): Seq[TermInfo] = partKeyIndex.indexValues(labelName, topK)

  /**
    * This method is to apply column filters and fetch matching time series partitions.
    *
    * @param filter column filter
    * @param labelNames labels to return in the response
    * @param endTime end time
    * @param startTime start time
    * @param limit series limit
    * @return returns an iterator of map of label key value pairs of each matching time series
    */
  def labelValuesWithFilters(filter: Seq[ColumnFilter],
                             labelNames: Seq[String],
                             endTime: Long,
                             startTime: Long,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    LabelValueResultIterator(partKeyIndex.partIdsFromFilters(filter, startTime, endTime), labelNames, limit)
  }

  /**
    * Iterator for lazy traversal of partIdIterator, value for the given label will be extracted from the ParitionKey.
    */
  case class LabelValueResultIterator(partIds: debox.Buffer[Int], labelNames: Seq[String], limit: Int)
    extends Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] {
    var currVal: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = _
    var numResultsReturned = 0
    var partIndex = 0

    override def hasNext: Boolean = {
      var foundValue = false
      while(partIndex < partIds.length && numResultsReturned < limit && !foundValue) {
        val partId = partIds(partIndex)

        //retrieve PartKey either from In-memory map or from PartKeyIndex
        val nextPart = partKeyFromPartId(partId)

        // FIXME This is non-performant and temporary fix for fetching label values based on filter criteria.
        // Other strategies needs to be evaluated for making this performant - create facets for predefined fields or
        // have a centralized service/store for serving metadata
        currVal = schemas.part.binSchema.toStringPairs(nextPart.base, nextPart.offset)
          .filter(labelNames contains _._1).map(pair => {
          pair._1.utf8 -> pair._2.utf8
        }).toMap
        foundValue = currVal.nonEmpty
        partIndex += 1
      }
      foundValue
    }

    override def next(): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = {
      numResultsReturned += 1
      currVal
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
      partKeyIndex.partKeyRecordsFromFilters(filter, startTime, endTime).iterator.map { pk =>
        val partKeyMap = convertPartKeyWithTimesToMap(
          PartKeyWithTimes(pk.partKey, UnsafeUtils.arayOffset, pk.startTime, pk.endTime))
        partKeyMap ++ Map(
          ("_firstSampleTime_".utf8, pk.startTime.toString.utf8),
          ("_lastSampleTime_".utf8, pk.endTime.toString.utf8))
      } take(limit)
    } else {
      val partIds = partKeyIndex.partIdsFromFilters(filter, startTime, endTime)
      val inMem = InMemPartitionIterator2(partIds)
      val inMemPartKeys = inMem.map { p =>
        convertPartKeyWithTimesToMap(PartKeyWithTimes(p.partKeyBase, p.partKeyOffset, -1, -1))}
      val skippedPartKeys = inMem.skippedPartIDs.iterator().map(partId => {
        convertPartKeyWithTimesToMap(partKeyFromPartId(partId))})
      (inMemPartKeys ++ skippedPartKeys).take(limit)
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
      val partKeyByteBuf = partKeyIndex.partKeyFromPartId(partId)
      if (partKeyByteBuf.isDefined) PartKeyWithTimes(partKeyByteBuf.get.bytes, UnsafeUtils.arayOffset, -1, -1)
      else throw new IllegalStateException("This is not an expected behavior." +
        " PartId should always have a corresponding PartKey!")
    }
  }

  /**
    * WARNING: Not performant. Use only in tests, or during initial bootstrap.
    */
  def refreshPartKeyIndexBlocking(): Unit = partKeyIndex.refreshReadersBlocking()

  def closePartKeyIndex(): Unit = partKeyIndex.closeIndex()

  def numRowsIngested: Long = ingested

  def numActivePartitions: Int = partSet.size

  def latestOffset: Long = _offset

  /**
    * Sets the watermark for each subgroup.  If an ingested record offset is below this watermark then it will be
    * assumed to already have been persisted, and the record will be discarded.  Use only for recovery.
    * @param watermarks a Map from group number to watermark
    */
  def setGroupWatermarks(watermarks: Map[Int, Long]): Unit =
    watermarks.foreach { case (group, mark) => groupWatermark(group) = mark }

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
    InMemPartitionIterator(partitionGroups(groupNum).intIterator).foreach(_.switchBuffers(overflowBlockFactory))

    val dirtyPartKeys = if (groupNum == dirtyPartKeysFlushGroup) {
      logger.debug(s"Switching dirty part keys in dataset=$ref shard=$shardNum out for flush. ")
      purgeExpiredPartitions()
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
    val partsToPurge = partKeyIndex.partIdsEndedBefore(
      System.currentTimeMillis() - storeConfig.demandPagedRetentionPeriod.toMillis)
    var numDeleted = 0
    val removedParts = debox.Buffer.empty[Int]
    InMemPartitionIterator2(partsToPurge).foreach { p =>
      if (!p.ingesting) {
        logger.debug(s"Purging partition with partId=${p.partID}  ${p.stringPartition} from " +
          s"memory in dataset=$ref shard=$shardNum")
        removePartition(p)
        removedParts += p.partID
        numDeleted += 1
      }
    }
    if (!removedParts.isEmpty) partKeyIndex.removePartKeys(removedParts)
    if (numDeleted > 0) logger.info(s"Purged $numDeleted partitions from memory and " +
                        s"index from dataset=$ref shard=$shardNum")
    shardStats.purgedPartitions.increment(numDeleted)
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
        if (oldTimestamp / flushBoundaryMillis != newTimestamp / flushBoundaryMillis) {
          // Flush out the group before ingesting records for a new hour (by group offset).
          tasks += createFlushTask(prepareFlushGroup(group))
        }
        oldTimestamp -= flushOffsetMillis
        newTimestamp -= flushOffsetMillis
      }
    }

    // Only update stuff if no exception was thrown.

    if (ingestionTime != lastIngestionTime) {
        lastIngestionTime = ingestionTime
        shardStats.ingestionClockDelay.update(System.currentTimeMillis() - ingestionTime)
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
    PartKeyRecord(p.partKeyBytes, startTime, endTime, Some(p.partKeyHash))
  }

  // scalastyle:off method.length
  private def doFlushSteps(flushGroup: FlushGroup,
                           partitionIt: Iterator[TimeSeriesPartition]): Task[Response] = {
    assertThreadName(IngestSchedName)

    val tracer = Kamon.spanBuilder("chunk-flush-task-latency-after-retries")
      .asChildOf(Kamon.currentSpan())
      .tag("dataset", ref.dataset)
      .tag("shard", shardNum).start()

    // Only allocate the blockHolder when we actually have chunks/partitions to flush
    val blockHolder = blockFactoryPool.checkout(Map("flushGroup" -> flushGroup.groupNum.toString))

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
        blockHolder.markUsedBlocksReclaimable()
        blockFactoryPool.release(blockHolder)
        flushDoneTasks(flushGroup, resp)
        tracer.finish()
      } catch { case e: Throwable =>
        logger.error(s"Error when wrapping up doFlushSteps in dataset=$ref shard=$shardNum", e)
      }
    }(ingestSched)
    // Note: The data structures accessed by flushDoneTasks can only be safely accessed by the
    //       ingestion thread, hence the onComplete steps are run from that thread.
    Task.fromFuture(result)
  }

  protected def flushDoneTasks(flushGroup: FlushGroup, resTry: Try[Response]): Unit = {
    assertThreadName(IngestSchedName)
    resTry.foreach { resp =>
      logger.info(s"Flush of dataset=$ref shard=$shardNum group=${flushGroup.groupNum} " +
        s"flushWatermark=${flushGroup.flushWatermark} response=$resp offset=${_offset}")
    }
    partitionMaker.cleanupOldestBuckets()
    // Some partitions might be evictable, see if need to free write buffer memory
    checkEnableAddPartitions()
    updateGauges()
  }

  // scalastyle:off method.length
  private def writeDirtyPartKeys(flushGroup: FlushGroup): Future[Response] = {
    assertThreadName(IOSchedName)
    val partKeyRecords = InMemPartitionIterator2(flushGroup.dirtyPartsToFlush).map(toPartKeyRecord)
    val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
    colStore.writePartKeys(ref, shardNum,
                           Observable.fromIterator(partKeyRecords),
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

    val chunkSetStream = Observable.fromIterator(chunkSetIt)
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
        activelyIngesting -= p.partID
        p.ingesting = false
      }
    }
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

  private[memstore] val addPartitionsDisabled = AtomicBoolean(false)

  // scalastyle:off null
  private[filodb] def getOrAddPartitionForIngestion(recordBase: Any, recordOff: Long,
                                                    group: Int, schema: Schema) = {
    var part = partSet.getWithIngestBR(recordBase, recordOff, schema)
    if (part == null) {
      part = addPartitionForIngestion(recordBase, recordOff, schema, group)
    }
    part
  }
  // scalastyle:on

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
    logger.debug(s"Adding ingestion record details: ${schema.ingestionSchema.debugString(recordBase, recordOff)}")
    val partKeyOffset = schema.comparator.buildPartKeyFromIngest(recordBase, recordOff, partKeyBuilder)
    val previousPartId = lookupPreviouslyAssignedPartId(partKeyArray, partKeyOffset)
    // TODO: remove when no longer needed
    logger.debug(s"Adding part key details: ${schema.partKeySchema.debugString(partKeyArray, partKeyOffset)}")
    val newPart = createNewPartition(partKeyArray, partKeyOffset, group, previousPartId, schema)
    if (newPart != OutOfMemPartition) {
      val partId = newPart.partID
      val startTime = schema.ingestionSchema.getLong(recordBase, recordOff, 0)
      if (previousPartId == CREATE_NEW_PARTID) {
        // add new lucene entry if this partKey was never seen before
        // causes endTime to be set to Long.MaxValue
        partKeyIndex.addPartKey(newPart.partKeyBytes, partId, startTime)()
      } else {
        // newly created partition is re-ingesting now, so update endTime
        updatePartEndTimeInIndex(newPart, Long.MaxValue)
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
        tsp.ingest(ingestionTime, brRowReader, overflowBlockFactory, maxChunkTime)
        // Below is coded to work concurrently with logic in updateIndexWithEndTime
        // where we try to de-activate an active time series
        if (!tsp.ingesting) {
          // DO NOT use activelyIngesting to check above condition since it is slow and is called for every sample
          activelyIngesting.synchronized {
            if (!tsp.ingesting) {
              // time series was inactive and has just started re-ingesting
              updatePartEndTimeInIndex(part.asInstanceOf[TimeSeriesPartition], Long.MaxValue)
              dirtyPartitionsForIndexFlush += part.partID
              activelyIngesting += part.partID
              tsp.ingesting = true
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
                                   initMapSize: Int = initInfoMapSize): TimeSeriesPartition = {
    assertThreadName(IngestSchedName)
    // Check and evict, if after eviction we still don't have enough memory, then don't proceed
    if (addPartitionsDisabled() || !ensureFreeSpace()) {
      OutOfMemPartition
    }
    else {
      // PartitionKey is copied to offheap bufferMemory and stays there until it is freed
      // NOTE: allocateAndCopy and allocNew below could fail if there isn't enough memory.  It is CRUCIAL
      // that min-write-buffers-free setting is large enough to accommodate the below use cases ALWAYS
      val (_, partKeyAddr, _) = BinaryRegionLarge.allocateAndCopy(partKeyBase, partKeyOffset, bufferMemoryManager)
      val partId = if (usePartId == CREATE_NEW_PARTID) createPartitionID() else usePartId
      val pool = bufferPools(schema.schemaHash)
      val newPart = if (shouldTrace(partKeyAddr)) {
        logger.debug(s"Adding tracing TSPartition dataset=$ref shard=$shardNum group=$group partId=$partId")
        new TracingTimeSeriesPartition(
          partId, ref, schema, partKeyAddr, shardNum, pool, shardStats, bufferMemoryManager, initMapSize)
      } else {
        new TimeSeriesPartition(
          partId, schema, partKeyAddr, shardNum, pool, shardStats, bufferMemoryManager, initMapSize)
      }
      partitions.put(partId, newPart)
      shardStats.partitionsCreated.increment()
      partitionGroups(group).set(partId)
      newPart
    }
  }

  private def disableAddPartitions(): Unit = {
    assertThreadName(IngestSchedName)
    if (addPartitionsDisabled.compareAndSet(false, true))
      logger.warn(s"dataset=$ref shard=$shardNum: Out of buffer memory and not able to evict enough; " +
        s"adding partitions disabled")
    shardStats.dataDropped.increment()
  }

  private def checkEnableAddPartitions(): Unit = {
    assertThreadName(IngestSchedName)
    if (addPartitionsDisabled()) {
      if (ensureFreeSpace()) {
        logger.info(s"dataset=$ref shard=$shardNum: Enough free space to add partitions again!  Yay!")
        addPartitionsDisabled := false
      }
    }
  }

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

  def analyzeAndLogCorruptPtr(cve: CorruptVectorException): Unit =
    logger.error(cve.getMessage + "\n" + BlockDetective.stringReport(cve.ptr, blockStore, blockFactoryPool))

  /**
   * Check and evict partitions to free up memory and heap space.  NOTE: This must be called in the ingestion
   * stream so that there won't be concurrent other modifications.  Ideally this is called when trying to add partitions
   * @return true if able to evict enough or there was already space, false if not able to evict and not enough mem
   */
  // scalastyle:off method.length
  private[filodb] def ensureFreeSpace(): Boolean = {
    assertThreadName(IngestSchedName)
    var lastPruned = EmptyBitmap
    while (evictionPolicy.shouldEvict(partSet.size, bufferMemoryManager)) {
      // Eliminate partitions evicted from last cycle so we don't have an endless loop
      val prunedPartitions = partitionsToEvict().andNot(lastPruned)
      if (prunedPartitions.isEmpty) {
        logger.warn(s"dataset=$ref shard=$shardNum: No partitions to evict but we are still low on space. " +
          s"DATA WILL BE DROPPED")
        return false
      }
      lastPruned = prunedPartitions

      // Pruning group bitmaps
      for { group <- 0 until numGroups } {
        partitionGroups(group) = partitionGroups(group).andNot(prunedPartitions)
      }

      // Finally, prune partitions and keyMap data structures
      logger.info(s"Evicting partitions from dataset=$ref shard=$shardNum, watermark=$evictionWatermark...")
      val intIt = prunedPartitions.intIterator
      var partsRemoved = 0
      var partsSkipped = 0
      var maxEndTime = evictionWatermark
      while (intIt.hasNext) {
        val partitionObj = partitions.get(intIt.next)
        if (partitionObj != UnsafeUtils.ZeroPointer) {
          // TODO we can optimize fetching of endTime by getting them along with top-k query
          val endTime = partKeyIndex.endTimeFromPartId(partitionObj.partID)
          if (partitionObj.ingesting)
            logger.warn(s"Partition ${partitionObj.partID} is ingesting, but it was eligible for eviction. How?")
          if (endTime == PartKeyLuceneIndex.NOT_FOUND || endTime == Long.MaxValue) {
            logger.warn(s"endTime $endTime was not correct. how?", new IllegalStateException())
          } else {
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
            partsRemoved += 1
            maxEndTime = Math.max(maxEndTime, endTime)
          }
        } else {
          partsSkipped += 1
        }
      }
      val elemCount = evictedPartKeys.synchronized {
        if (!evictedPartKeysDisposed) {
          evictedPartKeys.approximateElementCount()
        } else {
          0
        }
      }
      shardStats.evictedPkBloomFilterSize.update(elemCount)
      evictionWatermark = maxEndTime + 1
      // Plus one needed since there is a possibility that all partitions evicted in this round have same endTime,
      // and there may be more partitions that are not evicted with same endTime. If we didnt advance the watermark,
      // we would be processing same partIds again and again without moving watermark forward.
      // We may skip evicting some partitions by doing this, but the imperfection is an acceptable
      // trade-off for performance and simplicity. The skipped partitions, will ve removed during purge.
      logger.info(s"dataset=$ref shard=$shardNum: evicted $partsRemoved partitions," +
        s"skipped $partsSkipped, h20=$evictionWatermark")
      shardStats.partitionsEvicted.increment(partsRemoved)
    }
    true
  }
  //scalastyle:on

  // Permanently removes the given partition ID from our in-memory data structures
  // Also frees partition key if necessary
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

  private def partitionsToEvict(): EWAHCompressedBitmap = {
    // Iterate and add eligible partitions to delete to our list
    // Need to filter out partitions with no endTime. Any endTime calculated would not be set within one flush interval.
    partKeyIndex.partIdsOrderedByEndTime(storeConfig.numToEvict, evictionWatermark, Long.MaxValue - 1)
  }

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
          val unsafeKeyOffset = PartKeyLuceneIndex.bytesRefToUnsafeOffset(pkBytesRef.offset)
          RecordSchema.schemaID(pkBytesRef.bytes, unsafeKeyOffset)
        }.getOrElse(-1)
      case p: TimeSeriesPartition => p.schema.schemaHash
    }
  }

  /**
    * Looks up partitions and schema info from ScanMethods, usually by doing a Lucene search.
    * Also returns detailed information about what is in memory and not, and does schema discovery.
    */
  def lookupPartitions(partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod,
                       querySession: QuerySession): PartLookupResult = {
    querySession.lock = Some(reclaimLock)
    reclaimLock.lock()
    // any exceptions thrown here should be caught by a wrapped Task.
    // At the end, MultiSchemaPartitionsExec.execute releases the lock when the task is complete
    partMethod match {
      case SinglePartitionScan(partition, _) =>
        val partIds = debox.Buffer.empty[Int]
        getPartition(partition).foreach(p => partIds += p.partID)
        PartLookupResult(shardNum, chunkMethod, partIds, Some(RecordSchema.schemaID(partition)))
      case MultiPartitionScan(partKeys, _)   =>
        val partIds = debox.Buffer.empty[Int]
        partKeys.flatMap(getPartition).foreach(p => partIds += p.partID)
        PartLookupResult(shardNum, chunkMethod, partIds, partKeys.headOption.map(RecordSchema.schemaID))
      case FilteredPartitionScan(_, filters) =>
        // No matter if there are filters or not, need to run things through Lucene so we can discover potential
        // TSPartitions to read back from disk
        val matches = partKeyIndex.partIdsFromFilters(filters, chunkMethod.startTime, chunkMethod.endTime)
        shardStats.queryTimeRangeMins.record((chunkMethod.endTime - chunkMethod.startTime) / 60000 )

        Kamon.currentSpan().tag(s"num-partitions-from-index-$shardNum", matches.length)

        // first find out which partitions are being queried for data not in memory
        val firstPartId = if (matches.isEmpty) None else Some(matches(0))
        val _schema = firstPartId.map(schemaIDFromPartID)
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
        PartLookupResult(shardNum, chunkMethod, matches, _schema, startTimes, partIdsNotInMem)
    }
  }

  def scanPartitions(iterResult: PartLookupResult,
                     colIds: Seq[Types.ColumnId],
                     querySession: QuerySession): Observable[ReadablePartition] = {

    val partIter = new InMemPartitionIterator2(iterResult.partsInMemory)
    Observable.fromIterator(partIter.map { p =>
      shardStats.partitionsQueried.increment()
      p
    })
  }

  private def startHeadroomTask(sched: Scheduler) = {
    sched.scheduleWithFixedDelay(1, 1, TimeUnit.MINUTES, new Runnable {
      var numFailures = 0

      def run() = {
        val numFree = blockStore.ensureHeadroom(storeConfig.ensureHeadroomPercent)
        if (numFree > 0) {
          numFailures = 0
        } else {
          numFailures += 1
          if (numFailures >= 5) {
            Shutdown.haltAndCatchFire(new RuntimeException(s"Headroom task was unable to free memory " +
              s"for $numFailures consecutive attempts. Shutting down process. shard=$shardNum"))
          }
        }
      }
    })
  }

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
    evictedPartKeys.synchronized {
      if (!evictedPartKeysDisposed) {
        evictedPartKeysDisposed = true
        evictedPartKeys.dispose()
      }
    }
    reset()   // Not really needed, but clear everything just to be consistent
    logger.info(s"Shutting down dataset=$ref shard=$shardNum")
    /* Don't explcitly free the memory just yet. These classes instead rely on a finalize
       method to ensure that no threads are accessing the memory before it's freed.
    blockStore.releaseBlocks()
    */
    headroomTask.cancel()
    ingestSched.shutdown()
  }
}
