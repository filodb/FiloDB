package filodb.core.memstore

import java.util.concurrent.locks.StampedLock

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Random, Try}

import bloomfilter.CanGenerateHashFrom
import bloomfilter.mutable.BloomFilter
import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.scalalogging.StrictLogging
import debox.Buffer
import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import monix.execution.atomic.AtomicBoolean
import monix.reactive.Observable
import org.apache.lucene.util.BytesRef
import org.jctools.maps.NonBlockingHashMapLong
import scalaxy.loops._

import filodb.core.{ErrorResponse, _}
import filodb.core.binaryrecord2._
import filodb.core.downsample.{DownsampleConfig, DownsamplePublisher, ShardDownsampler}
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, ColumnInfo, Filter}
import filodb.core.store._
import filodb.memory._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.ZeroCopyUTF8String._

class TimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

  val rowsIngested = Kamon.counter("memstore-rows-ingested").refine(tags)
  val partitionsCreated = Kamon.counter("memstore-partitions-created").refine(tags)
  val dataDropped = Kamon.counter("memstore-data-dropped").refine(tags)
  val oldContainers = Kamon.counter("memstore-incompatible-containers").refine(tags)
  val offsetsNotRecovered = Kamon.counter("memstore-offsets-not-recovered").refine(tags)
  val outOfOrderDropped = Kamon.counter("memstore-out-of-order-samples").refine(tags)
  val rowsSkipped  = Kamon.counter("recovery-row-skipped").refine(tags)
  val rowsPerContainer = Kamon.histogram("num-samples-per-container")
  val numSamplesEncoded = Kamon.counter("memstore-samples-encoded").refine(tags)
  val encodedBytes  = Kamon.counter("memstore-encoded-bytes-allocated", MeasurementUnit.information.bytes).refine(tags)
  val encodedHistBytes = Kamon.counter("memstore-hist-encoded-bytes", MeasurementUnit.information.bytes).refine(tags)
  val flushesSuccessful = Kamon.counter("memstore-flushes-success").refine(tags)
  val flushesFailedPartWrite = Kamon.counter("memstore-flushes-failed-partition").refine(tags)
  val flushesFailedChunkWrite = Kamon.counter("memstore-flushes-failed-chunk").refine(tags)
  val flushesFailedOther = Kamon.counter("memstore-flushes-failed-other").refine(tags)

  val currentIndexTimeBucket = Kamon.gauge("memstore-index-timebucket-current").refine(tags)
  val indexTimeBucketBytesWritten = Kamon.counter("memstore-index-timebucket-bytes-total").refine(tags)
  val numKeysInLatestTimeBucket = Kamon.counter("memstore-index-timebucket-num-keys-total").refine(tags)
  val numRolledKeysInLatestTimeBucket = Kamon.counter("memstore-index-timebucket-num-rolled-keys-total").refine(tags)
  val indexRecoveryNumRecordsProcessed = Kamon.counter("memstore-index-recovery-records-processed").refine(tags)
  val downsampleRecordsCreated = Kamon.counter("memstore-downsample-records-created").refine(tags)

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
  val offsetLatestInMem = Kamon.gauge("shard-offset-latest-inmemory").refine(tags)
  val offsetLatestFlushed = Kamon.gauge("shard-offset-flushed-latest").refine(tags)
  val offsetEarliestFlushed = Kamon.gauge("shard-offset-flushed-earliest").refine(tags)
  val numPartitions = Kamon.gauge("num-partitions").refine(tags)
  val numActivelyIngestingParts = Kamon.gauge("num-ingesting-partitions").refine(tags)

  val numChunksPagedIn = Kamon.counter("chunks-paged-in").refine(tags)
  val partitionsPagedFromColStore = Kamon.counter("memstore-partitions-paged-in").refine(tags)
  val partitionsQueried = Kamon.counter("memstore-partitions-queried").refine(tags)
  val purgedPartitions = Kamon.counter("memstore-partitions-purged").refine(tags)
  val partitionsRestored = Kamon.counter("memstore-partitions-paged-restored").refine(tags)
  val chunkIdsEvicted = Kamon.counter("memstore-chunkids-evicted").refine(tags)
  val partitionsEvicted = Kamon.counter("memstore-partitions-evicted").refine(tags)
  val memoryStats = new MemoryStats(tags)

  val bufferPoolSize = Kamon.gauge("memstore-writebuffer-pool-size").refine(tags)
  val indexEntries = Kamon.gauge("memstore-index-entries").refine(tags)
  val indexBytes   = Kamon.gauge("memstore-index-ram-bytes").refine(tags)

  val evictedPartKeyBloomFilterQueries = Kamon.counter("evicted-pk-bloom-filter-queries").refine(tags)
  val evictedPartKeyBloomFilterFalsePositives = Kamon.counter("evicted-pk-bloom-filter-fp").refine(tags)
  val evictedPkBloomFilterSize = Kamon.gauge("evicted-pk-bloom-filter-approx-size").refine(tags)
  val evictedPartIdLookupMultiMatch = Kamon.counter("evicted-partId-lookup-multi-match").refine(tags)
}

object TimeSeriesShard {
  /**
    * Writes metadata for TSPartition where every vector is written
    */
  def writeMeta(addr: Long, partitionID: Int, info: ChunkSetInfo, vectors: Array[BinaryVectorPtr]): Unit = {
    UnsafeUtils.setInt(UnsafeUtils.ZeroPointer, addr, partitionID)
    ChunkSetInfo.copy(info, addr + 4)
    for { i <- 0 until vectors.size optimized } {
      ChunkSetInfo.setVectorPtr(addr + 4, i, vectors(i))
    }
  }

  /**
    * Copies serialized ChunkSetInfo bytes from persistent storage / on-demand paging.
    */
  def writeMeta(addr: Long, partitionID: Int, bytes: Array[Byte], vectors: Array[BinaryVectorPtr]): Unit = {
    UnsafeUtils.setInt(UnsafeUtils.ZeroPointer, addr, partitionID)
    ChunkSetInfo.copy(bytes, addr + 4)
    for { i <- 0 until vectors.size optimized } {
      ChunkSetInfo.setVectorPtr(addr + 4, i, vectors(i))
    }
  }

  val indexTimeBucketSchema = new RecordSchema(Seq(ColumnInfo("startTime", ColumnType.LongColumn),
    ColumnInfo("endTime", ColumnType.LongColumn),
    ColumnInfo("partKey", ColumnType.StringColumn)))

  // TODO make configurable if necessary
  val indexTimeBucketTtlPaddingSeconds = 24.hours.toSeconds.toInt
  val indexTimeBucketSegmentSize = 1024 * 1024 // 1MB

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

  private[memstore] final case class PartKey(base: Any, offset: Long)
  private[memstore] final val CREATE_NEW_PARTID = -1
}

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
  * @param downsampleConfig configuration for downsample operations
  * @param downsamplePublisher is shared among all shards of the dataset on the node
  */
class TimeSeriesShard(val dataset: Dataset,
                      val storeConfig: StoreConfig,
                      val shardNum: Int,
                      val bufferMemoryManager: MemFactory,
                      colStore: ColumnStore,
                      metastore: MetaStore,
                      evictionPolicy: PartitionEvictionPolicy,
                      downsampleConfig: DownsampleConfig,
                      downsamplePublisher: DownsamplePublisher)
                     (implicit val ioPool: ExecutionContext) extends StrictLogging {
  import collection.JavaConverters._

  import TimeSeriesShard._
  import FiloSchedulers._

  val shardStats = new TimeSeriesShardStats(dataset.ref, shardNum)

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
  private[memstore] final val partKeyIndex = new PartKeyLuceneIndex(dataset, shardNum, storeConfig)

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

  private val reclaimListener = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {
      assert(numBytes == dataset.blockMetaSize)
      val partID = UnsafeUtils.getInt(metaAddr)
      val chunkID = UnsafeUtils.getLong(metaAddr + 4)
      val partition = partitions.get(partID)
      if (partition != UnsafeUtils.ZeroPointer) {
        partition.removeChunksAt(chunkID)
      }
    }
  }

  // Create a single-threaded scheduler just for ingestion.  Name the thread for ease of debugging
  // NOTE: to control intermixing of different Observables/Tasks in this thread, customize ExecutionModel param
  val ingestSched = Scheduler.singleThread(s"$IngestSchedName-${dataset.ref}-$shardNum",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in TimeSeriesShard.ingestSched", _)))

  private val blockMemorySize = storeConfig.shardMemSize
  protected val numGroups = storeConfig.groupsPerShard
  private val chunkRetentionHours = (storeConfig.demandPagedRetentionPeriod.toSeconds / 3600).toInt
  val pagingEnabled = storeConfig.demandPagingEnabled

  private val ingestSchema = dataset.ingestionSchema
  private val recordComp = dataset.comparator
  private val timestampColId = dataset.timestampColumn.id

  /**
    * PartitionSet - access TSPartition using ingest record partition key in O(1) time.
    */
  private[memstore] final val partSet = PartitionSet.ofSize(InitialNumPartitions)
  // Use a StampedLock because it supports optimistic read locking. This means that no blocking
  // occurs in the common case, when there isn't any contention reading from partSet.
  private[memstore] final val partSetLock = new StampedLock

  // The off-heap block store used for encoded chunks
  private val context = Map("dataset" -> dataset.ref.dataset, "shard" -> shardNum.toString)
  private val blockStore = new PageAlignedBlockManager(blockMemorySize, shardStats.memoryStats, reclaimListener,
    storeConfig.numPagesPerBlock)
  private val blockFactoryPool = new BlockMemFactoryPool(blockStore, dataset.blockMetaSize, context)

  // Each shard has a single ingestion stream at a time.  This BlockMemFactory is used for buffer overflow encoding
  // strictly during ingest() and switchBuffers().
  private[core] val overflowBlockFactory = new BlockMemFactory(blockStore, None, dataset.blockMetaSize,
                                             context ++ Map("overflow" -> "true"), true)
  val partitionMaker = new DemandPagedChunkStore(this, blockStore, chunkRetentionHours)

  private val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, reuseOneContainer = true)
  private val partKeyArray = partKeyBuilder.allContainers.head.base.asInstanceOf[Array[Byte]]
  private[memstore] val bufferPool = new WriteBufferPool(bufferMemoryManager, dataset, storeConfig)

  private final val partitionGroups = Array.fill(numGroups)(new EWAHCompressedBitmap)

  /**
    * Bitmap to track actively ingesting partitions.
    * This bitmap is maintained in addition to the ingesting flag per partition.
    * TSP.ingesting is MUCH faster than bit.get(i) but we need the bitmap for faster operations
    * for all partitions of shard (like ingesting cardinality counting, rollover of time buckets etc).
    */
  private final val activelyIngesting = new EWAHCompressedBitmap

  private final val numTimeBucketsToRetain = Math.ceil(chunkRetentionHours.hours / storeConfig.flushInterval).toInt

  // Use 1/4 of max # buckets for initial ChunkMap size
  private val initInfoMapSize = Math.max((numTimeBucketsToRetain / 4) + 4, 20)

  /**
    * Current time bucket number. Time bucket number is initialized from last value stored in metastore
    * and is incremented each time a new bucket is prepared for flush.
    *
    * This value is mutated only from the ingestion thread, but read from both flush and ingestion threads.
    */
  @volatile
  private var currentIndexTimeBucket = 0

  /**
    * Timestamp to start searching for partitions to evict. Advances as more and more partitions are evicted.
    * Used to ensure we keep searching for newer and newer partitions to evict.
    */
  private[core] var evictionWatermark: Long = 0L

  /**
    * Keeps track of the list of partIds of partKeys to store in each index time bucket.
    * This is used to persist the time buckets, and track the partIds to roll over to latest
    * time bucket
    */
  private[memstore] final val timeBucketBitmaps = new NonBlockingHashMapLong[EWAHCompressedBitmap]()

  /**
    * This is the group during which this shard will flush time buckets. Randomized to
    * ensure we dont flush time buckets across shards at same time
    */
  private final val indexTimeBucketFlushGroup = Random.nextInt(numGroups)
  logger.info(s"Index time buckets for shard=$shardNum will flush in group $indexTimeBucketFlushGroup")

  initTimeBuckets()

  /**
    * The offset up to and including the last record in this group to be successfully persisted.
    * Also used during recovery to figure out what incoming records to skip (since it's persisted)
    */
  private final val groupWatermark = Array.fill(numGroups)(Long.MinValue)

  /**
    * Helper for downsampling ingested data for long term retention.
    */
  private final val shardDownsampler = new ShardDownsampler(dataset.name, shardNum,
    dataset.schema, dataset.schema.downsample.getOrElse(dataset.schema), downsampleConfig.enabled,
    downsampleConfig.resolutions, downsamplePublisher, shardStats)

  private[memstore] val evictedPartKeys =
    BloomFilter[PartKey](storeConfig.evictedPkBfCapacity, falsePositiveRate = 0.01)(new CanGenerateHashFrom[PartKey] {
      override def generateHash(from: PartKey): Long = {
        dataset.partKeySchema.partitionHash(from.base, from.offset)
      }
    })
  private var evictedPartKeysDisposed = false

  /**
    * Detailed filtered ingestion record logging.  See "trace-filters" StoreConfig setting.  Warning: may blow up
    * logs, use at your own risk.
    */
  val tracedPartFilters = storeConfig.traceFilters

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

  // An iterator over partitions looked up from partition keys stored as byte[]'s
  // Note that we cannot give skippedPartIDs because we don't have IDs only have keys
  // and we cannot look up IDs from keys since part keys are not indexed in Lucene
  case class ByteKeysPartitionIterator(keys: Seq[Array[Byte]]) extends PartitionIterator {
    val skippedPartIDs = debox.Buffer.empty[Int]
    private val partIt = keys.toIterator.flatMap(getPartition)
    final def hasNext: Boolean = partIt.hasNext
    final def next: TimeSeriesPartition = partIt.next
  }

  private val binRecordReader = new BinaryRecordRowReader(dataset.ingestionSchema)

  private[memstore] def initTimeBuckets() = {
    val highestIndexTimeBucket = Await.result(metastore.readHighestIndexTimeBucket(dataset.ref, shardNum), 1.minute)
    currentIndexTimeBucket = highestIndexTimeBucket.map(_ + 1).getOrElse(0)
    val earliestTimeBucket = Math.max(0, currentIndexTimeBucket - numTimeBucketsToRetain)
    for { i <- currentIndexTimeBucket to earliestTimeBucket by -1 optimized } {
      timeBucketBitmaps.put(i, new EWAHCompressedBitmap())
    }
  }

  // RECOVERY: Check the watermark for the group that this record is part of.  If the ingestOffset is < watermark,
  // then do not bother with the expensive partition key comparison and ingestion.  Just skip it
  class IngestConsumer(var ingestionTime: Long = 0,
                       var numActuallyIngested: Int = 0,
                       var ingestOffset: Long = -1L) extends BinaryRegionConsumer {
    // Receives a new ingestion BinaryRecord
    final def onNext(recBase: Any, recOffset: Long): Unit = {
      val group = partKeyGroup(ingestSchema, recBase, recOffset, numGroups)
      if (ingestOffset < groupWatermark(group)) {
        shardStats.rowsSkipped.increment
        try {
          // Needed to update index with new partitions added during recovery with correct startTime.
          // TODO:
          // explore aligning index time buckets with chunks, and we can then
          // remove this partition existence check per sample.
          val part: FiloPartition = getOrAddPartitionForIngestion(recBase, recOffset, group, ingestOffset)
          if (part == OutOfMemPartition) { disableAddPartitions() }
        } catch {
          case e: OutOfOffheapMemoryException => disableAddPartitions()
          case e: Exception                   => logger.error(s"Unexpected ingestion err", e); disableAddPartitions()
        }
      } else {
        binRecordReader.recordOffset = recOffset
        getOrAddPartitionAndIngest(ingestionTime, recBase, recOffset, group, ingestOffset)
        numActuallyIngested += 1
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
        binRecordReader.recordBase = container.base
        container.consumeRecords(ingestConsumer)
        shardStats.rowsIngested.increment(ingestConsumer.numActuallyIngested)
        shardStats.rowsPerContainer.record(ingestConsumer.numActuallyIngested)
        ingested += ingestConsumer.numActuallyIngested
        _offset = offset
      }
    } else {
      shardStats.oldContainers.increment
    }
    _offset
  }

  def startFlushingIndex(): Unit = partKeyIndex.startFlushThread()

  def ingest(data: SomeData): Long = ingest(data.records, data.offset)

  def recoverIndex(): Future[Unit] = {
    val p = Promise[Unit]()
    Future {
      assertThreadName(IngestSchedName)
      val tracer = Kamon.buildSpan("memstore-recover-index-latency")
        .withTag("dataset", dataset.name)
        .withTag("shard", shardNum).start()

      /* We need this map to track partKey->partId because lucene index cannot be looked up
       using partKey efficiently, and more importantly, it is eventually consistent.
        The map and contents will be garbage collected after we are done with recovery */
      val partIdMap = debox.Map.empty[BytesRef, Int]

      val earliestTimeBucket = Math.max(0, currentIndexTimeBucket - numTimeBucketsToRetain)
      logger.info(s"Recovering timebuckets $earliestTimeBucket to ${currentIndexTimeBucket - 1} " +
        s"for dataset=${dataset.ref} shard=$shardNum ")
      // go through the buckets in reverse order to first one wins and we need not rewrite
      // entries in lucene
      // no need to go into currentIndexTimeBucket since it is not present in cass
      val timeBuckets = for {tb <- currentIndexTimeBucket - 1 to earliestTimeBucket by -1} yield {
        colStore.getPartKeyTimeBucket(dataset, shardNum, tb).map { b =>
          new IndexData(tb, b.segmentId, RecordContainer(b.segment.array()))
        }
      }
      Observable.flatten(timeBuckets: _*)
        .foreach(tb => extractTimeBucket(tb, partIdMap))(ingestSched)
        .map(_ => completeIndexRecovery())(ingestSched)
        .onComplete { _ =>
          tracer.finish()
          p.success(())
        }(ingestSched)
    }(ingestSched)
    p.future
  }

  def completeIndexRecovery(): Unit = {
    assertThreadName(IngestSchedName)
    refreshPartKeyIndexBlocking()
    startFlushingIndex() // start flushing index now that we have recovered
    logger.info(s"Bootstrapped index for dataset=${dataset.ref} shard=$shardNum")
  }

  // scalastyle:off method.length
  private[memstore] def extractTimeBucket(segment: IndexData, partIdMap: debox.Map[BytesRef, Int]): Unit = {
    assertThreadName(IngestSchedName)
    var numRecordsProcessed = 0
    segment.records.iterate(indexTimeBucketSchema).foreach { row =>
      // read binary record and extract the indexable data fields
      val startTime: Long = row.getLong(0)
      val endTime: Long = row.getLong(1)
      val partKeyBaseOnHeap = row.getBlobBase(2).asInstanceOf[Array[Byte]]
      val partKeyOffset = row.getBlobOffset(2)
      val partKeyNumBytes = row.getBlobNumBytes(2)
      val partKeyBytesRef = new BytesRef(partKeyBaseOnHeap,
                                         PartKeyLuceneIndex.unsafeOffsetToBytesRefOffset(partKeyOffset),
                                         partKeyNumBytes)

      // look up partKey in partIdMap if it already exists before assigning new partId.
      // We cant look it up in lucene because we havent flushed index yet
      if (partIdMap.get(partKeyBytesRef).isEmpty) {
        val partId = if (endTime == Long.MaxValue) {
          // this is an actively ingesting partition
          val group = partKeyGroup(dataset.partKeySchema, partKeyBaseOnHeap, partKeyOffset, numGroups)
          val part = createNewPartition(partKeyBaseOnHeap, partKeyOffset, group, CREATE_NEW_PARTID, 4)
          // In theory, we should not get an OutOfMemPartition here since
          // it should have occurred before node failed too, and with data stopped,
          // index would not be updated. But if for some reason we see it, drop data
          if (part == OutOfMemPartition) {
            logger.error("Could not accommodate partKey while recovering index. " +
              "WriteBuffer size may not be configured correctly")
            None
          } else {
            val stamp = partSetLock.writeLock()
            try {
              partSet.add(part) // createNewPartition doesn't add part to partSet
              part.ingesting = true
              Some(part.partID)
            } finally {
              partSetLock.unlockWrite(stamp)
            }
          }
        } else {
          // partition assign a new partId to non-ingesting partition,
          // but no need to create a new TSPartition heap object
          // instead add the partition to evictedPArtKeys bloom filter so that it can be found if necessary
          evictedPartKeys.synchronized {
            require(!evictedPartKeysDisposed)
            evictedPartKeys.add(PartKey(partKeyBaseOnHeap, partKeyOffset))
          }
          Some(createPartitionID())
        }

        // add newly assigned partId to lucene index
        partId.foreach { partId =>
          partIdMap(partKeyBytesRef) = partId
          partKeyIndex.addPartKey(partKeyBaseOnHeap, partId, startTime, endTime,
            PartKeyLuceneIndex.unsafeOffsetToBytesRefOffset(partKeyOffset))(partKeyNumBytes)
          timeBucketBitmaps.get(segment.timeBucket).set(partId)
          activelyIngesting.synchronized {
            if (endTime == Long.MaxValue) activelyIngesting.set(partId)
            else activelyIngesting.clear(partId)
          }
        }
      } else {
        // partId has already been assigned for this partKey because we previously processed a later record in time.
        // Time buckets are processed in reverse order, and given last one wins and is used for index,
        // we skip this record and move on.
      }
      numRecordsProcessed += 1
    }
    shardStats.indexRecoveryNumRecordsProcessed.increment(numRecordsProcessed)
    logger.info(s"Recovered partKeys for dataset=${dataset.ref} shard=$shardNum" +
      s" timebucket=${segment.timeBucket} segment=${segment.segment} numRecordsInBucket=$numRecordsProcessed" +
      s" numPartsInIndex=${partIdMap.size} numIngestingParts=${partitions.size}")
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
  case class LabelValueResultIterator(partIterator: IntIterator, labelNames: Seq[String], limit: Int)
    extends Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] {
    var currVal: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = _
    var index = 0

    override def hasNext: Boolean = {
      var foundValue = false
      while(partIterator.hasNext && index < limit && !foundValue) {
        val partId = partIterator.next()
        val nextPart = getPartitionFromPartId(partId)
        if (nextPart != UnsafeUtils.ZeroPointer) {
          // FIXME This is non-performant and temporary fix for fetching label values based on filter criteria.
          // Other strategies needs to be evaluated for making this performant - create facets for predefined fields or
          // have a centralized service/store for serving metadata
          currVal = dataset.partKeySchema.toStringPairs(nextPart.partKeyBase, nextPart.partKeyOffset)
            .filter(labelNames contains _._1).map(pair => {
            (pair._1.utf8 -> pair._2.utf8)
          }).toMap
          foundValue = currVal.size > 0
        } else {
          // FIXME partKey is evicted. Get partition key from lucene index
        }
      }
      foundValue
    }

    override def next(): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = {
      index += 1
      currVal
    }
  }

  /**
    * This method is to apply column filters and fetch matching time series partition keys.
    */
  def partKeysWithFilters(filter: Seq[ColumnFilter],
                          endTime: Long,
                          startTime: Long,
                          limit: Int): Iterator[TimeSeriesPartition] = {
    partKeyIndex.partIdsFromFilters(filter, startTime, endTime)
      .map(getPartitionFromPartId, limit)
      .filter(_ != UnsafeUtils.ZeroPointer) // Needed since we have not addressed evicted partitions yet
  }

  implicit class IntIteratorMapper[T](intIterator: IntIterator) {
    def map(f: Int => T, limit: Int): Iterator[T] = new Iterator[T] {
      var currIndex: Int = 0
      override def hasNext: Boolean = intIterator.hasNext && currIndex < limit
      override def next(): T = {
        currIndex += 1; f(intIterator.next())
      }
    }
  }

  /**
    * WARNING, returns null for evicted partitions
    */
  private def getPartitionFromPartId(partId: Int): TimeSeriesPartition = {
    val nextPart = partitions.get(partId)
    if (nextPart == UnsafeUtils.ZeroPointer)
      logger.warn(s"PartId=$partId was not found in memory and was not included in metadata query result. ")
    // TODO Revisit this code for evicted partitions
    /*if (nextPart == UnsafeUtils.ZeroPointer) {
      val partKey = partKeyIndex.partKeyFromPartId(partId)
      //map partKey bytes to UTF8String
    }*/
    nextPart
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

  // Rapidly switch all of the input buffers for a particular group
  // This MUST be done in the same thread/stream as input records to avoid concurrency issues
  // and ensure that all the partitions in a group are switched at the same watermark
  def switchGroupBuffers(groupNum: Int): Unit = {
    logger.debug(s"Switching write buffers for group $groupNum in dataset=${dataset.ref} shard=$shardNum")
    InMemPartitionIterator(partitionGroups(groupNum).intIterator).foreach(_.switchBuffers(overflowBlockFactory))
  }

  /**
    * Prepare to flush current index records, switch current currentIndexTimeBucket partId bitmap with new one.
    * Return Some if part keys need to be flushed (happens for last flush group). Otherwise, None.
    *
    * NEEDS TO RUN ON INGESTION THREAD since it removes entries from the partition data structures.
    */
  def prepareIndexTimeBucketForFlush(group: Int): Option[FlushIndexTimeBuckets] = {
    assertThreadName(IngestSchedName)
    if (group == indexTimeBucketFlushGroup) {
      logger.debug(s"Switching timebucket=$currentIndexTimeBucket in dataset=${dataset.ref}" +
        s"shard=$shardNum out for flush. ")
      currentIndexTimeBucket += 1
      shardStats.currentIndexTimeBucket.set(currentIndexTimeBucket)
      timeBucketBitmaps.put(currentIndexTimeBucket, new EWAHCompressedBitmap())
      purgeExpiredPartitions()
      Some(FlushIndexTimeBuckets(currentIndexTimeBucket-1))
    } else {
      None
    }
  }

  private def purgeExpiredPartitions(): Unit = ingestSched.executeTrampolined { () =>
    assertThreadName(IngestSchedName)
    val partsToPurge = partKeyIndex.partIdsEndedBefore(
      System.currentTimeMillis() - storeConfig.demandPagedRetentionPeriod.toMillis)
    var numDeleted = 0
    val removedParts = new EWAHCompressedBitmap()
    InMemPartitionIterator(partsToPurge.intIterator()).foreach { p =>
      if (!p.ingesting) {
        logger.debug(s"Purging partition with partId=${p.partID} from memory in dataset=${dataset.ref} shard=$shardNum")
        removePartition(p)
        removedParts.set(p.partID)
        numDeleted += 1
      }
    }
    if (!removedParts.isEmpty) partKeyIndex.removePartKeys(removedParts)
    if (numDeleted > 0) logger.info(s"Purged $numDeleted partitions from memory and " +
                        s"index from dataset=${dataset.ref} shard=$shardNum")
    shardStats.purgedPartitions.increment(numDeleted)
  }

  def createFlushTask(flushGroup: FlushGroup): Task[Response] = {
    assertThreadName(IngestSchedName)
    // clone the bitmap so that reads on the flush thread do not conflict with writes on ingestion thread
    val partitionIt = InMemPartitionIterator(partitionGroups(flushGroup.groupNum).clone().intIterator)
    doFlushSteps(flushGroup, partitionIt)
  }

  private def updateGauges(): Unit = {
    assertThreadName(IngestSchedName)
    shardStats.bufferPoolSize.set(bufferPool.poolSize)
    shardStats.indexEntries.set(partKeyIndex.indexNumEntries)
    shardStats.indexBytes.set(partKeyIndex.indexRamBytes)
    shardStats.numPartitions.set(numActivePartitions)
    val cardinality = activelyIngesting.synchronized { activelyIngesting.cardinality() }
    shardStats.numActivelyIngestingParts.set(cardinality)

    // Also publish MemFactory stats. Instance is expected to be shared, but no harm in
    // publishing a little more often than necessary.
    bufferMemoryManager.updateStats()
  }

  private def addPartKeyToTimebucketRb(timebucketNum: Int, indexRb: RecordBuilder, p: TimeSeriesPartition) = {
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
    indexRb.startNewRecord(indexTimeBucketSchema, 0)
    indexRb.addLong(startTime)
    indexRb.addLong(endTime)
    // Need to add 4 to include the length bytes
    indexRb.addBlob(p.partKeyBase, p.partKeyOffset, BinaryRegionLarge.numBytes(p.partKeyBase, p.partKeyOffset) + 4)
    logger.debug(s"Added entry into timebucket=${timebucketNum} partId=${p.partID} in dataset=${dataset.ref} " +
      s"shard=$shardNum partKey[${p.stringPartition}] with startTime=$startTime endTime=$endTime")
    indexRb.endRecord(false)
  }

  // scalastyle:off method.length
  private def doFlushSteps(flushGroup: FlushGroup,
                           partitionIt: Iterator[TimeSeriesPartition]): Task[Response] = {
    assertThreadName(IngestSchedName)

    val tracer = Kamon.buildSpan("chunk-flush-task-latency-after-retries")
      .withTag("dataset", dataset.name)
      .withTag("shard", shardNum).start()

    // Only allocate the blockHolder when we actually have chunks/partitions to flush
    val blockHolder = blockFactoryPool.checkout(Map("flushGroup" -> flushGroup.groupNum.toString))

    // This initializes the containers for the downsample records. Yes, we create new containers
    // and not reuse them at the moment and there is allocation for every call of this method
    // (once per minute). We can perhaps use a thread-local or a pool if necessary after testing.
    val downsampleRecords = shardDownsampler.newEmptyDownsampleRecords

    val chunkSetIter = partitionIt.flatMap { p =>

      // TODO re-enable following assertion. Am noticing that monix uses TrampolineExecutionContext
      // causing the iterator to be consumed synchronously in some cases. It doesnt
      // seem to be consistent environment to environment.
      // assertThreadName(IOSchedName)

      /* Step 2: Make chunks to be flushed for each partition */
      val chunks = p.makeFlushChunks(blockHolder)

      /* VERY IMPORTANT: This block is lazy and is executed when chunkSetIter is consumed
         in writeChunksFuture below */

      /* Step 3: Add downsample records for the chunks into the downsample record builders */
      shardDownsampler.populateDownsampleRecords(p, p.infosToBeFlushed, downsampleRecords)

      /* Step 4: Update endTime of all partKeys that stopped ingesting in this flush period.
         If we are flushing time buckets, use its timeBucketId, otherwise, use currentTimeBucket id. */
      updateIndexWithEndTime(p, chunks, flushGroup.flushTimeBuckets.map(_.timeBucket).getOrElse(currentIndexTimeBucket))
      chunks
    }

    // Note that all cassandra writes below  will have included retries. Failures after retries will imply data loss
    // in order to keep the ingestion moving. It is important that we don't fall back far behind.

    /* Step 1: Kick off partition iteration to persist chunks to column store */
    val writeChunksFuture = writeChunks(flushGroup, chunkSetIter, partitionIt, blockHolder)

    /* Step 5.1: Publish the downsample record data collected to the downsample dataset.
     * We recover future since we want to proceed to publish downsample data even if chunk flush failed.
     * This is done after writeChunksFuture because chunkSetIter is lazy. */
    val pubDownsampleFuture = writeChunksFuture.recover {case _ => Success}
      .flatMap { _ =>
        assertThreadName(IOSchedName)
        shardDownsampler.publishToDownsampleDataset(downsampleRecords)
      }

    /* Step 5.2: We flush index time buckets in the one designated group for each shard
     * We recover future since we want to proceed to write time buckets even if chunk flush failed.
     * This is done after writeChunksFuture because chunkSetIter is lazy. */
    val writeIndexTimeBucketsFuture = writeChunksFuture.recover {case _ => Success}
      .flatMap( _=> writeTimeBuckets(flushGroup))

    /* Step 6: Checkpoint after time buckets and chunks are flushed */
    val result = Future.sequence(Seq(writeChunksFuture, writeIndexTimeBucketsFuture, pubDownsampleFuture)).map {
      _.find(_.isInstanceOf[ErrorResponse]).getOrElse(Success)
    }.flatMap {
      case Success           => blockHolder.markUsedBlocksReclaimable()
                                commitCheckpoint(dataset.ref, shardNum, flushGroup)
      case er: ErrorResponse => Future.successful(er)
    }.recover { case e =>
      logger.error(s"Internal Error when persisting chunks in dataset=${dataset.ref} shard=$shardNum - should " +
        s"have not reached this state", e)
      DataDropped
    }
    result.onComplete { resp =>
      assertThreadName(IngestSchedName)
      try {
        blockFactoryPool.release(blockHolder)
        flushDoneTasks(flushGroup, resp)
        tracer.finish()
      } catch { case e: Throwable =>
        logger.error(s"Error when wrapping up doFlushSteps in dataset=${dataset.ref} shard=$shardNum", e)
      }
    }(ingestSched)
    // Note: The data structures accessed by flushDoneTasks can only be safely accessed by the
    //       ingestion thread, hence the onComplete steps are run from that thread.
    Task.fromFuture(result)
  }

  protected def flushDoneTasks(flushGroup: FlushGroup, resTry: Try[Response]): Unit = {
    assertThreadName(IngestSchedName)
    resTry.foreach { resp =>
      logger.info(s"Flush of dataset=${dataset.ref} shard=$shardNum group=${flushGroup.groupNum} " +
        s"timebucket=${flushGroup.flushTimeBuckets.map(_.timeBucket)} " +
        s"flushWatermark=${flushGroup.flushWatermark} response=$resp offset=${_offset}")
    }
    partitionMaker.cleanupOldestBuckets()
    // Some partitions might be evictable, see if need to free write buffer memory
    checkEnableAddPartitions()
    updateGauges()
  }

  // scalastyle:off method.length
  private def writeTimeBuckets(flushGroup: FlushGroup): Future[Response] = {
    assertThreadName(IOSchedName)
    flushGroup.flushTimeBuckets.map { cmd =>
      val rbTrace = Kamon.buildSpan("memstore-index-timebucket-populate-timebucket")
        .withTag("dataset", dataset.name)
        .withTag("shard", shardNum).start()

      /* Note regarding thread safety of accessing time bucket bitmaps:

         Every flush task reads bits on the earliest time bucket bitmap and sets bits on the
         latest timeBucket, both of which are uniquely associated with the flush group. Since
         each flush group is associated with different latest and earliest time buckets,
         concurrent threads should not be reading or writing to same time bucket bitmaps, or
         even setting the same time bucket in the collection. This can in theory happen only if
         a flush task lasts more than the retention period (not possible).
      */

      /* Add to timeBucketRb partKeys for (earliestTimeBucketBitmap && ~stoppedIngesting).
       These keys are from earliest time bucket that are still ingesting */
      val earliestTimeBucket = cmd.timeBucket - numTimeBucketsToRetain
      if (earliestTimeBucket >= 0) {
        var partIdsToRollOver = timeBucketBitmaps.get(earliestTimeBucket)
        activelyIngesting.synchronized {
          partIdsToRollOver = partIdsToRollOver.and(activelyIngesting)
        }
        val newBitmap = timeBucketBitmaps.get(cmd.timeBucket).or(partIdsToRollOver)
        timeBucketBitmaps.put(cmd.timeBucket, newBitmap)
        shardStats.numRolledKeysInLatestTimeBucket.increment(partIdsToRollOver.cardinality())
      }

      /* Remove the earliest time bucket from memory now that we have rolled over data */
      timeBucketBitmaps.remove(earliestTimeBucket)

      /* create time bucket using record builder */
      val timeBucketRb = new RecordBuilder(MemFactory.onHeapFactory, indexTimeBucketSegmentSize)
      InMemPartitionIterator(timeBucketBitmaps.get(cmd.timeBucket).intIterator).foreach { p =>
        addPartKeyToTimebucketRb(cmd.timeBucket, timeBucketRb, p)
      }
      val numPartKeysInBucket = timeBucketBitmaps.get(cmd.timeBucket).cardinality()
      logger.debug(s"Number of records in timebucket=${cmd.timeBucket} of " +
        s"dataset=${dataset.ref} shard=$shardNum is $numPartKeysInBucket")
      shardStats.numKeysInLatestTimeBucket.increment(numPartKeysInBucket)

      /* compress and persist index time bucket bytes */
      val blobToPersist = timeBucketRb.optimalContainerBytes(true)
      rbTrace.finish()
      shardStats.indexTimeBucketBytesWritten.increment(blobToPersist.map(_.length).sum)
      // we pad to C* ttl to ensure that data lives for longer than time bucket roll over time
      colStore.writePartKeyTimeBucket(dataset, shardNum, cmd.timeBucket, blobToPersist,
        flushGroup.diskTimeToLiveSeconds + indexTimeBucketTtlPaddingSeconds).flatMap {
        case Success => /* Persist the highest time bucket id in meta store */
          writeHighestTimebucket(shardNum, cmd.timeBucket)
        case er: ErrorResponse =>
          logger.error(s"Failure for flush of timeBucket=${cmd.timeBucket} and rollover of " +
            s"earliestTimeBucket=$earliestTimeBucket for dataset=${dataset.ref} shard=$shardNum : $er")
          // TODO missing persistence of a time bucket even after c* retries may result in inability to query
          // existing data. Revisit later for better resilience for long c* failure
          Future.successful(er)
      }.map { case resp =>
        logger.info(s"Finished flush for timeBucket=${cmd.timeBucket} with ${blobToPersist.length} segments " +
          s"and rollover of earliestTimeBucket=$earliestTimeBucket with resp=$resp for dataset=${dataset.ref} " +
          s"shard=$shardNum")
        resp
      }.recover { case e =>
        logger.error(s"Internal Error when persisting time bucket in dataset=${dataset.ref} shard=$shardNum - " +
          "should have not reached this state", e)
        DataDropped
      }
    }.getOrElse(Future.successful(Success))
  }
  // scalastyle:on method.length

  private def writeChunks(flushGroup: FlushGroup,
                          chunkSetIt: Iterator[ChunkSet],
                          partitionIt: Iterator[TimeSeriesPartition],
                          blockHolder: BlockMemFactory): Future[Response] = {
    assertThreadName(IngestSchedName)

    val chunkSetStream = Observable.fromIterator(chunkSetIt)
    logger.debug(s"Created flush ChunkSets stream for group ${flushGroup.groupNum} in " +
      s"dataset=${dataset.ref} shard=$shardNum")

    colStore.write(dataset, chunkSetStream, flushGroup.diskTimeToLiveSeconds).recover { case e =>
      logger.error(s"Critical! Chunk persistence failed after retries and skipped in dataset=${dataset.ref} " +
        s"shard=$shardNum", e)
      shardStats.flushesFailedChunkWrite.increment

      // Encode and free up the remainder of the WriteBuffers that have not been flushed yet.  Otherwise they will
      // never be freed.
      partitionIt.foreach(_.encodeAndReleaseBuffers(blockHolder))
      // If the above futures fail with ErrorResponse because of DB failures, skip the chunk.
      // Sorry - need to drop the data to keep the ingestion moving
      DataDropped
    }
  }

  private def writeHighestTimebucket(shardNum: Int, timebucket: Int): Future[Response] = {
    assertThreadName(IOSchedName)
    metastore.writeHighestIndexTimeBucket(dataset.ref, shardNum, timebucket).recover { case e =>
      logger.error(s"Critical! Highest Time Bucket persistence skipped after retries failed in " +
        s"dataset=${dataset.ref} shard=$shardNum", e)
      // Sorry - need to skip to keep the ingestion moving
      DataDropped
    }
  }

  private[memstore] def updatePartEndTimeInIndex(p: TimeSeriesPartition, endTime: Long): Unit =
    partKeyIndex.updatePartKeyWithEndTime(p.partKeyBytes, p.partID, endTime)()

  private def updateIndexWithEndTime(p: TimeSeriesPartition,
                                     partFlushChunks: Iterator[ChunkSet],
                                     timeBucket: Int) = {
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
        timeBucketBitmaps.get(timeBucket).set(p.partID)
        activelyIngesting.clear(p.partID)
        p.ingesting = false
      }
    }
  }

  private def commitCheckpoint(ref: DatasetRef, shardNum: Int, flushGroup: FlushGroup): Future[Response] = {
    assertThreadName(IOSchedName)
    // negative checkpoints are refused by Kafka, and also offsets should be positive
    if (flushGroup.flushWatermark > 0) {
      val fut = metastore.writeCheckpoint(ref, shardNum, flushGroup.groupNum, flushGroup.flushWatermark).map { r =>
        shardStats.flushesSuccessful.increment
        r
      }.recover { case e =>
        logger.error(s"Critical! Checkpoint persistence skipped in dataset=${dataset.ref} shard=$shardNum", e)
        shardStats.flushesFailedOther.increment
        // skip the checkpoint write
        // Sorry - need to skip to keep the ingestion moving
        DataDropped
      }
      // Update stats
      if (_offset >= 0) shardStats.offsetLatestInMem.set(_offset)
      groupWatermark(flushGroup.groupNum) = flushGroup.flushWatermark
      val maxWatermark = groupWatermark.max
      val minWatermark = groupWatermark.min
      if (maxWatermark >= 0) shardStats.offsetLatestFlushed.set(maxWatermark)
      if (minWatermark >= 0) shardStats.offsetEarliestFlushed.set(minWatermark)
      fut
    } else {
      Future.successful(NotApplied)
    }
  }

  private[memstore] val addPartitionsDisabled = AtomicBoolean(false)

  // scalastyle:off null
  private[filodb] def getOrAddPartitionForIngestion(recordBase: Any, recordOff: Long,
                                                    group: Int, ingestOffset: Long) = {
    var part = partSet.getWithIngestBR(recordBase, recordOff, dataset)
    if (part == null) {
      part = addPartitionForIngestion(recordBase, recordOff, group)
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
      val filters = dataset.partKeySchema.toStringPairs(partKeyBase, partKeyOffset)
        .map { pair => ColumnFilter(pair._1, Filter.Equals(pair._2)) }
      val matches = partKeyIndex.partIdsFromFilters2(filters, 0, Long.MaxValue)
      matches.cardinality() match {
        case 0 =>           shardStats.evictedPartKeyBloomFilterFalsePositives.increment()
                            CREATE_NEW_PARTID
        case c if c >= 1 => // NOTE: if we hit one partition, we cannot directly call it out as the result without
                            // verifying the partKey since the matching partition may have had an additional tag
                            if (c > 1) shardStats.evictedPartIdLookupMultiMatch.increment()
                            val iter = matches.intIterator()
                            var partId = -1
                            do {
                              // find the most specific match for the given ingestion record
                              val nextPartId = iter.next
                              partKeyIndex.partKeyFromPartId(nextPartId).foreach { candidate =>
                                if (dataset.partKeySchema.equals(partKeyBase, partKeyOffset,
                                      candidate.bytes, PartKeyLuceneIndex.bytesRefToUnsafeOffset(candidate.offset))) {
                                  partId = nextPartId
                                  logger.debug(s"There is already a partId=$partId assigned for " +
                                    s"${dataset.partKeySchema.stringify(partKeyBase, partKeyOffset)} in" +
                                    s" dataset=${dataset.ref} shard=$shardNum")
                                }
                              }
                            } while (iter.hasNext && partId != -1)
                            if (partId == CREATE_NEW_PARTID)
                              shardStats.evictedPartKeyBloomFilterFalsePositives.increment()
                            partId
      }
    } else CREATE_NEW_PARTID
  }

  /**
    * Adds new partition with appropriate partId. If it is a newly seen partKey, then new partId is assigned.
    * If it is a previously seen partKey that is already in index, it reassigns same partId so that indexes
    * are still valid.
    *
    * This method also updates lucene index and time bucket bitmaps properly.
    */
  private def addPartitionForIngestion(recordBase: Any, recordOff: Long, group: Int) = {
    assertThreadName(IngestSchedName)
    val partKeyOffset = recordComp.buildPartKeyFromIngest(recordBase, recordOff, partKeyBuilder)
    val previousPartId = lookupPreviouslyAssignedPartId(partKeyArray, partKeyOffset)
    val newPart = createNewPartition(partKeyArray, partKeyOffset, group, previousPartId)
    if (newPart != OutOfMemPartition) {
      val partId = newPart.partID
      // NOTE: Don't use binRecordReader here.  recordOffset might not be set correctly
      val startTime = dataset.ingestionSchema.getLong(recordBase, recordOff, timestampColId)
      if (previousPartId == CREATE_NEW_PARTID) {
        // add new lucene entry if this partKey was never seen before
        partKeyIndex.addPartKey(newPart.partKeyBytes, partId, startTime)() // causes endTime to be set to Long.MaxValue
      } else {
        // newly created partition is re-ingesting now, so update endTime
        updatePartEndTimeInIndex(newPart, Long.MaxValue)
      }
      timeBucketBitmaps.get(currentIndexTimeBucket).set(partId) // causes current time bucket to include this partId
      activelyIngesting.synchronized {
        activelyIngesting.set(partId)
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
                                 group: Int, ingestOffset: Long): Unit = {
    assertThreadName(IngestSchedName)
    try {
      val part: FiloPartition = getOrAddPartitionForIngestion(recordBase, recordOff, group, ingestOffset)
      if (part == OutOfMemPartition) {
        disableAddPartitions()
      }
      else {
        val tsp = part.asInstanceOf[TimeSeriesPartition]
        tsp.ingest(ingestionTime, binRecordReader, overflowBlockFactory)
        // Below is coded to work concurrently with logic in updateIndexWithEndTime
        // where we try to de-activate an active time series
        if (!tsp.ingesting) {
          // DO NOT use activelyIngesting to check above condition since it is slow and is called for every sample
          activelyIngesting.synchronized {
            if (!tsp.ingesting) {
              // time series was inactive and has just started re-ingesting
              updatePartEndTimeInIndex(part.asInstanceOf[TimeSeriesPartition], Long.MaxValue)
              timeBucketBitmaps.get(currentIndexTimeBucket).set(part.partID)
              activelyIngesting.set(part.partID)
              tsp.ingesting = true
            }
          }
        }
      }
    } catch {
      case e: OutOfOffheapMemoryException => disableAddPartitions()
      case e: Exception => logger.error(s"Unexpected ingestion err in dataset=${dataset.ref} " +
        s"shard=$shardNum", e);
        disableAddPartitions()
    }
  }

  private def shouldTrace(partKeyAddr: Long): Boolean = {
    tracedPartFilters.nonEmpty && {
      val partKeyPairs = dataset.partKeySchema.toStringPairs(UnsafeUtils.ZeroPointer, partKeyAddr)
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
                                   group: Int, usePartId: Int,
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
      val newPart = if (shouldTrace(partKeyAddr)) {
        logger.debug(s"Adding tracing TSPartition dataset=${dataset.ref} shard=$shardNum group=$group partId=$partId")
        new TracingTimeSeriesPartition(
          partId, dataset, partKeyAddr, shardNum, bufferPool, shardStats, bufferMemoryManager, initMapSize)
      } else {
        new TimeSeriesPartition(
          partId, dataset, partKeyAddr, shardNum, bufferPool, shardStats, bufferMemoryManager, initMapSize)
      }
      partitions.put(partId, newPart)
      shardStats.partitionsCreated.increment
      partitionGroups(group).set(partId)
      newPart
    }
  }

  private def disableAddPartitions(): Unit = {
    assertThreadName(IngestSchedName)
    if (addPartitionsDisabled.compareAndSet(false, true))
      logger.warn(s"dataset=${dataset.ref} shard=$shardNum: Out of buffer memory and not able to evict enough; " +
        s"adding partitions disabled")
    shardStats.dataDropped.increment
  }

  private def checkEnableAddPartitions(): Unit = {
    assertThreadName(IngestSchedName)
    if (addPartitionsDisabled()) {
      if (ensureFreeSpace()) {
        logger.info(s"dataset=${dataset.ref} shard=$shardNum: Enough free space to add partitions again!  Yay!")
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
        logger.info(s"dataset=${dataset.ref} shard=$shardNum nextPartitionID has wrapped around to 0 again")
      }
    } while (partitions.containsKey(nextPartitionID))

    id
  }

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
        logger.warn(s"dataset=${dataset.ref} shard=$shardNum: No partitions to evict but we are still low on space. " +
          s"DATA WILL BE DROPPED")
        return false
      }
      lastPruned = prunedPartitions

      // Pruning group bitmaps
      for { group <- 0 until numGroups } {
        partitionGroups(group) = partitionGroups(group).andNot(prunedPartitions)
      }

      // Finally, prune partitions and keyMap data structures
      logger.info(s"Evicting partitions from dataset=${dataset.ref} shard=$shardNum, watermark=$evictionWatermark...")
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
            logger.debug(s"Evicting partId=${partitionObj.partID} from dataset=${dataset.ref} shard=$shardNum")
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
      shardStats.evictedPkBloomFilterSize.set(elemCount)
      evictionWatermark = maxEndTime + 1
      // Plus one needed since there is a possibility that all partitions evicted in this round have same endTime,
      // and there may be more partitions that are not evicted with same endTime. If we didnt advance the watermark,
      // we would be processing same partIds again and again without moving watermark forward.
      // We may skip evicting some partitions by doing this, but the imperfection is an acceptable
      // trade-off for performance and simplicity. The skipped partitions, will ve removed during purge.
      logger.info(s"dataset=${dataset.ref} shard=$shardNum: evicted $partsRemoved partitions," +
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
      part = partSet.getWithPartKeyBR(partKey, UnsafeUtils.arayOffset, dataset)
    }
    if (!partSetLock.validate(stamp)) {
      // Because the stamp changed, the write lock was acquired and the set likely changed.
      // Try again with a full read lock, which will block if necessary so as to not run
      // concurrently with any thread making changes to the set. This guarantees that
      // the correct partition is returned.
      stamp = partSetLock.readLock()
      try {
        part = partSet.getWithPartKeyBR(partKey, UnsafeUtils.arayOffset, dataset)
      } finally {
        partSetLock.unlockRead(stamp)
      }
    }
    part.map(_.asInstanceOf[TimeSeriesPartition])
  }

  /**
    * Result of a iteratePartitions method call.
    *
    * Note that there is a leak in abstraction here we should not be talking about ODP here.
    * ODPagingShard really should not have been a sub-class of TimeSeriesShard. Instead
    * composition should have been used instead of inheritance. Overriding the iteratePartitions()
    * method is the best I could do to keep the leak minimal and not increase scope.
    *
    * TODO: clean this all up in a bigger refactoring effort later.
    *
    * @param partsInMemoryIter iterates through the in-Memory partitions, some of which may not need ODP.
    *                          Caller needs to filter further
    * @param partIdsInMemoryMayNeedOdp has partIds from partsInMemoryIter in memory that may need chunk ODP. Their
    *                          startTimes from Lucene are included
    * @param partIdsNotInMemory is a collection of partIds fully not in memory
    */
  case class IterationResult(partsInMemoryIter: PartitionIterator,
                             partIdsInMemoryMayNeedOdp: debox.Map[Int, Long] = debox.Map.empty,
                             partIdsNotInMemory: debox.Buffer[Int] = debox.Buffer.empty)

  /**
    * See documentation for IterationResult.
    */
  def iteratePartitions(partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod): IterationResult = partMethod match {
    case SinglePartitionScan(partition, _) => IterationResult(ByteKeysPartitionIterator(Seq(partition)))
    case MultiPartitionScan(partKeys, _)   => IterationResult(ByteKeysPartitionIterator(partKeys))
    case FilteredPartitionScan(split, filters) =>
      // TODO: There are other filters that need to be added and translated to Lucene queries
      if (filters.nonEmpty) {
        val indexIt = partKeyIndex.partIdsFromFilters(filters, chunkMethod.startTime, chunkMethod.endTime)
        IterationResult(new InMemPartitionIterator(indexIt))
      } else {
        IterationResult(PartitionIterator.fromPartIt(partitions.values.iterator.asScala))
      }
  }

  def scanPartitions(columnIDs: Seq[Types.ColumnId],
                     partMethod: PartitionScanMethod,
                     chunkMethod: ChunkScanMethod): Observable[ReadablePartition] = {
    Observable.fromIterator(iteratePartitions(partMethod, chunkMethod).partsInMemoryIter.map { p =>
      shardStats.partitionsQueried.increment()
      p
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
    logger.info(s"Clearing all MemStore state for dataset=${dataset.ref} shard=$shardNum")
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
    logger.info(s"Shutting down dataset=${dataset.ref} shard=$shardNum")
    /* Don't explcitly free the memory just yet. These classes instead rely on a finalize
       method to ensure that no threads are accessing the memory before it's freed.
    blockStore.releaseBlocks()
    */
  }
}
