package filodb.core.memstore

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.execution.atomic.AtomicBoolean
import monix.execution.Scheduler
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong
import scalaxy.loops._

import filodb.core.{ErrorResponse, _}
import filodb.core.binaryrecord2._
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.store._
import filodb.memory._
import filodb.memory.data.{OffheapLFSortedIDMap, OffheapLFSortedIDMapMutator}
import filodb.memory.format.{RowReader, UnsafeUtils, ZeroCopyUTF8String => UTF8Str}
import filodb.memory.format.BinaryVector.BinaryVectorPtr


class TimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

  val rowsIngested = Kamon.counter("memstore-rows-ingested").refine(tags)
  val partitionsCreated = Kamon.counter("memstore-partitions-created").refine(tags)
  val dataDropped = Kamon.counter("memstore-data-dropped").refine(tags)
  val rowsSkipped  = Kamon.counter("recovery-row-skipped").refine(tags)
  val rowsPerContainer = Kamon.histogram("num-samples-per-container")
  val numSamplesEncoded = Kamon.counter("memstore-samples-encoded").refine(tags)
  val encodedBytes  = Kamon.counter("memstore-encoded-bytes-allocated", MeasurementUnit.information.bytes).refine(tags)
  val flushesSuccessful = Kamon.counter("memstore-flushes-success").refine(tags)
  val flushesFailedPartWrite = Kamon.counter("memstore-flushes-failed-partition").refine(tags)
  val flushesFailedChunkWrite = Kamon.counter("memstore-flushes-failed-chunk").refine(tags)
  val flushesFailedOther = Kamon.counter("memstore-flushes-failed-other").refine(tags)
  val currentIndexTimeBucket = Kamon.gauge("memstore-index-timebucket-current").refine(tags)
  val indexTimeBucketBytesWritten = Kamon.counter("memstore-index-timebucket-bytes-total").refine(tags)
  val numKeysInLatestTimeBucket = Kamon.counter("memstore-index-timebucket-num-keys-total").refine(tags)
  val numRolledKeysInLatestTimeBucket = Kamon.counter("memstore-index-timebucket-num-rolled-keys-total").refine(tags)

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

  val partitionsPagedFromColStore = Kamon.counter("memstore-partitions-paged-in").refine(tags)
  val chunkIdsPagedFromColStore = Kamon.counter("memstore-chunkids-paged-in").refine(tags)
  val chunkIdsEvicted = Kamon.counter("memstore-chunkids-evicted").refine(tags)
  val partitionsQueried = Kamon.counter("memstore-partitions-queried").refine(tags)
  val numChunksQueried = Kamon.counter("memstore-chunks-queried").refine(tags)
  val partitionsEvicted = Kamon.counter("memstore-partitions-evicted").refine(tags)
  val memoryStats = new MemoryStats(tags)

  val bufferPoolSize = Kamon.gauge("memstore-writebuffer-pool-size").refine(tags)
  val indexEntries = Kamon.gauge("memstore-index-entries").refine(tags)
  val indexBytes   = Kamon.gauge("memstore-index-ram-bytes").refine(tags)
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

  val indexTimeBucketSchema = new RecordSchema(Seq(ColumnType.LongColumn,  // startTime
                                               ColumnType.LongColumn,       // endTime
                                               ColumnType.StringColumn))    // partKey bytes

  // TODO make configurable if necessary
  val indexTimeBucketTtlPaddingSeconds = 24.hours.toSeconds.toInt
  val indexTimeBucketSegmentSize = 1024 * 1024 // 1MB

  // Initial size of partitionSet and partition map structures.  Make large enough to avoid too many resizes.
  val InitialNumPartitions = 128 * 1024

  // Not a real partition, just a special marker for "out of memory"
  val OutOfMemPartition = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeriesPartition]

  val EmptyBitmap = new EWAHCompressedBitmap()
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
  * @param storeConfig the store portion of the sourceconfig, not the global FiloDB application config
  */
class TimeSeriesShard(val dataset: Dataset,
                      storeConfig: StoreConfig,
                      val shardNum: Int,
                      colStore: ColumnStore,
                      metastore: MetaStore,
                      evictionPolicy: PartitionEvictionPolicy)
                     (implicit val ec: ExecutionContext) extends StrictLogging {
  import collection.JavaConverters._
  import TimeSeriesShard._

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

  private val blockMemorySize = storeConfig.shardMemSize
  private final val numGroups = storeConfig.groupsPerShard
  private val bufferMemorySize = storeConfig.ingestionBufferMemSize
  private val chunkRetentionHours = (storeConfig.demandPagedRetentionPeriod.toSeconds / 3600).toInt
  val pagingEnabled = storeConfig.demandPagingEnabled

  private val ingestSchema = dataset.ingestionSchema
  private val recordComp = dataset.comparator
  private val timestampColId = dataset.timestampColumn.id

  /**
    * PartitionSet - access TSPartition using ingest record partition key in O(1) time.
    */
  private final val partSet = PartitionSet.ofSize(InitialNumPartitions, ingestSchema, recordComp)

  // The off-heap block store used for encoded chunks
  private val blockStore = new PageAlignedBlockManager(blockMemorySize, shardStats.memoryStats, reclaimListener,
                                                       storeConfig.numPagesPerBlock, chunkRetentionHours)
  private val blockFactoryPool = new BlockMemFactoryPool(blockStore, dataset.blockMetaSize)

  private val queryScheduler = monix.execution.Scheduler.computation()

  // Each shard has a single ingestion stream at a time.  This BlockMemFactory is used for buffer overflow encoding
  // strictly during ingest() and switchBuffers().
  private[core] val overflowBlockFactory = new BlockMemFactory(blockStore, None, dataset.blockMetaSize, true)
  val partitionMaker = new DemandPagedChunkStore(this, blockStore, chunkRetentionHours, pagingEnabled)(queryScheduler)

  /**
    * Unencoded/unoptimized ingested data is stored in buffers that are allocated from this off-heap pool
    * Note that this pool is also used to store partition keys.
    */
  logger.info(s"Allocating $bufferMemorySize bytes for WriteBufferPool/PartitionKeys for shard $shardNum")
  protected val bufferMemoryManager = new NativeMemoryManager(bufferMemorySize)
  private val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, dataset.partKeySchema,
                                                 reuseOneContainer = true)
  private val partKeyArray = partKeyBuilder.allContainers.head.base
  private val bufferPool = new WriteBufferPool(bufferMemoryManager, dataset, storeConfig.maxChunksSize,
                                               storeConfig.allocStepSize)

  private final val partitionGroups = Array.fill(numGroups)(new EWAHCompressedBitmap)
  private final val stoppedIngesting = new EWAHCompressedBitmap

  private final val numTimeBucketsToRetain = Math.ceil(chunkRetentionHours.hours / storeConfig.flushInterval).toInt

  private var indexBootstrapped = false
  private var ingestedPartIdsToIndexAfterBootstrap = new EWAHCompressedBitmap()

  // Not really one instance of a map; more like an accessor class shared amongst all TSPartition instances
  private val offheapInfoMap = new OffheapLFSortedIDMapMutator(bufferMemoryManager, classOf[TimeSeriesPartition])
  // Use 1/4 of max # buckets for initial ChunkInfo map size
  private val initInfoMapSize = Math.max((numTimeBucketsToRetain / 4) + 4, 20)

  /**
    * Current time bucket number. Time bucket number is initialized from last value stored in metastore
    * and is incremented each time a new bucket is prepared for flush
    */
  private var currentIndexTimeBucket: Int = _

  /**
   * Timestamp to start searching for partitions to evict. Advances as more and more partitions are evicted.
   * Used to ensure we keep searching for newer and newer partitions to evict.
   */
  private[core] var evictionWatermark: Long = 0L

  /**
    * Bitmap representing the partIds to add to the current time bucket.
    * Incrementally built for the flush period as partKeys are added.
    * At the end of flush period, the time bucket is created with the partKeys and persisted
    */
  private final var currentIndexTimeBucketPartIds: EWAHCompressedBitmap = _

  /**
    * Keeps track of the list of partIds of partKeys to store in each index time bucket.
    * This is used to help identify the partIds to rollover to latest time bucket
    */
  private[memstore] final val timeBucketToPartIds = new mutable.HashMap[Int, EWAHCompressedBitmap]()

  /**
    * This is the group during which this shard will flush time buckets. Randomized to
    * ensure we dont flush time buckets across shards at same time
    */
  private final val indexTimeBucketFlushGroup = Random.nextInt(numGroups)
  logger.info(s"Index time buckets for shard=$shardNum will flush in group $indexTimeBucketFlushGroup")
  // TODO: With flush of time buckets for different shards happening at different groups, I identified a failing
  // corner case especially in dev deployments. If the number of time series is very low, and the time buckets
  // for shard are scheduled to be flushed in a group that is empty, the time buckets may never get flushed.
  // Interim fix for that is to pump more metrics. We can address this issue post MVP.

  /**
    * List of partKeys that have been assigned partIds during kafka/index recovery.
    * They are stored here to ensure we do not add duplicate partIds to lucene index during recovery.
    */
  private final val partKeyToPartIdDuringRecovery = PartitionSet.ofSize(1000000,
                                                            dataset.partKeySchema,
                                                            new RecordComparator(dataset.partKeySchema))

  initTimeBuckets()

  /**
    * The offset up to and including the last record in this group to be successfully persisted.
    * Also used during recovery to figure out what incoming records to skip (since it's persisted)
    */
  private final val groupWatermark = Array.fill(numGroups)(Long.MinValue)

  class PartitionIterator(intIt: IntIterator) extends Iterator[TimeSeriesPartition] {
    var nextPart = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeriesPartition]
    def hasNext: Boolean = {
      while (intIt.hasNext && nextPart == UnsafeUtils.ZeroPointer) {
        nextPart = partitions.get(intIt.next)
      }
      nextPart != UnsafeUtils.ZeroPointer
    }
    def next: TimeSeriesPartition = {
      val toReturn = nextPart
      nextPart = UnsafeUtils.ZeroPointer.asInstanceOf[TimeSeriesPartition]    // reset so that hasNext can keep going
      toReturn
    }
  }

  private val binRecordReader = new BinaryRecordRowReader(dataset.ingestionSchema)

  private def initTimeBuckets() = {
    val highestIndexTimeBucket = Await.result(metastore.readHighestIndexTimeBucket(dataset.ref, shardNum), 1.minute)
    currentIndexTimeBucket = highestIndexTimeBucket.map(_ + 1).getOrElse(0)
    val earliestTimeBucket = Math.max(0, currentIndexTimeBucket - numTimeBucketsToRetain)
    for { i <- currentIndexTimeBucket to earliestTimeBucket by -1 optimized } {
      timeBucketToPartIds.put(i, new EWAHCompressedBitmap())
    }
    currentIndexTimeBucketPartIds = timeBucketToPartIds(currentIndexTimeBucket)
  }

  // RECOVERY: Check the watermark for the group that this record is part of.  If the ingestOffset is < watermark,
  // then do not bother with the expensive partition key comparison and ingestion.  Just skip it
  class IngestConsumer(var numActuallyIngested: Int = 0, var ingestOffset: Long = -1L) extends BinaryRegionConsumer {
    // Receives a new ingestion BinaryRecord
    final def onNext(recBase: Any, recOffset: Long): Unit = {
      val group = Math.abs(ingestSchema.partitionHash(recBase, recOffset) % numGroups)
      if (ingestOffset < groupWatermark(group)) {
        shardStats.rowsSkipped.increment
      } else {
        binRecordReader.recordOffset = recOffset
        getOrAddPartitionAndIngest(recBase, recOffset, group, binRecordReader, overflowBlockFactory)
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
    ingestConsumer.numActuallyIngested = 0
    ingestConsumer.ingestOffset = offset
    binRecordReader.recordBase = container.base
    container.consumeRecords(ingestConsumer)
    shardStats.rowsIngested.increment(ingestConsumer.numActuallyIngested)
    shardStats.rowsPerContainer.record(ingestConsumer.numActuallyIngested)
    ingested += ingestConsumer.numActuallyIngested
    if (!container.isEmpty) _offset = offset
    _offset
  }

  def startFlushingIndex(): Unit = partKeyIndex.startFlushThread()

  def ingest(data: SomeData): Long = ingest(data.records, data.offset)

  def indexRecoveryStream: Observable[IndexData] = {
    val earliestTimeBucket = Math.max(0, currentIndexTimeBucket - numTimeBucketsToRetain)
    val timeBuckets = for { tb <- earliestTimeBucket until currentIndexTimeBucket } yield {
      colStore.getPartKeyTimeBucket(dataset, shardNum, tb).map { b =>
        new IndexData(tb, b.segmentId, RecordContainer(b.segment.array()))
      }
    }
    Observable.flatten(timeBuckets: _*)
  }

  def recoverIndex(segment: IndexData): Unit = {
    var numRecordsProcessed = 0
    segment.records.iterate(indexTimeBucketSchema).foreach { row =>
      // read binary record and extract the indexable data fields
      val startTime: Long = row.getLong(0)
      val endTime: Long = row.getLong(1)
      val partKeyBaseOnHeap = row.getBlobBase(2).asInstanceOf[Array[Byte]]
      val partKeyOffset = row.getBlobOffset(2)
      val partKeyNumBytes = row.getBlobNumBytes(2)
      val partId = newPartId(partKeyBaseOnHeap, partKeyOffset)
      partKeyIndex.upsertPartKey(partKeyBaseOnHeap, partId, startTime, endTime,
        partKeyOffset.toInt - UnsafeUtils.arayOffset)(partKeyNumBytes)
        // upsert since there can be multiple records for one partKey, and most recent wins.
      numRecordsProcessed += 1
    }
    logger.info(s"Recovered partition keys from timebucket for shard=$shardNum timebucket=${segment.timeBucket} " +
      s"segment=${segment.segment} numRecordsProcessed=${numRecordsProcessed}")
  }

  /**
    * State change operations from index-being-bootstrapped, to index-bootstrapped.
    */
  def onIndexBootstrapped(): Unit = {
    val start = System.currentTimeMillis()
    var newPartitionsIngested = 0
    partKeyIndex.commitBlocking() // this is needed to enable read
    new PartitionIterator(ingestedPartIdsToIndexAfterBootstrap.intIterator()).foreach { p =>
      if (partKeyIndex.partKeyFromPartId(p.partID).isEmpty) { // avoid duplicate entries in lucene
        partKeyIndex.addPartKey(p.partKeyBytes, p.partID, p.earliestTime)()
        currentIndexTimeBucketPartIds.set(p.partID)
        timeBucketToPartIds(currentIndexTimeBucket).set(p.partID)
        newPartitionsIngested += 1
      }
    }
    partKeyToPartIdDuringRecovery.clear() // so the containers can be garbage collected
    startFlushingIndex() // start flushing index now that we have recovered
    val end = System.currentTimeMillis()
    logger.info(s"Index has been bootstrapped with partKeys from time buckets for " +
      s"shard=$shardNum. partitionsIngested=${ingestedPartIdsToIndexAfterBootstrap.sizeInBits()} " +
      s"newPartitionsIngested=$newPartitionsIngested. " +
      s"Time taken by onIndexBootstrapped = ${end-start}ms")
    ingestedPartIdsToIndexAfterBootstrap = UnsafeUtils.ZeroPointer.asInstanceOf[EWAHCompressedBitmap]
    indexBootstrapped = true
  }

  def indexNames: Iterator[String] = partKeyIndex.indexNames

  def indexValues(indexName: String): Iterator[UTF8Str] = partKeyIndex.indexValues(indexName)

  /**
    * WARNING: use only for testing. Not performant
    */
  def commitPartKeyIndexBlocking(): Unit = partKeyIndex.commitBlocking()

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
    logger.debug(s"Switching write buffers for group $groupNum in shard $shardNum")
    new PartitionIterator(partitionGroups(groupNum).intIterator).foreach(_.switchBuffers(overflowBlockFactory))
  }

  /**
    * Prepare to flush current index records, switch current currentIndexTimeBucketPartIds with new one.
    * Return Some if part keys need to be flushed (happens for last flush group). Otherwise, None.
    */
  def prepareIndexTimeBucketForFlush(group: Int): Option[FlushIndexTimeBuckets] = {
    if (group == indexTimeBucketFlushGroup) {
      val ret = currentIndexTimeBucketPartIds
      currentIndexTimeBucketPartIds = new EWAHCompressedBitmap()
      currentIndexTimeBucket += 1
      logger.debug(s"Switched timebucket=${currentIndexTimeBucket-1} in shard=$shardNum out for flush. " +
        s"Now currentIndexTimeBucket=$currentIndexTimeBucket")
      shardStats.currentIndexTimeBucket.set(currentIndexTimeBucket)
      timeBucketToPartIds.put(currentIndexTimeBucket, new EWAHCompressedBitmap())
      Some(FlushIndexTimeBuckets(ret, currentIndexTimeBucket-1))
    } else {
      None
    }
  }

  def createFlushTask(flushGroup: FlushGroup)(implicit ingestionScheduler: Scheduler): Task[Response] = {
    val tracer = Kamon.buildSpan("chunk-flush-task-latency-after-retries").start() // TODO tags: shardStats.tags
    val taskToReturn = partitionGroups(flushGroup.groupNum).isEmpty match {
      case false =>
        val partitionIt = new PartitionIterator(partitionGroups(flushGroup.groupNum).intIterator)
        doFlushSteps(flushGroup, partitionIt)
      case true =>
        logger.warn(s"Group ${flushGroup.groupNum} did not have any partitions. " +
          s"flushTimeBuckets=${flushGroup.flushTimeBuckets}")
        // even though there were no records for the chunkset, we want to write checkpoint anyway
        // since we should not resume from earlier checkpoint for the group
        Task.fromFuture(commitCheckpoint(dataset.ref, shardNum, flushGroup))
    }
    taskToReturn.runOnComplete(_ => tracer.finish())
    updateGauges()
    taskToReturn
  }

  private def updateGauges(): Unit = {
    shardStats.bufferPoolSize.set(bufferPool.poolSize)
    shardStats.indexEntries.set(partKeyIndex.indexNumEntries)
    shardStats.indexBytes.set(partKeyIndex.indexRamBytes)
    shardStats.numPartitions.set(numActivePartitions)
  }

  // scalastyle:off method.length
  private def doFlushSteps(flushGroup: FlushGroup,
                           partitionIt: Iterator[TimeSeriesPartition]): Task[Response] = {
    // Only allocate the blockHolder when we actually have chunks/partitions to flush
    val blockHolder = blockFactoryPool.checkout()
    val chunkSetIt = partitionIt.flatMap { p =>

      /* Step 1: Make chunks to be flushed for each partition */
      val chunks = p.makeFlushChunks(blockHolder)

      /* Step 2: Update endTime of all partKeys that stopped ingesting in this flush period.
         If we are flushing time buckets, use its timeBucketId, otherwise, use currentTimeBucket id.
      */
      updateIndexWithEndTime(p, chunks, flushGroup.flushTimeBuckets.map(_.timeBucket).getOrElse(currentIndexTimeBucket))
      chunks
    }

    def addPartKeyToBuilder(builder: RecordBuilder, p: TimeSeriesPartition) = {
      var startTime = partKeyIndex.startTimeFromPartId(p.partID)
      if (startTime == -1) startTime = p.earliestTime// can remotely happen since lucene reads are eventually consistent
      builder.startNewRecord()
      builder.addLong(startTime)
      builder.addLong(p.ingestionEndTime)
      // Need to add 4 to include the length bytes
      builder.addBlob(p.partKeyBase, p.partKeyOffset, BinaryRegionLarge.numBytes(p.partKeyBase, p.partKeyOffset) + 4)
      builder.endRecord(false)
    }

    // Note that all cassandra writes will have included retries. Failures after retries will imply data loss
    // in order to keep the ingestion moving. It is important that we don't fall back far behind.

    // We flush index time buckets in the one designated group for each shard
    val flushIndexTimeBucketsFuture = flushGroup.flushTimeBuckets.map { cmd =>

      val timeBucketRb = new RecordBuilder(MemFactory.onHeapFactory, indexTimeBucketSchema, indexTimeBucketSegmentSize)
      val rbTrace = Kamon.buildSpan("memstore-index-timebucket-populate-timebucket")
        .withTag("dataset", dataset.name)
        .withTag("shard", shardNum).start()

      /* Step 3: Add to timeBucketRb partKeys for (earliestTimeBucketBitmap && ~stoppedIngesting).
       These keys are from earliest time bucket that are still ingesting */
      val earliestTimeBucket = cmd.timeBucket - numTimeBucketsToRetain
      var numPartKeysInCurrentBucket = 0
      if (earliestTimeBucket >= 0) {
        val partIdsToRollOver = timeBucketToPartIds(earliestTimeBucket).andNot(stoppedIngesting)
        numPartKeysInCurrentBucket += partIdsToRollOver.sizeInBits()
        shardStats.numRolledKeysInLatestTimeBucket.increment(partIdsToRollOver.sizeInBits())
        new PartitionIterator(partIdsToRollOver.intIterator())
          .foreach { p => addPartKeyToBuilder(timeBucketRb, p) }
      }
      /* Step 4: Remove the earliest time bucket from memory now that we have rolled over data */
      timeBucketToPartIds.remove(earliestTimeBucket)

      /* Step 5: Remove from lucene part keys that have not ingested for chunk retention period */
      partKeyIndex.removePartKeysEndedBefore(System.currentTimeMillis()-storeConfig.demandPagedRetentionPeriod.toMillis)

      numPartKeysInCurrentBucket += cmd.partIdsToPersist.sizeInBits()
      /* Step 6: add keys that started/stopped ingesting in this flush period to time bucket */
      new PartitionIterator(cmd.partIdsToPersist.intIterator()).foreach { p => addPartKeyToBuilder(timeBucketRb, p) }

      logger.debug(s"Number of records in timebucket=${cmd.timeBucket} of " +
        s"shard=$shardNum is $numPartKeysInCurrentBucket")
      shardStats.numKeysInLatestTimeBucket.increment(numPartKeysInCurrentBucket)

      /* Step 7: compress and persist index time bucket bytes */
      val blobToPersist = timeBucketRb.optimalContainerBytes(true)
      rbTrace.finish()
      shardStats.indexTimeBucketBytesWritten.increment(blobToPersist.map(_.length).sum)
      // we pad to C* ttl to ensure that data lives for longer than time bucket roll over time
      colStore.writePartKeyTimeBucket(dataset, shardNum, cmd.timeBucket, blobToPersist,
                         flushGroup.diskTimeToLiveSeconds + indexTimeBucketTtlPaddingSeconds).flatMap {
        case Success =>           /* Step 8: Persist the highest time bucket id in meta store */
                                  writeHighestTimebucket(shardNum, cmd.timeBucket)
        case er: ErrorResponse =>
          logger.error(s"Failure for flush of timeBucket=${cmd.timeBucket} and rollover of " +
            s"earliestTimeBucket=$earliestTimeBucket for shard=$shardNum : $er")
          // TODO missing persistence of a time bucket even after c* retries may result in inability to query
          // existing data. Revisit later for better resilience for long c* failure
          Future.successful(er)
      }.map { case resp =>
        logger.info(s"Finished flush for timeBucket=${cmd.timeBucket} with ${blobToPersist.length} segments " +
          s" and rollover of earliestTimeBucket=$earliestTimeBucket with resp=$resp for shard=$shardNum")
        resp
      }.recover { case e =>
        logger.error("Internal Error when persisting time bucket - should have not reached this state", e)
        DataDropped
      }
    }.getOrElse(Future.successful(Success))

    /* Step 9: Persist chunks to column store */
    val chunkSetStream = Observable.fromIterator(chunkSetIt)
    logger.debug(s"Created flush ChunkSets stream for group ${flushGroup.groupNum} in shard $shardNum")

    // If the above futures fail with ErrorResponse because of DB failures, skip the chunk.
    // Sorry - need to drop the data to keep the ingestion moving
    val writeChunksFut = writeChunksFuture(flushGroup, chunkSetStream, partitionIt, blockHolder).flatMap {
      case Success           => commitCheckpoint(dataset.ref, shardNum, flushGroup)
      // No chunks written?  Don't checkpoint in this case
      case NotApplied        => Future.successful(NotApplied)
      case er: ErrorResponse => Future.successful(er)
    }.map { case resp =>
      logger.info(s"Flush of shard=$shardNum group=${flushGroup.groupNum} " +
        s"flushWatermark=${flushGroup.flushWatermark} response=$resp offset=${_offset}")
      blockHolder.markUsedBlocksReclaimable()
      blockFactoryPool.release(blockHolder)
      // Some partitions might be evictable, see if need to free write buffer memory
      checkEnableAddPartitions()
      resp
    }.recover { case e =>
      logger.error("Internal Error when persisting chunks - should have not reached this state", e)
      blockFactoryPool.release(blockHolder)
      DataDropped
    }

    /* Step 10: Combine the futures to obtain the return value */
    val result = Future.sequence(Seq(writeChunksFut, flushIndexTimeBucketsFuture)).map {
      _.find(_.isInstanceOf[ErrorResponse]).getOrElse(Success)
    }
    Task.fromFuture(result)
  }

  private def writeChunksFuture(flushGroup: FlushGroup,
                                chunkSetStream: Observable[ChunkSet],
                                partitionIt: Iterator[TimeSeriesPartition],
                                blockHolder: BlockMemFactory) = {
    colStore.write(dataset, chunkSetStream, flushGroup.diskTimeToLiveSeconds).recover { case e =>
      logger.error("Critical! Chunk persistence failed after retries and skipped", e)
      shardStats.flushesFailedChunkWrite.increment
      // Encode and free up the remainder of the WriteBuffers that have not been flushed yet.  Otherwise they will
      // never be freed.
      partitionIt.foreach(_.encodeAndReleaseBuffers(blockHolder))
      DataDropped
    }
  }

  private def writeHighestTimebucket(shardNum: Int, timebucket: Int): Future[Response] = {
    metastore.writeHighestIndexTimeBucket(dataset.ref, shardNum, timebucket).recover { case e =>
      logger.error("Critical! Highest Time Bucket persistence skipped after retries failed", e)
      // Sorry - need to skip to keep the ingestion moving
      DataDropped
    }
  }

  private[memstore] def updatePartEndTime(p: TimeSeriesPartition, endTime: Long): Unit =
    partKeyIndex.updatePartKeyWithEndTime(p.partKeyBytes, p.partID, endTime)()

  private def updateIndexWithEndTime(p: TimeSeriesPartition,
                                     chunks: Iterator[ChunkSet],
                                     timeBucket: Int) = {
    if (chunks.isEmpty && !stoppedIngesting.get(p.partID)) {
      updatePartEndTime(p, p.ingestionEndTime)
      timeBucketToPartIds(timeBucket).set(p.partID)
      stoppedIngesting.set(p.partID)
      currentIndexTimeBucketPartIds.set(p.partID)
    } else if (chunks.nonEmpty && stoppedIngesting.get(p.partID)) {
      // Partition started re-ingesting.
      // TODO: we can do better than this for intermittent time series. Address later.
      updatePartEndTime(p, Long.MaxValue)
      timeBucketToPartIds(timeBucket).set(p.partID)
      stoppedIngesting.clear(p.partID)
      currentIndexTimeBucketPartIds.set(p.partID)
    }
  }

  private def commitCheckpoint(ref: DatasetRef, shardNum: Int, flushGroup: FlushGroup): Future[Response] =
    // negative checkpoints are refused by Kafka, and also offsets should be positive
    if (flushGroup.flushWatermark > 0) {
      val fut = metastore.writeCheckpoint(ref, shardNum, flushGroup.groupNum, flushGroup.flushWatermark).map { r =>
        shardStats.flushesSuccessful.increment
        r
      }.recover { case e =>
        logger.error("Critical! Checkpoint persistence skipped", e)
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

  def newPartId(partKeyBase: Array[Byte], partKeyOffset: Long): Int = {
    if (!indexBootstrapped) {
      val partId = partKeyToPartIdDuringRecovery.getWithPartKeyBR(partKeyBase, partKeyOffset) match {
        case None =>     val partId = nextPartitionID
                         incrementPartitionID()
                         partId
        case Some(p) =>  p.partID
      }
      partKeyToPartIdDuringRecovery.add(new EmptyPartition(dataset, partKeyBase, partKeyOffset, partId))
      partId
    } else {
      val partId = nextPartitionID
      incrementPartitionID()
      partId
    }
  }

  private[memstore] val addPartitionsDisabled = AtomicBoolean(false)

  /**
   * Retrieves or creates a new TimeSeriesPartition, updating indices, then ingests the sample from record.
   * partition portion of ingest BinaryRecord is used to look up existing TSPartition.
   * Copies the partition portion of the ingest BinaryRecord to offheap write buffer memory.
   * NOTE: ingestion is skipped if there is an error allocating WriteBuffer space.
   * @param recordBase the base of the ingestion BinaryRecord
   * @param recordOff the offset of the ingestion BinaryRecord
   * @param group the group number, from abs(record.partitionHash % numGroups)
   */
  def getOrAddPartitionAndIngest(recordBase: Any, recordOff: Long, group: Int,
                                 binRecordReader: RowReader,
                                 overflowBlockFactory: BlockMemFactory): Unit =
    try {
      val part = partSet.getOrAddWithIngestBR(recordBase, recordOff, {
        val newPart = createNewPartition(recordBase, recordOff, group)
        if (newPart != OutOfMemPartition) {
          val partId = newPart.partID
          if (indexBootstrapped) {
            val startTime = binRecordReader.getLong(timestampColId)
            partKeyIndex.addPartKey(newPart.partKeyBytes, partId, startTime)()
            currentIndexTimeBucketPartIds.set(partId)
            timeBucketToPartIds(currentIndexTimeBucket).set(partId)
          } else {
            // Keep track of partId in a bitmap for addition to index and time buckets later.
            // We prefer to load the index with start/end times as recovered from cassandra.
            ingestedPartIdsToIndexAfterBootstrap.set(partId)
            partKeyToPartIdDuringRecovery.add(new EmptyPartition(dataset, newPart.partKeyBase,
              newPart.partKeyOffset, partId))
          }
        }
        newPart
      })
      if (part == OutOfMemPartition) { disableAddPartitions() }
      else { part.asInstanceOf[TimeSeriesPartition].ingest(binRecordReader, overflowBlockFactory) }
    } catch {
      case e: OutOfOffheapMemoryException => disableAddPartitions()
      case e: Exception                   => logger.error(s"Unexpected ingestion err", e); disableAddPartitions()
    }

  private def createNewPartition(recordBase: Any, recordOff: Long, group: Int): TimeSeriesPartition =
    // Check and evict, if after eviction we still don't have enough memory, then don't proceed
    if (addPartitionsDisabled() || !ensureFreeSpace()) { OutOfMemPartition }
    else {
      // PartitionKey is copied to offheap bufferMemory and stays there until it is freed
      val partKeyOffset = recordComp.buildPartKeyFromIngest(recordBase, recordOff, partKeyBuilder)
      // NOTE: allocateAndCopy and allocNew below could fail if there isn't enough memory.  It is CRUCIAL
      // that min-write-buffers-free setting is large enough to accommodate the below use cases ALWAYS
      val (_, partKeyAddr, _) = BinaryRegionLarge.allocateAndCopy(partKeyArray, partKeyOffset, bufferMemoryManager)
      val partId = newPartId(UnsafeUtils.ZeroPointer.asInstanceOf[Array[Byte]], partKeyAddr)
      val infoMapAddr = OffheapLFSortedIDMap.allocNew(bufferMemoryManager, initInfoMapSize)
      val newPart = new TimeSeriesPartition(partId, dataset, partKeyAddr, shardNum, bufferPool, shardStats,
        infoMapAddr, offheapInfoMap)
      partitions.put(partId, newPart)
      shardStats.partitionsCreated.increment
      partitionGroups(group).set(partId)
      newPart
    }

  private def disableAddPartitions(): Unit = {
    if (addPartitionsDisabled.compareAndSet(false, true))
      logger.warn(s"Out of buffer memory and not able to evict enough; adding partitions disabled")
    shardStats.dataDropped.increment
  }

  private def checkEnableAddPartitions(): Unit = if (addPartitionsDisabled()) {
    if (ensureFreeSpace()) {
      logger.info(s"Enough free space to add partitions again!  Yay!")
      addPartitionsDisabled := false
    }
  }

  // Ensures partition ID wraps around safely to 0, not negative numbers (which don't work with bitmaps)
  private def incrementPartitionID(): Unit = {
    nextPartitionID += 1
    if (nextPartitionID < 0) {
      nextPartitionID = 0
      logger.info(s"nextPartitionID has wrapped around to 0 again")
    }
    // Given that we have 2^31 range of partitionIDs, we should pretty much never run into this problem where
    // we wraparound and hit a previously used partitionID.  Actually dealing with this is not easy;  what if
    // the used one is still actively ingesting?  We need to solve the issue of evicting actively ingesting
    // partitions first.  For now, assert so at least we will catch this condition.
    require(!partitions.containsKey(nextPartitionID), s"Partition ID $nextPartitionID ran into existing partition")
  }

  /**
   * Check and evict partitions to free up memory and heap space.  NOTE: This should be called in the ingestion
   * stream so that there won't be concurrent other modifications.  Ideally this is called when trying to add partitions
   * @return true if able to evict enough or there was already space, false if not able to evict and not enough mem
   */
  private[filodb] def ensureFreeSpace(): Boolean = {
    var lastPruned = EmptyBitmap
    while (evictionPolicy.shouldEvict(partSet.size, bufferMemoryManager)) {
      // Eliminate partitions evicted from last cycle so we don't have an endless loop
      val prunedPartitions = partitionsToEvict().andNot(lastPruned)
      if (prunedPartitions.isEmpty) {
        logger.warn(s"Cannot find any partitions to evict but we are still low on space.  DATA WILL BE DROPPED")
        return false
      }
      logger.debug(s"About to prune ${prunedPartitions.cardinality} partitions...")
      lastPruned = prunedPartitions

      // Pruning group bitmaps
      for { group <- 0 until numGroups } {
        partitionGroups(group) = partitionGroups(group).andNot(prunedPartitions)
      }

      // Finally, prune partitions and keyMap data structures
      logger.info(s"Pruning ${prunedPartitions.cardinality} partitions from shard $shardNum")
      val intIt = prunedPartitions.intIterator
      while (intIt.hasNext) {
        val partitionObj = partitions.get(intIt.next)
        // Update the evictionWatermark BEFORE we free the partition and can't read data any more
        if (partitionObj != OutOfMemPartition) {
          val partEndTime = partitionObj.ingestionEndTime
          removePartition(partitionObj)
          if (partEndTime < Long.MaxValue) evictionWatermark = Math.max(evictionWatermark, partEndTime)
        }
      }
      shardStats.partitionsEvicted.increment(prunedPartitions.cardinality)
    }
    true
  }

  // Permanently removes the given partition ID from our in-memory data structures
  // Also frees partition key if necessary
  private def removePartition(partitionObj: TimeSeriesPartition): Unit = {
    partSet.remove(partitionObj)
    offheapInfoMap.free(partitionObj)
    bufferMemoryManager.freeMemory(partitionObj.partKeyOffset)
    partitions.remove(partitionObj.partID)
  }

  private def partitionsToEvict(): EWAHCompressedBitmap = {
    // Iterate and add eligible partitions to delete to our list
    // Need to filter out partitions with no endTime. Any endTime calculated would not be set within one flush interval.
    partKeyIndex.partIdsOrderedByEndTime(storeConfig.numToEvict, evictionWatermark, Long.MaxValue - 1)
  }

  private[core] def getPartition(partKey: Array[Byte]): Option[ReadablePartition] =
    partSet.getWithPartKeyBR(partKey, UnsafeUtils.arayOffset).map(_.asInstanceOf[ReadablePartition])
    // TODO make PartitionSet generic

  def scanPartitions(columnIDs: Seq[Types.ColumnId],
                     partMethod: PartitionScanMethod,
                     chunkMethod: ChunkScanMethod): Observable[ReadablePartition] = {
    val indexIt = partMethod match {
      case SinglePartitionScan(partition, _) =>
        getPartition(partition).map(Iterator.single).getOrElse(Iterator.empty)
      case MultiPartitionScan(partKeys, _)   =>
        partKeys.toIterator.flatMap(getPartition)
      case FilteredPartitionScan(split, filters) =>
        // TODO: There are other filters that need to be added and translated to Lucene queries
        if (filters.nonEmpty) {
          val indexIt = partKeyIndex.partIdsFromFilters(filters, chunkMethod.startTime, chunkMethod.endTime)
          new PartitionIterator(indexIt)
        } else {
          partitions.values.iterator.asScala
        }
    }
    Observable.fromIterator(indexIt.map { p =>
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
    * cannot be used anymore.
    */
  def reset(): Unit = {
    logger.info(s"Clearing all MemStore state for shard $shardNum")
    partitions.clear()
    partSet.clear()
    partKeyIndex.reset()
    ingested = 0L
    for { group <- 0 until numGroups } {
      partitionGroups(group) = new EWAHCompressedBitmap()
      groupWatermark(group) = Long.MinValue
    }
  }

  def shutdown(): Unit = {
    reset()   // Not really needed, but clear everything just to be consistent
    logger.info(s"Shutting down and releasing offheap memory for shard $shardNum")
    bufferMemoryManager.shutdown()
    blockStore.releaseBlocks()
  }
}