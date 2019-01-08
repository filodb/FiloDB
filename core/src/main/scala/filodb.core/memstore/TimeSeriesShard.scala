package filodb.core.memstore

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Random, Try}

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.scalalogging.StrictLogging
import debox.Buffer
import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import monix.execution.atomic.AtomicBoolean
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong
import scalaxy.loops._

import filodb.core.{ErrorResponse, _}
import filodb.core.binaryrecord2._
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, ColumnInfo}
import filodb.core.store._
import filodb.memory._
import filodb.memory.data.{OffheapLFSortedIDMap, OffheapLFSortedIDMapMutator}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.BinaryVector.BinaryVectorPtr

class TimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

  val rowsIngested = Kamon.counter("memstore-rows-ingested").refine(tags)
  val partitionsCreated = Kamon.counter("memstore-partitions-created").refine(tags)
  val dataDropped = Kamon.counter("memstore-data-dropped").refine(tags)
  val outOfOrderDropped = Kamon.counter("memstore-out-of-order-samples").refine(tags)
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
  val indexRecoveryNumRecordsProcessed = Kamon.counter("memstore-index-recovery-records-processed").refine(tags)

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
  * @param storeConfig the store portion of the sourceconfig, not the global FiloDB application config
  */
class TimeSeriesShard(val dataset: Dataset,
                      val storeConfig: StoreConfig,
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

  // Create a single-threaded scheduler just for ingestion.  Name the thread for ease of debugging
  // NOTE: to control intermixing of different Observables/Tasks in this thread, customize ExecutionModel param
  val ingestSched = Scheduler.singleThread(s"ingestion-shard-$shardNum",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in TimeSeriesShard.ingestSched", _)))

  private val blockMemorySize = storeConfig.shardMemSize
  protected val numGroups = storeConfig.groupsPerShard
  private val bufferMemorySize = storeConfig.ingestionBufferMemSize
  private val chunkRetentionHours = (storeConfig.demandPagedRetentionPeriod.toSeconds / 3600).toInt
  val pagingEnabled = storeConfig.demandPagingEnabled

  private val ingestSchema = dataset.ingestionSchema
  private val recordComp = dataset.comparator
  private val timestampColId = dataset.timestampColumn.id

  /**
    * PartitionSet - access TSPartition using ingest record partition key in O(1) time.
    */
  private[memstore] final val partSet = PartitionSet.ofSize(InitialNumPartitions, ingestSchema, recordComp)

  // The off-heap block store used for encoded chunks
  private val blockStore = new PageAlignedBlockManager(blockMemorySize, shardStats.memoryStats, reclaimListener,
                                                       storeConfig.numPagesPerBlock)
  private val blockFactoryPool = new BlockMemFactoryPool(blockStore, dataset.blockMetaSize)

  // Each shard has a single ingestion stream at a time.  This BlockMemFactory is used for buffer overflow encoding
  // strictly during ingest() and switchBuffers().
  private[core] val overflowBlockFactory = new BlockMemFactory(blockStore, None, dataset.blockMetaSize, true)
  val partitionMaker = new DemandPagedChunkStore(this, blockStore, chunkRetentionHours)

  /**
    * Unencoded/unoptimized ingested data is stored in buffers that are allocated from this off-heap pool
    * Note that this pool is also used to store partition keys.
    */
  logger.info(s"Allocating $bufferMemorySize bytes for WriteBufferPool/PartitionKeys for shard $shardNum")
  protected val bufferMemoryManager = new NativeMemoryManager(bufferMemorySize)
  private val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, dataset.partKeySchema,
                                                 reuseOneContainer = true)
  private val partKeyArray = partKeyBuilder.allContainers.head.base.asInstanceOf[Array[Byte]]
  private val bufferPool = new WriteBufferPool(bufferMemoryManager, dataset, storeConfig.maxChunksSize,
                                               storeConfig.allocStepSize)

  private final val partitionGroups = Array.fill(numGroups)(new EWAHCompressedBitmap)
  private final val activelyIngesting = new EWAHCompressedBitmap

  private final val numTimeBucketsToRetain = Math.ceil(chunkRetentionHours.hours / storeConfig.flushInterval).toInt

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
  class IngestConsumer(var numActuallyIngested: Int = 0, var ingestOffset: Long = -1L) extends BinaryRegionConsumer {
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
          val part: FiloPartition = getOrAddPartition(recBase, recOffset, group, ingestOffset)
          if (part == OutOfMemPartition) { disableAddPartitions() }
        } catch {
          case e: OutOfOffheapMemoryException => disableAddPartitions()
          case e: Exception                   => logger.error(s"Unexpected ingestion err", e); disableAddPartitions()
        }
      } else {
        binRecordReader.recordOffset = recOffset
        getOrAddPartitionAndIngest(recBase, recOffset, group, ingestOffset)
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

  def recoverIndex(): Future[Unit] = {
    val tracer = Kamon.buildSpan("memstore-recover-index-latency")
      .withTag("dataset", dataset.name)
      .withTag("shard", shardNum).start()

    val earliestTimeBucket = Math.max(0, currentIndexTimeBucket - numTimeBucketsToRetain)
    logger.info(s"Recovering timebuckets $earliestTimeBucket until $currentIndexTimeBucket for shard=$shardNum ")
    val timeBuckets = for { tb <- earliestTimeBucket until currentIndexTimeBucket } yield {
      colStore.getPartKeyTimeBucket(dataset, shardNum, tb).map { b =>
        new IndexData(tb, b.segmentId, RecordContainer(b.segment.array()))
      }
    }
    val fut = Observable.flatten(timeBuckets: _*)
                        .foreach(tb => extractTimeBucket(tb))(ingestSched)
                        .map(_ => completeIndexRecovery())
    fut.onComplete(_ => tracer.finish())
    fut
  }

  def completeIndexRecovery(): Unit = {
    commitPartKeyIndexBlocking()
    startFlushingIndex() // start flushing index now that we have recovered
    logger.info(s"Bootstrapped index for shard $shardNum")
  }

  private[memstore] def extractTimeBucket(segment: IndexData): Unit = {
    var numRecordsProcessed = 0
    segment.records.iterate(indexTimeBucketSchema).foreach { row =>
      // read binary record and extract the indexable data fields
      val startTime: Long = row.getLong(0)
      val endTime: Long = row.getLong(1)
      val partKeyBaseOnHeap = row.getBlobBase(2).asInstanceOf[Array[Byte]]
      val partKeyOffset = row.getBlobOffset(2)
      val partKeyNumBytes = row.getBlobNumBytes(2)

      // look up partId in partSet if it already exists before assigning new partId.
      // We cant look it up in lucene because we havent flushed index yet
      val partId = partSet.getWithPartKeyBR(partKeyBaseOnHeap, partKeyOffset) match {
        case None =>     val group = partKeyGroup(dataset.partKeySchema, partKeyBaseOnHeap, partKeyOffset, numGroups)
                         val part = createNewPartition(partKeyBaseOnHeap, partKeyOffset, group, 4)
                         // In theory, we should not get an OutOfMemPartition here since
                         // it should have occurred before node failed too, and with data sropped,
                         // index would not be updated. But if for some reason we see it, drop data
                         if (part == OutOfMemPartition) {
                           logger.error("Could not accommodate partKey while recovering index. " +
                             "WriteBuffer size may not be configured correctly")
                           -1
                         } else {
                           partSet.add(part) // createNewPartition doesnt add part to partSet
                           part.partID
                         }
        case Some(p) =>  p.partID
      }
      if (partId != -1) {
        // upsert into lucene since there can be multiple records for one partKey, and most recent wins.
        partKeyIndex.upsertPartKey(partKeyBaseOnHeap, partId, startTime, endTime,
          PartKeyLuceneIndex.unsafeOffsetToBytesRefOffset(partKeyOffset))(partKeyNumBytes)
        timeBucketBitmaps.get(segment.timeBucket).set(partId)
        if (endTime == Long.MaxValue) activelyIngesting.set(partId) else activelyIngesting.clear(partId)
      }
      numRecordsProcessed += 1
    }
    shardStats.indexRecoveryNumRecordsProcessed.increment(numRecordsProcessed)
    logger.info(s"Recovered partition keys from timebucket for shard=$shardNum timebucket=${segment.timeBucket} " +
      s"segment=${segment.segment} numRecordsProcessed=$numRecordsProcessed")
  }

  def indexNames: Iterator[String] = partKeyIndex.indexNames

  def indexValues(indexName: String, topK: Int): Seq[TermInfo] = partKeyIndex.indexValues(indexName, topK)

  /**
    * This method is to apply column filters and fetch matching time series partitions.
    */
  def indexValuesWithFilters(filter: Seq[ColumnFilter],
                             indexName: String,
                             endTime: Long,
                             startTime: Long,
                             limit: Int): Iterator[ZeroCopyUTF8String] = {
    IndexValueResultIterator(partKeyIndex.partIdsFromFilters(filter, startTime, endTime), indexName, limit)
  }

  /**
    * Iterator for lazy traversal of partIdIterator, value for the given label will be extracted from the ParitionKey.
    */
  case class IndexValueResultIterator(partIterator: IntIterator, labelName: String, limit: Int)
      extends Iterator[ZeroCopyUTF8String] {
    var currVal: ZeroCopyUTF8String = _
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
          dataset.partKeySchema.toStringPairs(nextPart.partKeyBase, nextPart.partKeyOffset)
            .find(_._1 equals labelName).foreach(pair => {
              currVal = ZeroCopyUTF8String(pair._2)
              foundValue = true
          })
        } else {
          // FIXME partKey is evicted. Get partition key from lucene index
        }
      }
      foundValue
    }

    override def next(): ZeroCopyUTF8String = {
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
      logger.warn(s"PartId $partId was not found in memory and was not included in metadata query result. ")
    // TODO Revisit this code for evicted partitions
    /*if (nextPart == UnsafeUtils.ZeroPointer) {
      val partKey = partKeyIndex.partKeyFromPartId(partId)
      //map partKey bytes to UTF8String
    }*/
    nextPart
  }

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
    InMemPartitionIterator(partitionGroups(groupNum).intIterator).foreach(_.switchBuffers(overflowBlockFactory))
  }

  /**
    * Prepare to flush current index records, switch current currentIndexTimeBucketPartIds with new one.
    * Return Some if part keys need to be flushed (happens for last flush group). Otherwise, None.
    */
  def prepareIndexTimeBucketForFlush(group: Int): Option[FlushIndexTimeBuckets] = {
    if (group == indexTimeBucketFlushGroup) {
      logger.debug(s"Switching timebucket=$currentIndexTimeBucket in shard=$shardNum out for flush. ")
      currentIndexTimeBucket += 1
      shardStats.currentIndexTimeBucket.set(currentIndexTimeBucket)
      timeBucketBitmaps.put(currentIndexTimeBucket, new EWAHCompressedBitmap())
      purgeExpiredPartitions()
      Some(FlushIndexTimeBuckets(currentIndexTimeBucket-1))
    } else {
      None
    }
  }

  private def purgeExpiredPartitions(): Unit = {
    val deletedParts = partKeyIndex.removePartKeysEndedBefore(
      System.currentTimeMillis() - storeConfig.demandPagedRetentionPeriod.toMillis)
    var numDeleted = 0
    InMemPartitionIterator(deletedParts).foreach { p =>
      logger.debug(s"Purging partition with partId ${p.partID} from memory")
      removePartition(p)
      numDeleted += 1
    }
    if (numDeleted > 0) logger.info(s"Purged $numDeleted partitions from memory and index")
    shardStats.purgedPartitions.increment(numDeleted)
  }

  def createFlushTask(flushGroup: FlushGroup): Task[Response] = {
    val partitionIt = InMemPartitionIterator(partitionGroups(flushGroup.groupNum).intIterator)
    doFlushSteps(flushGroup, partitionIt)
  }

  private def updateGauges(): Unit = {
    shardStats.bufferPoolSize.set(bufferPool.poolSize)
    shardStats.indexEntries.set(partKeyIndex.indexNumEntries)
    shardStats.indexBytes.set(partKeyIndex.indexRamBytes)
    shardStats.numPartitions.set(numActivePartitions)
    shardStats.numActivelyIngestingParts.set(activelyIngesting.cardinality())
  }

  private def addPartKeyToTimebucket(indexRb: RecordBuilder, p: TimeSeriesPartition) = {
    var startTime = partKeyIndex.startTimeFromPartId(p.partID)
    if (startTime == -1) startTime = p.earliestTime// can remotely happen since lucene reads are eventually consistent
    val endTime = if (activelyIngesting.get(p.partID)) {
      Long.MaxValue
    } else {
      val et = p.timestampOfLatestSample  // -1 can be returned if no sample after reboot
      if (et == -1) System.currentTimeMillis() else et
    }
    indexRb.startNewRecord()
    indexRb.addLong(startTime)
    indexRb.addLong(endTime)
    // Need to add 4 to include the length bytes
    indexRb.addBlob(p.partKeyBase, p.partKeyOffset, BinaryRegionLarge.numBytes(p.partKeyBase, p.partKeyOffset) + 4)
    logger.debug(s"Added into timebucket partId ${p.partID} in shard=$shardNum partKey[${p.stringPartition}] with " +
      s"startTime=$startTime endTime=$endTime")
    indexRb.endRecord(false)
  }

  private def doFlushSteps(flushGroup: FlushGroup,
                           partitionIt: Iterator[TimeSeriesPartition]): Task[Response] = {
    val tracer = Kamon.buildSpan("chunk-flush-task-latency-after-retries")
      .withTag("dataset", dataset.name)
      .withTag("shard", shardNum).start()
    // Only allocate the blockHolder when we actually have chunks/partitions to flush
    val blockHolder = blockFactoryPool.checkout()
    val chunkSetIt = partitionIt.flatMap { p =>
      /* Step 1: Make chunks to be flushed for each partition */
      val chunks = p.makeFlushChunks(blockHolder)

      /* Step 2: Update endTime of all partKeys that stopped ingesting in this flush period.
         If we are flushing time buckets, use its timeBucketId, otherwise, use currentTimeBucket id. */
      updateIndexWithEndTime(p, chunks, flushGroup.flushTimeBuckets.map(_.timeBucket).getOrElse(currentIndexTimeBucket))
      chunks
    }

    // Note that all cassandra writes below  will have included retries. Failures after retries will imply data loss
    // in order to keep the ingestion moving. It is important that we don't fall back far behind.

    // Step 3: We flush index time buckets in the one designated group for each shard
    val writeIndexTimeBucketsFuture = writeTimeBuckets(flushGroup)

    /* Step 4: Persist chunks to column store */
    val writeChunksFut = writeChunks(flushGroup, chunkSetIt, partitionIt, blockHolder)

    /* Step 5: Checkpoint after time buckets and chunks are flushed */
    val result = Future.sequence(Seq(writeChunksFut, writeIndexTimeBucketsFuture)).map {
      _.find(_.isInstanceOf[ErrorResponse]).getOrElse(Success)
    }.flatMap {
      case Success           => blockHolder.markUsedBlocksReclaimable()
                                commitCheckpoint(dataset.ref, shardNum, flushGroup)
      case er: ErrorResponse => Future.successful(er)
    }.recover { case e =>
      logger.error("Internal Error when persisting chunks - should have not reached this state", e)
      DataDropped
    }
    result.onComplete { resp =>
      try {
        blockFactoryPool.release(blockHolder)
        flushDoneTasks(flushGroup, resp)
        tracer.finish()
      } catch { case e: Throwable =>
        logger.error("Error when wrapping up doFlushSteps", e)
      }
    }
    Task.fromFuture(result)
  }

  protected def flushDoneTasks(flushGroup: FlushGroup, resTry: Try[Response]): Unit = {
    resTry.foreach { resp =>
      logger.info(s"Flush of shard=$shardNum group=${flushGroup.groupNum} " +
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
    flushGroup.flushTimeBuckets.map { cmd =>
      val rbTrace = Kamon.buildSpan("memstore-index-timebucket-populate-timebucket")
        .withTag("dataset", dataset.name)
        .withTag("shard", shardNum).start()

      /* Add to timeBucketRb partKeys for (earliestTimeBucketBitmap && ~stoppedIngesting).
       These keys are from earliest time bucket that are still ingesting */
      val earliestTimeBucket = cmd.timeBucket - numTimeBucketsToRetain
      if (earliestTimeBucket >= 0) {
        val partIdsToRollOver = timeBucketBitmaps.get(earliestTimeBucket).and(activelyIngesting)
        val newBitmap = timeBucketBitmaps.get(cmd.timeBucket).or(partIdsToRollOver)
        timeBucketBitmaps.put(cmd.timeBucket, newBitmap)
        shardStats.numRolledKeysInLatestTimeBucket.increment(partIdsToRollOver.cardinality())
      }

      /* Remove the earliest time bucket from memory now that we have rolled over data */
      timeBucketBitmaps.remove(earliestTimeBucket)

      /* create time bucket using record builder */
      val timeBucketRb = new RecordBuilder(MemFactory.onHeapFactory, indexTimeBucketSchema, indexTimeBucketSegmentSize)
      InMemPartitionIterator(timeBucketBitmaps.get(cmd.timeBucket).intIterator).foreach { p =>
        addPartKeyToTimebucket(timeBucketRb, p)
      }
      val numPartKeysInBucket = timeBucketBitmaps.get(cmd.timeBucket).cardinality()
      logger.debug(s"Number of records in timebucket=${cmd.timeBucket} of " +
        s"shard=$shardNum is $numPartKeysInBucket")
      shardStats.numKeysInLatestTimeBucket.increment(numPartKeysInBucket)

      /* compress and persist index time bucket bytes */
      val blobToPersist = timeBucketRb.optimalContainerBytes(true)
      rbTrace.finish()
      shardStats.indexTimeBucketBytesWritten.increment(blobToPersist.map(_.length).sum)
      // we pad to C* ttl to ensure that data lives for longer than time bucket roll over time
      colStore.writePartKeyTimeBucket(dataset, shardNum, cmd.timeBucket, blobToPersist,
        flushGroup.diskTimeToLiveSeconds + indexTimeBucketTtlPaddingSeconds).flatMap {
        case Success => /* Step 8: Persist the highest time bucket id in meta store */
          writeHighestTimebucket(shardNum, cmd.timeBucket)
        case er: ErrorResponse =>
          logger.error(s"Failure for flush of timeBucket=${cmd.timeBucket} and rollover of " +
            s"earliestTimeBucket=$earliestTimeBucket for shard=$shardNum : $er")
          // TODO missing persistence of a time bucket even after c* retries may result in inability to query
          // existing data. Revisit later for better resilience for long c* failure
          Future.successful(er)
      }.map { case resp =>
        logger.info(s"Finished flush for timeBucket=${cmd.timeBucket} with ${blobToPersist.length} segments " +
          s"and rollover of earliestTimeBucket=$earliestTimeBucket with resp=$resp for shard=$shardNum")
        resp
      }.recover { case e =>
        logger.error("Internal Error when persisting time bucket - should have not reached this state", e)
        DataDropped
      }
    }.getOrElse(Future.successful(Success))
  }
  // scalastyle:on method.length

  private def writeChunks(flushGroup: FlushGroup,
                          chunkSetIt: Iterator[ChunkSet],
                          partitionIt: Iterator[TimeSeriesPartition],
                          blockHolder: BlockMemFactory): Future[Response] = {
    if (chunkSetIt.isEmpty) {
      Future.successful(Success)
    } else {
      val chunkSetStream = Observable.fromIterator(chunkSetIt)
      logger.debug(s"Created flush ChunkSets stream for group ${flushGroup.groupNum} in shard $shardNum")

      colStore.write(dataset, chunkSetStream, flushGroup.diskTimeToLiveSeconds).recover { case e =>
        logger.error("Critical! Chunk persistence failed after retries and skipped", e)
        shardStats.flushesFailedChunkWrite.increment
        // Encode and free up the remainder of the WriteBuffers that have not been flushed yet.  Otherwise they will
        // never be freed.
        partitionIt.foreach(_.encodeAndReleaseBuffers(blockHolder))
        // If the above futures fail with ErrorResponse because of DB failures, skip the chunk.
        // Sorry - need to drop the data to keep the ingestion moving
        DataDropped
      }
    }
  }

  private def writeHighestTimebucket(shardNum: Int, timebucket: Int): Future[Response] = {
    metastore.writeHighestIndexTimeBucket(dataset.ref, shardNum, timebucket).recover { case e =>
      logger.error("Critical! Highest Time Bucket persistence skipped after retries failed", e)
      // Sorry - need to skip to keep the ingestion moving
      DataDropped
    }
  }

  private[memstore] def updatePartEndTimeInIndex(p: TimeSeriesPartition, endTime: Long): Unit =
    partKeyIndex.updatePartKeyWithEndTime(p.partKeyBytes, p.partID, endTime)()

  private def updateIndexWithEndTime(p: TimeSeriesPartition,
                                     partFlushChunks: Iterator[ChunkSet],
                                     timeBucket: Int) = {
    if (partFlushChunks.isEmpty && activelyIngesting.get(p.partID)) {
      var endTime = p.timestampOfLatestSample
      if (endTime == -1) endTime = System.currentTimeMillis() // this can happen if no sample after reboot
      updatePartEndTimeInIndex(p, endTime)
      timeBucketBitmaps.get(timeBucket).set(p.partID)
      activelyIngesting.clear(p.partID)
    } else if (partFlushChunks.nonEmpty && !activelyIngesting.get(p.partID)) {
      // Partition started re-ingesting.
      // TODO: we can do better than this for intermittent time series. Address later.
      updatePartEndTimeInIndex(p, Long.MaxValue)
      timeBucketBitmaps.get(timeBucket).set(p.partID)
      activelyIngesting.set(p.partID)
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

  private[memstore] val addPartitionsDisabled = AtomicBoolean(false)

  private[filodb] def getOrAddPartition(recordBase: Any, recordOff: Long, group: Int, ingestOffset: Long) = {
    val part = partSet.getOrAddWithIngestBR(recordBase, recordOff, {
      val partKeyOffset = recordComp.buildPartKeyFromIngest(recordBase, recordOff, partKeyBuilder)
      val newPart = createNewPartition(partKeyArray, partKeyOffset, group)
      if (newPart != OutOfMemPartition) {
        val partId = newPart.partID
        // NOTE: Don't use binRecordReader here.  recordOffset might not be set correctly
        val startTime = dataset.ingestionSchema.getLong(recordBase, recordOff, timestampColId)
        partKeyIndex.addPartKey(newPart.partKeyBytes, partId, startTime)()
        timeBucketBitmaps.get(currentIndexTimeBucket).set(partId)
        activelyIngesting.set(partId)
        logger.trace(s"Created new partition ${newPart.stringPartition} on shard $shardNum at offset $ingestOffset")
      }
      newPart
    })
    part
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
  def getOrAddPartitionAndIngest(recordBase: Any, recordOff: Long, group: Int, ingestOffset: Long): Unit =
    try {
      val part: FiloPartition = getOrAddPartition(recordBase, recordOff, group, ingestOffset)
      if (part == OutOfMemPartition) { disableAddPartitions() }
      else { part.asInstanceOf[TimeSeriesPartition].ingest(binRecordReader, overflowBlockFactory) }
    } catch {
      case e: OutOfOffheapMemoryException => disableAddPartitions()
      case e: Exception                   => logger.error(s"Unexpected ingestion err", e); disableAddPartitions()
    }

  protected def createNewPartition(partKeyBase: Array[Byte], partKeyOffset: Long,
                                   group: Int, initMapSize: Int = initInfoMapSize): TimeSeriesPartition =
    // Check and evict, if after eviction we still don't have enough memory, then don't proceed
    if (addPartitionsDisabled() || !ensureFreeSpace()) { OutOfMemPartition }
    else {
      // PartitionKey is copied to offheap bufferMemory and stays there until it is freed
      // NOTE: allocateAndCopy and allocNew below could fail if there isn't enough memory.  It is CRUCIAL
      // that min-write-buffers-free setting is large enough to accommodate the below use cases ALWAYS
      val (_, partKeyAddr, _) = BinaryRegionLarge.allocateAndCopy(partKeyBase, partKeyOffset, bufferMemoryManager)
      val infoMapAddr = OffheapLFSortedIDMap.allocNew(bufferMemoryManager, initMapSize)
      val partId = nextPartitionID
      incrementPartitionID()
      val newPart = new TimeSeriesPartition(partId, dataset, partKeyAddr, shardNum, bufferPool, shardStats,
        infoMapAddr, offheapInfoMap)
      partitions.put(partId, newPart)
      shardStats.partitionsCreated.increment
      partitionGroups(group).set(partId)
      newPart
    }

  private def disableAddPartitions(): Unit = {
    if (addPartitionsDisabled.compareAndSet(false, true))
      logger.warn(s"Shard $shardNum: Out of buffer memory and not able to evict enough; adding partitions disabled")
    shardStats.dataDropped.increment
  }

  private def checkEnableAddPartitions(): Unit = if (addPartitionsDisabled()) {
    if (ensureFreeSpace()) {
      logger.info(s"Shard $shardNum: Enough free space to add partitions again!  Yay!")
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
        logger.warn(s"Shard $shardNum: No partitions to evict but we are still low on space.  DATA WILL BE DROPPED")
        return false
      }
      lastPruned = prunedPartitions

      // Pruning group bitmaps
      for { group <- 0 until numGroups } {
        partitionGroups(group) = partitionGroups(group).andNot(prunedPartitions)
      }

      // Finally, prune partitions and keyMap data structures
      logger.info(s"Evicting partitions from shard $shardNum, watermark = $evictionWatermark....")
      val intIt = prunedPartitions.intIterator
      var partsRemoved = 0
      var partsSkipped = 0
      var maxEndTime = evictionWatermark
      while (intIt.hasNext) {
        val partitionObj = partitions.get(intIt.next)
        if (partitionObj != UnsafeUtils.ZeroPointer) {
          // TODO we can optimize fetching of endTime by getting them along with top-k query
          val endTime = partKeyIndex.endTimeFromPartId(partitionObj.partID)
          if (activelyIngesting.get(partitionObj.partID))
            logger.warn(s"Partition ${partitionObj.partID} is ingesting, but it was eligible for eviction. How?")
          if (endTime == PartKeyLuceneIndex.NOT_FOUND || endTime == Long.MaxValue) {
            logger.warn(s"endTime ${endTime} was not correct. how?", new IllegalStateException())
          } else {
            removePartition(partitionObj)
            partsRemoved += 1
            maxEndTime = Math.max(maxEndTime, endTime)
          }
        } else {
          partsSkipped += 1
        }
      }
      evictionWatermark = maxEndTime + 1
      // Plus one needed since there is a possibility that all partitions evicted in this round have same endTime,
      // and there may be more partitions that are not evicted with same endTime. If we didnt advance the watermark,
      // we would be processing same partIds again and again without moving watermark forward.
      // We may skip evicting some partitions by doing this, but the imperfection is an acceptable
      // trade-off for performance and simplicity. The skipped partitions, will ve removed during purge.
      logger.info(s"Shard $shardNum: evicted $partsRemoved partitions, skipped $partsSkipped, h20=$evictionWatermark")
      shardStats.partitionsEvicted.increment(partsRemoved)
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

  private[core] def getPartition(partKey: Array[Byte]): Option[TimeSeriesPartition] =
    partSet.getWithPartKeyBR(partKey, UnsafeUtils.arayOffset).map(_.asInstanceOf[TimeSeriesPartition])

  def iteratePartitions(partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod): PartitionIterator = partMethod match {
    case SinglePartitionScan(partition, _) => ByteKeysPartitionIterator(Seq(partition))
    case MultiPartitionScan(partKeys, _)   => ByteKeysPartitionIterator(partKeys)
    case FilteredPartitionScan(split, filters) =>
      // TODO: There are other filters that need to be added and translated to Lucene queries
      if (filters.nonEmpty) {
        val indexIt = partKeyIndex.partIdsFromFilters(filters, chunkMethod.startTime, chunkMethod.endTime)
        new InMemPartitionIterator(indexIt)
      } else {
        PartitionIterator.fromPartIt(partitions.values.iterator.asScala)
      }
  }

  def scanPartitions(columnIDs: Seq[Types.ColumnId],
                     partMethod: PartitionScanMethod,
                     chunkMethod: ChunkScanMethod): Observable[ReadablePartition] = {
    Observable.fromIterator(iteratePartitions(partMethod, chunkMethod).map { p =>
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
    logger.info(s"Clearing all MemStore state for shard $shardNum")
    partitions.values.asScala.foreach(removePartition)
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
