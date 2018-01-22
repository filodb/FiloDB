package filodb.core.memstore

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DataDropped, DatasetRef, ErrorResponse, Response, Success}
import filodb.core.Types.PartitionKey
import filodb.core.metadata.Dataset
import filodb.core.query.PartitionKeyIndex
import filodb.core.store._
import filodb.memory.{BlockHolder, MemoryStats, NativeMemoryManager, PageAlignedBlockManager}
import filodb.memory.format.{SchemaRowReader, ZeroCopyUTF8String}


class TimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

  val rowsIngested = Kamon.metrics.counter("memstore-rows-ingested", tags)
  val partitionsCreated = Kamon.metrics.counter("memstore-partitions-created", tags)
  val rowsSkipped  = Kamon.metrics.counter("recovery-row-skipped", tags)
  val numChunksEncoded = Kamon.metrics.counter("memstore-chunks-encoded", tags)
  val numSamplesEncoded = Kamon.metrics.counter("memstore-samples-encoded", tags)
  val encodedBytes  = Kamon.metrics.counter("memstore-encoded-bytes-allocated", tags)
  val flushesSuccessful = Kamon.metrics.counter("memstore-flushes-success", tags)
  val flushesFailedPartWrite = Kamon.metrics.counter("memstore-flushes-failed-partition", tags)
  val flushesFailedChunkWrite = Kamon.metrics.counter("memstore-flushes-failed-chunk", tags)
  val flushesFailedOther = Kamon.metrics.counter("memstore-flushes-failed-other", tags)

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
  val offsetLatestInMem = Kamon.metrics.gauge("shard-offset-latest-inmemory", tags)(0L)
  val offsetLatestFlushed = Kamon.metrics.gauge("shard-offset-flushed-latest", tags)(0L)
  val offsetEarliestFlushed = Kamon.metrics.gauge("shard-offset-flushed-earliest", tags)(0L)

  val partitionsPagedFromColStore = Kamon.metrics.counter("memstore-partitions-paged-in", tags)
  val chunkIdsPagedFromColStore = Kamon.metrics.counter("memstore-chunkids-paged-in", tags)
  val partitionsQueried = Kamon.metrics.counter("memstore-partitions-queried", tags)
  val numChunksQueried = Kamon.metrics.counter("memstore-chunks-queried", tags)
  val memoryStats = new MemoryStats(tags)
}

// TODO for scalability: get rid of stale partitions?
// This would involve something like this:
//    - Go through oldest (lowest index number) partitions
//    - If partition still used, move it to a higher (latest) index
//    - Re-use number for newer partition?  Something like a ring index

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
  */
class TimeSeriesShard(dataset: Dataset, config: Config, val shardNum: Int, sink: ColumnStore, metastore: MetaStore)
                     (implicit val ec: ExecutionContext)
  extends StrictLogging {

  val shardStats = new TimeSeriesShardStats(dataset.ref, shardNum)

  /**
    * List of all partitions in the shard stored in memory
    */
  private final val partitions = new ArrayBuffer[TimeSeriesPartition]

  /**
    * Hash Map from Partition Key to Partition
    */
  private final val keyMap = new HashMap[SchemaRowReader, TimeSeriesPartition]

  /**
    * This index helps identify which partitions have any given column-value.
    * Used to answer queries not involving the full partition key.
    * Maintained using a high-performance bitmap index.
    */
  private final val keyIndex = new PartitionKeyIndex(dataset)

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

  private val maxChunksSize = config.getInt("memstore.max-chunks-size")
  private val shardMemoryMB = config.getInt("memstore.shard-memory-mb")
  private val numPagesPerBlock = Try(config.getInt("memstore.num-block-pages")).getOrElse(1000)
  private final val numGroups = config.getInt("memstore.groups-per-shard")
  private val maxNumPartitions = config.getInt("memstore.max-num-partitions")

  // The off-heap block store used for encoded chunks
  protected val blockMemorySize: Long = shardMemoryMB * 1024 * 1024L
  private val blockStore = new PageAlignedBlockManager(blockMemorySize, shardStats.memoryStats, numPagesPerBlock)
  private val numColumns = dataset.dataColumns.size

  // The off-heap buffers used for ingesting the newest data samples
  protected val bufferMemorySize: Long = maxChunksSize * 8L * maxNumPartitions * numColumns
  logger.info(s"Allocating $bufferMemorySize bytes for WriteBufferPool for shard $shardNum")
  protected val bufferMemoryManager = new NativeMemoryManager(bufferMemorySize)

  /**
    * Unencoded/unoptimized ingested data is stored in buffers that are allocated from this off-heap pool
    */
  private val bufferPool = new WriteBufferPool(bufferMemoryManager, dataset, maxChunksSize / 8, maxNumPartitions)
  logger.info(s"Finished initializing memory pools for shard $shardNum")

  private final val partitionGroups = Array.fill(numGroups)(new EWAHCompressedBitmap)

  /**
    * The offset up to and including the last record in this group to be successfully persisted.
    * Also used during recovery to figure out what incoming records to skip (since it's persisted)
    */
  private final val groupWatermark = Array.fill(numGroups)(Long.MinValue)

  /**
    * This holds two bitmap indexes per group and tracks the partition keys that are pending
    * flush to the persistent sink's partition list tracker. When one bitmap is being updated with ingested partitions,
    * the other is being flushed. When buffers are switched for the group, the indexes are swapped.
    */
  private final val partKeysToFlush = Array.fill(numGroups, 2)(new EWAHCompressedBitmap)

  class PartitionIterator(intIt: IntIterator) extends Iterator[TimeSeriesPartition] {
    def hasNext: Boolean = intIt.hasNext
    def next: TimeSeriesPartition = partitions(intIt.next)
  }

  def ingest(rows: Seq[IngestRecord]): Long = {
    // now go through each row, find the partition and call partition ingest
    var numActuallyIngested = 0
    rows.foreach { case IngestRecord(partKey, data, offset) =>
      // RECOVERY: Check the watermark for the group that this record is part of.  If the offset is < watermark,
      // then do not bother with the expensive partition key comparison and ingestion.  Just skip it
      if (offset < groupWatermark(group(partKey))) {
        shardStats.rowsSkipped.increment
      } else {
        val partition = keyMap.getOrElse(partKey, addPartition(partKey, true))
        partition.ingest(data, offset)
        numActuallyIngested += 1
      }
    }
    shardStats.rowsIngested.increment(numActuallyIngested)
    ingested += numActuallyIngested
    if (rows.nonEmpty) _offset = rows.last.offset
    _offset
  }

  def indexNames: Iterator[String] = keyIndex.indexNames

  def indexValues(indexName: String): Iterator[ZeroCopyUTF8String] = keyIndex.indexValues(indexName)

  def numRowsIngested: Long = ingested

  def numActivePartitions: Int = keyMap.size

  def latestOffset: Long = _offset

  /**
    * Sets the watermark for each subgroup.  If an ingested record offset is below this watermark then it will be
    * assumed to already have been persisted, and the record will be discarded.  Use only for recovery.
    * @param watermarks a Map from group number to watermark
    */
  def setGroupWatermarks(watermarks: Map[Int, Long]): Unit =
    watermarks.foreach { case (group, mark) => groupWatermark(group) = mark }

  private final def group(partKey: SchemaRowReader): Int = Math.abs(partKey.hashCode % numGroups)

  // Rapidly switch all of the input buffers for a particular group
  // Preferably this is done in the same thread/stream as input records to avoid concurrency issues
  // and ensure that all the partitions in a group are switched at the same watermark
  def switchGroupBuffers(groupNum: Int): Unit = {
    logger.debug(s"Switching write buffers for group $groupNum in shard $shardNum")
    new PartitionIterator(partitionGroups(groupNum).intIterator).foreach(_.switchBuffers())

    // swap the partKeysToFlush bitmap indexes too. Clear the one that has been flushed
    val temp = partKeysToFlush(groupNum)(0)
    partKeysToFlush(groupNum)(0) = partKeysToFlush(groupNum)(1)
    partKeysToFlush(groupNum)(1) = temp
    partKeysToFlush(groupNum)(0).clear()
  }

  def createFlushTask(flushGroup: FlushGroup)(implicit ingestionScheduler: Scheduler): Task[Response] = {
    val blockHolder = new BlockHolder(blockStore)
    // Given the flush group, create an observable of ChunkSets
    val chunkSetIt = new PartitionIterator(partitionGroups(flushGroup.groupNum).intIterator)
      .flatMap(_.makeFlushChunks(blockHolder))
    val tracer = Kamon.tracer.newContext("chunk-flush-task-latency-after-retries", None, shardStats.tags)
    val taskToReturn = chunkSetIt.isEmpty match {
      case false =>
        doFlushSteps(flushGroup, chunkSetIt)
      case true =>
        // even though there were no records for the chunkset, we want to write checkpoint anyway
        // since we should not resume from earlier checkpoint for the group
        if (flushGroup.flushWatermark > 0) // negative checkpoints are refused by Kafka
          Task.fromFuture(commitCheckpoint(dataset.ref, shardNum, flushGroup))
        else
          Task { Success }
    }
    taskToReturn.runOnComplete(_ => tracer.finish())
    taskToReturn
  }

  private def doFlushSteps(flushGroup: FlushGroup, chunkSetIt: Iterator[ChunkSet]): Task[Response] = {
    // Note that all cassandra writes will have included retries. Failures after retries will imply data loss
    // in order to keep the ingestion moving. It is important that we don't fall back far behind.
    val chunkSetStream = Observable.fromIterator(chunkSetIt)
    logger.debug(s"Created flush ChunkSets stream for group ${flushGroup.groupNum} in shard $shardNum")
    // Write the stream and partition keys to the sink, checkpoint, mark buffers as eligible for cleanup
    val partKeysInGroup =
      new PartitionIterator(partKeysToFlush(flushGroup.groupNum)(1).intIterator()).map(_.binPartition)

    val writeChunksFuture = sink.write(dataset, chunkSetStream).recover { case e =>
      logger.error("Critical! Chunk persistence failed after retries and skipped", e)
      shardStats.flushesFailedChunkWrite.increment
      DataDropped
    }
    val writePartKeyFuture = sink.addPartitions(dataset, partKeysInGroup, shardNum).recover { case e =>
      logger.error("Critical! Partition Key(s) persistence failed after retries and skipped", e)
      shardStats.flushesFailedPartWrite.increment
      DataDropped
    }

    // If the above futures fail with ErrorResponse because of DB failures, skip the chunk.
    // Sorry - need to drop the data to keep the ingestion moving
    val taskFuture = Future.sequence(Seq(writeChunksFuture, writePartKeyFuture)).flatMap {
      case Seq(Success, Success)     => commitCheckpoint(dataset.ref, shardNum, flushGroup)
      case Seq(er: ErrorResponse, _) => Future.successful(er)
      case Seq(_, er: ErrorResponse) => Future.successful(er)
    }.recover { case e =>
      logger.error("Should have not reached this point. Possible programming error", e)
      DataDropped
    }
    Task.fromFuture(taskFuture)
  }

  private def commitCheckpoint(ref: DatasetRef, shardNum: Int, flushGroup: FlushGroup): Future[Response] = {
    val fut = metastore.writeCheckpoint(ref, shardNum, flushGroup.groupNum,
                                    flushGroup.flushWatermark).recover { case e =>
      logger.error("Critical! Checkpoint persistence skipped", e)
      shardStats.flushesFailedOther.increment
      // skip the checkpoint write
      // Sorry - need to skip to keep the ingestion moving
      DataDropped
    }
    // Update stats
    if (_offset >= 0) shardStats.offsetLatestInMem.record(_offset)
    groupWatermark(flushGroup.groupNum) = flushGroup.flushWatermark
    val maxWatermark = groupWatermark.max
    val minWatermark = groupWatermark.min
    if (maxWatermark >= 0) shardStats.offsetLatestFlushed.record(maxWatermark)
    if (minWatermark >= 0) shardStats.offsetEarliestFlushed.record(minWatermark)
    fut
  }

  // Creates a new TimeSeriesPartition, updating indexes
  // NOTE: it's important to use an actual BinaryRecord instead of just a RowReader in the internal
  // data structures.  The translation to BinaryRecord only happens here (during first time creation
  // of a partition) and keeps internal data structures from having to keep copies of incoming records
  // around, which might be much more expensive memory wise.  One consequence though is that internal
  // and external partition key components need to yield the same hashCode.  IE, use UTF8Strings everywhere.
  def addPartition(newPartKey: SchemaRowReader, needsPersistence: Boolean): TimeSeriesPartition = {
    val binPartKey = dataset.partKey(newPartKey)
    val newPart = new TimeSeriesPartition(dataset, binPartKey, shardNum, sink, bufferPool, shardStats)
    val newIndex = partitions.length
    keyIndex.addKey(binPartKey, newIndex)
    partitions += newPart
    shardStats.partitionsCreated.increment
    keyMap(binPartKey) = newPart
    partitionGroups(group(newPartKey)).set(newIndex)
    // if we are in the restore execution flow, we should not need to write this key
    if (needsPersistence) partKeysToFlush(group(binPartKey))(0).set(newIndex)
    newPart
  }

  private def getPartition(partKey: PartitionKey): Option[FiloPartition] = keyMap.get(partKey)

  def scanPartitions(partMethod: PartitionScanMethod): Observable[FiloPartition] = {
    val indexIt = partMethod match {
      case SinglePartitionScan(partition, _) =>
        getPartition(partition).map(Iterator.single).getOrElse(Iterator.empty)
      case MultiPartitionScan(partKeys, _)   =>
        partKeys.toIterator.flatMap(getPartition)
      case FilteredPartitionScan(split, filters) =>
        // TODO: Use filter func for columns not in index
        if (filters.nonEmpty) {
          val (indexIt, _) = keyIndex.parseFilters(filters)
          new PartitionIterator(indexIt)
        } else {
          partitions.toIterator
        }
    }
    Observable.fromIterator(indexIt.map { p =>
      shardStats.partitionsQueried.increment()
      p
    })
  }

  /**
    * Reset all state in this shard.  Memory is not released as once released, then this class
    * cannot be used anymore.
    */
  def reset(): Unit = {
    logger.info(s"Clearing all MemStore state for shard $shardNum")
    partitions.clear()
    keyMap.clear()
    keyIndex.reset()
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