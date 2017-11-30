package filodb.core.memstore

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.instrument.Gauge
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong

import filodb.core.{DatasetRef, ErrorResponse, Response, Success}
import filodb.core.Types.PartitionKey
import filodb.core.metadata.Dataset
import filodb.core.query.PartitionKeyIndex
import filodb.core.store._
import filodb.memory.{BlockHolder, NativeMemoryManager, PageAlignedBlockManager}
import filodb.memory.format.{SchemaRowReader, ZeroCopyUTF8String}

class TimeSeriesMemStore(config: Config, val sink: ColumnStore, val metastore: MetaStore)
                        (implicit val ec: ExecutionContext)
extends MemStore with StrictLogging {

  import collection.JavaConverters._

  type Shards = NonBlockingHashMapLong[TimeSeriesShard]
  private val datasets = new HashMap[DatasetRef, Shards]
  val numGroups = config.getInt("memstore.groups-per-shard")

  val stats = new ChunkSourceStats

  private val numParallelFlushes = config.getInt("memstore.flush-task-parallelism")

  // TODO: Change the API to return Unit Or ShardAlreadySetup, instead of throwing.  Make idempotent.
  def setup(dataset: Dataset, shard: Int): Unit = synchronized {
    val shards = datasets.getOrElseUpdate(dataset.ref, {
                   val shardMap = new NonBlockingHashMapLong[TimeSeriesShard](32, false)
                   Kamon.metrics.gauge(s"num-partitions-${dataset.name}",
                     5.seconds)(new Gauge.CurrentValueCollector {
                      def currentValue: Long =
                        shardMap.values.asScala.map(_.numActivePartitions).sum.toLong
                     })
                   shardMap
                 })
    if (shards contains shard) {
      throw ShardAlreadySetup
    } else {
      val tsdb = new TimeSeriesShard(dataset, config, shard, sink, metastore)
      shards.put(shard, tsdb)
    }
  }

  /**
    * Retrieve shard for given dataset and shard number as an Option
    */
  private def getShard(dataset: DatasetRef, shard: Int): Option[TimeSeriesShard] =
    datasets.get(dataset).flatMap { shards => Option(shards.get(shard)) }

  /**
    * Retrieve shard for given dataset and shard number. Raises exception if
    * the shard is not setup.
    */
  private def getShardE(dataset: DatasetRef, shard: Int): TimeSeriesShard = {
    datasets.get(dataset)
            .flatMap(shards => Option(shards.get(shard)))
            .getOrElse(throw new IllegalArgumentException(s"Dataset $dataset and shard $shard have not been set up"))
  }

  def ingest(dataset: DatasetRef, shard: Int, rows: Seq[IngestRecord]): Unit =
    getShardE(dataset, shard).ingest(rows)

  def restorePartitions(dataset: Dataset, shardNum: Int)(implicit sched: Scheduler): Future[Long] = {
    val partitionKeys = sink.scanPartitionKeys(dataset, shardNum)
    val shard = getShardE(dataset.ref, shardNum)
    partitionKeys.map { p =>
      shard.addPartition(p, false)
    }.countL.runAsync.map { count =>
      logger.info(s"Restored $count time series partitions into memstore for shard $shardNum")
      count
    }
  }

  // Should only be called once per dataset/shard
  def ingestStream(dataset: DatasetRef,
                   shardNum: Int,
                   stream: Observable[Seq[IngestRecord]],
                   flushStream: Observable[FlushCommand] = FlushStream.empty)
                  (errHandler: Throwable => Unit)
                  (implicit sched: Scheduler): CancelableFuture[Unit] = {
    val shard = getShardE(dataset, shardNum)
    val combinedStream = Observable.merge(stream.map(SomeData), flushStream)
    combinedStream.map {
                    case SomeData(records) => shard.ingest(records)
                                              None
                    // The write buffers for all partitions in a group are switched here, in line with ingestion
                    // stream.  This avoids concurrency issues and ensures that buffers for a group are switched
                    // at the same offset/watermark
                    case FlushCommand(group) => shard.switchGroupBuffers(group)
                                                Some(FlushGroup(shard.shardNum, group, shard.latestOffset))
                  }.collect { case Some(flushGroup) => flushGroup }
                  .mapAsync(numParallelFlushes)(shard.createFlushTask _)
                  .foreach { x => }
                  .recover { case ex: Exception => errHandler(ex) }
  }

  // a more optimized ingest stream handler specifically for recovery
  // TODO: See if we can parallelize ingestion stream for even better throughput
  def recoverStream(dataset: DatasetRef,
                    shardNum: Int,
                    stream: Observable[Seq[IngestRecord]],
                    checkpoints: Map[Int, Long],
                    reportingInterval: Long): Observable[Long] = {
    val shard = getShardE(dataset, shardNum)
    shard.setGroupWatermarks(checkpoints)
    var targetOffset = checkpoints.values.min + reportingInterval
    stream.map(shard.ingest(_))
          .collect {
            case offset if offset > targetOffset =>
              targetOffset += reportingInterval
              offset
          }
  }

  def indexNames(dataset: DatasetRef): Iterator[(String, Int)] =
    datasets.get(dataset).map { shards =>
      shards.entrySet.iterator.asScala.flatMap { entry =>
        val shardNum = entry.getKey.toInt
        entry.getValue.indexNames.map { s => (s, shardNum) }
      }
    }.getOrElse(Iterator.empty)

  def indexValues(dataset: DatasetRef, shard: Int, indexName: String): Iterator[ZeroCopyUTF8String] =
    getShard(dataset, shard).map(_.indexValues(indexName)).getOrElse(Iterator.empty)

  def numPartitions(dataset: DatasetRef, shard: Int): Int =
    getShard(dataset, shard).map(_.numActivePartitions).getOrElse(-1)

  def scanPartitions(dataset: Dataset,
                     partMethod: PartitionScanMethod): Observable[FiloPartition] =
    datasets(dataset.ref).get(partMethod.shard).scanPartitions(partMethod)

  def numRowsIngested(dataset: DatasetRef, shard: Int): Long =
    getShard(dataset, shard).map(_.numRowsIngested).getOrElse(-1L)

  def latestOffset(dataset: DatasetRef, shard: Int): Long =
    getShard(dataset, shard).get.latestOffset

  def activeShards(dataset: DatasetRef): Seq[Int] =
    datasets.get(dataset).map(_.keySet.asScala.map(_.toInt).toSeq).getOrElse(Nil)

  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit] =
    activeShards(dataset).map(ShardSplit)

  def reset(): Unit = {
    datasets.clear()
    sink.reset()
  }

  def truncate(dataset: DatasetRef): Unit = {
    datasets.get(dataset).foreach(_.values.asScala.foreach(_.reset()))
    sink.truncate(dataset)
  }

  def shutdown(): Unit = {}

  override def scanPartitionKeys(dataset: Dataset, shardNum: Int): Observable[PartitionKey] = {
    scanPartitions(dataset, FilteredPartitionScan(ShardSplit(shardNum))).map(_.binPartition)
  }
}

object TimeSeriesShard {
  val rowsIngested = Kamon.metrics.counter("memstore-rows-ingested")
  val partitionsCreated = Kamon.metrics.counter("memstore-partitions-created")
  val rowsSkipped  = Kamon.metrics.counter("recovery-row-skipped")
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
  import TimeSeriesShard._

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
  private final val numGroups = config.getInt("memstore.groups-per-shard")
  private val maxNumPartitions = config.getInt("memstore.max-num-partitions")

  private val blockStore = new PageAlignedBlockManager(shardMemoryMB * 1024 * 1024)
  protected val bufferMemoryManager = new NativeMemoryManager(maxChunksSize * 8 * maxNumPartitions)

  /**
    * Unencoded/unoptimized ingested data is stored in buffers that are allocated from this off-heap pool
    */
  private val bufferPool = new WriteBufferPool(bufferMemoryManager, dataset, maxChunksSize / 8, maxNumPartitions)

  private final val partitionGroups = Array.fill(numGroups)(new EWAHCompressedBitmap)

  /**
    * The offset up to and including the last record in this group to be successfully persisted
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
        rowsSkipped.increment
      } else {
        val partition = keyMap.getOrElse(partKey, addPartition(partKey, true))
        partition.ingest(data, offset)
        numActuallyIngested += 1
      }
    }
    rowsIngested.increment(numActuallyIngested)
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
    chunkSetIt.isEmpty match {
      case false =>
        val chunkSetStream = Observable.fromIterator (chunkSetIt)
        logger.debug(s"Created flush ChunkSets stream for group ${flushGroup.groupNum} in shard $shardNum")

        // Write the stream and partition keys to the sink, checkpoint, mark buffers as eligible for cleanup

        val partKeysInGroup =
          new PartitionIterator(partKeysToFlush(flushGroup.groupNum)(1).intIterator()).map(_.binPartition)
        val writePartKeyFuture = sink.addPartitions(dataset, partKeysInGroup, shardNum)
        val writeChunksFuture = sink.write(dataset, chunkSetStream)
        val combined = Future.sequence(Seq(writeChunksFuture, writePartKeyFuture))
        val taskFuture = combined flatMap {
          case Seq(Success, Success) =>
            metastore.writeCheckpoint(dataset.ref, shardNum, flushGroup.groupNum, flushGroup.flushWatermark)
          case Seq(e: ErrorResponse, _) => throw FlushError (e)
          case Seq(_, e: ErrorResponse) => throw FlushError (e)
        } map {
          case Success => blockHolder.markUsedBlocksReclaimable ()
                          Success
              //TODO What does it mean for the flush to fail.
              //Flush fail means 2 things. The write to the sink failed or writing the checkpoint failed.
              //We need to add logic to retry these aspects. If the retry succeeds we need mark the blocks reusable.
              //If the retry fails, we are in a bad state. We have to discards both the buffers and blocks.
              //Then we ingest from Kafka again.
          case e: ErrorResponse => throw FlushError (e)
        }
        Task.fromFuture (taskFuture)
      case true =>
        // even though there were no records for the chunkset, we want to write checkpoint anyway
        // since we should not resume from earlier checkpoint for the group
        if (flushGroup.flushWatermark > 0) // negative checkpoints are refused by Kafka
          Task.fromFuture(
            metastore.writeCheckpoint (dataset.ref, shardNum, flushGroup.groupNum, flushGroup.flushWatermark))
        else
          Task { Success }
    }
  }

  // Creates a new TimeSeriesPartition, updating indexes
  // NOTE: it's important to use an actual BinaryRecord instead of just a RowReader in the internal
  // data structures.  The translation to BinaryRecord only happens here (during first time creation
  // of a partition) and keeps internal data structures from having to keep copies of incoming records
  // around, which might be much more expensive memory wise.  One consequence though is that internal
  // and external partition key components need to yield the same hashCode.  IE, use UTF8Strings everywhere.
  def addPartition(newPartKey: SchemaRowReader, needsPersistence: Boolean): TimeSeriesPartition = {
    val binPartKey = dataset.partKey(newPartKey)
    val newPart = new TimeSeriesPartition(dataset, binPartKey, shardNum, sink, bufferPool)
    val newIndex = partitions.length
    keyIndex.addKey(binPartKey, newIndex)
    partitions += newPart
    partitionsCreated.increment
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
    Observable.fromIterator(indexIt)
  }

  /**
   * Release all memory and reset all state in this shard
   */
  def reset(): Unit = {
    partitions.clear()
    keyMap.clear()
    keyIndex.reset()
    ingested = 0L
    for { group <- 0 until numGroups } {
      partitionGroups(group) = new EWAHCompressedBitmap()
      groupWatermark(group) = Long.MinValue
    }
  }
}