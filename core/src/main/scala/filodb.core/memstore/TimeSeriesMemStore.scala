package filodb.core.memstore

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import filodb.core.DatasetRef
import filodb.core.Types.PartitionKey
import filodb.core.metadata.Dataset
import filodb.core.query.PartitionKeyIndex
import filodb.core.store._
import filodb.memory.format.{SchemaRowReader, ZeroCopyUTF8String}
import filodb.memory.impl.PageAlignedBlockManager
import filodb.memory.{BlockHolder, BlockManager}

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.instrument.Gauge
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong

final case class DatasetAlreadySetup(dataset: DatasetRef) extends Exception(s"Dataset $dataset already setup")

class TimeSeriesMemStore(config: Config, val sink: ChunkSink)(implicit val ec: ExecutionContext)
extends MemStore with StrictLogging {
  import collection.JavaConverters._

  type Shards = NonBlockingHashMapLong[TimeSeriesShard]
  private val datasets = new HashMap[DatasetRef, Shards]

  val stats = new ChunkSourceStats

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
      val tsdb = new TimeSeriesShard(dataset, config, shard)
      shards.put(shard, tsdb)
    }
  }

  private def getShard(dataset: DatasetRef, shard: Int): Option[TimeSeriesShard] =
    datasets.get(dataset).flatMap { shards => Option(shards.get(shard)) }

  def ingest(dataset: DatasetRef, shard: Int, rows: Seq[IngestRecord]): Unit =
    getShard(dataset, shard).map { shard => shard.ingest(rows)
    }.getOrElse(throw new IllegalArgumentException(s"dataset $dataset / shard $shard not setup"))

  def ingestStream(dataset: DatasetRef, shard: Int, stream: Observable[Seq[IngestRecord]])
                  (errHandler: Throwable => Unit)
                  (implicit sched: Scheduler): CancelableFuture[Unit] = {
    getShard(dataset, shard).map { shard =>
      stream.foreach { records => shard.ingest(records) }
            .recover { case ex: Exception => errHandler(ex) }
    }.getOrElse(throw new IllegalArgumentException(s"dataset $dataset / shard $shard not setup"))
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
class TimeSeriesShard(dataset: Dataset, config: Config, shardNum: Int) extends StrictLogging {
  import TimeSeriesShard._

  private final val partitions = new ArrayBuffer[TimeSeriesPartition]
  private final val keyMap = new HashMap[SchemaRowReader, TimeSeriesPartition]
  private final val keyIndex = new PartitionKeyIndex(dataset)
  private final var ingested = 0L

  private val chunksToKeep = config.getInt("memstore.chunks-to-keep")
  private val maxChunksSize = config.getInt("memstore.max-chunks-size")
  private val shardMemoryMB = config.getInt("memstore.shard-memory-mb")
  private final val numGroups = config.getInt("memstore.groups-per-shard")
  private val maxNumPartitions = config.getInt("memstore.max-num-partitions")

  private val blockStore = new PageAlignedBlockManager(shardMemoryMB * 1024 * 1024)
  protected implicit val blockHolder = new BlockHolder(blockStore, BlockManager.reclaimAnyPolicy)
  private val bufferPool = new WriteBufferPool(dataset, maxChunksSize / 8, maxNumPartitions)

  private final val partitionGroups = Array.fill(numGroups)(new EWAHCompressedBitmap)
  // The offset up to and including the last record in this group to be successfully persisted
  private final val groupWatermark = Array.fill(numGroups)(Long.MinValue)

  class PartitionIterator(intIt: IntIterator) extends Iterator[TimeSeriesPartition] {
    def hasNext: Boolean = intIt.hasNext
    def next: TimeSeriesPartition = partitions(intIt.next)
  }

  def ingest(rows: Seq[IngestRecord]): Unit = {
    // now go through each row, find the partition and call partition ingest
    rows.foreach { case IngestRecord(partKey, data, offset) =>
      // RECOVERY: Check the watermark for the group that this record is part of.  If the offset is < watermark,
      // then do not bother with the expensive partition key comparison and ingestion.  Just skip it
      if (offset < groupWatermark(group(partKey))) {
        rowsSkipped.increment
      } else {
        val partition = keyMap.getOrElse(partKey, addPartition(partKey))
        partition.ingest(data, offset)
      }
    }
    rowsIngested.increment(rows.length)
    ingested += rows.length
  }

  def indexNames: Iterator[String] = keyIndex.indexNames

  def indexValues(indexName: String): Iterator[ZeroCopyUTF8String] = keyIndex.indexValues(indexName)

  def numRowsIngested: Long = ingested

  def numActivePartitions: Int = keyMap.size

  private final def group(partKey: SchemaRowReader): Int = Math.abs(partKey.hashCode % numGroups)

  // Creates a new TimeSeriesPartition, updating indexes
  // NOTE: it's important to use an actual BinaryRecord instead of just a RowReader in the internal
  // data structures.  The translation to BinaryRecord only happens here (during first time creation
  // of a partition) and keeps internal data structures from having to keep copies of incoming records
  // around, which might be much more expensive memory wise.  One consequence though is that internal
  // and external partition key components need to yield the same hashCode.  IE, use UTF8Strings everywhere.
  private def addPartition(newPartKey: SchemaRowReader): TimeSeriesPartition = {
    val binPartKey = dataset.partKey(newPartKey)
    val newPart = new TimeSeriesPartition(dataset, binPartKey, shardNum, bufferPool)
    val newIndex = partitions.length
    keyIndex.addKey(binPartKey, newIndex)
    partitions += newPart
    partitionsCreated.increment
    keyMap(binPartKey) = newPart
    partitionGroups(group(newPartKey)).set(newIndex)
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