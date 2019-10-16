package filodb.core.downsample

import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong

import filodb.core.{DatasetRef, Response, Types}
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

object DownsampledTimeSeriesStore {
  def downsampleDatasetRefs(rawDatasetRef: DatasetRef,
                            downsampleResolutions: Seq[FiniteDuration]): Map[FiniteDuration, DatasetRef] = {
    downsampleResolutions.map { res =>
      res -> DatasetRef(s"${rawDatasetRef}_ds_${res.toMinutes}")
    }.toMap
  }
}

class DownsampledTimeSeriesStore(val store: ColumnStore,
                                 val metastore: MetaStore,
                                 val filodbConfig: Config)
                                (implicit val ioPool: ExecutionContext)
extends MemStore with StrictLogging {
  import collection.JavaConverters._

  type Shards = NonBlockingHashMapLong[DownsampledTimeSeriesShard]

  private val datasets = new HashMap[DatasetRef, Shards]

  val stats = new ChunkSourceStats

  override def isReadOnly: Boolean = true

  // TODO: Change the API to return Unit Or ShardAlreadySetup, instead of throwing.  Make idempotent.
  def setup(ref: DatasetRef, schemas: Schemas, shard: Int, storeConf: StoreConfig,
            downsample: DownsampleConfig = DownsampleConfig.disabled): Unit = synchronized {
    val shards = datasets.getOrElseUpdate(ref, new NonBlockingHashMapLong[DownsampledTimeSeriesShard](32, false))
    if (shards.containsKey(shard)) {
      throw ShardAlreadySetup(ref, shard)
    } else {
      val tsdb = new DownsampledTimeSeriesShard(ref, schemas, shard, config)
      shards.put(shard, tsdb)
    }
  }

  /**
    * WARNING: use only for testing. Not performant
    */
  def refreshIndexForTesting(dataset: DatasetRef): Unit =
    datasets.get(dataset).foreach(_.values().asScala.foreach { s =>
      s.refreshPartKeyIndexBlocking()
    })

  /**
    * Retrieve shard for given dataset and shard number as an Option
    */
  private[filodb] def getShard(dataset: DatasetRef, shard: Int): Option[DownsampledTimeSeriesShard] =
    datasets.get(dataset).flatMap { shards => Option(shards.get(shard)) }

  /**
    * Retrieve shard for given dataset and shard number. Raises exception if
    * the shard is not setup.
    */
  private[filodb] def getShardE(dataset: DatasetRef, shard: Int): DownsampledTimeSeriesShard = {
    datasets.get(dataset)
            .flatMap(shards => Option(shards.get(shard)))
            .getOrElse(throw new IllegalArgumentException(s"dataset=$dataset shard=$shard have not been set up"))
  }

  def recoverIndex(dataset: DatasetRef, shard: Int): Future[Unit] =
    getShardE(dataset, shard).recoverIndex()


  def indexNames(dataset: DatasetRef, limit: Int): Seq[(String, Int)] =
    datasets.get(dataset).map { shards =>
      shards.entrySet.asScala.flatMap { entry =>
        val shardNum = entry.getKey.toInt
        entry.getValue.indexNames(limit).map { s => (s, shardNum) }
      }.toSeq
    }.getOrElse(Nil)

  def labelValues(dataset: DatasetRef, shard: Int, labelName: String, topK: Int = 100): Seq[TermInfo] =
    getShard(dataset, shard).map(_.labelValues(labelName, topK)).getOrElse(Nil)

  def labelValuesWithFilters(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                             labelNames: Seq[String], end: Long,
                             start: Long, limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
    = getShard(dataset, shard)
        .map(_.labelValuesWithFilters(filters, labelNames, end, start, limit)).getOrElse(Iterator.empty)

  def partKeysWithFilters(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                             end: Long, start: Long, limit: Int): Iterator[PartKey] =
    getShard(dataset, shard).map(_.partKeysWithFilters(filters, end, start, limit)).getOrElse(Iterator.empty)

  def readRawPartitions(ref: DatasetRef,
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData] = ???

  def scanPartitions(ref: DatasetRef,
                     columnIDs: Seq[Types.ColumnId],
                     partMethod: PartitionScanMethod,
                     chunkMethod: ChunkScanMethod = AllChunkScan): Observable[ReadablePartition] = {
    val shard = datasets(ref).get(partMethod.shard)

    if (shard == UnsafeUtils.ZeroPointer) {
      throw new IllegalArgumentException(s"Shard ${partMethod.shard} of dataset $ref is not assigned to " +
        s"this node. Was it was recently reassigned to another node? Prolonged occurrence indicates an issue.")
    }
    shard.scanPartitions(columnIDs, partMethod, chunkMethod)
  }

  def shardMetrics(dataset: DatasetRef, shard: Int): TimeSeriesShardStats =
    getShard(dataset, shard).get.shardStats

  def activeShards(dataset: DatasetRef): Seq[Int] =
    datasets.get(dataset).map(_.keySet.asScala.map(_.toInt).toSeq).getOrElse(Nil)

  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit] =
    activeShards(dataset).map(ShardSplit)

  def groupsInDataset(ref: DatasetRef): Int = throw new UnsupportedOperationException()

  def analyzeAndLogCorruptPtr(ref: DatasetRef, cve: CorruptVectorException): Unit =
    throw new UnsupportedOperationException()

  def reset(): Unit = {
    datasets.clear()
    store.reset()
  }

  def removeShard(dataset: DatasetRef, shardNum: Int, shard: DownsampledTimeSeriesShard): Boolean = {
    datasets.get(dataset).map(_.remove(shardNum, shard)).getOrElse(false)
  }

  def shutdown(): Unit = {
    reset()
  }

  /**
    * Ingests new rows, making them immediately available for reads
    * NOTE: this method is not intended to be the main ingestion method, just used for testing.
    * Instead the more reactive ingestStream method should be used.
    *
    * @param dataset the dataset to ingest into
    * @param shard   shard number to ingest into
    * @param data    a RecordContainer with BinaryRecords conforming to the schema used in setup(), and offset
    */
  override def ingest(dataset: DatasetRef, shard: Int,
                      data: SomeData): Unit = ???

  /**
    * Sets up a shard of a dataset to continuously ingest new sets of records from a stream.
    * The records are immediately available for reads from that shard of the memstore.
    * Errors during ingestion are handled by the errHandler.
    * Flushes to the ChunkSink are initiated by a potentially independent stream, the flushStream, which emits
    * flush events of a specific subgroup of a shard.
    * NOTE: does not check that existing streams are not already writing to this store.  That needs to be
    * handled by an upper layer.  Multiple stream ingestion is not guaranteed to be thread safe, a single
    * stream is safe for now.
    * NOTE2: ingest happens on the shard's single ingestion thread, except for flushes which are scheduled on the
    * passed in flushSched
    *
    * @param dataset               the dataset to ingest into
    * @param shard                 shard number to ingest into
    * @param stream                the stream of SomeData() with records conforming to dataset ingestion schema
    * @param flushSched            the Scheduler to use to schedule flush tasks
    * @param flushStream           the stream of FlushCommands for regular flushing of chunks to ChunkSink
    * @return a CancelableFuture for cancelling the stream subscription, which should be done on teardown
    *         the Future completes when both stream and flushStream ends.  It is up to the caller to ensure this.
    */
  override def ingestStream(dataset: DatasetRef, shard: Int,
                            stream: Observable[SomeData],
                            flushSched: Scheduler,
                            flushStream: Observable[FlushCommand],
                            cancelTask: Task[Unit]): CancelableFuture[Unit] = ???

  /**
    * Sets up streaming recovery of a shard from ingest records.  This is a separate API for several reasons:
    * 1. No flushing occurs during recovery.  We are recovering the write buffers before they get flushed.
    * 2. Ingested records that have an offset below the group watermark in checkpoints are skipped. They should have
    * been flushed already.
    * 3. This returns an Observable of offsets that are read in, at roughly every "reportingInterval" offsets.  This
    * is used for reporting recovery progress and to know when to end the recovery stream.
    *
    * This allows a MemStore to implement a more efficient recovery stream.  Some assumptions are made:
    * - The stream should restart from the min(checkpoints)
    * - The caller is responsible for subscribing the resulting stream, ending it, and handling errors
    *
    * @param dataset           the dataset to ingest/recover into
    * @param shard             shard number to ingest/recover into
    * @param stream            the stream of SomeData() with records conforming to dataset ingestion schema.
    *                          It should restart from the min(checkpoints)
    * @param checkpoints       the write checkpoints for each subgroup, a Map(subgroup# -> checkpoint). Records for that
    *                          subgroup with an offset below the checkpoint will be skipped, since they
    *                          have been persisted.
    * @param reportingInterval the interval at which the latest offsets ingested will be sent back
    * @return an Observable of the latest ingested offsets.  Caller is responsible for subscribing and ending the stream
    */
  override def recoverStream(dataset: DatasetRef, shard: Int,
                             stream: Observable[SomeData],
                             startOffset: Long, endOffset: Long, checkpoints: Map[Int, Long],
                             reportingInterval: Long): Observable[Long] = ???

  /**
    * Returns the number of partitions being maintained in the memtable for a given shard
    *
    * @return -1 if dataset not found, otherwise number of active partitions
    */
  override def numPartitions(dataset: DatasetRef, shard: Int): Int = ???

  /**
    * Number of total rows ingested for that shard
    */
  override def numRowsIngested(dataset: DatasetRef, shard: Int): Long = ???

  /**
    * Returns the latest offset of a given shard
    */
  override def latestOffset(dataset: DatasetRef, shard: Int): Long = ???

  /**
    * WARNING: truncates all the data in the memstore for the given dataset, and also the data
    * in any underlying ChunkSink too.
    *
    * @return Success, or some ErrorResponse
    */
  override def truncate(dataset: DatasetRef): Future[Response] = ???
}
