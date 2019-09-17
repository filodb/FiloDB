package filodb.core.memstore

import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong

import filodb.core.{DatasetRef, Response, Types}
import filodb.core.downsample.{DownsampleConfig, DownsamplePublisher}
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store._
import filodb.memory.MemFactory
import filodb.memory.NativeMemoryManager
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

class TimeSeriesMemStore(config: Config,
                         val store: ColumnStore,
                         val metastore: MetaStore,
                         evictionPolicy: Option[PartitionEvictionPolicy] = None)
                        (implicit val ioPool: ExecutionContext)
extends MemStore with StrictLogging {
  import collection.JavaConverters._
  import FiloSchedulers._

  type Shards = NonBlockingHashMapLong[TimeSeriesShard]
  private val datasets = new HashMap[DatasetRef, Shards]
  private val datasetMemFactories = new HashMap[DatasetRef, MemFactory]

  /**
    * The Downsample Publisher is per dataset on the memstore and is shared among all shards of the dataset
    */
  private val downsamplePublishers = new HashMap[DatasetRef, DownsamplePublisher]

  val stats = new ChunkSourceStats

  private val numParallelFlushes = config.getInt("memstore.flush-task-parallelism")

  private val partEvictionPolicy = evictionPolicy.getOrElse {
    new WriteBufferFreeEvictionPolicy(config.getMemorySize("memstore.min-write-buffers-free").toBytes)
  }

  private def makeAndStartPublisher(downsample: DownsampleConfig): DownsamplePublisher = {
    val pub = downsample.makePublisher()
    pub.start()
    pub
  }

  // TODO: Change the API to return Unit Or ShardAlreadySetup, instead of throwing.  Make idempotent.
  def setup(ref: DatasetRef, schemas: Schemas, shard: Int, storeConf: StoreConfig,
            downsample: DownsampleConfig = DownsampleConfig.disabled): Unit = synchronized {
    val shards = datasets.getOrElseUpdate(ref, new NonBlockingHashMapLong[TimeSeriesShard](32, false))
    if (shards.containsKey(shard)) {
      throw ShardAlreadySetup(ref, shard)
    } else {
      val memFactory = datasetMemFactories.getOrElseUpdate(ref, {
        val bufferMemorySize = storeConf.ingestionBufferMemSize
        logger.info(s"Allocating $bufferMemorySize bytes for WriteBufferPool/PartitionKeys for dataset=${ref}")
        val tags = Map("dataset" -> ref.toString)
        new NativeMemoryManager(bufferMemorySize, tags)
      })

      val publisher = downsamplePublishers.getOrElseUpdate(ref, makeAndStartPublisher(downsample))
      val tsdb = new OnDemandPagingShard(ref, schemas, storeConf, shard, memFactory, store, metastore,
                              partEvictionPolicy, downsample, publisher)
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
  private[filodb] def getShard(dataset: DatasetRef, shard: Int): Option[TimeSeriesShard] =
    datasets.get(dataset).flatMap { shards => Option(shards.get(shard)) }

  /**
    * Retrieve shard for given dataset and shard number. Raises exception if
    * the shard is not setup.
    */
  private[filodb] def getShardE(dataset: DatasetRef, shard: Int): TimeSeriesShard = {
    datasets.get(dataset)
            .flatMap(shards => Option(shards.get(shard)))
            .getOrElse(throw new IllegalArgumentException(s"dataset=$dataset shard=$shard have not been set up"))
  }

  def ingest(dataset: DatasetRef, shard: Int, data: SomeData): Unit =
    getShardE(dataset, shard).ingest(data.records, data.offset)

  // Should only be called once per dataset/shard
  def ingestStream(dataset: DatasetRef,
                   shard: Int,
                   stream: Observable[SomeData],
                   flushSched: Scheduler,
                   flushStream: Observable[FlushCommand] = FlushStream.empty,
                   cancelTask: Task[Unit] = Task {}): CancelableFuture[Unit] = {
    val ingestCommands = Observable.merge(stream, flushStream)
    ingestStream(dataset, shard, ingestCommands, flushSched, cancelTask)
  }

  def recoverIndex(dataset: DatasetRef, shard: Int): Future[Unit] =
    getShardE(dataset, shard).recoverIndex()

  // NOTE: Each ingestion message is a SomeData which has a RecordContainer, which can hold hundreds or thousands
  // of records each.  For this reason the object allocation of a SomeData and RecordContainer is not that bad.
  // If it turns out the batch size is small, consider using object pooling.
  def ingestStream(dataset: DatasetRef,
                   shardNum: Int,
                   combinedStream: Observable[DataOrCommand],
                   flushSched: Scheduler,
                   cancelTask: Task[Unit]): CancelableFuture[Unit] = {
    val shard = getShardE(dataset, shardNum)
    combinedStream.map {
                    case d: SomeData =>       shard.ingest(d)
                                              None
                    // The write buffers for all partitions in a group are switched here, in line with ingestion
                    // stream.  This avoids concurrency issues and ensures that buffers for a group are switched
                    // at the same offset/watermark
                    case FlushCommand(group) => assertThreadName(IngestSchedName)
                                                shard.switchGroupBuffers(group)
                                                // step below purges partitions and needs to run on ingestion thread
                                                val flushTimeBucket = shard.prepareIndexTimeBucketForFlush(group)
                                                Some(FlushGroup(shard.shardNum, group, shard.latestOffset,
                                                                flushTimeBucket))
                    case a: Any => throw new IllegalStateException(s"Unexpected DataOrCommand $a")
                  }.collect { case Some(flushGroup) => flushGroup }
                  .mapAsync(numParallelFlushes) { f => shard.createFlushTask(f).executeOn(flushSched).asyncBoundary }
                           // asyncBoundary so subsequent computations in pipeline happen in default threadpool
                  .completedL
                  .doOnCancel(cancelTask)
                  .runAsync(shard.ingestSched)
  }

  // a more optimized ingest stream handler specifically for recovery
  // TODO: See if we can parallelize ingestion stream for even better throughput
  def recoverStream(dataset: DatasetRef,
                    shardNum: Int,
                    stream: Observable[SomeData],
                    startOffset: Long,
                    endOffset: Long,
                    checkpoints: Map[Int, Long],
                    reportingInterval: Long): Observable[Long] = {
    val shard = getShardE(dataset, shardNum)
    shard.setGroupWatermarks(checkpoints)
    if (endOffset < startOffset) Observable.empty
    else {
      var targetOffset = startOffset + reportingInterval
      var startOffsetValidated = false
      stream.map { r =>
        if (!startOffsetValidated) {
          if (r.offset > startOffset) {
            val offsetsNotRecovered = r.offset - startOffset
            logger.error(s"Could not recover dataset=$dataset shard=$shardNum from check pointed offset possibly " +
                         s"because of retention issues. recoveryStartOffset=$startOffset " +
                         s"firstRecordOffset=${r.offset} offsetsNotRecovered=$offsetsNotRecovered")
            shard.shardStats.offsetsNotRecovered.increment(offsetsNotRecovered)
          }
          startOffsetValidated = true
        }
        shard.ingest(r)
      }.collect {
        case offset: Long if offset >= endOffset => // last offset reached
          offset
        case offset: Long if offset > targetOffset => // reporting interval reached
          targetOffset += reportingInterval
          offset
      }
    }
  }

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
                             end: Long, start: Long, limit: Int): Iterator[TimeSeriesPartition] =
    getShard(dataset, shard).map(_.partKeysWithFilters(filters, end, start, limit)).getOrElse(Iterator.empty)

  def numPartitions(dataset: DatasetRef, shard: Int): Int =
    getShard(dataset, shard).map(_.numActivePartitions).getOrElse(-1)

  def readRawPartitions(ref: DatasetRef,
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData] = Observable.empty

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

  def numRowsIngested(dataset: DatasetRef, shard: Int): Long =
    getShard(dataset, shard).map(_.numRowsIngested).getOrElse(-1L)

  def latestOffset(dataset: DatasetRef, shard: Int): Long =
    getShard(dataset, shard).get.latestOffset

  def shardMetrics(dataset: DatasetRef, shard: Int): TimeSeriesShardStats =
    getShard(dataset, shard).get.shardStats

  def activeShards(dataset: DatasetRef): Seq[Int] =
    datasets.get(dataset).map(_.keySet.asScala.map(_.toInt).toSeq).getOrElse(Nil)

  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit] =
    activeShards(dataset).map(ShardSplit)

  def groupsInDataset(ref: DatasetRef): Int =
    datasets.get(ref).map(_.values.asScala.head.storeConfig.groupsPerShard).getOrElse(1)

  def analyzeAndLogCorruptPtr(ref: DatasetRef, cve: CorruptVectorException): Unit =
    getShard(ref, cve.shard).get.analyzeAndLogCorruptPtr(cve)

  def reset(): Unit = {
    datasets.clear()
    downsamplePublishers.valuesIterator.foreach { _.stop() }
    downsamplePublishers.clear()
    store.reset()
  }

  def truncate(dataset: DatasetRef): Future[Response] = {
    datasets.get(dataset).foreach(_.values.asScala.foreach(_.reset()))
    store.truncate(dataset)
  }

  def removeShard(dataset: DatasetRef, shardNum: Int, shard: TimeSeriesShard): Boolean = {
    datasets.get(dataset).map(_.remove(shardNum, shard)).getOrElse(false)
  }

  // Release memory etc.
  def shutdown(): Unit = {
    datasets.values.foreach(_.values.asScala.foreach(_.shutdown()))
    datasets.values.foreach(_.values().asScala.foreach(_.closePartKeyIndex()))
    reset()
  }

}
