package filodb.core.downsample

import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong

import filodb.core.{DatasetRef, Response}
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, QuerySession}
import filodb.core.store._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

class DownsampledTimeSeriesStore(val store: ColumnStore,
                                 rawColStore: ColumnStore,
                                 val filodbConfig: Config)
                                (implicit val ioPool: ExecutionContext)
extends MemStore with StrictLogging {
  import collection.JavaConverters._

  private val datasets = new HashMap[DatasetRef, NonBlockingHashMapLong[DownsampledTimeSeriesShard]]

  val stats = new ChunkSourceStats

  override def isDownsampleStore: Boolean = true

  override def metastore: MetaStore = ??? // Not needed

  // TODO: Change the API to return Unit Or ShardAlreadySetup, instead of throwing.  Make idempotent.
  def setup(ref: DatasetRef, schemas: Schemas, shard: Int, storeConf: StoreConfig,
            downsample: DownsampleConfig = DownsampleConfig.disabled): Unit = synchronized {
    val shards = datasets.getOrElseUpdate(ref, new NonBlockingHashMapLong[DownsampledTimeSeriesShard](32, false))
    if (shards.containsKey(shard)) {
      throw ShardAlreadySetup(ref, shard)
    } else {
      val tsdb = new DownsampledTimeSeriesShard(ref, storeConf, schemas, store,
                                                rawColStore, shard, filodbConfig, downsample)
      shards.put(shard, tsdb)
    }
  }

  def refreshIndexForTesting(dataset: DatasetRef): Unit =
    datasets.get(dataset).foreach(_.values().asScala.foreach { s =>
      s.refreshPartKeyIndexBlocking()
    })

  private[filodb] def getShard(dataset: DatasetRef, shard: Int): Option[DownsampledTimeSeriesShard] =
    datasets.get(dataset).flatMap { shards => Option(shards.get(shard)) }

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
                          fetchFirstLastSampleTimes: Boolean, end: Long, start: Long,
                          limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] =
    getShard(dataset, shard).map(_.partKeysWithFilters(filters, fetchFirstLastSampleTimes,
                                                       end, start, limit)).getOrElse(Iterator.empty)

  def lookupPartitions(ref: DatasetRef,
                       partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod,
                       querySession: QuerySession): PartLookupResult = {
    val shard = datasets(ref).get(partMethod.shard)

    if (shard == UnsafeUtils.ZeroPointer) {
      throw new IllegalArgumentException(s"Shard $shard of dataset $ref is not assigned to " +
        s"this node. Was it was recently reassigned to another node? Prolonged occurrence indicates an issue.")
    }
    shard.lookupPartitions(partMethod, chunkMethod, querySession)
  }

  def scanPartitions(ref: DatasetRef,
                     lookupRes: PartLookupResult,
                     querySession: QuerySession): Observable[ReadablePartition] = {
    val shard = datasets(ref).get(lookupRes.shard)

    if (shard == UnsafeUtils.ZeroPointer) {
      throw new IllegalArgumentException(s"Shard $shard of dataset $ref is not assigned to " +
        s"this node. Was it was recently reassigned to another node? Prolonged occurrence indicates an issue.")
    }
    shard.scanPartitions(lookupRes, querySession)
  }

  def activeShards(dataset: DatasetRef): Seq[Int] =
    datasets.get(dataset).map(_.keySet.asScala.map(_.toInt).toSeq).getOrElse(Nil)

  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit] =
    activeShards(dataset).map(ShardSplit)

  def groupsInDataset(ref: DatasetRef): Int =
    datasets.get(ref).map(_.values.asScala.head.rawStoreConfig.groupsPerShard).getOrElse(1)

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

  override def ingest(dataset: DatasetRef, shard: Int,
                      data: SomeData): Unit = throw new UnsupportedOperationException()

  override def ingestStream(dataset: DatasetRef,
                   shard: Int,
                   stream: Observable[SomeData],
                   flushSched: Scheduler,
                   cancelTask: Task[Unit] = Task {}): CancelableFuture[Unit] = throw new UnsupportedOperationException()

  override def recoverStream(dataset: DatasetRef, shard: Int,
                             stream: Observable[SomeData],
                             startOffset: Long, endOffset: Long, checkpoints: Map[Int, Long],
                             reportingInterval: Long): Observable[Long] = throw new UnsupportedOperationException()

  override def numPartitions(dataset: DatasetRef, shard: Int): Int = throw new UnsupportedOperationException()

  override def numRowsIngested(dataset: DatasetRef, shard: Int): Long = throw new UnsupportedOperationException()

  override def latestOffset(dataset: DatasetRef, shard: Int): Long = throw new UnsupportedOperationException()

  override def truncate(dataset: DatasetRef, numShards: Int): Future[Response] =
    throw new UnsupportedOperationException()

  override def schemas(ref: DatasetRef): Option[Schemas] = {
    datasets.get(ref).map(_.values.asScala.head.schemas)
  }

  override def readRawPartitions(ref: DatasetRef, maxChunkTime: Long,
                                 partMethod: PartitionScanMethod,
                                 chunkMethod: ChunkScanMethod): Observable[RawPartData] = ???
}
