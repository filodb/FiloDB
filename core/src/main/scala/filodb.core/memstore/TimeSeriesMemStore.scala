package filodb.core.memstore

import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong

import filodb.core.{DatasetRef, QueryTimeoutException, Response, Types}
import filodb.core.downsample.DownsampleConfig
import filodb.core.memstore.ratelimit.{CardinalityRecord, ConfigQuotaSource}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QuerySession, ServiceUnavailableException}
import filodb.core.store._
import filodb.memory.NativeMemoryManager
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

/**
 * An implementation of TimeSeriesStore with data cached in memory as needed
 */
class TimeSeriesMemStore(filodbConfig: Config,
                         val store: ColumnStore,
                         val metastore: MetaStore,
                         evictionPolicy: Option[PartitionEvictionPolicy] = None)
                        (implicit val ioPool: ExecutionContext)
extends TimeSeriesStore with StrictLogging {
  import collection.JavaConverters._

  type Shards = NonBlockingHashMapLong[TimeSeriesShard]
  private val datasets = new HashMap[DatasetRef, Shards]
  private val datasetMemFactories = new HashMap[DatasetRef, NativeMemoryManager]
  private val quotaSources = new HashMap[DatasetRef, ConfigQuotaSource]

  val stats = new ChunkSourceStats

  private val ensureTspHeadroomPercent = filodbConfig.getDouble("memstore.ensure-tsp-count-headroom-percent")
  private val ensureNmmHeadroomPercent = filodbConfig.getDouble("memstore.ensure-native-memory-headroom-percent")

  private val partEvictionPolicy = evictionPolicy.getOrElse(
    new CompositeEvictionPolicy(ensureTspHeadroomPercent, ensureNmmHeadroomPercent))

  private lazy val ingestionMemory = filodbConfig.getMemorySize("memstore.ingestion-buffer-mem-size").toBytes

  private[this] lazy val ingestionMemFactory: NativeMemoryManager = {
    logger.info(s"Allocating $ingestionMemory bytes for WriteBufferPool/PartitionKeys")
    val tags = Map.empty[String, String]
    new NativeMemoryManager(ingestionMemory, tags)
  }

  def isDownsampleStore: Boolean = false

  def checkReadyForQuery(ref: DatasetRef,
                         shard: Int,
                         querySession: QuerySession): Unit = {
    if (!getShardE(ref: DatasetRef, shard: Int).isReadyForQuery) {
      if (!querySession.qContext.plannerParams.allowPartialResults) {
        throw new ServiceUnavailableException(s"Unable to answer query since shard $shard is still bootstrapping")
      }
      querySession.resultCouldBePartial = true
      querySession.partialResultsReason = Some("Result may be partial since some shards are still bootstrapping")
    }
  }

  // TODO: Change the API to return Unit Or ShardAlreadySetup, instead of throwing.  Make idempotent.
  def setup(ref: DatasetRef, schemas: Schemas, shard: Int, storeConf: StoreConfig,
            downsample: DownsampleConfig = DownsampleConfig.disabled): Unit = synchronized {
    val shards = datasets.getOrElseUpdate(ref, new NonBlockingHashMapLong[TimeSeriesShard](32, false))
    val quotaSource = quotaSources.getOrElseUpdate(ref,
      new ConfigQuotaSource(filodbConfig, schemas.part.options.shardKeyColumns.length))
    if (shards.containsKey(shard)) {
      throw ShardAlreadySetup(ref, shard)
    } else {
      val tsdb = new OnDemandPagingShard(ref, schemas, storeConf, quotaSource, shard, ingestionMemFactory, store,
        metastore, partEvictionPolicy, filodbConfig)
      shards.put(shard, tsdb)
    }
  }

  def scanTsCardinalities(ref: DatasetRef, shards: Seq[Int],
                          shardKeyPrefix: Seq[String], depth: Int): Seq[CardinalityRecord] = {
    datasets.get(ref).toSeq
      .flatMap { ts =>
        ts.values().asScala
          .filter(s => shards.isEmpty || shards.contains(s.shardNum))
          .flatMap(_.scanTsCardinalities(shardKeyPrefix, depth))
      }
  }

  /**
    * WARNING: use only for testing. Not performant
    */
  def refreshIndexForTesting(dataset: DatasetRef): Unit =
    datasets.get(dataset).foreach(_.values().asScala.foreach { s =>
      s.refreshPartKeyIndexBlocking()
      s.isReadyForQuery = true
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
    getShardE(dataset, shard).ingest(data)

  // NOTE: Each ingestion message is a SomeData which has a RecordContainer, which can hold hundreds or thousands
  // of records each.  For this reason the object allocation of a SomeData and RecordContainer is not that bad.
  // If it turns out the batch size is small, consider using object pooling.
  def startIngestion(dataset: DatasetRef,
                     shardNum: Int,
                     stream: Observable[SomeData],
                     flushSched: Scheduler,
                     cancelTask: Task[Unit]): CancelableFuture[Unit] = {
    val shard = getShardE(dataset, shardNum)
    shard.isReadyForQuery = true
    logger.info(s"Shard now ready for query dataset=$dataset shard=$shardNum")
    shard.shardStats.shardTotalRecoveryTime.update(System.currentTimeMillis() - shard.creationTime)
    shard.startIngestion(stream, cancelTask, flushSched)
  }

  def recoverIndex(dataset: DatasetRef, shard: Int): Future[Long] =
    getShardE(dataset, shard).recoverIndex()

  def createDataRecoveryObservable(dataset: DatasetRef,
                                   shardNum: Int,
                                   stream: Observable[SomeData],
                                   startOffset: Long,
                                   endOffset: Long,
                                   checkpoints: Map[Int, Long],
                                   reportingInterval: Long)
                                  (implicit timeout: FiniteDuration = 60.seconds): Observable[Long] = {
    val shard = getShardE(dataset, shardNum)
    shard.createDataRecoveryObservable(dataset, shardNum, stream, startOffset, endOffset,
      checkpoints, reportingInterval, timeout)
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

  def labelNames(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                 end: Long, start: Long): Seq[String] =
    getShard(dataset, shard).map(_.labelNames(filters, end, start)).getOrElse(Seq.empty)

  def labelValuesWithFilters(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                             labelNames: Seq[String], end: Long,
                             start: Long, querySession: QuerySession,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] =
    getShard(dataset, shard)
        .map(_.labelValuesWithFilters(filters, labelNames, end, start, querySession, limit)).getOrElse(Iterator.empty)

  def singleLabelValueWithFilters(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                             label: String, end: Long,
                             start: Long, querySession: QuerySession, limit: Int): Iterator[ZeroCopyUTF8String] =
    getShard(dataset, shard)
        .map(_.singleLabelValuesWithFilters(filters, label, end, start, querySession, limit)).getOrElse(Iterator.empty)

  def partKeysWithFilters(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                          fetchFirstLastSampleTimes: Boolean, end: Long, start: Long,
                          limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] =
    getShard(dataset, shard).map(_.partKeysWithFilters(filters, fetchFirstLastSampleTimes,
      end, start, limit)).getOrElse(Iterator.empty)

  def numPartitions(dataset: DatasetRef, shard: Int): Int =
    getShard(dataset, shard).map(_.numActivePartitions).getOrElse(-1)

  def readRawPartitions(ref: DatasetRef, maxChunkTime: Long,
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData] = Observable.empty

  def scanPartitions(ref: DatasetRef,
                     iter: PartLookupResult,
                     colIds: Seq[Types.ColumnId],
                     querySession: QuerySession): Observable[ReadablePartition] = {
    val shard = datasets(ref).get(iter.shard)

    if (shard == UnsafeUtils.ZeroPointer) {
      throw new IllegalArgumentException(s"Shard ${iter.shard} of dataset $ref is not assigned to " +
        s"this node. Was it was recently reassigned to another node? Prolonged occurrence indicates an issue.")
    }
    shard.scanPartitions(iter, colIds, querySession)
  }

  def acquireSharedLock(ref: DatasetRef,
                        shardNum: Int,
                        querySession: QuerySession): Unit = {
    val shard = datasets(ref).get(shardNum)
    val promQl = querySession.qContext.origQueryParams match {
      case p: PromQlQueryParams => p.promQl
      case _ => "unknown"
    }

    val queryTimeElapsed = System.currentTimeMillis() - querySession.qContext.submitTime
    val remainingTime = querySession.qContext.plannerParams.queryTimeoutMillis - queryTimeElapsed
    if (remainingTime <= 0) throw QueryTimeoutException(queryTimeElapsed, "beforeAcquireSharedLock")
    if (!shard.evictionLock.acquireSharedLock(remainingTime, querySession.qContext.queryId, promQl)) {
      val queryTimeElapsed2 = System.currentTimeMillis() - querySession.qContext.submitTime
      throw QueryTimeoutException(queryTimeElapsed2, "acquireSharedLockTimeout")
    }
    // we acquired lock, so add to session so it will be released
    querySession.setLock(shard.evictionLock)
  }

  def lookupPartitions(ref: DatasetRef,
                       partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod,
                       querySession: QuerySession): PartLookupResult = {
    val shard = datasets(ref).get(partMethod.shard)

    if (shard == UnsafeUtils.ZeroPointer) {
      throw new IllegalArgumentException(s"Shard ${partMethod.shard} of dataset $ref is not assigned to " +
        s"this node. Was it was recently reassigned to another node? Prolonged occurrence indicates an issue.")
    }
    shard.lookupPartitions(partMethod, chunkMethod, querySession)
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

  def schemas(ref: DatasetRef): Option[Schemas] =
    datasets.get(ref).map(_.values.asScala.head.schemas)

  def analyzeAndLogCorruptPtr(ref: DatasetRef, cve: CorruptVectorException): Unit =
    getShard(ref, cve.shard).get.analyzeAndLogCorruptPtr(cve)

  def reset(): Unit = {
    datasets.clear()
    quotaSources.clear()
    store.reset()
  }

  def truncate(dataset: DatasetRef, numShards: Int): Future[Response] = {
    datasets.get(dataset).foreach(_.values.asScala.foreach(_.reset()))
    store.truncate(dataset, numShards)
  }

  def removeShard(dataset: DatasetRef, shardNum: Int, shard: TimeSeriesShard): Boolean = {
    datasets.get(dataset).map(_.remove(shardNum, shard)).getOrElse(false)
  }

  // Release memory etc.
  def shutdown(): Unit = {
    quotaSources.clear()
    datasets.values.foreach(_.values.asScala.foreach(_.shutdown()))
    reset()
  }

}
