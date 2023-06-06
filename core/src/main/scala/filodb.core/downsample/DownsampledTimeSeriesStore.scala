package filodb.core.downsample

import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler, UncaughtExceptionReporter}
import monix.execution.schedulers.CanBlock
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong

import filodb.core.{DatasetRef, Response, Types}
import filodb.core.memstore._
import filodb.core.memstore.ratelimit.{CardinalityRecord, ConfigQuotaSource}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, QuerySession, ServiceUnavailableException}
import filodb.core.store._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}


/**
 * An implementation of TimeSeriesStore with data fetched from a column store
 *
 * @param store the store which houses the downsampled data
 * @param rawColStore the store which houses the original raw data from which downsampled data was derived
 * @param filodbConfig filodb configuration
 * @param ioPool the executor used to perform network IO
 */
class DownsampledTimeSeriesStore(val store: ColumnStore,
                                 rawColStore: ColumnStore,
                                 val filodbConfig: Config)
                                (implicit val ioPool: ExecutionContext)
extends TimeSeriesStore with StrictLogging {

  import collection.JavaConverters._

  private val datasets = new HashMap[DatasetRef, NonBlockingHashMapLong[DownsampledTimeSeriesShard]]
  private val quotaSources = new HashMap[DatasetRef, ConfigQuotaSource]

  val stats = new ChunkSourceStats

  override def isDownsampleStore: Boolean = true

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

  override def metastore: MetaStore = ??? // Not needed

  // TODO: Change the API to return Unit Or ShardAlreadySetup, instead of throwing.  Make idempotent.
  def setup(ref: DatasetRef, schemas: Schemas, shard: Int, storeConf: StoreConfig,
            downsampleConfig: DownsampleConfig = DownsampleConfig.disabled): Unit = synchronized {
    val shards = datasets.getOrElseUpdate(ref, new NonBlockingHashMapLong[DownsampledTimeSeriesShard](32, false))
    val quotaSource = quotaSources.getOrElseUpdate(ref,
      new ConfigQuotaSource(filodbConfig, schemas.part.options.shardKeyColumns.length))
    if (shards.containsKey(shard)) {
      throw ShardAlreadySetup(ref, shard)
    } else {
      val tsdb = new DownsampledTimeSeriesShard(ref, storeConf, schemas, store,
        rawColStore, shard, filodbConfig, downsampleConfig, quotaSource)
      shards.put(shard, tsdb)
    }
  }

  def refreshIndexForTesting(dataset: DatasetRef): Unit =
    datasets.get(dataset).foreach(_.values().asScala.foreach { s =>
      s.refreshPartKeyIndexBlocking()
    })

  def refreshIndexForTesting(dataset: DatasetRef, fromHour: Long, toHour: Long): Unit = {
    val sched = Scheduler.computation(
      name = "testScheduler",
      reporter = UncaughtExceptionReporter(
        logger.error("Uncaught Exception in Housekeeping Scheduler", _)
      )
    )
    val ds = datasets.get(dataset)
    ds.foreach(_.values().asScala.foreach { s =>
      s.indexRefresh(fromHour, toHour).runSyncUnsafe()(sched, CanBlock.permit)
    })
    sched.shutdown()
  }

  private[filodb] def getShard(dataset: DatasetRef, shard: Int): Option[DownsampledTimeSeriesShard] =
    datasets.get(dataset).flatMap { shards => Option(shards.get(shard)) }

  private[filodb] def getShardE(dataset: DatasetRef, shard: Int): DownsampledTimeSeriesShard = {
    datasets.get(dataset)
            .flatMap(shards => Option(shards.get(shard)))
            .getOrElse(throw new IllegalArgumentException(s"dataset=$dataset shard=$shard have not been set up"))
  }

  def recoverIndex(dataset: DatasetRef, shard: Int): Future[Long] =
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
                             start: Long, querySession: QuerySession,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
    = getShard(dataset, shard)
        .map(_.labelValuesWithFilters(filters, labelNames, end, start, querySession, limit)).getOrElse(Iterator.empty)

  def singleLabelValueWithFilters(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                                  label: String, end: Long,
                                  start: Long, querySession: QuerySession, limit: Int): Iterator[ZeroCopyUTF8String] =
    getShard(dataset, shard)
      .map(_.singleLabelValuesWithFilters(filters, label, end, start, querySession, limit)).getOrElse(Iterator.empty)

  def labelNames(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                 end: Long, start: Long): Seq[String] =
    getShard(dataset, shard).map(_.labelNames(filters, end, start)).getOrElse(Seq.empty)

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

  def acquireSharedLock(ref: DatasetRef,
                        shardNum: Int,
                        querySession: QuerySession): Unit = {
    // no op
  }

  def scanPartitions(ref: DatasetRef,
                     lookupRes: PartLookupResult,
                     colIds: Seq[Types.ColumnId],
                     querySession: QuerySession): Observable[ReadablePartition] = {
    val shard = datasets(ref).get(lookupRes.shard)

    if (shard == UnsafeUtils.ZeroPointer) {
      throw new IllegalArgumentException(s"Shard $shard of dataset $ref is not assigned to " +
        s"this node. Was it was recently reassigned to another node? Prolonged occurrence indicates an issue.")
    }
    shard.scanPartitions(lookupRes, colIds, querySession)
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
    datasets.valuesIterator.foreach { d =>
      d.values().asScala.foreach(_.shutdown())
    }
    reset()
  }

  override def ingest(dataset: DatasetRef, shard: Int,
                      data: SomeData): Unit = throw new UnsupportedOperationException()

  override def startIngestion(dataset: DatasetRef,
                              shard: Int,
                              stream: Observable[SomeData],
                              flushSched: Scheduler,
                              cancelTask: Task[Unit] = Task {}): CancelableFuture[Unit] =
    throw new UnsupportedOperationException()

  override def createDataRecoveryObservable(dataset: DatasetRef, shard: Int,
                                            stream: Observable[SomeData],
                                            startOffset: Long, endOffset: Long, checkpoints: Map[Int, Long],
                                            reportingInterval: Long)
                                           (implicit timeout: FiniteDuration)
                              : Observable[Long] = throw new UnsupportedOperationException()

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

  // TODO we need breakdown for downsample store too, but in a less memory intensive way
  override def scanTsCardinalities(ref: DatasetRef, shards: Seq[Int],
                                   shardKeyPrefix: Seq[String], depth: Int): scala.Seq[CardinalityRecord] = ???
}
