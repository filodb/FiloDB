package filodb.core.downsample

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import java.util
import java.util.concurrent.atomic.AtomicLong
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler, UncaughtExceptionReporter}
import monix.reactive.Observable
import org.apache.lucene.search.CollectionTerminatedException

import filodb.core.{DatasetRef, Types}
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore._
import filodb.core.memstore.ratelimit.{CardinalityManager, CardinalityRecord, QuotaSource}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, QueryContext, QuerySession}
import filodb.core.store._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.ZeroCopyUTF8String._

class DownsampledTimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

  val shardTotalRecoveryTime = Kamon.gauge("downsample-total-shard-recovery-time",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))
  val partitionsQueried = Kamon.counter("downsample-partitions-queried").withTags(TagSet.from(tags))
  val queryTimeRangeMins = Kamon.histogram("query-time-range-minutes").withTags(TagSet.from(tags))
  val queriesBySchema = Kamon.counter("leaf-queries-by-schema").withTags(TagSet.from(tags))
  val indexEntriesRefreshed = Kamon.counter("index-entries-refreshed").withTags(TagSet.from(tags))
  val indexEntriesPurged = Kamon.counter("index-entries-purged").withTags(TagSet.from(tags))
  val indexRefreshFailed = Kamon.counter("index-refresh-failed").withTags(TagSet.from(tags))
  val indexPurgeFailed = Kamon.counter("index-purge-failed").withTags(TagSet.from(tags))
  val indexEntries = Kamon.gauge("downsample-store-index-entries").withTags(TagSet.from(tags))
  val indexRamBytes = Kamon.gauge("downsample-store-index-ram-bytes").withTags(TagSet.from(tags))
  val singlePartCassFetchLatency = Kamon.histogram("single-partition-cassandra-latency",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))
  val purgeIndexEntriesLatency = Kamon.histogram("downsample-store-purge-index-entries-latency",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))

  val dataShapeKeyLength = Kamon.histogram("data-shape")
    .withTags(TagSet.from(tags)).withTag("dimension", "key-length")
  val dataShapeValueLength = Kamon.histogram("data-shape")
    .withTags(TagSet.from(tags)).withTag("dimension", "value-length")
  val dataShapeLabelCount = Kamon.histogram("data-shape")
    .withTags(TagSet.from(tags)).withTag("dimension", "label-count")
  val dataShapeMetricLength = Kamon.histogram("data-shape")
    .withTags(TagSet.from(tags)).withTag("dimension", "metric-length")
  val dataShapeTotalLength = Kamon.histogram("data-shape")
    .withTags(TagSet.from(tags)).withTag("dimension", "total-length")
  val dataShapeBucketCount = Kamon.histogram("data-shape")
    .withTags(TagSet.from(tags)).withTag("dimension", "bucket-count")
}

class DownsampledTimeSeriesShard(rawDatasetRef: DatasetRef,
                                 val rawStoreConfig: StoreConfig,
                                 val schemas: Schemas,
                                 store: ColumnStore, // downsample colStore
                                 rawColStore: ColumnStore,
                                 val shardNum: Int,
                                 filodbConfig: Config,
                                 downsampleConfig: DownsampleConfig,
                                 quotaSource: QuotaSource)
                                (implicit val ioPool: ExecutionContext) extends StrictLogging {

  val creationTime = System.currentTimeMillis()
  @volatile var isReadyForQuery = false

  private val downsampleTtls = downsampleConfig.ttls
  private val downsampledDatasetRefs = downsampleConfig.downsampleDatasetRefs(rawDatasetRef.dataset)

  private val indexDataset = downsampledDatasetRefs.last
  private val indexTtlMs = downsampleTtls.last.toMillis
  private val clusterType = filodbConfig.getString("cluster-type")
  private val deploymentPartitionName = filodbConfig.getString("deployment-partition-name")

  private val downsampleStoreConfig = StoreConfig(filodbConfig.getConfig("downsampler.downsample-store-config"))

  private val stats = new DownsampledTimeSeriesShardStats(rawDatasetRef, shardNum)

  private val indexMetadataStore : Option[IndexMetadataStore] = {
    downsampleConfig.indexMetastoreImplementation match {
      case IndexMetastoreImplementation.NoImp       => None
      case IndexMetastoreImplementation.File        =>
        Some(new FileSystemBasedIndexMetadataStore(downsampleConfig.indexLocation.get,
          FileSystemBasedIndexMetadataStore.expectedVersion(
            sys.env.get(FileSystemBasedIndexMetadataStore.expectedGenerationEnv)
          ),
          downsampleConfig.maxRefreshHours))
      case IndexMetastoreImplementation.Ephemeral   => Some(new EphemeralIndexMetadataStore())
    }
  }

  private val partKeyIndex = new PartKeyLuceneIndex(indexDataset, schemas.part, false,
    false, shardNum, indexTtlMs,
    downsampleConfig.indexLocation.map(new java.io.File(_)),
    indexMetadataStore
  )

  private val indexUpdatedHour = new AtomicLong(0)

  // used for initial index loading
  private val indexBootstrapper =
    new DownsampleIndexBootstrapper(store, schemas, stats, indexDataset, downsampleConfig)

  val houseKeepingSchedParallelism = Math.round(Runtime.getRuntime.availableProcessors() *
                                        downsampleConfig.housekeepingParallelismMultiplier).toInt.max(1)
  private val housekeepingSched = Scheduler.computation(
    parallelism = houseKeepingSchedParallelism,
    name = "housekeeping",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in Housekeeping Scheduler", _)))

  // used for periodic refresh of index, happens from raw tables
  private val indexRefresher =
    new DownsampleIndexBootstrapper(rawColStore, schemas, stats, rawDatasetRef, downsampleConfig)

  private var houseKeepingFuture: CancelableFuture[Unit] = _
  private var gaugeUpdateFuture: CancelableFuture[Unit] = _

  // CardinalityManager object helps with measuring and storing the cardinality count for the shard
  private val cardManager: CardinalityManager =
    new CardinalityManager(rawDatasetRef, shardNum, schemas.part.options.shardKeyColumns.length,
      partKeyIndex, schemas.part, filodbConfig, downsampleStoreConfig.meteringEnabled, quotaSource)

  // indexRefreshCount tracks the number of times the indexRefresh jobs have successfully ran
  private var indexRefreshCount = 0

  def indexNames(limit: Int): Seq[String] = Seq.empty

  def labelValues(labelName: String, topK: Int): Seq[TermInfo] = partKeyIndex.indexValues(labelName, topK)

  def labelValuesWithFilters(filters: Seq[ColumnFilter],
                             labelNames: Seq[String],
                             endTime: Long,
                             startTime: Long,
                             querySession: QuerySession,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    val metricShardKeys = schemas.part.options.shardKeyColumns
    val metricGroupBy = deploymentPartitionName +: clusterType +: shardKeyValuesFromFilter(metricShardKeys, filters)
    LabelValueResultIterator(filters, startTime, endTime, labelNames, querySession, metricGroupBy, limit)
  }

  def singleLabelValuesWithFilters(filters: Seq[ColumnFilter],
                                   label: String,
                                   endTime: Long,
                                   startTime: Long,
                                   querySession: QuerySession,
                                   limit: Int): Iterator[ZeroCopyUTF8String] = {
    val metricShardKeys = schemas.part.options.shardKeyColumns
    val metricGroupBy = deploymentPartitionName +: clusterType +: shardKeyValuesFromFilter(metricShardKeys, filters)
    SingleLabelValuesResultIterator(filters, startTime, endTime, label, querySession, metricGroupBy, limit)
  }

  def labelNames(filter: Seq[ColumnFilter],
                 endTime: Long,
                 startTime: Long): Seq[String] =
    labelNamesFromPartKeys(partKeyIndex.singlePartKeyFromFilters(filter, startTime, endTime))

  def partKeysWithFilters(filter: Seq[ColumnFilter],
                          fetchFirstLastSampleTimes: Boolean,
                          endTime: Long,
                          startTime: Long,
                          limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    partKeyIndex.partKeyRecordsFromFilters(filter, startTime, endTime).iterator.take(limit).map { pk =>
      val partKey = PartKeyWithTimes(pk.partKey, UnsafeUtils.arayOffset, pk.startTime, pk.endTime)
      schemas.part.binSchema.toStringPairs(partKey.base, partKey.offset).map(pair => {
        pair._1.utf8 -> pair._2.utf8
      }).toMap ++
        Map("_type_".utf8 -> Schemas.global.schemaName(RecordSchema.schemaID(partKey.base, partKey.offset)).utf8)
    }
  }

  private def hour(millis: Long = System.currentTimeMillis()) = millis / 1000 / 60 / 60

  def recoverIndex(): Future[Long] = {
    if (downsampleConfig.enablePersistentIndexing) {
      partKeyIndex.getCurrentIndexState() match {
        case (IndexState.Empty, _)   |
             (IndexState.TriggerRebuild, _)                    =>
          logger.info("Found index state empty/rebuild triggered, bootstrapping downsample index")
          recoverIndexInternal(None)
        case (IndexState.Synced, checkpointMillis)             =>
          logger.warn(s"Found index state synced, bootstrapping downsample index from time(ms) $checkpointMillis")
          recoverIndexInternal(checkpointMillis)
        case _                                                 =>
          logger.info(s"Nothing to recover the index for dataset=$indexDataset shard=$shardNum" +
                      s" starting index refresh thread")
          indexRefresh().runToFuture(housekeepingSched)
      }
    } else {
      // Index persistence is not enabled, this will simply follow the path for existing index recovery
      recoverIndexInternal(None)
    }
  }

  private def recoverIndexInternal(checkpointMillis: Option[Long]): Future[Long] = {
    // By passing -1 for partId, numeric partId will not be persisted in the index
    // Since we are recovering, always start from last synced time and update till current timestamp.
    val endHour = hour()

    (checkpointMillis match {
      case Some(tsMillis)   => // We know we have to refresh only since this hour, it is possible we do not
                                // find any data for this refresh as the index is already updated
                                val startHour = hour(tsMillis) - 1
                                // do not notify listener as the map operation will be updating the state
                                indexRefresh(endHour, startHour, periodicRefresh = false)
      case None             => // No checkpoint time found, start refresh from scratch
                                val parallelism = Math.round(downsampleConfig.indexRebuildParallelismMultiplier *
                                                    Runtime.getRuntime.availableProcessors()).toInt.max(1)
                                logger.info("Rebuilding index with parallelism {}", parallelism)
                                indexBootstrapper
                                  .bootstrapIndexDownsample(
                                    partKeyIndex, shardNum, indexDataset, indexTtlMs, parallelism)
    }).map { count =>
        logger.info(s"Bootstrapped index for dataset=$indexDataset shard=$shardNum with $count records")
        indexUpdatedHour.set(endHour)
        partKeyIndex.notifyLifecycleListener(IndexState.Synced, endHour * 3600 * 1000L)
        stats.shardTotalRecoveryTime.update(System.currentTimeMillis() - creationTime)
        cardManager.triggerCardinalityCount(indexRefreshCount)
        startHousekeepingTask()
        startStatsUpdateTask()
        logger.info(s"Shard now ready for query dataset=$indexDataset shard=$shardNum")
        isReadyForQuery = true
        count
      }.runToFuture(housekeepingSched)
  }

  private def startHousekeepingTask(): Unit = {
    // Run index refresh at same frequency of raw dataset's flush interval.
    // This is important because each partition's start/end time can be updated only once
    // in cassandra per flush interval. Less frequent update can result in multiple events
    // per partKey, and order (which we have  not persisted) would become important.
    // Also, addition of keys to index can be parallelized using mapAsync below only if
    // we are sure that in one raw dataset flush period, we wont get two updated part key
    // records with same part key. This is true since we update part keys only once per flush interval in raw dataset.
    logger.info(s"Starting housekeeping for downsample cluster of dataset=$rawDatasetRef shard=$shardNum " +
      s"every ${rawStoreConfig.flushInterval}")
    houseKeepingFuture = Observable.intervalWithFixedDelay(rawStoreConfig.flushInterval,
      rawStoreConfig.flushInterval).mapEval { _ =>
      purgeExpiredIndexEntries()
      indexRefresh()
    }.map { _ =>
      partKeyIndex.refreshReadersBlocking()
      indexRefreshCount += 1
      cardManager.triggerCardinalityCount(indexRefreshCount)
    }.onErrorRestartUnlimited.completedL.runToFuture(housekeepingSched)
  }

  private def purgeExpiredIndexEntries(): Unit = {
    val start = System.currentTimeMillis()
    try {
      val numPartsPurged = partKeyIndex.removePartitionsEndedBefore(start - downsampleTtls.last.toMillis)
      logger.info(s"Purged $numPartsPurged entries from downsample index of dataset=$rawDatasetRef shard=$shardNum")
      stats.indexEntriesPurged.increment(numPartsPurged)
    } catch { case e: Exception =>
      logger.error(s"Error occurred when purging index entries dataset=$rawDatasetRef shard=$shardNum", e)
      stats.indexPurgeFailed.increment()
    } finally {
      stats.purgeIndexEntriesLatency.record(System.currentTimeMillis() - start)
    }
  }

  def indexRefresh(): Task[Long] = {
    // Update keys until hour()-2 hours ago. hour()-1 hours ago can cause missed records if
    // refresh was triggered exactly at end of the hour. All partKeys for the hour would need to be flushed
    // before refresh happens because we will not revist the hour again.
    if (downsampleConfig.enablePersistentIndexing) {
      indexUpdatedHour.set(partKeyIndex.getCurrentIndexState() match {
        case (IndexState.Synced, Some(ts))           => ts / 3600 / 1000
        case _                                       => indexUpdatedHour.get()
      })
    }

    val toHour = hour() - 2
    val fromHour = indexUpdatedHour.get() + 1
    indexRefresh(toHour, fromHour)
  }

  /**
   *
   * @param toHour The end hour of the refresh
   * @param fromHour The start hour of the refresh
   * @param periodicRefresh boolean flag indicating whether this is a periodic refresh or the one called as part of
   *                        index bootstrap.
   * @return
   */
  def indexRefresh(toHour: Long, fromHour: Long, periodicRefresh: Boolean = true): Task[Long] = {
    indexRefresher.refreshWithDownsamplePartKeys(
      partKeyIndex, shardNum, rawDatasetRef, fromHour, toHour, schemas)
      .map { count =>
        stats.indexEntriesRefreshed.increment(count)
        logger.info(s"Refreshed downsample index with new records numRecords=$count " +
          s"dataset=$rawDatasetRef shard=$shardNum fromHour=$fromHour toHour=$toHour")
        if(periodicRefresh) {
          indexUpdatedHour.set(toHour)
          partKeyIndex.notifyLifecycleListener(IndexState.Synced, toHour * 3600 * 1000L)
        }
        count
      }
      .onErrorHandle { e =>
        stats.indexRefreshFailed.increment()
        logger.error(s"Error occurred when refreshing downsample index " +
          s"dataset=$rawDatasetRef shard=$shardNum fromHour=$fromHour toHour=$toHour", e)
        0L
      }
  }

  private def startStatsUpdateTask(): Unit = {
    logger.info(s"Starting Stats Update task from raw dataset=$rawDatasetRef shard=$shardNum every 1 minute")
    gaugeUpdateFuture = Observable.intervalWithFixedDelay(1.minute).map { _ =>
      updateGauges()
    }.onErrorRestartUnlimited.completedL.runToFuture(housekeepingSched)
  }

  private def updateGauges(): Unit = {
    stats.indexEntries.update(partKeyIndex.indexNumEntries)
    stats.indexRamBytes.update(partKeyIndex.indexRamBytes)
  }

  def refreshPartKeyIndexBlocking(): Unit = {}

  def lookupPartitions(partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod,
                       querySession: QuerySession): PartLookupResult = {
    partMethod match {
      case SinglePartitionScan(partition, _) => throw new UnsupportedOperationException
      case MultiPartitionScan(partKeys, _) => throw new UnsupportedOperationException
      case FilteredPartitionScan(split, filters) =>

        if (filters.nonEmpty) {
          // This API loads all part keys into heap and can potentially be large size for
          // high cardinality queries, but it is needed to do multiple
          // iterations over the part keys. First iteration is for data size estimation.
          // Second iteration is for query result evaluation. Loading everything to heap
          // is expensive, but we do it to handle data sizing for metrics that have
          // continuous churn. See capDataScannedPerShardCheck method.
          val recs = partKeyIndex.partKeyRecordsFromFilters(filters, chunkMethod.startTime, chunkMethod.endTime)
          val _schema = recs.headOption.map { pkRec =>
            RecordSchema.schemaID(pkRec.partKey, UnsafeUtils.arayOffset)
          }
          _schema.foreach { s =>
            stats.queriesBySchema.withTag("schema", schemas(s).name).increment()
          }
          stats.queryTimeRangeMins.record((chunkMethod.endTime - chunkMethod.startTime) / 60000 )
          val metricShardKeys = schemas.part.options.shardKeyColumns
          val metricGroupBy = deploymentPartitionName +: clusterType +: metricShardKeys.map { col =>
            filters.collectFirst {
              case ColumnFilter(c, Filter.Equals(filtVal: String)) if c == col => filtVal
            }.getOrElse("unknown")
          }.toList
          querySession.queryStats.getTimeSeriesScannedCounter(metricGroupBy).addAndGet(recs.length)
          val chunksReadCounter = querySession.queryStats.getDataBytesScannedCounter(metricGroupBy)

          PartLookupResult(shardNum, chunkMethod, debox.Buffer.empty,
            _schema, debox.Map.empty, debox.Buffer.empty, recs, chunksReadCounter)
        } else {
          throw new UnsupportedOperationException("Cannot have empty filters")
        }
    }
  }

  def shutdown(): Unit = {
    try {
      partKeyIndex.closeIndex()
      houseKeepingFuture.cancel()
      gaugeUpdateFuture.cancel()
      cardManager.close()
    } catch { case e: Exception =>
      logger.error("Exception when shutting down downsample shard", e)
    }
  }

  def scanTsCardinalities(shardKeyPrefix: Seq[String], depth: Int): Seq[CardinalityRecord] = {
    cardManager.scan(shardKeyPrefix, depth)
  }

  def scanPartitions(lookup: PartLookupResult,
                     colIds: Seq[Types.ColumnId],
                     querySession: QuerySession): Observable[ReadablePartition] = {
    // Step 1: Choose the downsample level depending on the range requested
    val (resolutionMs, downsampledDataset) = chooseDownsampleResolution(lookup.chunkMethod)
    logger.debug(s"Chose resolution $downsampledDataset for chunk method ${lookup.chunkMethod}")
    capDataScannedPerShardCheck(lookup, colIds, resolutionMs, querySession.qContext)
    // Step 2: Query Cassandra table for that downsample level using downsampleColStore
    // Create a ReadablePartition objects that contain the time series data. This can be either a
    // PagedReadablePartitionOnHeap or PagedReadablePartitionOffHeap. This will be garbage collected/freed
    // when query is complete.
    Observable.fromIterable(lookup.pkRecords)
      .mapParallelUnordered(downsampleStoreConfig.demandPagingParallelism) { partRec =>
        val startExecute = System.currentTimeMillis()
        // TODO test multi-partition scan if latencies are high
        // IMPORTANT: The Raw partition reads need to honor the start time in the index. Suppose, the shards for the
        // time series is migrated, the time series will show up in two shards but not in both at any given point in
        // time. However if the start and end date range cover the point in time when the shard migration occurred, and
        // if both shards query for the same user provided time range, we will have the same data returned twice,
        // instead if the shards return the data for time duration they owned the data, we will not have duplicates
        // Read raw partition adjusts the start time and takes it back by downsampleStoreConfig.maxChunkTime.toMillis.
        // Consider the following scenario for downsample chunks
        // T..........T + 6..........T + 12.........T + 18.........T + 24
        //                   ^-------------------^
        //                 start                end
        // We notice (ds freq is 6 hrs), the start time is taken back by 6 hrs to ensure that the chunk  at T + 6
        // is included in the result as the CQL in for chunk filter in cassandra will be chunkId => ? and chunkId <= ?
        // This query will at the maximum get 12 hrs of additional data and thus the duplicate results will still occur
        // but the impact is now reduced to a maximum of 12 (2*6) hours (whatever the downsampling frequency is) of data
        // We believe this is a good enough fix and aiming for 0 duplicate results will require more changes possibly
        // introducing regression to the stable codebase. However, if at later point of time no duplicates are tolerated
        // we will have to revisit the logic and fix accordingly
        store.readRawPartitions(downsampledDataset,
          downsampleStoreConfig.maxChunkTime.toMillis,
          SinglePartitionScan(partRec.partKey, shardNum),
          TimeRangeChunkScan(
            partRec.startTime.max(lookup.chunkMethod.startTime),
            partRec.endTime.min(lookup.chunkMethod.endTime)))
          .map { pd =>
            val part = makePagedPartition(pd, lookup.firstSchemaId.get, resolutionMs, colIds)
            stats.partitionsQueried.increment()
            stats.singlePartCassFetchLatency.record(Math.max(0, System.currentTimeMillis - startExecute))
            part
          }
          .defaultIfEmpty(makePagedPartition(RawPartData(partRec.partKey, Seq.empty),
            lookup.firstSchemaId.get, resolutionMs, colIds))
          .headL
      }
  }

  private def capDataScannedPerShardCheck(lookup: PartLookupResult,
                                          colIds: Seq[Types.ColumnId],
                                          resolution: Long,
                                          qContext: QueryContext) = {
    lookup.firstSchemaId.foreach { schId =>
      ensureQueriedDataSizeWithinLimit(schId, colIds, lookup.pkRecords,
        downsampleStoreConfig.flushInterval.toMillis,
        resolution, lookup.chunkMethod, qContext)
    }
  }

  /**
   * This method estimates data size with much better accuracy than ensureQueriedDataSizeWithinLimitApprox
   * since it accepts the start/end times of each matching part key. It is able to handle estimation with
   * time series churn much better
   */
  def ensureQueriedDataSizeWithinLimit(schemaId: Int,
                                       colIds: Seq[Types.ColumnId],
                                       pkRecs: Seq[PartKeyLuceneIndexRecord],
                                       chunkDurationMillis: Long,
                                       resolutionMs: Long,
                                       chunkMethod: ChunkScanMethod,
                                       qContext: QueryContext): Unit = {
    val estDataSize = schemas.estimateByteScan(
      schemaId,
      colIds,
      pkRecs,
      chunkDurationMillis,
      resolutionMs,
      chunkMethod)
    val quRange = chunkMethod.endTime - chunkMethod.startTime + 1
    val enforcedLimits = qContext.plannerParams.enforcedLimits
    val timeSeries = pkRecs.length
    val warnLimits = qContext.plannerParams.warnLimits
    // TODO the below does not return a particular kind of error code. Most likely this would translate to
    // an internal error of FiloDB which is not appropriate for the case
    require(
      estDataSize < enforcedLimits.timeSeriesSamplesScannedBytes,
      s"With match of ${pkRecs.length} time series, estimate of $estDataSize bytes exceeds limit of " +
        s"${enforcedLimits.timeSeriesSamplesScannedBytes} bytes queried " +
        s"per shard for ${schemas.apply(schemaId).name} schema. " +
        s"Try one or more of these: " +
        s"(a) narrow your query filters to reduce to fewer than the current ${pkRecs.length} matches " +
        s"(b) reduce query time range, currently at ${quRange / 1000 / 60} minutes")
    require(timeSeries < enforcedLimits.timeSeriesScanned,
      s"Query matched ${timeSeries} time series, which exceeds a max enforced limit of " +
        s"${enforcedLimits.timeSeriesScanned} time series allowed to be queried per shard. " +
        s"Try one or more of these: " +
        s"(a) narrow your query filters to reduce to fewer than the current ${timeSeries} matches " +
        s"(b) reduce query time range, currently at ${quRange / 1000 / 60} minutes")
    if (timeSeries > warnLimits.timeSeriesScanned) {
      val msg =
        s"Query matched $timeSeries time series, which exceeds a max warn limit of " +
          s"${warnLimits.timeSeriesScanned} time series allowed to be queried per shard. "
      logger.info(qContext.getQueryLogLine(msg))
    }
    if (estDataSize > warnLimits.timeSeriesSamplesScannedBytes) {
      val msg =
        s"With match of $timeSeries time series, estimate of $estDataSize bytes exceeds " +
          s"limit of ${warnLimits.timeSeriesSamplesScannedBytes} bytes queried per shard " +
          s"for ${schemas.apply(schemaId).name} schema. "
      logger.info(qContext.getQueryLogLine(msg))
    }
  }

  private def chooseDownsampleResolution(chunkScanMethod: ChunkScanMethod): (Int, DatasetRef) = {
    chunkScanMethod match {
      case AllChunkScan =>
        // pick last since it is the highest resolution
        downsampleConfig.resolutions.last.toMillis.toInt -> downsampledDatasetRefs.last
      case TimeRangeChunkScan(startTime, _) =>
        var ttlIndex = downsampleTtls.indexWhere(t => startTime > System.currentTimeMillis() - t.toMillis)
        // -1 return value means query startTime is before the earliest retention. Just pick the highest resolution
        if (ttlIndex == -1) ttlIndex = downsampleTtls.size - 1
        downsampleConfig.resolutions(ttlIndex).toMillis.toInt -> downsampledDatasetRefs(ttlIndex)
      case _ => ???
    }
  }

  private def makePagedPartition(part: RawPartData, firstSchemaId: Int,
                                 minResolutionMs: Int,
                                 colIds: Seq[Types.ColumnId]): ReadablePartition = {
    val schemaId = RecordSchema.schemaID(part.partitionKey, UnsafeUtils.arayOffset)
    if (schemaId != firstSchemaId) {
      throw SchemaMismatch(schemas.schemaName(firstSchemaId), schemas.schemaName(schemaId), getClass.getSimpleName)
    }
    // FIXME It'd be nice to pass in the correct partId here instead of -1
    new PagedReadablePartition(schemas(schemaId), shardNum, -1, part, minResolutionMs, colIds)
  }


  private def labelNamesFromPartKeys(partKeyOption: Option[Array[Byte]]): Seq[String] = partKeyOption match {
      // Is to set needed as label name for a given part key is always unique?
      case Some(partKey)    => schemas.part.binSchema.colNames(partKey, UnsafeUtils.arayOffset).toSet.toSeq
      case None             => Seq.empty
    }

  private def shardKeyValuesFromFilter(shardKeyColumns: Seq[String], filters: Seq[ColumnFilter]): Seq[String] = {
    shardKeyColumns.map { col =>
      filters.collectFirst {
        case ColumnFilter(c, Filter.Equals(filtVal: String)) if c == col => filtVal
      }.getOrElse("unknown")
    }.toList
  }

  /**
   * Iterate through the matching partKeys one at a time matching the given filters and extract the required
   * label values. We map label/values eagerly, to dedup the results. However, not all partKeys are loaded in memory
   * at once and iterated one by one
   */
  case class LabelValueResultIterator(filters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                                      labelNames: Seq[String], querySession: QuerySession,
                                      statsGroup: Seq[String], limit: Int)
    extends Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] {
    private lazy val rows = labelValues
    override def size: Int = rows.size

    def labelValues: Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {

      val rows = new mutable.HashSet[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]()
      var partLoopIndex = 0
      val matched = partKeyIndex.foreachPartKeyMatchingFilter(filters, startTime, endTime,
        nextPart => {
          if (rows.size < limit) {
            // FIXME This is non-performant and temporary fix for fetching label values based on filter criteria.
            // Other strategies needs to be evaluated for making this performant - create facets for predefined fields
            // or have a centralized service/store for serving metadata
            // TODO: Use the BytesRef directly instead of copying to an array

            val pk = util.Arrays.copyOfRange(nextPart.bytes, nextPart.offset, nextPart.offset + nextPart.length)
            val currVal = schemas.part.binSchema.colValues(pk, UnsafeUtils.arayOffset, labelNames)
              .zipWithIndex.filter(_._1 != null)
              .map{case(value, ind) => labelNames(ind).utf8 -> value.utf8}.toMap

            if (currVal.nonEmpty) rows.add(currVal)
            partLoopIndex += 1
          } else throw new CollectionTerminatedException
        }
      )
      querySession.queryStats.getTimeSeriesScannedCounter(statsGroup).addAndGet(matched)
      rows.toIterator
    }

    override def hasNext: Boolean = rows.hasNext

    override def next(): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = rows.next
  }

  case class SingleLabelValuesResultIterator(filters: Seq[ColumnFilter], startTime: Long, endTime: Long, label: String,
                                             querySession: QuerySession, statsGroup: Seq[String], limit: Int)
    extends Iterator[ZeroCopyUTF8String] {
    private val rows = labels

    def labels: Iterator[ZeroCopyUTF8String] = {
      // Ideally when we use Iterator, the memory usage should be constant and should execute lazily, but with the
      // original and current approach, we use linear memory. Previously we used linear space to pass a list of partIds
      // and linear space to store the rows for the result. Note that partKeys are not all in memory at the same time
      // and only one was referenced in heap inside the loop. Current approach to eliminate the use if partIds in
      // DownsampleTimeSeriesShard eliminates the use of partIds, keeps just one partKey referenced in heap and takes
      // linear space to store rows. Ideally we should use search and searchAfter from IndexSearcher to ensure
      // we dont keep all the processed rows in memory and pull from partKey from index,process it to extract the
      // required field and return the result but the challenge would be de-duping the results
      val rows = new mutable.HashSet[ZeroCopyUTF8String]()
      val colIndex = schemas.part.binSchema.colNames.indexOf(label)
      var partLoopIndex = 0
      val matched = partKeyIndex.foreachPartKeyMatchingFilter(filters, startTime, endTime,
        nextPart => {
          if (rows.size < limit) {
            val pk = util.Arrays.copyOfRange(nextPart.bytes, nextPart.offset, nextPart.offset + nextPart.length)
            if (colIndex > -1)
              rows.add(
                schemas.part.binSchema.asZCUTF8Str(pk, UnsafeUtils.arayOffset, colIndex)
              )
            else
              schemas.part.binSchema.singleColValues(pk, UnsafeUtils.arayOffset, label, rows)

            partLoopIndex += 1
          } else throw new CollectionTerminatedException
        }
      )
      querySession.queryStats.getTimeSeriesScannedCounter(statsGroup).addAndGet(matched)
      rows.toIterator
    }

    def hasNext: Boolean = rows.hasNext

    def next(): ZeroCopyUTF8String = rows.next
  }

  /**
   * Retrieve partKey for a given PartId by looking up index
   */
  private def partKeyFromPartId(partId: Int): Array[Byte] = {
    val partKeyBytes = partKeyIndex.partKeyFromPartId(partId)
    if (partKeyBytes.isDefined)
    // make a copy because BytesRef from lucene can have additional length bytes in its array
    // TODO small optimization for some other day
      util.Arrays.copyOfRange(partKeyBytes.get.bytes, partKeyBytes.get.offset,
        partKeyBytes.get.offset + partKeyBytes.get.length)
    else throw new IllegalStateException(s"Could not find partKey or partId $partId. This is not a expected behavior.")
  }

  def cleanup(): Unit = {
    Option(houseKeepingFuture).foreach(_.cancel())
    Option(gaugeUpdateFuture).foreach(_.cancel())
    cardManager.close()
  }

  override protected def finalize(): Unit = {
    cleanup()
  }

}
