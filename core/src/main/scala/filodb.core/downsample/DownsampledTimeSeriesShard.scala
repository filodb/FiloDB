package filodb.core.downsample

import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.tag.TagSet
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler, UncaughtExceptionReporter}
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

class DownsampledTimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

  val partitionsQueried = Kamon.counter("downsample-partitions-queried").withTags(TagSet.from(tags))
  val chunksQueried = Kamon.counter("downsample-chunks-queried").withTags(TagSet.from(tags))
  val queryTimeRangeMins = Kamon.histogram("query-time-range-minutes").withTags(TagSet.from(tags))
  val indexEntriesRefreshed = Kamon.counter("index-entries-refreshed").withTags(TagSet.from(tags))
  val indexEntriesPurged = Kamon.counter("index-entries-purged").withTags(TagSet.from(tags))
  val indexRefreshFailed = Kamon.counter("index-refresh-failed").withTags(TagSet.from(tags))
  val indexPurgeFailed = Kamon.counter("index-purge-failed").withTags(TagSet.from(tags))
  val indexEntries = Kamon.gauge("downsample-store-index-entries").withTags(TagSet.from(tags))
  val indexRamBytes = Kamon.gauge("downsample-store-index-ram-bytes").withTags(TagSet.from(tags))
}

class DownsampledTimeSeriesShard(rawDatasetRef: DatasetRef,
                                 val storeConfig: StoreConfig,
                                 val schemas: Schemas,
                                 store: ColumnStore, // downsample colStore
                                 rawColStore: ColumnStore,
                                 shardNum: Int,
                                 filodbConfig: Config,
                                 downsampleConfig: DownsampleConfig)
                                (implicit val ioPool: ExecutionContext) extends StrictLogging {

  private val downsampleTtls = downsampleConfig.ttls
  private val downsampledDatasetRefs = downsampleConfig.downsampleDatasetRefs(rawDatasetRef.dataset)

  private val indexDataset = downsampledDatasetRefs.last
  private val indexTtl = downsampleTtls.last

  // since all partitions are paged from store, this would be much lower than what is configured for raw data
  private val maxQueryMatches = storeConfig.maxQueryMatches * 0.5 // TODO configure if really necessary

  private val nextPartitionID = new AtomicInteger(0)

  private val stats = new DownsampledTimeSeriesShardStats(rawDatasetRef, shardNum)

  private val partKeyIndex = new PartKeyLuceneIndex(indexDataset, schemas.part, shardNum, indexTtl)

  private val indexUpdatedHour = new AtomicLong(0)

  private val indexBootstrapper = new IndexBootstrapper(store) // used for initial index loading

  private val housekeepingSched = Scheduler.computation(
    name = "housekeeping",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in Housekeeping Scheduler", _)))

  // used for periodic refresh of index, happens from raw tables
  private val indexRefresher = new IndexBootstrapper(rawColStore)

  private var houseKeepingFuture: CancelableFuture[Unit] = _
  private var gaugeUpdateFuture: CancelableFuture[Unit] = _

  def indexNames(limit: Int): Seq[String] = Seq.empty

  def labelValues(labelName: String, topK: Int): Seq[TermInfo] = partKeyIndex.indexValues(labelName, topK)

  def labelValuesWithFilters(filter: Seq[ColumnFilter],
                             labelNames: Seq[String],
                             endTime: Long,
                             startTime: Long,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    LabelValueResultIterator(partKeyIndex.partIdsFromFilters(filter, startTime, endTime), labelNames, limit)
  }

  def partKeysWithFilters(filter: Seq[ColumnFilter],
                          endTime: Long,
                          startTime: Long,
                          limit: Int): Iterator[PartKey] = {
    partKeyIndex.partIdsFromFilters(filter, startTime, endTime).iterator().take(limit).map { pId =>
      PartKey(partKeyFromPartId(pId), UnsafeUtils.arayOffset)
    }
  }

  private def hour(millis: Long = System.currentTimeMillis()) = millis / 1000 / 60 / 60

  def recoverIndex(): Future[Unit] = {
    indexUpdatedHour.set(hour() - 1)
    indexBootstrapper.bootstrapIndex(partKeyIndex, shardNum, indexDataset){ _ => createPartitionID() }
      .map { count =>
        logger.info(s"Bootstrapped index for dataset=$indexDataset shard=$shardNum with $count records")
      }.map { _ =>
        startHousekeepingTask()
        startStatsUpdateTask()
      }.runAsync(housekeepingSched)
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
                s"every ${storeConfig.flushInterval}")
    houseKeepingFuture = Observable.intervalWithFixedDelay(storeConfig.flushInterval,
                                                          storeConfig.flushInterval).mapAsync { _ =>
      purgeExpiredIndexEntries()
      indexRefresh()
    }.map { _ =>
      partKeyIndex.refreshReadersBlocking()
    }.onErrorRestartUnlimited.completedL.runAsync(housekeepingSched)
  }

  private def purgeExpiredIndexEntries(): Unit = {
    val tracer = Kamon.spanBuilder("downsample-store-purge-index-entries-latency")
      .asChildOf(Kamon.currentSpan())
      .tag("dataset", rawDatasetRef.toString)
      .tag("shard", shardNum).start()
    try {
      val partsToPurge = partKeyIndex.partIdsEndedBefore(System.currentTimeMillis() - downsampleTtls.last.toMillis)
      partKeyIndex.removePartKeys(partsToPurge)
      logger.info(s"Purged ${partsToPurge.length} entries from downsample " +
        s"index of dataset=$rawDatasetRef shard=$shardNum")
      stats.indexEntriesPurged.increment(partsToPurge.length)
    } catch { case e: Exception =>
      logger.error(s"Error occurred when purging index entries dataset=$rawDatasetRef shard=$shardNum", e)
      stats.indexPurgeFailed.increment()
    } finally {
      tracer.finish()
    }
  }

  private def indexRefresh(): Task[Unit] = {
    // Update keys until hour()-2 hours ago. hour()-1 hours ago can cause missed records if
    // refresh was triggered exactly at end of the hour. All partKeys for the hour would need to be flushed
    // before refresh happens because we will not revist the hour again.
    val toHour = hour() - 2
    val fromHour = indexUpdatedHour.get() + 1
    indexRefresher.refreshIndex(partKeyIndex, shardNum, rawDatasetRef, fromHour, toHour)(lookupOrCreatePartId)
      .map { count =>
        indexUpdatedHour.set(toHour)
        stats.indexEntriesRefreshed.increment(count)
        logger.info(s"Refreshed downsample index with new records numRecords=$count " +
          s"dataset=$rawDatasetRef shard=$shardNum fromHour=$fromHour toHour=$toHour")
      }
      .onErrorHandle { e =>
        stats.indexRefreshFailed.increment()
        logger.error(s"Error occurred when refreshing downsample index " +
          s"dataset=$rawDatasetRef shard=$shardNum fromHour=$fromHour toHour=$toHour", e)
      }
  }

  private def startStatsUpdateTask(): Unit = {
    logger.info(s"Starting Index Refresh task from raw dataset=$rawDatasetRef shard=$shardNum " +
      s"every ${storeConfig.flushInterval}")
    gaugeUpdateFuture = Observable.intervalWithFixedDelay(1.minute).map { _ =>
      updateGauges()
    }.onErrorRestartUnlimited.completedL.runAsync(housekeepingSched)
  }

  private def updateGauges(): Unit = {
    stats.indexEntries.update(partKeyIndex.indexNumEntries)
    stats.indexRamBytes.update(partKeyIndex.indexRamBytes)
  }

  private def lookupOrCreatePartId(pk: PartKeyRecord): Int = {
    partKeyIndex.partIdFromPartKeySlow(pk.partKey, UnsafeUtils.arayOffset).getOrElse(createPartitionID())
  }

  /**
    * Returns a new non-negative partition ID which isn't used by any existing parition. A negative
    * partition ID wouldn't work with bitmaps.
    */
  private def createPartitionID(): Int = {
    val next = nextPartitionID.incrementAndGet()
    if (next == 0) {
      throw new IllegalStateException("Too many partitions. Reached int capacity")
    }
    next
  }

  def refreshPartKeyIndexBlocking(): Unit = {}

  def lookupPartitions(partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod): PartLookupResult = {
    partMethod match {
      case SinglePartitionScan(partition, _) => throw new UnsupportedOperationException
      case MultiPartitionScan(partKeys, _) => throw new UnsupportedOperationException
      case FilteredPartitionScan(split, filters) =>

        if (filters.nonEmpty) {
          val res = partKeyIndex.partIdsFromFilters(filters,
            chunkMethod.startTime,
            chunkMethod.endTime)
          val firstPartId = if (res.isEmpty) None else Some(res(0))

          val _schema = firstPartId.map(schemaIDFromPartID)
          stats.queryTimeRangeMins.record((chunkMethod.endTime - chunkMethod.startTime) / 60000 )

          // send index result in the partsInMemory field of lookup
          PartLookupResult(shardNum, chunkMethod, res,
            _schema, debox.Map.empty[Int, Long], debox.Buffer.empty)
        } else {
          throw new UnsupportedOperationException("Cannot have empty filters")
        }
    }
  }

  def scanPartitions(lookup: PartLookupResult): Observable[ReadablePartition] = {

    // Step 1: Choose the downsample level depending on the range requested
    val downsampledDataset = chooseDownsampleResolution(lookup.chunkMethod)
    logger.debug(s"Chose resolution $downsampledDataset for chunk method ${lookup.chunkMethod}")
    // Step 2: Query Cassandra table for that downsample level using downsampleColStore
    // Create a ReadablePartition objects that contain the time series data. This can be either a
    // PagedReadablePartitionOnHeap or PagedReadablePartitionOffHeap. This will be garbage collected/freed
    // when query is complete.

    if (lookup.partsInMemory.length > maxQueryMatches)
      throw new IllegalArgumentException(s"Seeing ${lookup.partsInMemory.length} matching time series per shard. Try " +
        s"to narrow your query by adding more filters so there is less than $maxQueryMatches matches " +
        s"or request for increasing number of shards this metric lives in")

    val partKeys = lookup.partsInMemory.iterator().map(partKeyFromPartId)
    Observable.fromIterator(partKeys)
      // 3 times value configured for raw dataset since expected throughput for downsampled cluster is much lower
      .mapAsync(storeConfig.demandPagingParallelism * 3) { partBytes =>
        val partLoadSpan = Kamon.spanBuilder(s"single-partition-cassandra-latency")
          .asChildOf(Kamon.currentSpan())
          .tag("dataset", rawDatasetRef.toString)
          .tag("shard", shardNum)
          .start()
        // TODO test multi-partition scan if latencies are high
        store.readRawPartitions(downsampledDataset,
                                storeConfig.maxChunkTime.toMillis,
                                SinglePartitionScan(partBytes, shardNum),
                                lookup.chunkMethod)
          .map { pd =>
            val part = makePagedPartition(pd, lookup.firstSchemaId.get)
            stats.partitionsQueried.increment()
            stats.chunksQueried.increment(part.numChunks)
            partLoadSpan.finish()
            part
          }
          .defaultIfEmpty(makePagedPartition(RawPartData(partBytes, Seq.empty), lookup.firstSchemaId.get))
          .headL
      }
  }

  protected def schemaIDFromPartID(partID: Int): Int = {
    partKeyIndex.partKeyFromPartId(partID).map { pkBytesRef =>
      val unsafeKeyOffset = PartKeyLuceneIndex.bytesRefToUnsafeOffset(pkBytesRef.offset)
      RecordSchema.schemaID(pkBytesRef.bytes, unsafeKeyOffset)
    }.getOrElse(throw new IllegalStateException("PartId returned by lucene, but partKey not found"))
  }

  private def chooseDownsampleResolution(chunkScanMethod: ChunkScanMethod): DatasetRef = {
    chunkScanMethod match {
      case AllChunkScan => downsampledDatasetRefs.last // since it is the highest resolution/ttl
      case TimeRangeChunkScan(startTime, _) =>
        val ttlIndex = downsampleTtls.indexWhere(t => startTime > System.currentTimeMillis() - t.toMillis)
        downsampledDatasetRefs(ttlIndex)
      case _ => ???
    }
  }

  private def makePagedPartition(part: RawPartData, firstSchemaId: Int): ReadablePartition = {
    val schemaId = RecordSchema.schemaID(part.partitionKey, UnsafeUtils.arayOffset)
    if (schemaId != firstSchemaId)
      throw new IllegalArgumentException("Query involves results with multiple schema. " +
        "Use type tag to provide narrower query")
    // FIXME It'd be nice to pass in the correct partId here instead of -1
    new PagedReadablePartition(schemas(schemaId), shardNum, -1, part)
  }

  /**
    * Iterator for lazy traversal of partIdIterator, value for the given label will be extracted from the ParitionKey.
    */
  case class LabelValueResultIterator(partIds: debox.Buffer[Int], labelNames: Seq[String], limit: Int)
    extends Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] {
    var currVal: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = _
    var numResultsReturned = 0
    var partIndex = 0

    override def hasNext: Boolean = {
      var foundValue = false
      while(partIndex < partIds.length && numResultsReturned < limit && !foundValue) {
        val partId = partIds(partIndex)

        import ZeroCopyUTF8String._
        //retrieve PartKey either from In-memory map or from PartKeyIndex
        val nextPart = partKeyFromPartId(partId)

        // FIXME This is non-performant and temporary fix for fetching label values based on filter criteria.
        // Other strategies needs to be evaluated for making this performant - create facets for predefined fields or
        // have a centralized service/store for serving metadata
        currVal = schemas.part.binSchema.toStringPairs(nextPart, UnsafeUtils.arayOffset)
          .filter(labelNames contains _._1).map(pair => {
          pair._1.utf8 -> pair._2.utf8
        }).toMap
        foundValue = currVal.nonEmpty
        partIndex += 1
      }
      foundValue
    }

    override def next(): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = {
      numResultsReturned += 1
      currVal
    }
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
    else throw new IllegalStateException("This is not an expected behavior." +
      " PartId should always have a corresponding PartKey!")
  }

  def cleanup(): Unit = {
    Option(houseKeepingFuture).foreach(_.cancel())
    Option(gaugeUpdateFuture).foreach(_.cancel())
  }

  override protected def finalize(): Unit = {
    cleanup()
  }

}
