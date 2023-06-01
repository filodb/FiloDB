package filodb.core.memstore

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

import com.typesafe.config.Config
import debox.Buffer
import java.util
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Observable, OverflowStrategy}

import filodb.core.{DatasetRef, Types}
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.ratelimit.QuotaSource
import filodb.core.metadata.Schemas
import filodb.core.query.{QueryContext, QueryLimitException, QuerySession, ServiceUnavailableException}
import filodb.core.store._
import filodb.memory.NativeMemoryManager

/**
 * Extends TimeSeriesShard with on-demand paging functionality by populating in-memory partitions with chunks from
 * a raw chunk source which implements RawChunkSource.readRawPartitions API.
 */
class OnDemandPagingShard(ref: DatasetRef,
                          schemas: Schemas,
                          storeConfig: StoreConfig,
                          quotaSource: QuotaSource,
                          shardNum: Int,
                          bufferMemoryManager: NativeMemoryManager,
                          rawStore: ColumnStore,
                          metastore: MetaStore,
                          evictionPolicy: PartitionEvictionPolicy,
                          filodbConfig: Config)
                         (implicit ec: ExecutionContext) extends
TimeSeriesShard(ref, schemas, storeConfig, quotaSource, shardNum, bufferMemoryManager, rawStore,
                metastore, evictionPolicy, filodbConfig)(ec) {
  import TimeSeriesShard._
  import FiloSchedulers._

  private val singleThreadPool =
    Scheduler.singleThread(s"${FiloSchedulers.PopulateChunksSched}-$ref-$shardNum")
  // TODO: make this configurable
  private val strategy = OverflowStrategy.BackPressure(1000)

  private def startODPSpan(): Span = Kamon.spanBuilder(s"odp-cassandra-latency")
    .asChildOf(Kamon.currentSpan())
    .tag("dataset", ref.dataset)
    .tag("shard", shardNum)
    .start()

  private def capDataScannedPerShardCheck(lookup: PartLookupResult,
                                          colIds: Seq[Types.ColumnId],
                                          qContext : QueryContext): Unit = {
    lookup.firstSchemaId.foreach { schId =>
      lookup.chunkMethod match {
        case TimeRangeChunkScan(st, end) =>
          val numMatches = lookup.partsInMemory.length + lookup.partIdsNotInMemory.length
          ensureQueriedDataSizeWithinLimitApprox(
            schId, colIds, numMatches,
            storeConfig.flushInterval.toMillis,
            storeConfig.estimatedIngestResolutionMillis,
            end - st,
            qContext
          )
        case _ =>
      }
    }
  }

  /**
   * Note this approach below assumes the following for quick size estimation. The sizing is more
   * a swag than reality:
   * (a) every matched time series ingests at all query times. Looking up start/end times and more
   * precise size estimation is costly
   * (b) it also assigns bytes per sample based on schema which is much of a swag. In reality, it would depend on
   * number of histogram buckets, samples per chunk etc.
   */
  def ensureQueriedDataSizeWithinLimitApprox(
    schemaId: Int,
    colIds: Seq[Types.ColumnId],
    numTsPartitions: Int,
    chunkDurationMillis: Long,
    resolutionMs: Long,
    queryDurationMs: Long,
    qContext: QueryContext
  ): Unit = {
    val enforcedLimits = qContext.plannerParams.enforcedLimits
    val warnLimits = qContext.plannerParams.warnLimits
    val estDataSize = schemas.estimateBytesScan(
      schemaId, colIds, numTsPartitions, chunkDurationMillis, resolutionMs, queryDurationMs
    )
    // TODO the below does not return a particular kind of error code. Most likely this would translate to
    // an internal error of FiloDB which is not appropriate for the case
    if (estDataSize > enforcedLimits.timeSeriesSamplesScannedBytes) {
      val exMessage = s"With match of $numTsPartitions time series, estimate of $estDataSize bytes exceeds limit of " +
        s"${enforcedLimits.timeSeriesSamplesScannedBytes} bytes queried per shard " +
        s"for ${schemas.apply(schemaId).name} schema. " +
        s"Try one or more of these: " +
        s"(a) narrow your query filters to reduce to fewer than the current $numTsPartitions matches " +
        s"(b) reduce query time range, currently at ${queryDurationMs / 1000 / 60} minutes"
      throw QueryLimitException(exMessage, qContext.queryId)
    }
    if (numTsPartitions > enforcedLimits.timeSeriesScanned) {
      val exMessage =
        s"Query matched $numTsPartitions time series, which exceeds a max enforced limit of " +
          s"${enforcedLimits.timeSeriesScanned} time series allowed to be queried per shard. " +
          s"Try one or more of these: " +
          s"(a) narrow your query filters to reduce to fewer than the current $numTsPartitions matches " +
          s"(b) reduce query time range, currently at ${queryDurationMs / 1000 / 60} minutes"
      throw QueryLimitException(exMessage, qContext.queryId)
    }
    if (numTsPartitions > warnLimits.timeSeriesScanned) {
      val msg =
        s"Query matched $numTsPartitions time series, which exceeds a max warn limit of " +
          s"${warnLimits.timeSeriesScanned} time series allowed to be queried per shard. "
      logger.info(qContext.getQueryLogLine(msg))
    }
    if (estDataSize > warnLimits.timeSeriesSamplesScannedBytes) {
      val msg =
        s"With match of $numTsPartitions time series, estimate of $estDataSize bytes exceeds " +
          s"limit of ${warnLimits.timeSeriesSamplesScannedBytes} bytes queried per shard " +
          s"for ${schemas.apply(schemaId).name} schema. "
      logger.info(qContext.getQueryLogLine(msg))
    }
  }

  // NOTE: the current implementation is as follows
  //  1. Fetch partitions from memStore
  //  2. Determine, one at a time, what chunks are missing and could be fetched from disk
  //  3. Fetch missing chunks through a SinglePartitionScan
  //  4. upload to memory and return partition
  // Definitely room for improvement, such as fetching multiple partitions at once, more parallelism, etc.
  //scalastyle:off
  override def scanPartitions(partLookupRes: PartLookupResult,
                              colIds: Seq[Types.ColumnId],
                              querySession: QuerySession): Observable[ReadablePartition] = {

    capDataScannedPerShardCheck(partLookupRes, colIds, querySession.qContext)

    // For now, always read every data column.
    // 1. We don't have a good way to update just some columns of a chunkset for ODP
    // 2. Timestamp column almost always needed
    // 3. We don't have a safe way to prevent JVM crashes if someone reads a column that wasn't paged in

    // 1. Fetch partitions from memstore
    val partIdsNotInMemory = partLookupRes.partIdsNotInMemory

    // 2. Now determine list of partitions to ODP and the time ranges to ODP
    val partKeyBytesToPage = new ArrayBuffer[Array[Byte]]()
    val pagingMethods = new ArrayBuffer[ChunkScanMethod]
    val inMemOdp = debox.Set.empty[Int]

    partLookupRes.partIdsMemTimeGap.foreach { case (pId, startTime) =>
      val p = partitions.get(pId)
      if (p != null) {
        val odpChunkScan = chunksToODP(p, partLookupRes.chunkMethod, pagingEnabled, startTime)
        odpChunkScan.foreach { rawChunkMethod =>
          pagingMethods += rawChunkMethod // TODO: really determine range for all partitions
          partKeyBytesToPage += p.partKeyBytes
          inMemOdp += p.partID
        }
      } else {
        // in the very rare case that partition literally *just* got evicted
        // we do not want to thrash by paging this partition back in.
        logger.warn(s"Skipped ODP of partId=$pId in dataset=$ref " +
          s"shard=$shardNum since we are very likely thrashing")
      }
    }
    logger.debug(s"Query on dataset=$ref shard=$shardNum resulted in partial ODP of partIds ${inMemOdp}, " +
      s"and full ODP of partIds ${partLookupRes.partIdsNotInMemory}")

    // partitions that do not need ODP are those that are not in the inMemOdp collection
    val inMemParts = InMemPartitionIterator2(partLookupRes.partsInMemory)
    val noOdpPartitions = inMemParts.filterNot(p => inMemOdp(p.partID))

    // NOTE: multiPartitionODP mode does not work with AllChunkScan and unit tests; namely missing partitions will not
    // return data that is in memory.  TODO: fix
    val result = Observable.fromIteratorUnsafe(noOdpPartitions) ++ {
      if (storeConfig.multiPartitionODP) {
        Observable.fromTask(odpPartTask(partIdsNotInMemory, partKeyBytesToPage, pagingMethods,
                                        partLookupRes.chunkMethod)).flatMap { odpParts =>
          val multiPart = MultiPartitionScan(partKeyBytesToPage, shardNum)
          if (partKeyBytesToPage.nonEmpty) {
            val span = startODPSpan()
            rawStore.readRawPartitions(ref, maxChunkTime, multiPart, computeBoundingMethod(pagingMethods))
              // NOTE: this executes the partMaker single threaded.  Needed for now due to concurrency constraints.
              // In the future optimize this if needed.
              .mapEval { rawPart => partitionMaker.populateRawChunks(rawPart).executeOn(singleThreadPool) }
              .asyncBoundary(strategy) // This is needed so future computations happen in a different thread
              .guarantee(Task.eval(span.finish())) // not async
          } else { Observable.empty }
        }
      } else {
        Observable.fromTask(odpPartTask(partIdsNotInMemory, partKeyBytesToPage, pagingMethods,
                                        partLookupRes.chunkMethod)).flatMap { odpParts =>
          assertThreadName(QuerySchedName)
          logger.debug(s"Finished creating full ODP partitions ${odpParts.map(_.partID)}")
          if(logger.underlying.isDebugEnabled) {
            partKeyBytesToPage.zip(pagingMethods).foreach { case (pk, method) =>
              logger.debug(s"Paging in chunks for partId=${getPartition(pk).get.partID} chunkMethod=$method")
            }
          }
          if (partKeyBytesToPage.nonEmpty) {
            val span = startODPSpan()
            Observable.fromIterable(partKeyBytesToPage.zip(pagingMethods))
              .mapParallelUnordered(storeConfig.demandPagingParallelism) { case (partBytes, method) =>
                rawStore.readRawPartitions(ref, maxChunkTime, SinglePartitionScan(partBytes, shardNum), method)
                  .mapEval { rawPart => partitionMaker.populateRawChunks(rawPart).executeOn(singleThreadPool) }
                  .asyncBoundary(strategy) // This is needed so future computations happen in a different thread
                  .defaultIfEmpty(getPartition(partBytes).get)
                  .headL
                  // headL since we are fetching a SinglePartition above
              }
              .guarantee(Task.eval(span.finish())) // not async
          } else {
            Observable.empty
          }
        }
      }
    }
    result.map { p =>
      shardStats.partitionsQueried.increment()
      p
    }
  }

  // 3. Deal with partitions no longer in memory but still indexed in Lucene.
  //    Basically we need to create TSPartitions for them in the ingest thread -- if there's enough memory
  private def odpPartTask(partIdsNotInMemory: Buffer[Int], partKeyBytesToPage: ArrayBuffer[Array[Byte]],
                          pagingMethods: ArrayBuffer[ChunkScanMethod], chunkMethod: ChunkScanMethod) =
  if (partIdsNotInMemory.nonEmpty) {
    createODPPartitionsTask(partIdsNotInMemory, { case (pId, pkBytes) =>
      partKeyBytesToPage += pkBytes
      pagingMethods += chunkMethod
      logger.debug(s"Finished creating part for full odp. Now need to page partId=$pId chunkMethod=$chunkMethod")
      shardStats.partitionsRestored.increment()
    }).executeOn(ingestSched).asyncBoundary
    // asyncBoundary above will cause subsequent map operations to run on designated scheduler for task or observable
    // as opposed to ingestSched

  // No need to execute the task on ingestion thread if it's empty / no ODP partitions
  } else Task.eval(Nil)

  /**
   * Creates a Task which is meant ONLY TO RUN ON INGESTION THREAD
   * to create TSPartitions for partIDs found in Lucene but not in in-memory data structures
   * It runs in ingestion thread so it can correctly verify which ones to actually create or not
   */
  private def createODPPartitionsTask(partIDs: Buffer[Int], callback: (Int, Array[Byte]) => Unit):
                                                                  Task[Seq[TimeSeriesPartition]] = Task.evalAsync {
    assertThreadName(IngestSchedName)
    require(partIDs.nonEmpty)
    partIDs.map { id =>
      // for each partID: look up in partitions
      partitions.get(id) match {
        case TimeSeriesShard.OutOfMemPartition =>
          logger.debug(s"Creating TSPartition for ODP from part ID $id in dataset=$ref shard=$shardNum")
          // If not there, then look up in Lucene and get the details
          for { partKeyBytesRef <- partKeyIndex.partKeyFromPartId(id)
                unsafeKeyOffset = PartKeyLuceneIndex.bytesRefToUnsafeOffset(partKeyBytesRef.offset)
                group = partKeyGroup(schemas.part.binSchema, partKeyBytesRef.bytes, unsafeKeyOffset, numGroups)
                sch  <- Option(schemas(RecordSchema.schemaID(partKeyBytesRef.bytes, unsafeKeyOffset)))
                          } yield {
            val part = createNewPartition(partKeyBytesRef.bytes, unsafeKeyOffset, group, id, sch, true, 4)
            if (part == OutOfMemPartition) throw new ServiceUnavailableException("The server has too many ingesting " +
              "time series and does not have resources to serve this long time range query. Please try " +
              "after sometime.")
            val stamp = partSetLock.writeLock()
              try {
                markPartAsNotIngesting(part, odp = true)
                partSet.add(part)
              } finally {
                partSetLock.unlockWrite(stamp)
              }
              val pkBytes = util.Arrays.copyOfRange(partKeyBytesRef.bytes, partKeyBytesRef.offset,
                partKeyBytesRef.offset + partKeyBytesRef.length)
              callback(part.partID, pkBytes)
              part
            }
          // create the partition and update data structures (but no need to add to Lucene!)
          // NOTE: if no memory, then no partition!
        case p: TimeSeriesPartition =>
          // invoke callback even if we didn't create the partition
          callback(p.partID, p.partKeyBytes)
          Some(p)
      }
    }.toVector.flatten
  }

  private def computeBoundingMethod(methods: Seq[ChunkScanMethod]): ChunkScanMethod = if (methods.isEmpty) {
    AllChunkScan
  } else {
    var minTime = Long.MaxValue
    var maxTime = 0L
    methods.foreach { m =>
      minTime = Math.min(minTime, m.startTime)
      maxTime = Math.max(maxTime, m.endTime)
    }
    TimeRangeChunkScan(minTime, maxTime)
  }

  /**
    * Check if ODP is really needed for this partition which is in memory
    * @return Some(scanMethodForCass) if ODP is needed, None if ODP is not needed
    */
  private def chunksToODP(partition: ReadablePartition,
                          method: ChunkScanMethod,
                          enabled: Boolean,
                          partStartTime: Long): Option[ChunkScanMethod] = {
    if (enabled) {
      method match {
        // For now, allChunkScan will always load from disk.  This is almost never used, and without an index we have
        // no way of knowing what there is anyways.
        case AllChunkScan                 =>  Some(AllChunkScan)
        // Assume initial startKey of first chunk is the earliest - typically true unless we load in historical data
        // Compare desired time range with start key and see if in memory data covers desired range
        // Also assume we have in memory all data since first key.  Just return the missing range of keys.
        case req: TimeRangeChunkScan      =>  if (partition.numChunks > 0) {
                                                val memStartTime = partition.earliestTime
                                                if (req.startTime < memStartTime && partStartTime < memStartTime) {
                                                  val toODP = TimeRangeChunkScan(req.startTime, memStartTime)
                                                  logger.debug(s"Decided to ODP time range $toODP for " +
                                                    s"partID=${partition.partID} memStartTime=$memStartTime " +
                                                    s"shard=$shardNum ${partition.stringPartition}")
                                                  Some(toODP)
                                                }
                                                else None
                                              } else Some(req) // if no chunks ingested yet, read everything from disk
        case InMemoryChunkScan            =>  None // Return only in-memory data - ie return none so we never ODP
        case WriteBufferChunkScan         =>  None // Write buffers are always in memory only
      }
    } else {
      None
    }
  }
}
