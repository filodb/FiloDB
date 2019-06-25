package filodb.core.memstore

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

import debox.Buffer
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Observable, OverflowStrategy}

import filodb.core.Types
import filodb.core.downsample.{DownsampleConfig, DownsamplePublisher}
import filodb.core.metadata.Dataset
import filodb.core.store._
import filodb.memory.BinaryRegionLarge
import filodb.memory.MemFactory
import filodb.memory.format.UnsafeUtils

/**
 * Extends TimeSeriesShard with on-demand paging functionality by populating in-memory partitions with chunks from
 * a raw chunk source which implements RawChunkSource.readRawPartitions API.
 */
class OnDemandPagingShard(dataset: Dataset,
                          storeConfig: StoreConfig,
                          shardNum: Int,
                          bufferMemoryManager: MemFactory,
                          rawStore: ColumnStore,
                          metastore: MetaStore,
                          evictionPolicy: PartitionEvictionPolicy,
                          downsampleConfig: DownsampleConfig,
                          downsamplePublisher: DownsamplePublisher)
                         (implicit ec: ExecutionContext) extends
TimeSeriesShard(dataset, storeConfig, shardNum, bufferMemoryManager, rawStore, metastore, evictionPolicy,
                downsampleConfig, downsamplePublisher)(ec) {
  import TimeSeriesShard._

  private val singleThreadPool = Scheduler.singleThread(s"make-partition-${dataset.ref}-$shardNum")
  // TODO: make this configurable
  private val strategy = OverflowStrategy.BackPressure(1000)

  private def startODPSpan(): Span = Kamon.buildSpan(s"odp-cassandra-latency")
    .withTag("dataset", dataset.name)
    .withTag("shard", shardNum)
    .start()
  // NOTE: the current implementation is as follows
  //  1. Fetch partitions from memStore
  //  2. Determine, one at a time, what chunks are missing and could be fetched from disk
  //  3. Fetch missing chunks through a SinglePartitionScan
  //  4. upload to memory and return partition
  // Definitely room for improvement, such as fetching multiple partitions at once, more parallelism, etc.
  //scalastyle:off
  override def scanPartitions(columnIDs: Seq[Types.ColumnId],
                              partMethod: PartitionScanMethod,
                              chunkMethod: ChunkScanMethod = AllChunkScan): Observable[ReadablePartition] = {
    logger.debug(s"scanPartitions dataset=${dataset.ref} shard=$shardNum " +
      s"partMethod=$partMethod chunkMethod=$chunkMethod")
    // For now, always read every data column.
    // 1. We don't have a good way to update just some columns of a chunkset for ODP
    // 2. Timestamp column almost always needed
    // 3. We don't have a safe way to prevent JVM crashes if someone reads a column that wasn't paged in
    val allDataCols = dataset.dataColumns.map(_.id)

    // 1. Fetch partitions from memstore
    val iterResult = iteratePartitions(partMethod, chunkMethod)
    val partIdsNotInMemory = iterResult.partIdsNotInMemory
    // 2. Now determine list of partitions to ODP and the time ranges to ODP
    val partKeyBytesToPage = new ArrayBuffer[Array[Byte]]()
    val pagingMethods = new ArrayBuffer[ChunkScanMethod]
    val inMemOdp = debox.Set.empty[Int]

    iterResult.partIdsInMemoryMayNeedOdp.foreach { case (pId, startTime) =>
      val p = partitions.get(pId)
      if (p != null) {
        val odpChunkScan = chunksToODP(p, chunkMethod, pagingEnabled, startTime)
        odpChunkScan.foreach { rawChunkMethod =>
          pagingMethods += rawChunkMethod // TODO: really determine range for all partitions
          partKeyBytesToPage += p.partKeyBytes
          inMemOdp += p.partID
        }
      } else {
        // in the very rare case that partition literally *just* got evicted
        // we do not want to thrash by paging this partition back in.
        logger.warn(s"Skipped ODP of partId=$pId in dataset=${dataset.ref} " +
          s"shard=$shardNum since we are very likely thrashing")
      }
    }
    logger.debug(s"Query on dataset=${dataset.ref} shard=$shardNum resulted in partial ODP of partIds ${inMemOdp}, " +
      s"and full ODP of partIds ${iterResult.partIdsNotInMemory}")

    // partitions that do not need ODP are those that are not in the inMemOdp collection
    val noOdpPartitions = iterResult.partsInMemoryIter.filterNot(p => inMemOdp(p.partID))

    // NOTE: multiPartitionODP mode does not work with AllChunkScan and unit tests; namely missing partitions will not
    // return data that is in memory.  TODO: fix
    val result = Observable.fromIterator(noOdpPartitions) ++ {
      if (storeConfig.multiPartitionODP) {
        Observable.fromTask(odpPartTask(partIdsNotInMemory, partKeyBytesToPage, pagingMethods,
                                        chunkMethod)).flatMap { odpParts =>
          val multiPart = MultiPartitionScan(partKeyBytesToPage, shardNum)
          if (partKeyBytesToPage.nonEmpty) {
            val span = startODPSpan()
            rawStore.readRawPartitions(dataset, allDataCols, multiPart, computeBoundingMethod(pagingMethods))
              // NOTE: this executes the partMaker single threaded.  Needed for now due to concurrency constraints.
              // In the future optimize this if needed.
              .mapAsync { rawPart => partitionMaker.populateRawChunks(rawPart) }
              .doOnTerminate(ex => span.finish())
          } else { Observable.empty }
        }
      } else {
        Observable.fromTask(odpPartTask(partIdsNotInMemory, partKeyBytesToPage, pagingMethods,
                                        chunkMethod)).flatMap { odpParts =>
          if (partKeyBytesToPage.nonEmpty) {
            val span = startODPSpan()
            Observable.fromIterable(partKeyBytesToPage.zip(pagingMethods))
              .mapAsync(storeConfig.demandPagingParallelism) { case (partBytes, method) =>
                rawStore.readRawPartitions(dataset, allDataCols, SinglePartitionScan(partBytes, shardNum), method)
                  .mapAsync { rawPart => partitionMaker.populateRawChunks(rawPart) }
                  .defaultIfEmpty(getPartition(partBytes).get)
                  .headL
              }
              .doOnTerminate(ex => span.finish())
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
                          methods: ArrayBuffer[ChunkScanMethod], chunkMethod: ChunkScanMethod) =
  if (partIdsNotInMemory.nonEmpty) {
    createODPPartitionsTask(partIdsNotInMemory, { case (bytes, offset) =>
      val partKeyBytes = if (offset == UnsafeUtils.arayOffset) bytes
                         else BinaryRegionLarge.asNewByteArray(bytes, offset)
      partKeyBytesToPage += partKeyBytes
      methods += chunkMethod
      shardStats.partitionsRestored.increment
    }).executeOn(ingestSched).asyncBoundary
    // asyncBoundary above will cause subsequent map operations to run on designated scheduler for task or observable
    // as opposed to ingestSched

  // No need to execute the task on ingestion thread if it's empty / no ODP partitions
  } else Task.now(Nil)

  /**
   * Creates a Task which is meant ONLY TO RUN ON INGESTION THREAD
   * to create TSPartitions for partIDs found in Lucene but not in in-memory data structures
   * It runs in ingestion thread so it can correctly verify which ones to actually create or not
   */
  private def createODPPartitionsTask(partIDs: Buffer[Int], callback: (Array[Byte], Int) => Unit):
                                                                  Task[Seq[TimeSeriesPartition]] = Task {
    require(partIDs.nonEmpty)
    partIDs.map { id =>
      // for each partID: look up in partitions
      partitions.get(id) match {
        case TimeSeriesShard.OutOfMemPartition =>
          logger.debug(s"Creating TSPartition for ODP from part ID $id in dataset=${dataset.ref} shard=$shardNum")
          // If not there, then look up in Lucene and get the details
          for { partKeyBytesRef <- partKeyIndex.partKeyFromPartId(id)
                unsafeKeyOffset = PartKeyLuceneIndex.bytesRefToUnsafeOffset(partKeyBytesRef.offset)
                group = partKeyGroup(dataset.partKeySchema, partKeyBytesRef.bytes, unsafeKeyOffset, numGroups)
                part <- Option(createNewPartition(partKeyBytesRef.bytes, unsafeKeyOffset, group, id, 4)) } yield {
            val stamp = partSetLock.writeLock()
            try {
              part.ingesting = false
              partSet.add(part)
            } finally {
              partSetLock.unlockWrite(stamp)
            }
            callback(partKeyBytesRef.bytes, unsafeKeyOffset)
            part
          }
          // create the partition and update data structures (but no need to add to Lucene!)
          // NOTE: if no memory, then no partition!
        case p: TimeSeriesPartition => Some(p)
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
                                                  // do not include earliestTime, otherwise will pull in first chunk
                                                  Some(TimeRangeChunkScan(req.startTime, memStartTime - 1))
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

  override def iteratePartitions(partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod): IterationResult = {
    partMethod match {
      case SinglePartitionScan(partition, _) =>
        IterationResult(ByteKeysPartitionIterator(Seq(partition)), debox.Map.empty, debox.Buffer.empty)
      case MultiPartitionScan(partKeys, _)   =>
        IterationResult(ByteKeysPartitionIterator(partKeys), debox.Map.empty, debox.Buffer.empty)
      case FilteredPartitionScan(split, filters) =>
        // TODO: There are other filters that need to be added and translated to Lucene queries
        val coll = partKeyIndex.partIdsFromFilters2(filters, chunkMethod.startTime, chunkMethod.endTime)

        // first find out which partitions are being queried for data not in memory
        val it1 = InMemPartitionIterator(coll.intIterator())
        val partIdsToPage = it1.filter(_.earliestTime > chunkMethod.startTime).map(_.partID)
        val partIdsNotInMem = it1.skippedPartIDs
        val startTimes = if (partIdsToPage.nonEmpty) {
          val st = partKeyIndex.startTimeFromPartIds(partIdsToPage)
          logger.debug(s"Some partitions have earliestTime > queryStartTime(${chunkMethod.startTime}); " +
            s"startTime lookup for query in dataset=${dataset.ref} shard=$shardNum " +
            s"resulted in startTimes=$st")
          st
        }
        else {
          logger.debug(s"StartTime lookup was not needed. All partition's data for query in dataset=${dataset.ref} " +
            s"shard=$shardNum are in memory")
          debox.Map.empty[Int, Long]
        }
        // now provide an iterator that additionally supplies the startTimes for
        // those partitions that may need to be paged
        IterationResult(new InMemPartitionIterator(coll.intIterator()), startTimes, partIdsNotInMem)
    }
  }
}
