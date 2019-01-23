package filodb.core.memstore

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

import debox.Buffer
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Observable, OverflowStrategy}

import filodb.core.Types
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.store._
import filodb.memory.BinaryRegionLarge
import filodb.memory.format.UnsafeUtils

/**
 * Extends TimeSeriesShard with on-demand paging functionality by populating in-memory partitions with chunks from
 * a raw chunk source which implements RawChunkSource.readRawPartitions API.
 */
class OnDemandPagingShard(dataset: Dataset,
                          storeConfig: StoreConfig,
                          shardNum: Int,
                          rawStore: ColumnStore,
                          metastore: MetaStore,
                          evictionPolicy: PartitionEvictionPolicy)
                         (implicit ec: ExecutionContext) extends
TimeSeriesShard(dataset, storeConfig, shardNum, rawStore, metastore, evictionPolicy)(ec) {
  import TimeSeriesShard._

  private val singleThreadPool = Scheduler.singleThread("make-partition")
  // TODO: make this configurable
  private val strategy = OverflowStrategy.BackPressure(1000)

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
    logger.debug(s"scanPartitions partMethod=$partMethod chunkMethod=$chunkMethod")
    // For now, always read every data column.
    // 1. We don't have a good way to update just some columns of a chunkset for ODP
    // 2. Timestamp column almost always needed
    // 3. We don't have a safe way to prevent JVM crashes if someone reads a column that wasn't paged in
    val allDataCols = dataset.dataColumns.map(_.id)

    // 1. Fetch partitions from memstore
    val indexIt = iteratePartitions(partMethod, chunkMethod)

    // 2. Determine missing chunks per partition and what to fetch
    val partKeyBytesToPage = new ArrayBuffer[Array[Byte]]()
    val inMemoryPartitions = new ArrayBuffer[ReadablePartition]()
    val methods = new ArrayBuffer[ChunkScanMethod]
    indexIt.foreach { p =>
      chunksToFetch(p, chunkMethod, pagingEnabled).map { rawChunkMethod =>
        methods += rawChunkMethod   // TODO: really determine range for all partitions
        partKeyBytesToPage += p.partKeyBytes
      }.getOrElse {
        // add it to partitions which do not need to be ODP'ed, send these directly and first
        inMemoryPartitions += p
      }
    }
    shardStats.partitionsQueried.increment(inMemoryPartitions.length)

    logger.debug(s"[$shardNum] Querying ${inMemoryPartitions.length} in memory partitions, ODPing ${methods.length}")

    // NOTE: multiPartitionODP mode does not work with AllChunkScan and unit tests; namely missing partitions will not
    // return data that is in memory.  TODO: fix
    if (storeConfig.multiPartitionODP) {
      Observable.fromIterable(inMemoryPartitions) ++
      Observable.fromTask(odpPartTask(indexIt, partKeyBytesToPage, methods, chunkMethod)).flatMap { odpParts =>
        val multiPart = MultiPartitionScan(partKeyBytesToPage, shardNum)
        shardStats.partitionsQueried.increment(partKeyBytesToPage.length)
        (if (partKeyBytesToPage.nonEmpty) {
          rawStore.readRawPartitions(dataset, allDataCols, multiPart, computeBoundingMethod(methods))
            // NOTE: this executes the partMaker single threaded.  Needed for now due to concurrency constraints.
            // In the future optimize this if needed.
            .mapAsync { rawPart => partitionMaker.populateRawChunks(rawPart).executeOn(singleThreadPool) }
            // This is needed so future computations happen in a different thread
            .asyncBoundary(strategy)
        } else { Observable.empty })
      }
    } else {
      Observable.fromIterable(inMemoryPartitions) ++
      Observable.fromTask(odpPartTask(indexIt, partKeyBytesToPage, methods, chunkMethod)).flatMap { odpParts =>
        shardStats.partitionsQueried.increment(partKeyBytesToPage.length)
        Observable.fromIterable(partKeyBytesToPage.zip(methods))
          .mapAsync(storeConfig.demandPagingParallelism) { case (partBytes, method) =>
            rawStore.readRawPartitions(dataset, allDataCols, SinglePartitionScan(partBytes, shardNum), method)
              .mapAsync { rawPart => partitionMaker.populateRawChunks(rawPart).executeOn(singleThreadPool) }
              .defaultIfEmpty(getPartition(partBytes).get)
              .headL
          }
      }
    }
  }

  // 3. Deal with partitions no longer in memory but still indexed in Lucene.
  //    Basically we need to create TSPartitions for them in the ingest thread -- if there's enough memory
  private def odpPartTask(indexIt: PartitionIterator, partKeyBytesToPage: ArrayBuffer[Array[Byte]],
                          methods: ArrayBuffer[ChunkScanMethod], chunkMethod: ChunkScanMethod) =
  if (indexIt.skippedPartIDs.nonEmpty) {
    createODPPartitionsTask(indexIt.skippedPartIDs, { case (bytes, offset) =>
      val partKeyBytes = if (offset == UnsafeUtils.arayOffset) bytes
                         else BinaryRegionLarge.asNewByteArray(bytes, offset)
      partKeyBytesToPage += partKeyBytes
      methods += chunkMethod
      shardStats.partitionsRestored.increment
    }).executeOn(ingestSched)
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
          logger.debug(s"Creating TSPartition for ODP from part ID $id")
          // If not there, then look up in Lucene and get the details
          for { partKeyBytesRef <- partKeyIndex.partKeyFromPartId(id)
                unsafeKeyOffset = PartKeyLuceneIndex.bytesRefToUnsafeOffset(partKeyBytesRef.offset)
                group = partKeyGroup(dataset.partKeySchema, partKeyBytesRef.bytes, unsafeKeyOffset, numGroups)
                part <- Option(createNewPartition(partKeyBytesRef.bytes, unsafeKeyOffset, group, 4)) } yield {
            partSet.add(part)
            callback(partKeyBytesRef.bytes, unsafeKeyOffset)
            part
          }
          // create the partition and update data structures (but no need to add to Lucene!)
          // NOTE: if no memory, then no partition!
        case p: TimeSeriesPartition => Some(p)
      }
    }.toVector.flatten
  }

  def computeBoundingMethod(methods: Seq[ChunkScanMethod]): ChunkScanMethod = if (methods.isEmpty) {
    AllChunkScan
  } else {
    var minTime = Long.MaxValue
    var maxTime = 0L
    methods.foreach { m =>
      minTime = Math.min(minTime, m.startTime)
      maxTime = Math.max(maxTime, m.endTime)
    }
    RowKeyChunkScan(minTime, maxTime)
  }

  def chunksToFetch(partition: ReadablePartition,
                    method: ChunkScanMethod,
                    enabled: Boolean): Option[ChunkScanMethod] = {
    if (enabled) {
      method match {
        // For now, allChunkScan will always load from disk.  This is almost never used, and without an index we have
        // no way of knowing what there is anyways.
        case AllChunkScan               => Some(AllChunkScan)
        // Assume initial startKey of first chunk is the earliest - typically true unless we load in historical data
        // Compare desired time range with start key and see if in memory data covers desired range
        // Also assume we have in memory all data since first key.  Just return the missing range of keys.
        case r: RowKeyChunkScan         =>
          if (partition.numChunks > 0) {
            val memStartTime = partition.earliestTime
            val endQuery = memStartTime - 1   // do not include earliestTime, otherwise will pull in first chunk
            if (r.startTime < memStartTime) { Some(RowKeyChunkScan(r.startkey, BinaryRecord.timestamp(endQuery))) }
            else                            { None }
          } else {
            Some(r)    // if no chunks ingested yet, read everything from disk
          }
        // Return only in-memory data - ie return none so we never ODP
        case InMemoryChunkScan        => None
        // Write buffers are always in memory only
        case WriteBufferChunkScan      => None
      }
    } else {
      None
    }

  }
}