package filodb.core.memstore

import scala.concurrent.ExecutionContext

import monix.execution.Scheduler
import monix.reactive.{Observable, OverflowStrategy}

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.store._
import filodb.core.Types

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
  private val singleThreadPool = Scheduler.singleThread("make-partition")
  // TODO: make this configurable
  private val strategy = OverflowStrategy.BackPressure(1000)

  // NOTE: the current implementation is as follows
  //  1. Fetch partitions from memStore
  //  2. Determine, one at a time, what chunks are missing and could be fetched from disk
  //  3. Fetch missing chunks through a SinglePartitionScan
  //  4. upload to memory and return partition
  // Definitely room for improvement, such as fetching multiple partitions at once, more parallelism, etc.
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
    super.scanPartitions(columnIDs, partMethod, chunkMethod)
      .flatMap { tsPart =>

        // 2. Determine missing chunks per partition and what to fetch
        chunksToFetch(tsPart, chunkMethod, pagingEnabled).map { rawChunkMethod =>
          shardStats.partitionsPagedFromColStore.increment()
          val singlePart = SinglePartitionScan(tsPart.partKeyBytes, tsPart.shard)
          rawStore.readRawPartitions(dataset, allDataCols, singlePart, rawChunkMethod)
            // NOTE: this executes the partMaker single threaded.  Needed for now due to concurrency constraints.
            // In the future optimize this if needed.
            // TODO: add Kamon Executors monitoring to monitor this thread pool.  Do this as part of perf optimization
            .mapAsync { rawPart => partitionMaker.populateRawChunks(rawPart).executeOn(singleThreadPool) }
            // This is needed so future computations happen in a different thread
            .asyncBoundary(strategy)
            // Unfortunately switchIfEmpty doesn't work.  Need to do this to ensure TSPartition will be returned
            .countF
            .map { count =>
              tsPart
            }
        }.getOrElse {
          // None means just return the partition itself, no modification
          Observable.now(tsPart)
        }
      }
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
        // Only used for tests -> last sample is pretty much always in memory, so don't ODP
        case LastSampleChunkScan      => None
      }
    } else {
      None
    }

  }
}