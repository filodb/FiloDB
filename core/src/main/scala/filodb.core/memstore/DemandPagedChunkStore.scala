package filodb.core.memstore

import java.lang.management.ManagementFactory
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler

import filodb.core.metadata.Dataset
import filodb.core.query.ChunkSetReader
import filodb.core.store.ChunkSetInfo
import filodb.memory.{BlockManager, BlockMemFactory}
import filodb.memory.format.BinaryVector

/**
  * This class is responsible for the storage and expiration of On-Demand Paged chunks from
  * ChunkSource (example, cassandra).
  *
  * The orchestrator of on-demand paging calls the storeAsync function to store the chunks into this PagedChunkStore.
  * The storage call is asynchronous to avoid blocking the query path. The tasks are queued using a singleThreadExecutor
  * to remove resource contention in BlockManager.
  *
  * The chunks are bucketed into a configurable number of buckets based on class param chunkRetentionHours.
  * Off-heap for each bucket is served by a unique BlockMemStore that marks block as reclaimable as soon as they are
  * full. The latest block is marked reclaimable when the retention time is over.
  *
  * @param dataset the dataset this paged chunk store is allocated for.
  * @param blockManager The block manager to be used for block allocation
  * @param metadataAllocSize the additional size in bytes to ensure is free for writing metadata, per chunk
  * @param chunkRetentionHours number of hours chunks need to be retained for. A bucket will be created per hour
  *                            to reclaim blocks in a time ordered fashion
  */
class DemandPagedChunkStore(dataset: Dataset,
                            blockManager: BlockManager,
                            metadataAllocSize: Int,
                            chunkRetentionHours: Int,
                            shardNum: Int) extends StrictLogging {
  // It is important that the tasks be executed in a single thread  to remove resource contention in BlockManager
  private val scheduler = Scheduler(Executors.newSingleThreadScheduledExecutor(),
                            ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor()))

  // block factories for each time order
  private val memFactories = Array.tabulate(chunkRetentionHours) { i =>
    new BlockMemFactory(blockManager, Some(i), metadataAllocSize, markFullBlocksAsReclaimable = true)
  }

  private[memstore] var onDemandPagingEnabled = true // is enabled from start of jvm to retention period

  // initialize the time bucket ranges and index to be able to look up reclaimOrder from timestamps quickly
  private val jvmStartTime = ManagementFactory.getRuntimeMXBean.getStartTime
  val retentionStart = jvmStartTime - chunkRetentionHours.hours.toMillis

  // schedule cleanup task
  scheduler.scheduleOnce(chunkRetentionHours.hours)(cleanupAndDisableOnDemandPaging)

  private[memstore] def cleanupAndDisableOnDemandPaging() = {
    logger.info(s"cleanupAndDisableOnDemandPaging for shard $shardNum")
    blockManager.reclaimTimeOrderedBlocks()
    onDemandPagingEnabled = false
  }

  import TimeSeriesShard._

  /**
    * Stores the on heap chunkset into off-heap memory and indexes them into partition
    *
    * @param onHeap chunkset to store
    * @return Future of off-heap chunkset-infos that completes when partition has been paged to memory
    */
  def storeAsync(onHeap: Seq[ChunkSetReader], partition: TimeSeriesPartition): Future[Seq[ChunkSetInfo]] = {
    val p = Promise[Seq[ChunkSetInfo]]()
    scheduler.scheduleOnce(0.seconds) {
      if (onDemandPagingEnabled && !partition.cacheIsWarm()) {
        val offHeapChunkInfos = onHeap.flatMap { reader => // flatMap removes Nones
          timeOrderForChunkSet(reader.info).map { timeOrder =>
            val memFactory = memFactories(timeOrder)
            memFactory.startMetaSpan()
            val (info, chunks) = copyToOffHeap(reader, memFactory)
            partition.addChunkSet(info, chunks)
            memFactory.endMetaSpan(writeMeta(_, partition.partID, info.id), BlockMetaAllocSize)
            info
          }
        }
        p.success(offHeapChunkInfos)
      } else {
        p.failure(new IllegalArgumentException(s"Warning: Attempt to page data from chunk store into memory after " +
          s"retention period since JVM start on shard $shardNum. Continued occurrence of this " +
          s"exception indicates a bug."))
      }
    }
    p.future
  }

  /**
    * For a given chunkset, this method calculates the time order in which eviction of chunk should occur.
    * It is used in deciding which BlockMemFactory to use while allocating off-heap memory for this chunk.
    */
  private def timeOrderForChunkSet(info: ChunkSetInfo): Option[Int] = {
    val timestamp = info.firstKey.getLong(dataset.timestampColumn.get.id)
    if (timestamp < retentionStart)
      None
    else if (timestamp > jvmStartTime)
      Some(chunkRetentionHours - 1)
    else
      Some(((timestamp - retentionStart) / 1.hour.toMillis).toInt)
  }

  /**
    * Copies the onHeap contents read from ColStore into off-heap using the given memFactory
    */
  private def copyToOffHeap(onHeap: ChunkSetReader,
                            memFactory: BlockMemFactory): (ChunkSetInfo, Array[BinaryVector[_]]) = {
    val columnIds = dataset.dataColumns.map(_.id).toArray
    val offHeap = onHeap.vectors.map(_.asInstanceOf[BinaryVector[_]]).zipWithIndex.map { case (v, i) =>
      val bytes = v.toFiloBuffer.array()
      val byteBuf = memFactory.copyFromBytes(bytes)
      dataset.vectorMakers(columnIds(i))(byteBuf, onHeap.length).asInstanceOf[BinaryVector[_]]
    }
    (onHeap.info, offHeap)
  }

}
