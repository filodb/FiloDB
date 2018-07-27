package filodb.core.memstore

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import filodb.core.store._
import filodb.memory.{BlockManager, BlockMemFactory}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.UnsafeUtils

/**
  * This class is responsible for the storage and expiration of On-Demand Paged chunks from
  * ChunkSource (example, cassandra) to offheap block memory.  One of these should exist per shard.
  *
  * It is orchestrated via the RawChunkSource and RawToPartitionMaker API.  The makePartition task is called
  * for each incoming RawPartData.  This class will identify the right memstore TSPartition and populate it.
  * NOTE: for now these tasks must be run in serial and not concurrently, due to limitations in BlockManager.
  *
  * The chunks are bucketed into a configurable number of buckets based on class param chunkRetentionHours.
  * Off-heap for each bucket is served by a unique BlockMemStore that marks block as reclaimable as soon as they are
  * full. The latest block is marked reclaimable when the retention time is over.
  *
  * @param tsShard the TimeSeriesShard containing the time series for the given shard
  * @param blockManager The block manager to be used for block allocation
  * @param chunkRetentionHours number of hours chunks need to be retained for. A bucket will be created per hour
  *                            to reclaim blocks in a time ordered fashion
  */
class DemandPagedChunkStore(tsShard: TimeSeriesShard,
                            val blockManager: BlockManager,
                            chunkRetentionHours: Int,
                            var onDemandPagingEnabled: Boolean = true)(implicit scheduler: Scheduler)
extends RawToPartitionMaker with StrictLogging {
  // block factories for each time order
  private val memFactories = Array.tabulate(chunkRetentionHours) { i =>
    new BlockMemFactory(blockManager, Some(i), tsShard.dataset.blockMetaSize, markFullBlocksAsReclaimable = true)
  }

  // initialize the time bucket ranges and index to be able to look up reclaimOrder from timestamps quickly
  private val jvmStartTime = ManagementFactory.getRuntimeMXBean.getStartTime
  val retentionMillis = chunkRetentionHours.hours.toMillis
  val retentionStart = jvmStartTime - retentionMillis
  logger.info(s"Starting DemandPagedChunkStore, jvmStartTime=$jvmStartTime, retentionStart=$retentionStart")

  // schedule cleanup task
  scheduler.scheduleOnce(chunkRetentionHours.hours)(cleanupAndDisableOnDemandPaging)

  private[memstore] def cleanupAndDisableOnDemandPaging() = {
    logger.info(s"cleanupAndDisableOnDemandPaging for shard ${tsShard.shardNum}")
    blockManager.reclaimTimeOrderedBlocks()
    onDemandPagingEnabled = false
  }

  import TimeSeriesShard._

  /**
   * Stores raw chunks into offheap memory and populates chunks into partition
   */
  def populateRawChunks(rawPartition: RawPartData): Task[FiloPartition] = Task {
    if (onDemandPagingEnabled) {
      // Find the right partition given the partition key
      tsShard.getPartition(rawPartition.partitionKey).map { partition =>
        val tsPart = partition.asInstanceOf[TimeSeriesPartition]
        // One chunkset at a time, load them into offheap and populate the partition
        rawPartition.chunkSets.foreach { case RawChunkSet(infoBytes, rawVectors) =>
          timeOrderForChunkSet(ChunkSetInfo.getStartTime(infoBytes)).foreach { timeOrder =>
            val memFactory = memFactories(timeOrder)
            val chunkID = ChunkSetInfo.getChunkID(infoBytes)
            if (!partition.hasChunksAt(chunkID)) {
              tsShard.shardStats.chunkIdsPagedFromColStore.increment()
              memFactory.startMetaSpan()
              val chunkPtrs = copyToOffHeap(rawVectors, memFactory)
              val metaAddr = memFactory.endMetaSpan(writeMeta(_, tsPart.partID, infoBytes, chunkPtrs),
                                                    tsShard.dataset.blockMetaSize.toShort)
              require(metaAddr != 0)
              tsPart.addChunkSet(metaAddr + 4)   // Important: don't point at partID
            } else {
              logger.info(s"Chunks not copied to ${partition.stringPartition}, already has chunk $chunkID")
            }
          }
        }
        partition
      }.getOrElse {
        // What happens if it is not found?
        // TODO: do we want to have a "temporary" area for non persistent partitions which are not in the TSShard?
        // Do we want to add that partition to the TimeSeriesShard, if we have room?
        throw new UnsupportedOperationException("For now only partitions already in memstore will be uploaded")
      }
    } else {
      throw new IllegalArgumentException(s"Warning: Attempt to page data from chunk store into memory after " +
        s"retention period since JVM start on shard ${tsShard.shardNum}. Continued occurrence of this " +
        s"exception indicates a bug.")
    }
  }

  /**
    * For a given chunkset, this method calculates the time order in which eviction of chunk should occur.
    * It is used in deciding which BlockMemFactory to use while allocating off-heap memory for this chunk.
    */
  private def timeOrderForChunkSet(timestamp: Long): Option[Int] = {
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
  private def copyToOffHeap(buffers: Array[ByteBuffer],
                            memFactory: BlockMemFactory): Array[BinaryVectorPtr] = {
    buffers.map { buf =>
      // TODO: if in case the buffer is offheap/direct buffer already, maybe we don't need to copy it?
      val (bufBase, bufOffset, bufLen) = UnsafeUtils.BOLfromBuffer(buf)
      val vectorAddr = memFactory.allocateOffheap(bufLen)
      UnsafeUtils.unsafe.copyMemory(bufBase, bufOffset, UnsafeUtils.ZeroPointer, vectorAddr, bufLen)
      vectorAddr
    }
  }
}
