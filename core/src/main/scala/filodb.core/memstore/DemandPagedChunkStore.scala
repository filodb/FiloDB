package filodb.core.memstore

import java.nio.ByteBuffer

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import org.jctools.maps.NonBlockingHashMapLong

import filodb.core.store._
import filodb.memory.{BlockManager, BlockMemFactory}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.UnsafeUtils

/**
  * This class is responsible for the storage of On-Demand Paged chunks from
  * ChunkSource (example, cassandra) to offheap block memory.  One of these should exist per shard.
  *
  * It is orchestrated via the RawChunkSource and RawToPartitionMaker API.  The makePartition task is called
  * for each incoming RawPartData.  This class will identify the right memstore TSPartition and populate it.
  * NOTE: for now these tasks must be run in serial and not concurrently, due to limitations in BlockManager.
  *
  * Buckets are created on demand depending on the end time of incoming chunks.  One bucket per flush interval.
  * Memory blocks in buckets older than chunkRetentionHours are all marked reclaimable (even if not full) and
  * buckets are cleaned up as memory pressure forces reclamation.  The effect is that there is always guaranteed
  * at least one block for ODP data for chunkRetentionHours.
  *
  * @param tsShard the TimeSeriesShard containing the time series for the given shard
  * @param blockManager The block manager to be used for block allocation
  * @param chunkRetentionHours number of hours chunks need to be retained for. Beyond this time, ODP blocks will be
  *                    marked as reclaimable even if they are not full, so they can be reused for newer data.
  */
class DemandPagedChunkStore(tsShard: TimeSeriesShard,
                            val blockManager: BlockManager,
                            chunkRetentionHours: Int)
extends RawToPartitionMaker with StrictLogging {
  val flushIntervalMillis = tsShard.storeConfig.flushInterval.toMillis
  val retentionMillis = chunkRetentionHours * (1.hour.toMillis)

  // block factories for each time bucket
  private val memFactories = new NonBlockingHashMapLong[BlockMemFactory](chunkRetentionHours, false)

  import TimeSeriesShard._
  import collection.JavaConverters._

  private def getMemFactory(bucket: Long): BlockMemFactory = {
    val factory = memFactories.get(bucket)
    if (factory == UnsafeUtils.ZeroPointer) {
      val newFactory = new BlockMemFactory(blockManager,
                                           Some(bucket),
                                           tsShard.dataset.blockMetaSize,
                                           markFullBlocksAsReclaimable = true)
      memFactories.put(bucket, newFactory)
      newFactory
    } else {
      factory
    }
  }

  /**
   * Stores raw chunks into offheap memory and populates chunks into partition
   */
  def populateRawChunks(rawPartition: RawPartData): Task[ReadablePartition] = Task {
    // Find the right partition given the partition key
    tsShard.getPartition(rawPartition.partitionKey).map { partition =>
      val tsPart = partition.asInstanceOf[TimeSeriesPartition]
      tsShard.shardStats.partitionsPagedFromColStore.increment()
      // One chunkset at a time, load them into offheap and populate the partition
      rawPartition.chunkSets.foreach { case RawChunkSet(infoBytes, rawVectors) =>
        val memFactory = getMemFactory(timeBucketForChunkSet(infoBytes))
        val chunkID = ChunkSetInfo.getChunkID(infoBytes)

        memFactory.startMetaSpan()
        val chunkPtrs = copyToOffHeap(rawVectors, memFactory)
        val metaAddr = memFactory.endMetaSpan(writeMeta(_, tsPart.partID, infoBytes, chunkPtrs),
                                              tsShard.dataset.blockMetaSize.toShort)
        require(metaAddr != 0)
        val infoAddr = metaAddr + 4   // Important: don't point at partID
        val inserted = tsPart.addChunkInfoIfAbsent(chunkID, infoAddr)

        if (!inserted) {
          logger.info(s"Chunks not copied to ${partition.stringPartition}, already has chunk $chunkID.  " +
            s"Chunk time range (${ChunkSetInfo.getStartTime(infoBytes)}, ${ChunkSetInfo.getEndTime(infoBytes)})" +
            s" partition earliestTime=${partition.earliestTime}")
        }
      }
      partition
    }.getOrElse {
      // This should never happen.  The code in OnDemandPagingShard pre-creates partitions before we ODP them.
      throw new RuntimeException(s"Partition [${new String(rawPartition.partitionKey)}] not found, this is bad")
    }
  }

  /**
    * For a given chunkset, this method calculates the time bucket the chunks fall in.
    * It is used in deciding which BlockMemFactory to use while allocating off-heap memory for this chunk.
    */
  private def timeBucketForChunkSet(infoBytes: Array[Byte]): Long =
    (ChunkSetInfo.getEndTime(infoBytes) / flushIntervalMillis) * flushIntervalMillis

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

  /**
   * Ensures the oldest ODP time buckets, blocks, and BlockMemFactory's are reclaimable and cleaned up
   * so we don't leak memory and blocks.  Call this ideally every flushInterval.
   */
  def cleanupOldestBuckets(): Unit = {
    blockManager.markBucketedBlocksReclaimable(System.currentTimeMillis - retentionMillis)
    // Now, iterate through memFactories and clean up ones with no blocks
    memFactories.keySet.asScala.foreach { bucket =>
      if (!blockManager.hasTimeBucket(bucket)) memFactories.remove(bucket)
    }
  }
}
