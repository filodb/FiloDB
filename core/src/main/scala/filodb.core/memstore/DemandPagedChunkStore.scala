package filodb.core.memstore

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import spire.syntax.cfor._

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
  */
class DemandPagedChunkStore(tsShard: TimeSeriesShard,
                            val blockManager: BlockManager)
extends RawToPartitionMaker with StrictLogging {

  import TimeSeriesShard._

  private val baseContext = Map("dataset" -> tsShard.ref.toString,
                                "shard"   -> tsShard.shardNum.toString)

  /*
   * Only one BlockMemFactory for ODP per shard needed (pooling not needed) since all ODP
   * allocations happen on a single thread
   */
  val memFactory = new BlockMemFactory(blockManager,
    tsShard.maxMetaSize, baseContext ++ Map("odp" -> "true"),
    markFullBlocksAsReclaimable = true)

  /**
   * Stores raw chunks into offheap memory and populates chunks into partition
   */
  //scalastyle:off
  def populateRawChunks(rawPartition: RawPartData): Task[ReadablePartition] = Task {
    FiloSchedulers.assertThreadName(FiloSchedulers.PopulateChunksSched)
    // Find the right partition given the partition key
    tsShard.getPartition(rawPartition.partitionKey).map { tsPart =>
      logger.debug(s"Populating paged chunks for shard=${tsShard.shardNum} partId=${tsPart.partID}")
      tsShard.shardStats.partitionsPagedFromColStore.increment()
      tsShard.shardStats.numChunksPagedIn.increment(rawPartition.chunkSetsTimeOrdered.size)
      // One chunkset at a time, load them into offheap and populate the partition
      // Populate newest chunk first so concurrent queries dont assume all data is populated in to chunk-map already
      rawPartition.chunkSetsTimeOrdered.reverseIterator.foreach { case RawChunkSet(infoBytes, rawVectors) =>
        // If the chunk is empty, skip it. If no call to allocateOffheap is made, then no check
        // is made to ensure that the block has room even for metadata. The call to endMetaSpan
        // might end up returning 0, because the last block doesn't have any room. It's
        // possible to guard against this by forcing an allocation, but it doesn't make sense
        // to allocate a block just for storing an unnecessary metadata entry.
        if (!rawVectors.isEmpty) {
          val chunkID = ChunkSetInfo.getChunkID(infoBytes)

          if (!tsPart.chunkmapContains(chunkID)) {
            val chunkPtrs = new ArrayBuffer[BinaryVectorPtr](rawVectors.length)
            var metaAddr: Long = 0
            memFactory.synchronized {
              memFactory.startMetaSpan()
              try {
                copyToOffHeap(rawVectors, memFactory, chunkPtrs)
              } finally {
                metaAddr = memFactory.endMetaSpan(writeMeta(_, tsPart.partID, infoBytes, chunkPtrs),
                  tsPart.schema.data.blockMetaSize.toShort)
              }
            }
            require(metaAddr != 0)
            val infoAddr = metaAddr + 4 // Important: don't point at partID
            val inserted = tsPart.addChunkInfoIfAbsent(chunkID, infoAddr)

            logger.debug(s"Populating paged chunk into memory chunkId=$chunkID inserted=$inserted " +
              s"partId=${tsPart.partID} shard=${tsShard.shardNum} ${tsPart.stringPartition}")
            if (!inserted) {
              logger.error(s"Chunks not copied to partId=${tsPart.partID} ${tsPart.stringPartition}, already has " +
                  s"chunk $chunkID. Chunk time range (${ChunkSetInfo.getStartTime(infoBytes)}, " +
                  s"${ChunkSetInfo.getEndTime(infoBytes)}) partition currentEarliestTime=${tsPart.earliestTime}")
            }
          }
        }
      }
      tsPart
    }.getOrElse {
      // This should never happen.  The code in OnDemandPagingShard pre-creates partitions before we ODP them.
      throw new RuntimeException(s"Partition [${new String(rawPartition.partitionKey)}] not found, this is bad")
    }
  }
  //scalastyle:on

  /**
    * Copies the onHeap contents read from ColStore into off-heap using the given memFactory.
    * If an exception is thrown by this method, the tail of chunkPtrs sequence isn't filled in.
    *
    * @param chunkPtrs filled in by this method
    */
  private def copyToOffHeap(buffers: Array[ByteBuffer],
                            memFactory: BlockMemFactory,
                            chunkPtrs: ArrayBuffer[BinaryVectorPtr]): Unit = {
    cforRange { 0 until buffers.length } { i =>
      val buf = buffers(i)
      val (bufBase, bufOffset, bufLen) = UnsafeUtils.BOLfromBuffer(buf)
      val vectorAddr = memFactory.allocateOffheap(bufLen)
      UnsafeUtils.unsafe.copyMemory(bufBase, bufOffset, UnsafeUtils.ZeroPointer, vectorAddr, bufLen)
      chunkPtrs += vectorAddr
    }
  }

}
