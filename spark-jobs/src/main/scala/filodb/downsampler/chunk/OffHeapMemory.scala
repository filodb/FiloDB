package filodb.downsampler.chunk

import com.typesafe.scalalogging.StrictLogging

import filodb.core.memstore.WriteBufferPool
import filodb.core.metadata.Schema
import filodb.core.store.StoreConfig
import filodb.memory._

class OffHeapMemory(schemas: Seq[Schema],
                    kamonTags: Map[String, String],
                    maxMetaSize: Int,
                    storeConfig: StoreConfig)
  extends StrictLogging {

  private val blockMemSize = storeConfig.shardMemSize
  private val nativeMemSize = storeConfig.ingestionBufferMemSize

  logger.info(s"Allocating OffHeap memory $this with nativeMemManagerSize=$nativeMemSize " +
    s"and blockMemorySize=$blockMemSize")
  val blockStore = new PageAlignedBlockManager(blockMemSize,
    stats = new MemoryStats(kamonTags),
    reclaimer = new ReclaimListener {
      override def onReclaim(metadata: Long, numBytes: Int): Unit = {}
    },
    numPagesPerBlock = 50)

  val blockMemFactory = new BlockMemFactory(blockStore, None, maxMetaSize, kamonTags, false)

  val nativeMemoryManager = new NativeMemoryManager(nativeMemSize, kamonTags)

  /**
    * Buffer Pool keyed by Raw schema Id
    */
  val bufferPools = {
    val bufferPoolByRawSchemaId = debox.Map.empty[Int, WriteBufferPool]
    schemas.foreach { s =>
      val pool = new WriteBufferPool(nativeMemoryManager, s.data, storeConfig)
      bufferPoolByRawSchemaId += s.schemaHash -> pool
    }
    bufferPoolByRawSchemaId
  }

  def free(): Unit = {
    logger.info(s"Freeing OffHeap memory $this nativeMemFreeBytes=${nativeMemoryManager.numFreeBytes} " +
      s"nativeMemUsedBytes=${nativeMemoryManager.usedMemory} " +
      s"blockMemFreeBytes=${blockStore.numFreeBlocks * blockStore.blockSizeInBytes} " +
      s"blockMemUsedBytes=${blockStore.usedMemory}")
    blockStore.releaseBlocks()
    nativeMemoryManager.shutdown()

  }
}
