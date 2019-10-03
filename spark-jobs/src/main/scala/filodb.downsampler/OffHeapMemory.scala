package filodb.downsampler

import com.typesafe.scalalogging.StrictLogging

import filodb.core.memstore.WriteBufferPool
import filodb.core.metadata.Schema
import filodb.downsampler.BatchDownsampler.settings
import filodb.memory._

class OffHeapMemory(rawSchemas: Seq[Schema], kamonTags: Map[String, String], maxMetaSize: Int)
  extends StrictLogging {

  logger.info(s"Allocating OffHeap memory $this with nativeMemManagerSize=${settings.nativeMemManagerSize} " +
    s"and blockMemorySize=${settings.blockMemorySize}")
  val blockStore = new PageAlignedBlockManager(settings.blockMemorySize,
    stats = new MemoryStats(kamonTags),
    reclaimer = new ReclaimListener {
      override def onReclaim(metadata: Long, numBytes: Int): Unit = {}
    },
    numPagesPerBlock = 50)

  val blockMemFactory = new BlockMemFactory(blockStore, None, maxMetaSize, kamonTags, false)

  val nativeMemoryManager = new NativeMemoryManager(settings.nativeMemManagerSize, kamonTags)

  /**
    * Buffer Pool keyed by Raw schema Id
    */
  val bufferPools = {
    val bufferPoolByRawSchemaId = debox.Map.empty[Int, WriteBufferPool]
    rawSchemas.foreach { s =>
      val pool = new WriteBufferPool(nativeMemoryManager, s.downsample.get.data, settings.downsampleStoreConfig)
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