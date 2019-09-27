package filodb.downsampler

import filodb.core.memstore.WriteBufferPool
import filodb.core.metadata.Schema
import filodb.downsampler.BatchDownsampler.settings
import filodb.memory._

class PerThreadOffHeapMemory(rawSchemas: Seq[Schema], kamonTags: Map[String, String], maxMetaSize: Int) {

  val blockMemFactory = {
      val blockStore = new PageAlignedBlockManager(settings.blockMemorySize,
        stats = new MemoryStats(kamonTags),
        reclaimer = new ReclaimListener {
          override def onReclaim(metadata: Long, numBytes: Int): Unit = {}
        },
        numPagesPerBlock = 50)
      new BlockMemFactory(blockStore, None, maxMetaSize,
        kamonTags, false)
    }

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

}
