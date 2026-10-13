package filodb.core.memstore

import java.lang.management.ManagementFactory

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef
import filodb.core.store.StoreConfig

/**
 * Utility for automatic off-heap memory allocation across FiloDB's subsystems.
 *
 * When `memstore.memory-alloc.automatic-alloc-enabled = true`, each subsystem's memory
 * limit is derived from a single "available memory" figure rather than being configured
 * individually.  This makes it easy to right-size a deployment just by stating the
 * total budget and letting the percentages do the rest.
 *
 * == Available memory ==
 * Available memory is the off-heap budget for FiloDB. It is either read directly from
 * config (`memstore.memory-alloc.available-memory-bytes`, useful for testing or when
 * the container memory is misreported by the JVM) or calculated at runtime as:
 *
 *   availableMemory = containerTotalPhysicalMemory - JVM max heap - os-memory-needs
 *
 * `containerTotalPhysicalMemory` comes from [[java.lang.management.OperatingSystemMXBean]]
 * and reflects the total RAM visible to the container or host.
 * `JVM max heap` is `Runtime.getRuntime.maxMemory()` (-Xmx).
 * `os-memory-needs` is a configurable reserve for the OS (default 500 MB).
 *
 * == Memory pools ==
 * The available memory is divided into four named pools.  Their percents must sum to
 * exactly 100 (within a 0.001 floating-point tolerance):
 *
 *   lucene-memory-percent        – soft reserve for Lucene/Tantivy memory-mapped index
 *                                  files; not explicitly allocated, just left untouched
 *                                  so the OS page-cache can keep index pages warm.
 *   native-memory-manager-percent – explicitly allocated for the NativeMemoryManager,
 *                                   which holds partition keys, chunk maps, chunk infos,
 *                                   and ingestion write-buffers.
 *   block-memory-manager-percent  – explicitly allocated for the BlockMemoryManager,
 *                                   which stores encoded columnar chunks. This pool is
 *                                   shared across all datasets; each dataset's shard
 *                                   claims a slice (see below).
 *   flight-rpc-memory-percent     – explicitly allocated for the Arrow Flight root
 *                                   allocator. Must be non-zero when Flight is enabled.
 *
 * == Per-shard block memory ==
 * [[getPerShardBlockMemoryAllocSize]] divides the block-memory pool down to the shard
 * level using:
 *
 *   perShardBytes =
 *     availableMemory
 *       × (block-memory-manager-percent / 100)
 *       × (store.shard-mem-percent / 100)   // dataset's share of the block pool
 *       / ceil(numShards / min-num-nodes-in-cluster)
 *
 * `shard-mem-percent` allows multiple datasets on the same node to split the block
 * pool (e.g. 50 / 50 for two datasets).  `min-num-nodes-in-cluster` is used to infer
 * how many shards land on a typical node; ceiling is taken so nodes that receive an
 * extra shard are not over-provisioned.
 */
object AutoMemoryAllocUtil extends StrictLogging {

  def isAutoMemoryConfigEnabled(filodbConfig: Config): Boolean = {
    val enabled = filodbConfig.getBoolean("memstore.memory-alloc.automatic-alloc-enabled")
    if (enabled) {
      val nativeMemoryManagerPercent = filodbConfig.getDouble("memstore.memory-alloc.native-memory-manager-percent")
      val blockMemoryManagerPercent = filodbConfig.getDouble("memstore.memory-alloc.block-memory-manager-percent")
      val flightRpcMemoryPercent = filodbConfig.getDouble("memstore.memory-alloc.flight-rpc-memory-percent")
      val lucenePercent = filodbConfig.getDouble("memstore.memory-alloc.lucene-memory-percent")
      require(Math.abs(nativeMemoryManagerPercent + blockMemoryManagerPercent + lucenePercent +
        flightRpcMemoryPercent - 100) < 0.001,
        s"isAutoMemoryConfigEnabled but configured Native($nativeMemoryManagerPercent), " +
          s"Block($blockMemoryManagerPercent), Flight($flightRpcMemoryPercent) and " +
          s"Lucene($lucenePercent) memory percents don't sum to 100.0")
    }
    enabled
  }

  def getFlightRPCMemoryAllocSize(filodbConfig: Config): Long = {
    val availableMemoryBytes: Long = calculateAvailableOffHeapMemory(filodbConfig)
    val flightRpcMemoryPercent = filodbConfig.getDouble("memstore.memory-alloc.flight-rpc-memory-percent")
    (availableMemoryBytes * flightRpcMemoryPercent / 100).toLong
  }

  def getFlightServerMemoryAllocSize(filodbConfig: Config): Long = {
    (filodbConfig.getDouble("flight.server.fraction-allocator-limit") *
      getFlightRPCMemoryAllocSize(filodbConfig)).toLong
  }

  def getFlightClientMemoryAllocSize(filodbConfig: Config): Long = {
    (filodbConfig.getDouble("flight.client.fraction-allocator-limit") *
      getFlightRPCMemoryAllocSize(filodbConfig)).toLong
  }

  def getIngestionMemoryAllocSize(filodbConfig: Config): Long = {
    val availableMemoryBytes: Long = calculateAvailableOffHeapMemory(filodbConfig)
    val nativeMemoryManagerPercent = filodbConfig.getDouble("memstore.memory-alloc.native-memory-manager-percent")
    (availableMemoryBytes * nativeMemoryManagerPercent / 100).toLong
  }

  def getPerShardBlockMemoryAllocSize(filodbConfig: Config, numShards: Int,
                                      datasetRef: DatasetRef, storeConfig: StoreConfig): Long = {
    val numNodes = filodbConfig.getInt("min-num-nodes-in-cluster")
    val availableMemoryBytes: Long = calculateAvailableOffHeapMemory(filodbConfig)
    val blockMemoryManagerPercent = filodbConfig.getDouble("memstore.memory-alloc.block-memory-manager-percent")
    val blockMemForDatasetPercent = storeConfig.shardMemPercent // fraction of block memory for this dataset
    val numShardsPerNode = Math.ceil(numShards / numNodes.toDouble)
    logger.info(s"Calculating Block memory size with automatic allocation strategy. " +
      s"Dataset dataset=$datasetRef has blockMemForDatasetPercent=$blockMemForDatasetPercent " +
      s"numShardsPerNode=$numShardsPerNode")
    (availableMemoryBytes * blockMemoryManagerPercent *
      blockMemForDatasetPercent / 100 / 100 / numShardsPerNode).toLong
  }

  private lazy val containerMemory = ManagementFactory.getOperatingSystemMXBean()
    .asInstanceOf[com.sun.management.OperatingSystemMXBean].getTotalPhysicalMemorySize()
  private lazy val currentJavaHeapMemory = Runtime.getRuntime().maxMemory()

  private def calculateAvailableOffHeapMemory(filodbConfig: Config): Long = {
    require(isAutoMemoryConfigEnabled(filodbConfig), s"Automatic memory allocation is not enabled in config but" +
      s"calculateAvailableOffHeapMemory method was called")
    // If Xmx is not set, maxMemory() returns Long.MaxValue, which is not useful for calculating available memory.
    require(currentJavaHeapMemory < Long.MaxValue, s"Xmx was not set but auto memory configuration was enabled")
    val osMemoryNeeds = filodbConfig.getMemorySize("memstore.memory-alloc.os-memory-needs").toBytes
    logger.info(s"Detected available memory containerMemory=$containerMemory" +
      s" currentJavaHeapMemory=$currentJavaHeapMemory osMemoryNeeds=$osMemoryNeeds")

    logger.info(s"Memory Alloc Options: " +
      s"${filodbConfig.getConfig("memstore.memory-alloc").root().render(ConfigRenderOptions.concise())}")

    val availableMem = if (filodbConfig.hasPath("memstore.memory-alloc.available-memory-bytes")) {
      val avail = filodbConfig.getMemorySize("memstore.memory-alloc.available-memory-bytes").toBytes
      logger.info(s"Using automatic-memory-config using overridden memory-alloc.available-memory $avail")
      avail
    } else {
      logger.info(s"Using automatic-memory-config using without available memory override")
      containerMemory - currentJavaHeapMemory - osMemoryNeeds
    }
    logger.info(s"Available memory calculated or configured as $availableMem")
    availableMem
  }

}
