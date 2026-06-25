package filodb.core.memstore

import java.lang.management.ManagementFactory

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef
import filodb.core.store.StoreConfig

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
        s"isAutoMemoryConfigEnabled but configured Block($nativeMemoryManagerPercent), " +
          s"Native($blockMemoryManagerPercent), Flight($flightRpcMemoryPercent) and " +
          s"Lucene($lucenePercent) memory percents don't sum to 100.0")
    }
    enabled
  }

  def getFlightRPCMemoryAllocSize(filodbConfig: Config): Long = {
    val availableMemoryBytes: Long = calculateAvailableOffHeapMemory(filodbConfig)
    val flightRpcMemoryPercent = filodbConfig.getDouble("memstore.memory-alloc.flight-rpc-memory-percent")
    (availableMemoryBytes * flightRpcMemoryPercent / 100).toLong
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

  private def calculateAvailableOffHeapMemory(filodbConfig: Config): Long = {
    val containerMemory = ManagementFactory.getOperatingSystemMXBean()
      .asInstanceOf[com.sun.management.OperatingSystemMXBean].getTotalPhysicalMemorySize()
    val currentJavaHeapMemory = Runtime.getRuntime().maxMemory()
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
