package filodb.core

import java.lang.management.ManagementFactory

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

object Utils extends StrictLogging {
  private val threadMbean = ManagementFactory.getThreadMXBean
  private val cpuTimeEnabled = threadMbean.isCurrentThreadCpuTimeSupported && threadMbean.isThreadCpuTimeEnabled
  logger.info(s"Measurement of CPU Time Enabled: $cpuTimeEnabled")

  def currentThreadCpuTimeNanos: Long = {
    if (cpuTimeEnabled) threadMbean.getCurrentThreadCpuTime
    else System.nanoTime()
  }

  def calculateAvailableOffHeapMemory(filodbConfig: Config): Long = {
    val availableMem = if (filodbConfig.hasPath("memstore.memory-alloc.available-memory")) {
      logger.info("Using automatic-memory-config using overridden memory-alloc.available-memory")
      filodbConfig.getMemorySize("memstore.memory-alloc.available-memory").toBytes
    } else {
      val containerMemory = ManagementFactory.getOperatingSystemMXBean()
        .asInstanceOf[com.sun.management.OperatingSystemMXBean].getTotalPhysicalMemorySize()
      val currentJavaHeapMemory = Runtime.getRuntime().maxMemory()
      val osMemoryNeeds = filodbConfig.getMemorySize("memstore.memstore.os-memory-needs").toBytes
      logger.info(s"Using automatic-memory-config using detected available memory containerMemory=$containerMemory" +
        s" currentJavaHeapMemory=$currentJavaHeapMemory osMemoryNeeds=$osMemoryNeeds")
      containerMemory - currentJavaHeapMemory - osMemoryNeeds
    }
    logger.info(s"Available memory calculated or configured as $availableMem")
    availableMem
  }

}
