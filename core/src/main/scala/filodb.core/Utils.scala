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
    if (filodbConfig.hasPath("memstore.memory-alloc.available-memory")) {
      filodbConfig.getMemorySize("memstore.memory-alloc.available-memory").toBytes
    } else {
      val containerMemory = ManagementFactory.getOperatingSystemMXBean()
        .asInstanceOf[com.sun.management.OperatingSystemMXBean].getTotalPhysicalMemorySize()
      val currentJavaHeapMemory = Runtime.getRuntime().maxMemory()
      val osMemoryNeeds = filodbConfig.getMemorySize("memstore.memstore.os-memory-needs").toBytes
      containerMemory - currentJavaHeapMemory - osMemoryNeeds
      // TODO info logging
    }
  }


}
