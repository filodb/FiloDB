package filodb.core

import java.lang.management.ManagementFactory

import com.typesafe.config.{Config, ConfigRenderOptions}
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

  /**
   * Given a sorted sequence, returns the index of the first
   *   occurrence of the argument element.
   */
  def findFirstIndexSorted[T](sorted: Seq[T], elt: T)(implicit ord: Ordering[T]): Int = {
    var ileft = 0;
    var iright = sorted.size - 1;
    while (ileft < iright) {
      val imid = (ileft + iright) / 2
      val midValue = sorted(imid)
      if (ord.gt(elt, midValue)) {
        ileft = imid + 1
      } else {
        iright = imid
      }
    }
    if (sorted(ileft) == elt) ileft else -1
  }

  /**
   * Given a sorted sequence, returns the index of the last
   * occurrence of the argument element.
   */
  def findLastIndexSorted[T](sorted: Seq[T], elt: T)(implicit ord: Ordering[T]): Int = {
    var ileft = 0;
    var iright = sorted.size - 1;
    while (ileft < iright) {
      val imid = (ileft + iright + 1) / 2
      val midValue = sorted(imid)
      if (ord.lt(elt, midValue)) {
        iright = imid - 1
      } else {
        ileft = imid
      }
    }
    if (sorted(ileft) == elt) ileft else -1
  }
}
