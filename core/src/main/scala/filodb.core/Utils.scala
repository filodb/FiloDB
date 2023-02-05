package filodb.core

import java.lang.management.ManagementFactory

import com.typesafe.scalalogging.StrictLogging

object Utils extends StrictLogging {
  private val threadMbean = ManagementFactory.getThreadMXBean
  private val cpuUserTimeEnabled = threadMbean.isCurrentThreadCpuTimeSupported && threadMbean.isThreadCpuTimeEnabled
  logger.info(s" CPU User Time Enabled: $cpuUserTimeEnabled")

  def currentCpuUserTimeNanos: Long = {
    if (cpuUserTimeEnabled) threadMbean.getCurrentThreadCpuTime
    else System.nanoTime()
  }
}
