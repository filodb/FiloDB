package filodb.core

import java.io.{File, IOException}
import java.lang.management.ManagementFactory

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import scala.util.{Failure, Try}

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

  // Recursively delete a folder
  def deleteRecursively(f: File, deleteRoot: Boolean = false): Try[Boolean] = {
    val subDirDeletion: Try[Boolean] =
      if (f.isDirectory)
        f.listFiles match {
          case xs: Array[File] if xs != null && !xs.isEmpty =>
            val subDirDeletions: Array[Try[Boolean]] = xs map (f => deleteRecursively(f, true))
            subDirDeletions reduce ((reduced, thisOne) => {
              thisOne match {
                // Ensures even if one Right(_) is found, thr response will be Right(Throwable)
                case scala.util.Success(_) if reduced == scala.util.Success(true) => thisOne
                case Failure(_) => thisOne
                case _ => reduced
              }
            })
          case _ => scala.util.Success(true)
        }
      else
        scala.util.Success(true)

    subDirDeletion match {
      case scala.util.Success(_) =>
        if (deleteRoot) {
          if (f.delete()) scala.util.Success(true) else Failure(new IOException(s"Unable to delete $f"))
        } else scala.util.Success(true)
      case right@Failure(_) => right
    }

  }
}
