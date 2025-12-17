package filodb.core

import java.io.{File, IOException}
import java.lang.management.ManagementFactory

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import scala.util.{Failure, Try}

import filodb.core.metadata.Schemas

object Utils extends StrictLogging {
  private val threadMbean = ManagementFactory.getThreadMXBean
  private val cpuTimeEnabled = threadMbean.isCurrentThreadCpuTimeSupported && threadMbean.isThreadCpuTimeEnabled
  logger.info(s"Measurement of CPU Time Enabled: $cpuTimeEnabled")

  def currentThreadCpuTimeNanos: Long = {
    if (cpuTimeEnabled) threadMbean.getCurrentThreadCpuTime
    else System.nanoTime()
  }

  def calculateAvailableOffHeapMemory(filodbConfig: Config): Long = {
    // JDK 17/21 recommended approach: use getPlatformMXBean instead of casting
    val osMXBean = ManagementFactory.getPlatformMXBean(classOf[com.sun.management.OperatingSystemMXBean])
    val containerMemory = osMXBean.getTotalMemorySize
    val currentJavaHeapMemory = Runtime.getRuntime.maxMemory()
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

  /**
   * @param schemaName Source schema name.
   * @param schemaHash Source schema hash.
   * @param schemaNameToCheck the schema name to check against.
   * @param schemaHashToCheck the schema hash to check against.
   * @return true if the schema name and hash match or if the schemas are back compatible histograms
   */
  def doesSchemaMatchOrBackCompatibleHistograms(schemaName : String, schemaHash : Int,
                                                      schemaNameToCheck : String, schemaHashToCheck : Int) : Boolean = {
    if (schemaHash == schemaHashToCheck) { true }
    else {
      val sortedSchemas = Seq(schemaName, schemaNameToCheck).sortBy(_.length)
      val ret = if (
        (sortedSchemas(0) == Schemas.promHistogram.name) &&
        (sortedSchemas(1) == Schemas.otelCumulativeHistogram.name)
      ) true
      else if (
        (sortedSchemas(0) == Schemas.deltaHistogram.name) &&
        (sortedSchemas(1) == Schemas.otelDeltaHistogram.name)
      ) true
      else if (
        (sortedSchemas(0) == Schemas.preaggDeltaHistogram.name) &&
        (sortedSchemas(1) == Schemas.preaggOtelDeltaHistogram.name)
      ) true
      else false
      ret
    }
  }
}
