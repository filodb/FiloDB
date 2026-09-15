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

  lazy val compatibleMetricTypes: Set[Set[Int]] = Set(
    Set(Schemas.promHistogram.schemaHash, Schemas.otelCumulativeHistogram.schemaHash,
      Schemas.deltaHistogram.schemaHash, Schemas.otelDeltaHistogram.schemaHash),
    Set(Schemas.preaggDeltaHistogram.schemaHash, Schemas.preaggOtelDeltaHistogram.schemaHash),
    Set(Schemas.promCounter.schemaHash, Schemas.deltaCounter.schemaHash)
  )

  /**
   * Checks if the two schema ids are compatible metric types for spatial aggregation.
   * Typically used to avoid unintended aggregations over incompatible types like counters and gauges,
   * or histograms and counters etc.
   * @param schemaId Source schema id.
   * @param schemaIdToCheck other schema id
   * @return true if the schema hash match or if the schemas are compatible histograms or counters
   */
  def areCompatibleMetricTypes(schemaId : Int,
                               schemaIdToCheck : Int) : Boolean = {

    if (schemaId == schemaIdToCheck) true
    else compatibleMetricTypes.exists( s => s.contains(schemaId) && s.contains(schemaIdToCheck))
  }
}
