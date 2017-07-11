package filodb.core.reprojector

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import java.nio.ByteBuffer
import kamon.Kamon
import kamon.trace.Tracer
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{RichProjection, Column}
import filodb.core.store._

/**
 * A cache for segment state objects for the write path.
 * When you retrieve current state for a segment it reads it from cache first.
 * @param config Configuration for the cache
 * @param columnStore a ColumnStore to read the segment state when it's not in the cache
 *
 * ==Configuration==
 * {{{
 *   reprojector {
 *     segment-cache-size = 1000
 *   }
 * }}}
 */
class SegmentStateCache(config: Config, columnStore: ColumnStore with ColumnStoreScanner)
                       (implicit ec: Scheduler) extends StrictLogging {
  import Perftools._
  import SegmentStateSettings._

  /**
   * One note on cache concurrency.  SegmentStateCache will be shared by multiple threads and actors
   * doing reprojections; however technically only one thread at a time should be accessing each segment.
   * This is because DatasetCoordinatorActors are each responsible for one dataset and only flushes
   * memtables one at a time.
   */
  val cacheSize = config.getInt("reprojector.segment-cache-size")
  val cache = concurrentCache[(DatasetRef, SegmentInfo[_, _]), SegmentState](cacheSize)
  logger.info(s"Starting SegmentStateCache with $cacheSize entries...")

  val waitTimeout = config.as[Option[FiniteDuration]]("reprojector.segment-index-read-timeout")
                          .getOrElse(15 seconds)

  val stateSettings = SegmentStateSettings(timeout = waitTimeout)

  private val cacheReads = Kamon.metrics.counter("segment-cache-reads")
  private val cacheMisses = Kamon.metrics.counter("segment-cache-misses")

  /**
   * Retrieves the SegmentState from cache or creates a new one using the projection, schema, and segInfo.
   * The new one will be recreated from the ColumnStore if that segment exists. If not, then a brand new
   * one is created entirely.
   */
  def getSegmentState(projection: RichProjection,
                      schema: Seq[Column],
                      version: Int)
                     (segInfo: SegmentInfo[projection.PK, projection.SK]): SegmentState = {
    cacheReads.increment
    val cacheKey = (projection.datasetRef, segInfo)
    cache.getOrElseUpdate(cacheKey, { case (ref: DatasetRef,
                                            segmentInfo: SegmentInfo[_, _]) =>
      cacheMisses.increment
      ColumnStoreSegmentState(projection, schema, version, columnStore, stateSettings)(segInfo)(ec)
    })
  }

  def clear(): Unit = {
    logger.info(s"Clearing out segment state cache....")
    cache.clear()
  }
}