package filodb.core.reprojector

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import kamon.Kamon
import kamon.trace.Tracer
import net.ceedubs.ficus.Ficus._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{RichProjection, Column}
import filodb.core.store.{ColumnStoreScanner, SegmentInfo, SegmentState, SinglePartitionRangeScan}

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
class SegmentStateCache(config: Config, columnStore: ColumnStoreScanner)
                       (implicit ec: ExecutionContext) extends StrictLogging {
  import Perftools._

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

  private val cacheReads = Kamon.metrics.counter("segment-cache-reads")
  private val cacheMisses = Kamon.metrics.counter("segment-cache-misses")

  /**
   * Retrieves the SegmentState from cache or creates a new one using the projection, schema, and segInfo.
   * The new one will be recreated from the ColumnStore if that segment exists. If not, then a brand new
   * one is created entirely.
   */
  def getSegmentState(projection: RichProjection, schema: Seq[Column], version: Int)
                     (segInfo: SegmentInfo[projection.PK, projection.SK]): SegmentState = {
    cacheReads.increment
    val cacheKey = (projection.datasetRef, segInfo)
    cache.getOrElseUpdate(cacheKey, { case (ref: DatasetRef,
                                            segmentInfo: SegmentInfo[_, _]) =>
      cacheMisses.increment
      val range = KeyRange(segInfo.partition, segInfo.segment, segInfo.segment, endExclusive = false)
      logger.debug(s"Retrieving segment state from column store: $range")
      val indexRead = columnStore.scanIndices(projection, version, SinglePartitionRangeScan(range))
      val indexIt = Await.result(indexRead, waitTimeout)
      val infosAndSkips = indexIt.toSeq.headOption.map(_.infosAndSkips).getOrElse(Nil)
      new SegmentState(projection, schema, infosAndSkips.map(_._1),
                       columnStore, version, waitTimeout)(segInfo)
    })
  }

  def clear(): Unit = {
    logger.info(s"Clearing out segment state cache....")
    cache.clear()
  }
}