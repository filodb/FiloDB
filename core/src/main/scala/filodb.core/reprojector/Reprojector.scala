package filodb.core.reprojector

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.store.{ColumnStore, RowWriterSegment, Segment, SegmentInfo}
import filodb.core.metadata.{Dataset, Column, RichProjection}

/**
 * The Reprojector flushes rows out of the MemTable and writes out Segments to the ColumnStore.
 * All of the work should be done asynchronously.
 * The reprojector should be stateless.  It takes MemTables and creates Futures for reprojection tasks.
 */
trait Reprojector {
  import RowReader._

  /**
   * Does reprojection (columnar flushes from memtable) for a single dataset.
   * Should completely flush all segments out of the locked memtable.
   * Throttling is achieved via passing in an ExecutionContext which limits the number of futures, and
   * sizing the thread pool appropriately -- see CoordinatorSetup for an example
   *
   * Failures:
   * The Scheduler only schedules one reprojection task at a time per (dataset, version), so if this fails,
   * then it can be rerun.
   * TODO: keep track of failures so we don't have to repeat successful segment writes.
   *
   * Most likely this will involve scheduling a whole bunch of futures to write segments.
   * Be careful to do too much work, newTask is supposed to not take too much CPU time and use Futures
   * to do work asynchronously.  Also, scheduling too many futures leads to long blocking time and
   * memory issues.
   *
   * @return a Future[Seq[String]], representing info from individual segment flushes.
   */
  def reproject(memTable: MemTable, version: Int): Future[Seq[String]]

  /**
   * A simple function that reads rows out of a memTable and converts them to segments.
   * Used by reproject(), separated out for ease of testing.
   */
  def toSegments(memTable: MemTable, segments: Seq[(Any, Any)]): Seq[Segment]
}

/**
 * Default reprojector, which scans the Locked memtable, turning them into segments for flushing,
 * using fixed segment widths
 *
 * ==Config==
 * {{{
 *   reprojector {
 *     retries = 3
 *     retry-base-timeunit = 5 s
 *   }
 * }}}
 */
class DefaultReprojector(config: Config, columnStore: ColumnStore)
                        (implicit ec: ExecutionContext) extends Reprojector with StrictLogging {
  import Types._
  import RowReader._

  val retries = config.getInt("reprojector.retries")
  val retryBaseTime = config.as[FiniteDuration]("reprojector.retry-base-timeunit")

  def toSegments(memTable: MemTable, segments: Seq[(Any, Any)]): Seq[Segment] = {
    val dataset = memTable.projection.dataset
    segments.map { case (partition, segmentKey) =>
      // For each segment grouping of rows... set up a Segment
      val segInfo = SegmentInfo(partition, segmentKey).basedOn(memTable.projection)
      val segment = new RowWriterSegment(memTable.projection, memTable.projection.columns)(segInfo)
      val segmentRowsIt = memTable.readRows(segInfo)
      logger.debug(s"Created new segment $segment for encoding...")

      // Group rows into chunk sized bytes and add to segment
      // NOTE: because RowReaders could be mutable, we need to keep this a pure Iterator.  Turns out
      // this is also more efficient than Iterator.grouped
      while (segmentRowsIt.nonEmpty) {
        segment.addRichRowsAsChunk(segmentRowsIt.take(dataset.options.chunkSize))
      }
      segment
    }
  }

  import markatta.futiles.Retry._

  def reproject(memTable: MemTable, version: Int): Future[Seq[String]] = {
    val projection = memTable.projection
    val datasetName = projection.datasetName
    val segments = toSegments(memTable, memTable.getSegments.toSeq)
    val segmentTasks = segments.map { segment =>
      for { resp <- retryWithBackOff(retries, retryBaseTime) {
                      columnStore.appendSegment(projection, segment, version)
                    } if resp == Success }
      yield {
        logger.info(s"Finished merging segment ${segment.segInfo}, version $version...")
        // Return useful info about each successful reprojection
        segment.segInfo.toString
      }
    }
    logger.info(s"Starting reprojection for dataset $datasetName, version $version")
    Future.sequence(segmentTasks)
  }
}
