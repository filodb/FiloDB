package filodb.core.reprojector

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import kamon.trace.Tracer
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.store.{ColumnStore, ChunkSetSegment, Segment, SegmentInfo}
import filodb.core.metadata.{Dataset, Column, RichProjection}

/**
 * The Reprojector flushes rows out of the MemTable and writes out Segments to the ColumnStore.
 * All of the work should be done asynchronously.
 * It takes MemTables and creates Futures for reprojection tasks.
 */
trait Reprojector {
  import RowReader._

  /**
   * Does reprojection (columnar flushes from memtable) for a single dataset.
   * Should completely flush all segments out of the memtable.
   * Throttling is achieved by writing only segment-batch-size segments at a time, and not starting
   * the segment creation/appending of the next batch until the previous batch is done.
   *
   * NOTE: using special ExecutionContexts to throttle is a BAD idea.  The segment append is too complex
   * and its too easy to get into deadlock situations, plus it doesn't throttle memory use.
   *
   * @return a Future[Seq[SegmentInfo]], representing successful segment flushes
   */
  def reproject(memTable: MemTable, version: Int): Future[Seq[SegmentInfo[_, _]]]

  /**
   * A simple function that reads rows out of a memTable and converts them to segments.
   * Used by reproject(), separated out for ease of testing.
   */
  def toSegments(memTable: MemTable, segments: Seq[(Any, Any)], version: Int): Seq[ChunkSetSegment]

  protected def printSegInfos(infos: Seq[(Any, Any)]): String = {
    val ellipsis = if (infos.length > 3) Seq("...") else Nil
    val infoStrings = (infos.take(3).map(_.toString) ++ ellipsis).mkString(", ")
    s"${infos.length} segments: [$infoStrings]"
  }
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
 *     segment-batch-size = 64
 *   }
 * }}}
 */
class DefaultReprojector(config: Config,
                         columnStore: ColumnStore,
                         stateCache: SegmentStateCache)
                        (implicit ec: ExecutionContext) extends Reprojector with StrictLogging {
  import Types._
  import RowReader._
  import Perftools._

  val retries = config.getInt("reprojector.retries")
  val retryBaseTime = config.as[FiniteDuration]("reprojector.retry-base-timeunit")
  val segmentBatchSize = config.getInt("reprojector.segment-batch-size")

  def toSegments(memTable: MemTable, segments: Seq[(Any, Any)], version: Int): Seq[ChunkSetSegment] = {
    val dataset = memTable.projection.dataset
    segments.map { case (partition, segmentKey) =>
      Tracer.withNewContext("serialize-segment", true) {
        // For each segment grouping of rows... set up a Segment
        val segInfo = SegmentInfo(partition, segmentKey).basedOn(memTable.projection)
        val state = subtrace("get-segment-state", "ingestion") {
          stateCache.getSegmentState(memTable.projection, memTable.projection.columns, version)(segInfo)
        }
        val segment = new ChunkSetSegment(memTable.projection, segInfo)
        val segmentRowsIt = memTable.safeReadRows(segInfo)
        logger.debug(s"Created new segment ${segment.segInfo} for encoding...")

        // Group rows into chunk sized bytes and add to segment
        subtrace("add-chunk-set", "ingestion") {
          while (segmentRowsIt.nonEmpty) {
            segment.addChunkSet(state, segmentRowsIt.take(dataset.options.chunkSize).toSeq)
          }
        }
        segment
      }
    }
  }

  import markatta.futiles.Retry._
  import markatta.futiles.Traversal._

  def reproject(memTable: MemTable, version: Int): Future[Seq[SegmentInfo[_, _]]] = {
    val projection = memTable.projection
    val datasetName = projection.datasetName

    // First, group the segments into batches for throttling
    val batches = memTable.getSegments.grouped(segmentBatchSize).toSeq

    // Now, flush each batch sequentially.  Nice thing is this returns after at most one batch.
    foldLeftSequentially(batches)(Seq.empty[SegmentInfo[_, _]]) { case (acc, segmentBatch) =>
      val segInfos = printSegInfos(segmentBatch)
      logger.info(s"Reprojecting dataset ($datasetName, $version): $segInfos")
      val futures = toSegments(memTable, segmentBatch, version).map { segment =>
        retryWithBackOff(retries, retryBaseTime) {
          columnStore.appendSegment(projection, segment, version)
        }.map { resp => segment.segInfo.asInstanceOf[SegmentInfo[_, _]] }
      }
      Future.sequence(futures).map { successSegs =>
        logger.info(s"  >> Succeeded ($datasetName, $version): $segInfos")
        acc ++ successSegs
      }
    }
  }
}
