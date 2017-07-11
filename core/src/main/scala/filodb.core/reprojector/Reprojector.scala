package filodb.core.reprojector

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.trace.Tracer
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.Types.PartitionKey
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
  def toSegments(memTable: MemTable, partitions: Seq[PartitionKey], version: Int): Seq[ChunkSetSegment]

  def clear(): Unit = {}

  protected def printPartitions(parts: Seq[PartitionKey]): String = {
    val ellipsis = if (parts.length > 3) Seq("...") else Nil
    val infoStrings = (parts.take(3).map(_.toString) ++ ellipsis).mkString(", ")
    s"${parts.length} partitions: [$infoStrings]"
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
  val detectSkips = !config.getBoolean("reprojector.bulk-write-mode")

  def toSegments(memTable: MemTable, partitions: Seq[PartitionKey], version: Int): Seq[ChunkSetSegment] = {
    val dataset = memTable.projection.dataset
    partitions.map { partition =>
      Tracer.withNewContext("serialize-segment", true) {
        // For each segment grouping of rows... set up a Segment
        val segInfo = SegmentInfo(partition, "").basedOn(memTable.projection)
        val state = subtrace("get-segment-state", "ingestion") {
          stateCache.getSegmentState(memTable.projection,
                                     memTable.projection.columns,
                                     version)(segInfo)
        }
        val segment = new ChunkSetSegment(memTable.projection, segInfo)
        val segmentRowsIt = memTable.safeReadRows(partition)
        logger.debug(s"Created new segment ${segment.segInfo} for encoding...")

        // Group rows into chunk sized bytes and add to segment
        subtrace("add-chunk-set", "ingestion") {
          while (segmentRowsIt.nonEmpty) {
            segment.addChunkSet(state, segmentRowsIt.take(dataset.options.chunkSize), detectSkips)
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

    val partitions = memTable.partitions.toBuffer
    val segInfos = printPartitions(partitions)
    logger.info(s"Reprojecting dataset ($datasetName, $version): $segInfos")

    // Serialize one segment at a time.  This takes longer than the actual column flush, which is async,
    // so basically this future thread will intersperse writes in between serializations.
    // At same time, do everything in a separate thread so caller won't be blocked
    Future {
      partitions.map { partition =>
        val segment = toSegments(memTable, Seq(partition), version).head
        retryWithBackOff(retries, retryBaseTime) {
          columnStore.appendSegment(projection, segment, version)
        }.map { resp => segment.segInfo.asInstanceOf[SegmentInfo[_, _]] }
      }
    }.flatMap { futures =>
      Future.sequence(futures).map { successSegs =>
        logger.info(s"  >> Succeeded ($datasetName, $version): $segInfos")
        successSegs
      }
    }
  }

  override def clear(): Unit = { stateCache.clear() }
}
