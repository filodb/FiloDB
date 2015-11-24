package filodb.core.reprojector

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.RowReader
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.columnstore.{ColumnStore, RowWriterSegment, Segment}
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
  def reproject[K](memTable: MemTable[K], version: Int): Future[Seq[String]]

  /**
   * A simple function that reads rows out of a memTable and converts them to segments.
   * Used by reproject(), separated out for ease of testing.
   */
  def toSegments[K](memTable: MemTable[K]): Iterator[Segment[K]]
}

/**
 * Default reprojector, which scans the Locked memtable, turning them into segments for flushing,
 * using fixed segment widths
 */
class DefaultReprojector(columnStore: ColumnStore)
                        (implicit ec: ExecutionContext) extends Reprojector with StrictLogging {
  import Types._
  import RowReader._
  import filodb.core.Iterators._

  // PERF/TODO: Maybe we should pass in an Iterator[RowReader], and extract partition and sort keys
  // out.  Heck we could create a custom FiloRowReader which has methods to extract this out.
  // Might be faster than creating a Tuple3 for every row... or not, for complex sort and partition keys
  def toSegments[K](memTable: MemTable[K]): Iterator[Segment[K]] = {
    val dataset = memTable.projection.dataset
    implicit val helper = memTable.projection.helper
    val rows = memTable.readAllRows()
    rows.sortedGroupBy { case (partition, sortKey, row) =>
      // lazy grouping of partition/segment from the sortKey
      (partition, helper.getSegment(sortKey))
    }.map { case ((partition, (segStart, segUntil)), segmentRowsIt) =>
      // For each segment grouping of rows... set up a Segment
      val keyRange = KeyRange(dataset.name, partition, segStart, segStart)
      val segment = new RowWriterSegment(keyRange, memTable.projection.columns)
      logger.debug(s"Created new segment $segment for encoding...")

      // Group rows into chunk sized bytes and add to segment
      // NOTE: because RowReaders could be mutable, we need to keep this a pure Iterator.  Turns out
      // this is also more efficient than Iterator.grouped
      while (segmentRowsIt.nonEmpty) {
        segment.addRowsAsChunk(segmentRowsIt.take(dataset.options.chunkSize))
      }
      segment
    }
  }

  def reproject[K](memTable: MemTable[K], version: Int): Future[Seq[String]] = {
    val projection = memTable.projection
    val datasetName = projection.dataset.name
    val segments = toSegments(memTable)
    val segmentTasks = segments.map { segment =>
      for { resp <- columnStore.appendSegment(projection, segment, version) if resp == Success }
      yield {
        logger.info(s"Finished merging segment ${segment.keyRange}, version $version...")
        // Return useful info about each successful reprojection
        (segment.keyRange.partition, segment.keyRange.start).toString
      }
    }
    logger.info(s"Starting reprojection for dataset $datasetName, version $version")
    for { tasks <- Future { segmentTasks }
          results <- Future.sequence(tasks.toList) }
    yield {
      results
    }
  }
}
