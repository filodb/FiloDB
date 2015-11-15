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
   * Should flush all segments out of the memtable designated by the keyRanges.
   *
   * Throttling is achieved via passing in an ExecutionContext which limits the number of futures, and
   * sizing the thread pool appropriately -- see CoordinatorSetup for an example
   *
   * Most likely this will involve scheduling a whole bunch of futures to write segments.
   * Be careful to do too much work, newTask is supposed to not take too much CPU time and use Futures
   * to do work asynchronously.  Also, scheduling too many futures leads to long blocking time and
   * memory issues.
   *
   * @param memTable the MemTable to read from.  Assumed to be immutable.
   * @param version the version to write out to
   * @param keyRanges the list of keyRanges/segments to write to, from SegmentChopper.keyRanges() method.
   *                Could also be used in retries to limit the keyRanges to the ones needing retry.
   *                Should be in ascending partition/sortKey order, the same as rows in MemTable.
   *                Also, should not be any extraneous keyRanges, normally should be one covering every row.
   * @return a Future[Seq[KeyRange[K]]], representing successfully reprojected KeyRanges.
   */
  def reproject[K](memTable: MemTable[K], version: Int, keyRanges: Seq[KeyRange[K]]): Future[Seq[KeyRange[K]]]

  /**
   * A function that reads rows out of a memTable for a particular partition and
   * converts them to segments, based strictly on the list of keyRanges.
   *
   * @param memTable the MemTable to read from.  Assumed to be immutable.
   * @param keyRanges the list of keyRanges to write to. Rows not contained in any keyRange will be skipped.
   * @return an Iterator for segments
   */
  def toSegments[K](memTable: MemTable[K], keyRanges: Seq[KeyRange[K]]): Iterator[Segment[K]]
}

/**
 * Default reprojector, takes rows from the memTable delineated by specific keyRanges,
 * creates binary flushable segments from them and flushes them to the column store.
 */
class DefaultReprojector(columnStore: ColumnStore)
                        (implicit ec: ExecutionContext) extends Reprojector with StrictLogging {
  import Types._
  import RowReader._
  import filodb.core.Iterators._

  def toSegments[K](memTable: MemTable[K], keyRanges: Seq[KeyRange[K]]): Iterator[Segment[K]] = {
    val dataset = memTable.projection.dataset
    implicit val helper = memTable.projection.helper
    val sortKeyFunc = memTable.projection.sortKeyFunc

    keyRanges.toIterator.map { keyRange =>
      val segmentRowsIt = memTable.readRows(keyRange)

      // For each segment grouping of rows... set up a Segment
      val segment = new RowWriterSegment(keyRange, memTable.projection.columns)
      logger.debug(s"Created new segment $segment for encoding...")

      // Group rows into chunk sized bytes and add to segment
      segmentRowsIt.grouped(dataset.options.chunkSize).foreach { chunkRows =>
        // NOTE: chunkRows is a Seq, so beware this will not work with FastFiloRowReaders which use
        // mutation.  Instead, you need some kind of grouping which is a pure Iterator.  Or maybe
        // just write out everything in the segment as one huge chunk.  :-p
        segment.addRowsAsChunk(chunkRows.toIterator, sortKeyFunc)
      }
      segment
    }
  }

  def reproject[K](memTable: MemTable[K], version: Int, keyRanges: Seq[KeyRange[K]]):
      Future[Seq[KeyRange[K]]] = {
    val projection = memTable.projection
    val datasetName = projection.dataset.name
    val segments = toSegments(memTable, keyRanges)
    val segmentTasks = segments.map { segment =>
      for { resp <- columnStore.appendSegment(projection, segment, version) if resp == Success }
      yield {
        logger.info(s"Finished merging segment ${segment.keyRange}, version $version...")
        segment.keyRange
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
