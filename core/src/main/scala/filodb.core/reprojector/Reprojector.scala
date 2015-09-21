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
  import MemTable.IngestionSetup
  import RowReader._

  /**
   * Does reprojection (columnar flushes from memtable) for a single dataset.
   * Should be completely stateless.
   * Does not need to reproject all the rows from the Locked memtable, but should progressively
   * delete rows from the memtable until there are none left.  This is how the reprojector marks progress:
   * by deleting rows that has been committed to ColumnStore.
   *
   * Failures:
   * The Scheduler only schedules one reprojection task at a time per (dataset, version), so if this fails,
   * then it can be rerun.
   *
   * Most likely this will involve scheduling a whole bunch of futures to write segments.
   * Be careful to do too much work, newTask is supposed to not take too much CPU time and use Futures
   * to do work asynchronously.  Also, scheduling too many futures leads to long blocking time and
   * memory issues.
   *
   * @returns a Future[Seq[String]], representing info from individual segment flushes.
   */
  def newTask(memTable: MemTable, dataset: Types.TableName, version: Int): Future[Seq[String]] = {
    import Column.ColumnType._

    val setup = memTable.getIngestionSetup(dataset, version).getOrElse(
                  throw new IllegalArgumentException(s"Could not find $dataset/$version"))
    setup.schema(setup.sortColumnNum).columnType match {
      case LongColumn    => reproject[Long](memTable, setup, version)
      case IntColumn     => reproject[Int](memTable, setup, version)
      case DoubleColumn  => reproject[Double](memTable, setup, version)
      case other: Column.ColumnType => throw new RuntimeException("Illegal sort key type $other")
    }
  }

  // The inner, typed reprojection task launcher that must be implemented.
  def reproject[K: TypedFieldExtractor](memTable: MemTable, setup: IngestionSetup, version: Int):
      Future[Seq[String]]
}

/**
 * Default reprojector, which scans the Locked memtable, turning them into segments for flushing,
 * using fixed segment widths
 *
 * @param numSegments the number of segments to reproject for each reprojection task.
 */
class DefaultReprojector(columnStore: ColumnStore,
                         numSegments: Int = 3)
                        (implicit ec: ExecutionContext) extends Reprojector with StrictLogging {
  import MemTable._
  import Types._
  import RowReader._
  import filodb.core.Iterators._

  // PERF/TODO: Maybe we should pass in an Iterator[RowReader], and extract partition and sort keys
  // out.  Heck we could create a custom FiloRowReader which has methods to extract this out.
  // Might be faster than creating a Tuple3 for every row... or not, for complex sort and partition keys
  def chunkize[K](rows: Iterator[(PartitionKey, K, RowReader)],
                  setup: IngestionSetup): Iterator[Segment[K]] = {
    implicit val helper = setup.helper[K]
    rows.sortedGroupBy { case (partition, sortKey, row) =>
      // lazy grouping of partition/segment from the sortKey
      (partition, helper.getSegment(sortKey))
    }.map { case ((partition, (segStart, segUntil)), segmentRowsIt) =>
      // For each segment grouping of rows... set up a Segment
      val keyRange = KeyRange(setup.dataset.name, partition, segStart, segUntil)
      val segment = new RowWriterSegment(keyRange, setup.schema)
      logger.debug(s"Created new segment $segment for encoding...")

      // Group rows into chunk sized bytes and add to segment
      segmentRowsIt.grouped(setup.dataset.options.chunkSize).foreach { chunkRowsIt =>
        val chunkRows = chunkRowsIt.toSeq
        segment.addRowsAsChunk(chunkRows)
      }
      segment
    }.take(numSegments)
  }

  def reproject[K: TypedFieldExtractor](memTable: MemTable, setup: IngestionSetup, version: Int):
      Future[Seq[String]] = {
    implicit val helper = setup.helper[K]
    val datasetName = setup.dataset.name
    val projection = RichProjection(setup.dataset, setup.schema)
    val segments = chunkize(memTable.readAllRows[K](datasetName, version, Locked), setup)
    val segmentTasks: Seq[Future[String]] = segments.map { segment =>
      for { resp <- columnStore.appendSegment(projection, segment, version) if resp == Success }
      yield {
        logger.debug(s"Finished merging segment ${segment.keyRange}, version $version...")
        memTable.removeRows(segment.keyRange, version)
        logger.debug(s"Removed rows for segment $segment from Locked table...")
        // Return useful info about each successful reprojection
        (segment.keyRange.partition, segment.keyRange.start).toString
      }
    }.toSeq
    Future.sequence(segmentTasks)
  }
}
