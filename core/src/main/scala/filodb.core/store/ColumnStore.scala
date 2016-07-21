package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import kamon.trace.{TraceContext, Tracer}
import java.nio.ByteBuffer
import org.velvia.filo.RowReader
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.existentials

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}

sealed trait ScanMethod
case class SinglePartitionScan(partition: Any) extends ScanMethod
case class SinglePartitionRangeScan(keyRange: KeyRange[_, _]) extends ScanMethod
case class FilteredPartitionScan(split: ScanSplit,
                                 filter: Any => Boolean = (a: Any) => true) extends ScanMethod
case class FilteredPartitionRangeScan(split: ScanSplit,
                                      segmentRange: SegmentRange[_],
                                      filter: Any => Boolean = (a: Any) => true) extends ScanMethod
case class MultiPartitionScan(partitions: Seq[Any]) extends ScanMethod
case class MultiPartitionRangeScan(keyRanges: Seq[KeyRange[_, _]]) extends ScanMethod

trait ScanSplit {
  // Should return a set of hostnames or IP addresses describing the preferred hosts for that scan split
  def hostnames: Set[String]
}

/**
 * High-level interface of a column store.  Writes and reads segments, which are pretty high level.
 * Most implementations will probably want to use the ColumnStoreScanner, which implements much
 * of the read logic and gives lower level primitives.
 */
trait ColumnStore {
  import filodb.core.Types._
  import RowReaderSegment._

  def ec: ExecutionContext
  implicit val execContext = ec

  /**
   * Initializes the column store for a given dataset projection.  Must be called once before appending
   * segments to that projection.
   */
  def initializeProjection(projection: Projection): Future[Response]

  /**
   * Clears all data from the column store for that given projection, for all versions.
   * More like a truncation, not a drop.
   * NOTE: please make sure there are no reprojections or writes going on before calling this
   */
  def clearProjectionData(projection: Projection): Future[Response]

  /**
   * Completely and permanently drops the dataset from the column store.
   * @param dataset the DatasetRef for the dataset to drop.
   */
  def dropDataset(dataset: DatasetRef): Future[Response]

  /**
   * Appends the ChunkSets and incremental indices in the segment to the column store.
   * @param segment the ChunkSetSegment to write / merge to the columnar store
   * @param version the version # to write the segment to
   * @return Success. Future.failure(exception) otherwise.
   */
  def appendSegment(projection: RichProjection,
                    segment: ChunkSetSegment,
                    version: Int): Future[Response]

  /**
   * Scans segments from a dataset.  ScanMethod determines what gets scanned.
   * Not required to return entire segments - if the segment is too big this may break things up
   *
   * @param projection the Projection to read from
   * @param columns the set of columns to read back.  Order determines the order of columns read back
   *                in each row
   * @param version the version # to read from
   * @param method ScanMethod determining what to read from
   * @return An iterator over RowReaderSegment's
   */
  def scanSegments(projection: RichProjection,
                   columns: Seq[Column],
                   version: Int,
                   method: ScanMethod): Future[Iterator[Segment]]

  /**
   * Scans over segments, just like scanSegments, but returns an iterator of RowReader
   * for all of those row-oriented applications.  Contains a high performance iterator
   * implementation, probably faster than trying to do it yourself.  :)
   */
  def scanRows(projection: RichProjection,
               columns: Seq[Column],
               version: Int,
               method: ScanMethod,
               readerFactory: RowReaderFactory = DefaultReaderFactory): Future[Iterator[RowReader]] = {
    for { segmentIt <- scanSegments(projection, columns, version, method) }
    yield {
      if (segmentIt.hasNext) {
        // TODO: fork this kind of code into a macro, called fastFlatMap.
        // That's what we really need...  :-p
        new Iterator[RowReader] {
          final def getNextRowIt: Iterator[RowReader] = {
            val readerSeg = segmentIt.next.asInstanceOf[RowReaderSegment]
            readerSeg.rowIterator(readerFactory)
          }

          var rowIt: Iterator[RowReader] = getNextRowIt

          final def hasNext: Boolean = {
            var _hasNext = rowIt.hasNext
            while (!_hasNext) {
              if (segmentIt.hasNext) {
                rowIt = getNextRowIt
                _hasNext = rowIt.hasNext
              } else {
                // all done. No more segments.
                return false
              }
            }
            _hasNext
          }

          final def next: RowReader = rowIt.next.asInstanceOf[RowReader]
        }
      } else {
        Iterator.empty
      }
    }
  }

  /**
   * Determines how to split the scanning of a dataset across a columnstore.
   * @param dataset the name of the dataset to determine splits for
   * @param splitsPerNode the number of splits to target per node.  May not actually be possible.
   * @return a Seq[ScanSplit]
   */
  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit]

  /**
   * Resets the state of the column store, mostly used for testing
   */
  def reset(): Unit

  /**
   * Shuts down the ColumnStore, including any threads that might be hanging around
   */
  def shutdown(): Unit
}

case class ChunkedData(column: Types.ColumnId, chunks: Seq[(Types.SegmentId, Types.ChunkID, ByteBuffer)])
