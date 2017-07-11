package filodb.core.store

import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import org.velvia.filo.{RowReader, FiloVector}
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.existentials

import filodb.core._
import filodb.core.Types.PartitionKey
import filodb.core.binaryrecord.{BinaryRecord, BinaryRecordWrapper}
import filodb.core.metadata.{Column, Projection, RichProjection}
import filodb.core.query.ChunkSetReader
import filodb.core.query.ChunkSetReader._

sealed trait PartitionScanMethod
final case class SinglePartitionScan(partition: PartitionKey) extends PartitionScanMethod
final case class MultiPartitionScan(partitions: Seq[PartitionKey]) extends PartitionScanMethod
final case class FilteredPartitionScan(split: ScanSplit,
                                       filter: PartitionKey => Boolean = (a: PartitionKey) => true)
    extends PartitionScanMethod

sealed trait ChunkScanMethod
case object AllChunkScan extends ChunkScanMethod
// NOTE: BinaryRecordWrapper must be used as this case class might be Java Serialized
final case class RowKeyChunkScan(firstBinKey: BinaryRecordWrapper,
                                 lastBinKey: BinaryRecordWrapper) extends ChunkScanMethod {
  def startkey: BinaryRecord = firstBinKey.binRec
  def endKey: BinaryRecord = lastBinKey.binRec
}
final case class SingleChunkScan(firstBinKey: BinaryRecordWrapper,
                                 chunkId: Types.ChunkID) extends ChunkScanMethod {
  def startkey: BinaryRecord = firstBinKey.binRec
}

object RowKeyChunkScan {
  def apply(startKey: BinaryRecord, endKey: BinaryRecord): RowKeyChunkScan =
    RowKeyChunkScan(BinaryRecordWrapper(startKey), BinaryRecordWrapper(endKey))
}

trait ScanSplit {
  // Should return a set of hostnames or IP addresses describing the preferred hosts for that scan split
  def hostnames: Set[String]
}

/**
 * High-level interface of a column store.  Writes and reads chunks, which are pretty high level.
 * Most implementations will probably want to use the ColumnStoreScanner, which implements the high level
 * read logic and offers useful lower level primitives.
 */
trait ColumnStore extends StrictLogging {
  import filodb.core.Types._
  import ChunkSetReader._
  import Iterators._

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
   * Internal method.  Reads chunks from a dataset and returns an Observable of chunk readers.
   * The implementation should do the work concurrently so that the current thread can be
   * used mostly for processing data.
   *
   * @param projection the Projection to read from
   * @param columns the set of columns to read back.  Order determines the order of columns read back
   *                in each row
   * @param version the version # to read from
   * @param partMethod which partitions to scan
   * @param chunkMethod which chunks within a partition to scan
   * @param colToMaker a function to translate a Column to a VectorFactory
   * @return an Observable of ChunkSetReaders
   */
  def readChunks(projection: RichProjection,
                 columns: Seq[Column],
                 version: Int,
                 partMethod: PartitionScanMethod,
                 chunkMethod: ChunkScanMethod = AllChunkScan,
                 colToMaker: ColumnToMaker = defaultColumnToMaker): Observable[ChunkSetReader]

  /**
   * Scans chunks from a dataset.  ScanMethod params determines what gets scanned.
   *
   * @param projection the Projection to read from
   * @param columns the set of columns to read back.  Order determines the order of columns read back
   *                in each row
   * @param version the version # to read from
   * @param partMethod which partitions to scan
   * @param chunkMethod which chunks within a partition to scan
   * @return An iterator over ChunkSetReaders
   */
  def scanChunks(projection: RichProjection,
                 columns: Seq[Column],
                 version: Int,
                 partMethod: PartitionScanMethod,
                 chunkMethod: ChunkScanMethod = AllChunkScan,
                 colToMaker: ColumnToMaker = defaultColumnToMaker): Iterator[ChunkSetReader] =
    readChunks(projection, columns, version, partMethod, chunkMethod, colToMaker).toIterator()

  /**
   * Scans over chunks, just like scanChunks, but returns an iterator of RowReader
   * for all of those row-oriented applications.  Contains a high performance iterator
   * implementation, probably faster than trying to do it yourself.  :)
   */
  def scanRows(projection: RichProjection,
               columns: Seq[Column],
               version: Int,
               partMethod: PartitionScanMethod,
               chunkMethod: ChunkScanMethod = AllChunkScan,
               colToMaker: ColumnToMaker = defaultColumnToMaker,
               readerFactory: RowReaderFactory = DefaultReaderFactory): Iterator[RowReader] = {
    val chunkIt = scanChunks(projection, columns, version, partMethod, chunkMethod, colToMaker)
    if (chunkIt.hasNext) {
      // TODO: fork this kind of code into a macro, called fastFlatMap.
      // That's what we really need...  :-p
      new Iterator[RowReader] {
        final def getNextRowIt: Iterator[RowReader] = {
          val chunkReader = chunkIt.next
          chunkReader.rowIterator(readerFactory)
        }

        var rowIt: Iterator[RowReader] = getNextRowIt

        final def hasNext: Boolean = {
          var _hasNext = rowIt.hasNext
          while (!_hasNext) {
            if (chunkIt.hasNext) {
              rowIt = getNextRowIt
              _hasNext = rowIt.hasNext
            } else {
              // all done. No more chunks.
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

  /**
   * Only read chunks corresponding to the row key columns.
   */
  def readRowKeyVectors(projection: RichProjection,
                        version: Int,
                        partition: PartitionKey,
                        startKey: BinaryRecord,
                        chunkId: Types.ChunkID): Array[FiloVector[_]] = {
    val chunkReaderIt = scanChunks(projection, projection.rowKeyColumns, version,
                                   SinglePartitionScan(partition),
                                   SingleChunkScan(BinaryRecordWrapper(startKey), chunkId))
    try {
      chunkReaderIt.toSeq.head.vectors
    } catch {
      case e: NoSuchElementException =>
        logger.error(s"Error: no row key chunks for $partition / ($startKey, $chunkId)", e)
        throw e
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
