package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import kamon.trace.{TraceContext, Tracer}
import java.nio.ByteBuffer
import org.velvia.filo.RowReader
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.existentials
import spray.caching._

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
 * Most implementations will probably want to be based on something like the CachedmergingColumnStore
 * below, which implements much of the business logic and gives lower level primitives.  This trait
 * exists though to allow special implementations that want to use different lower level primitives.
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
   * Appends the segment to the column store.  The passed in segment must be somehow merged with an existing
   * segment to produce a new segment that has combined data such that rows with new unique primary keys
   * are appended and rows with existing primary keys will overwrite.  Also, the sort order must somehow be
   * preserved such that the chunk/row# in the ChunkRowMap can be read out in sort key order.
   * @param segment the partial Segment to write / merge to the columnar store
   * @param version the version # to write the segment to
   * @return Success. Future.failure(exception) otherwise.
   */
  def appendSegment(projection: RichProjection,
                    segment: Segment,
                    version: Int): Future[Response]

  /**
   * Scans segments from a dataset.  ScanMethod determines what gets scanned.
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

/**
 * A partial implementation of a ColumnStore, based on separating storage of chunks and ChunkRowMaps,
 * use of a segment cache to speed up merging, and a ChunkMergingStrategy to merge segments.  It defines
 * lower level primitives and implements the ColumnStore methods in terms of these primitives.
 */
trait CachedMergingColumnStore extends ColumnStore with ColumnStoreScanner with StrictLogging {
  import filodb.core.Types._
  import filodb.core.Iterators._
  import filodb.core.Perftools._

  def segmentCache: Cache[Segment]

  def mergingStrategy: ChunkMergingStrategy

  // This ExecutionContext is the default used for writing, it should have bounds set
  implicit val ec: ExecutionContext

  protected val stats = ColumnStoreStats()

  def clearProjectionData(projection: Projection): Future[Response] = {
    // Clear out any entries from segmentCache first
    logger.info(s"Clearing out segment cache for dataset ${projection.dataset}")
    segmentCache.keys.foreach { case key @ (dataset, _, _, _) =>
      if (dataset == projection.dataset) segmentCache.remove(key)
    }
    logger.info(s"Clearing all columnar projection data for dataset ${projection.dataset}")
    clearProjectionDataInner(projection)
  }

  def clearProjectionDataInner(projection: Projection): Future[Response]

  /**
   *  == Lower level storage engine primitives ==
   */

  /**
   * Writes chunks to underlying storage.
   * @param chunks an Iterator over triples of (columnName, chunkId, chunk bytes)
   * @return Success. Future.failure(exception) otherwise.
   */
  def writeChunks(dataset: DatasetRef,
                  partition: BinaryPartition,
                  version: Int,
                  segmentId: SegmentId,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response]

  def writeChunkRowMap(dataset: DatasetRef,
                       partition: BinaryPartition,
                       version: Int,
                       segmentId: SegmentId,
                       chunkRowMap: ChunkRowMap): Future[Response]

  /**
   * == Caching and merging implementation of the high level functions ==
   */
  def reset(): Unit = { clearSegmentCache() }

  def clearSegmentCache(): Unit = { segmentCache.clear() }

  def appendSegment(projection: RichProjection,
                    segment: Segment,
                    version: Int): Future[Response] = Tracer.withNewContext("append-segment") {
    val ctx = Tracer.currentContext
    stats.segmentAppend()
    if (segment.isEmpty) {
      stats.segmentEmpty()
      return(Future.successful(NotApplied))
    }
    for { oldSegment <- getSegFromCache(projection.toRowKeyOnlyProjection, segment, version)
          mergedSegment = subtrace( "segment-index-merge", "ingestion",ctx=Option(ctx)) {
                            mergingStrategy.mergeSegments(oldSegment, segment) }
          writeChunksResp <- writeBatchedChunks(projection.datasetRef, version, mergedSegment, ctx)
            if writeChunksResp == Success
          writeCRMapResp <- asyncSubtrace( "write-index", "ingest-io",ctx=Option(ctx)) {
                              writeChunkRowMap(projection.datasetRef, segment.binaryPartition, version,
                                               segment.segmentId, mergedSegment.index) }
    } yield {
      // Important!  Update the cache with the new merged segment.
      updateCache(projection, version, mergedSegment)
      ctx.finish()
      writeCRMapResp
    }
  }

  def chunkBatchSize: Int

  private def writeBatchedChunks(dataset: DatasetRef,
                                 version: Int,
                                 segment: Segment,
                                 ctx: TraceContext): Future[Response] = {
    val binPartition = segment.binaryPartition
    asyncSubtrace("write-chunks", "ingest-io", ctx=Option(ctx)) {
      Future.traverse(segment.getChunks.grouped(chunkBatchSize).toSeq) { chunks =>
        writeChunks(dataset, binPartition, version, segment.segmentId, chunks.toIterator)
      }.map { responses => responses.head }
    }
  }

  private def getSegFromCache(projection: RichProjection,
                              segment: Segment,
                              version: Int): Future[Segment] =
    asyncSubtrace(name="index-read", category="ingest-io") {
    segmentCache((projection.datasetName, segment.binaryPartition, version, segment.segmentId)) {
      val newSegInfo = segment.segInfo.basedOn(projection)
      stats.segmentIndexMissingRead()
      mergingStrategy.readSegmentForCache(projection, version)(newSegInfo)(readEc)
    }
  }

  private def updateCache(projection: RichProjection,
                          version: Int,
                          newSegment: Segment): Unit = {
    // NOTE: Spray caching doesn't have an update() method, so we have to delete and then repopulate. :(
    // TODO: consider if we need to lock the code below. Probably not since ColumnStore is single-writer but
    // we might want to update the spray-caching API to have an update method.
    val key = (projection.datasetName, newSegment.binaryPartition, version, newSegment.segmentId)
    segmentCache.remove(key)
    segmentCache(key)(mergingStrategy.pruneForCache(newSegment))
  }
}