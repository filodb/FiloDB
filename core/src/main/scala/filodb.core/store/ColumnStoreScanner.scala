package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import org.velvia.filo.RowReader
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.{Column, Projection, RichProjection}
import ChunkSetInfo.ChunkSkips

/**
 * Stores index info about each segment.  numChunks is the # of total (original) chunks in the segment.
 * infosAndSkips are the chunks that have been filtered by the query.
 */
final case class SegmentIndex[P, S](binPartition: Types.BinaryPartition,
                                    segmentId: Types.SegmentId,
                                    partition: P,
                                    segment: S,
                                    infosAndSkips: Array[(ChunkSetInfo, ChunkSkips)])

final case class ChunkedData(column: Types.ColumnId, chunks: Seq[(Types.ChunkID, ByteBuffer)])

/**
 * Encapsulates the reading and scanning logic of the ColumnStore.
 * We are careful to separate out ExecutionContext for reading only.
 */
trait ColumnStoreScanner extends StrictLogging {
  import filodb.core.Types._
  import filodb.core.Iterators._

  // Use a separate ExecutionContext for reading.  This is important to prevent deadlocks.
  // Also, we do not make this implicit so that this trait can be mixed in elsewhere.
  def readEc: ExecutionContext

  val stats = ColumnStoreStats()

  /**
   * Reads back a subset of chunks from a single segment, with columns returned in the same order
   * as passed in.  No paging is performed.  May be used to read back only row keys or part of a large seg.
   * @param dataset the DatasetRef of the dataset to read chunks from
   * @param version the version to read back
   * @param columns the columns to read back chunks from
   * @param partition the partition to read from
   * @param segment the segment to read from
   * @param key1    the inclusive start rowkey and chunkID to read from
   * @param key2    the inclusive end rowkey and chunkID to read from
   * @return a sequence of ChunkedData, each tuple is (chunkId, bytes) for each chunk
   */
  def readChunks(dataset: DatasetRef,
                 version: Int,
                 columns: Seq[ColumnId],
                 partition: Types.BinaryPartition,
                 segment: Types.SegmentId,
                 key1: (BinaryRecord, Types.ChunkID),
                 key2: (BinaryRecord, Types.ChunkID))
                (implicit ec: ExecutionContext): Future[Seq[ChunkedData]]

  /**
   * Same as above, but only read back chunks corresponding to columns for row keys for a particular
   * chunkID in a segment
   */
  def readRowKeyChunks(projection: RichProjection,
                       version: Int,
                       startKey: BinaryRecord,
                       chunkId: Types.ChunkID)
                      (segInfo: SegmentInfo[projection.PK, projection.SK])
                      (implicit ec: ExecutionContext): Future[Array[ByteBuffer]] = {
    val binPartition = projection.partitionType.toBytes(segInfo.partition)
    val binSegId = projection.segmentType.toBytes(segInfo.segment)
    val rowKeyCols = projection.rowKeyColumns.map(_.name)
    readChunks(projection.datasetRef, version, rowKeyCols, binPartition,
               binSegId, (startKey, chunkId), (startKey, chunkId)).map { seqChunkedData =>
      try {
        seqChunkedData.map { case ChunkedData(col, segIdChunks) => segIdChunks.head._2 }.toArray
      } catch {
        case e: NoSuchElementException =>
          logger.error(s"Error: no chunks for $segInfo / ($startKey, $chunkId), columns $rowKeyCols", e)
          throw e
      }
    }
  }

  /**
   * Reads back a subset of filters for ingestion row replacement / skip detection
   * @param dataset the DatasetRef of the dataset to read filters from
   * @param version the version to read back
   * @param chunkRange the inclusive start and end ChunkID to read from
   */
  def readFilters(dataset: DatasetRef,
                  version: Int,
                  partition: Types.BinaryPartition,
                  segment: Types.SegmentId,
                  chunkRange: (Types.ChunkID, Types.ChunkID))
                 (implicit ec: ExecutionContext): Future[Iterator[SegmentState.IDAndFilter]]

  def readFilters(projection: RichProjection,
                  version: Int,
                  chunkRange: (Types.ChunkID, Types.ChunkID))
                 (segInfo: SegmentInfo[projection.PK, projection.SK])
                 (implicit ec: ExecutionContext): Future[Iterator[SegmentState.IDAndFilter]] = {
    val binPartition = projection.partitionType.toBytes(segInfo.partition)
    val binSegId = projection.segmentType.toBytes(segInfo.segment)
    readFilters(projection.datasetRef, version, binPartition, binSegId, chunkRange)
  }

  /**
   * Scans over indices according to the method.
   * @return an iterator over SegmentIndex.
   */
  def scanIndices(projection: RichProjection,
                  version: Int,
                  method: ScanMethod)
                 (implicit ec: ExecutionContext):
    Future[Iterator[SegmentIndex[projection.PK, projection.SK]]]

  import BinaryRecord._   // Bring in Ordering[BinaryRecord]

  def toSegIndex(projection: RichProjection, binPart: BinaryPartition, segmentKey: SegmentId,
                 infosAndSkips: Array[(ChunkSetInfo, ChunkSkips)]):
        SegmentIndex[projection.PK, projection.SK] = {
    // unfortunately, Ordering is invariant in type, even though BinaryRecord mixes in RowReader
    implicit val keyOrder = projection.rowKeyType.rowReaderOrdering.asInstanceOf[Ordering[BinaryRecord]]
    val sortedInfos = infosAndSkips.sortBy(_._1.keyAndId)
    SegmentIndex(binPart,
                 segmentKey,
                 projection.partitionType.fromBytes(binPart),
                 projection.segmentType.fromBytes(segmentKey),
                 sortedInfos)
  }

  protected def filterSkips(projection: RichProjection,
                            skips: Array[(ChunkSetInfo, ChunkSkips)],
                            method: ScanMethod): Array[(ChunkSetInfo, ChunkSkips)] = method match {
    case SinglePartitionRowKeyScan(_, startKey, endKey) =>
      implicit val ordering = projection.rowKeyType.rowReaderOrdering
      if (ordering.lteq(startKey, endKey)) {
        skips.filter { case (info, skips) => info.intersection(startKey, endKey).isDefined }
      } else {  // If keys are reversed, do not even try!
        Array.empty
      }
    case other: Any =>
      skips
  }

  // NOTE: this is more or less a single-threaded implementation.  Reads of chunks for multiple columns
  // happen in parallel, but we still block to wait for all of them to come back.
  def scanSegments(projection: RichProjection,
                   columns: Seq[Column],
                   version: Int,
                   method: ScanMethod): Future[Iterator[Segment]] = {
    implicit val ec = readEc

    for { segmentIndexIt <- scanIndices(projection, version, method) }
    yield
    // NOTE: Need nested for's -- above is for Future, this is for an Iterator, cannot mix iteration types
    (for { index <- segmentIndexIt }
    yield {
      // Segmentless: read one segment index at a time, which may be a part of a segment
      stats.incrReadSegments(1)
      logger.trace(s"Chunk infos: ${index.infosAndSkips.map(_._1).toList}")
      val columnNames = columns.map(_.name)
      logger.debug(s"Reading partition ${index.partition} / ${index.segment}, " +
                   s"  ${index.infosAndSkips.size} ChunkInfos")
      if (index.infosAndSkips.isEmpty) {
        val segInfo = SegmentInfo(index.partition, index.segment)
        new RowReaderSegment(projection, segInfo, Nil, columns)
      } else {
        val chunks = Await.result(readChunks(projection.datasetRef, version, columnNames,
                                             index.binPartition, index.segmentId,
                                             index.infosAndSkips.head._1.keyAndId,
                                             index.infosAndSkips.last._1.keyAndId), 5.minutes)
        buildSegment(projection, index, chunks, columns)
      }
    })
  }

  private def buildSegment[P, S](projection: RichProjection,
                                 index: SegmentIndex[P, S],
                                 chunks: Seq[ChunkedData],
                                 schema: Seq[Column]): Segment = {
    val segInfo = SegmentInfo(index.partition, index.segment)
    val chunkInfos = ChunkSetInfo.collectSkips(index.infosAndSkips)
    val segment = new RowReaderSegment(projection, segInfo, chunkInfos, schema)
    chunks.foreach { case ChunkedData(columnName, idsAndBuffers) =>
      idsAndBuffers.foreach { case (chunkId, chunkBytes) =>
        segment.addChunk(chunkId, columnName, chunkBytes)
      }
    }
    segment
  }
}