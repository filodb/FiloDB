package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import org.velvia.filo.RowReader
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}

case class SegmentIndex[P, S](binPartition: Types.BinaryPartition,
                              segmentId: Types.SegmentId,
                              partition: P,
                              segment: S,
                              infosAndSkips: ChunkSetInfo.ChunkInfosAndSkips)

case class ChunkedData(column: Types.ColumnId, chunks: Seq[(Types.SegmentId, Types.ChunkID, ByteBuffer)])

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
   * Reads back all the chunks from multiple columns of a keyRange at once.  Note that no paging
   * is performed - so don't ask for too large of a range.  Recommendation is to read the indices
   * first and use the info there to determine how much to read.
   * @param dataset the DatasetRef of the dataset to read chunks from
   * @param columns the columns to read back chunks from
   * @param keyRange the binary range of segments to read from.  Note endExclusive flag.
   * @param version the version to read back
   * @return a sequence of ChunkedData, each triple is (segmentId, chunkId, bytes) for each chunk
   *          must be sorted in order of increasing segmentId
   */
  def readChunks(dataset: DatasetRef,
                 columns: Set[ColumnId],
                 keyRange: BinaryKeyRange,
                 version: Int)(implicit ec: ExecutionContext): Future[Seq[ChunkedData]]

  /**
   * Reads back a subset of chunkIds from a single segment, with columns returned in the same order
   * as passed in.  No paging is performed.  May be used to read back only row keys or part of a large seg.
   * @param dataset the DatasetRef of the dataset to read chunks from
   * @param version the version to read back
   * @param columns the columns to read back chunks from
   * @param chunkRange the inclusive start and end ChunkID to read from
   * @return a sequence of ChunkedData, each triple is (segmentId, chunkId, bytes) for each chunk
   *          must be sorted in order of increasing segmentId
   */
  def readChunks(dataset: DatasetRef,
                 version: Int,
                 columns: Seq[ColumnId],
                 partition: Types.BinaryPartition,
                 segment: Types.SegmentId,
                 chunkRange: (Types.ChunkID, Types.ChunkID))
                (implicit ec: ExecutionContext): Future[Seq[ChunkedData]]

  /**
   * Same as above, but only read back chunks corresponding to columns for row keys for a particular
   * chunkID in a segment
   */
  def readRowKeyChunks(projection: RichProjection,
                       version: Int,
                       chunk: Types.ChunkID)
                      (segInfo: SegmentInfo[projection.PK, projection.SK])
                      (implicit ec: ExecutionContext): Future[Array[ByteBuffer]] = {
    val binPartition = projection.partitionType.toBytes(segInfo.partition)
    val binSegId = projection.segmentType.toBytes(segInfo.segment)
    readChunks(projection.datasetRef, version, projection.rowKeyColumnIds,
               binPartition, binSegId, (chunk, chunk)).map { seqChunkedData =>
      seqChunkedData.map { case ChunkedData(col, segIdChunks) => segIdChunks.head._3 }.toArray
    }
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

  def toSegIndex(projection: RichProjection, binPart: BinaryPartition, segmentKey: SegmentId,
                 infosAndSkips: ChunkSetInfo.ChunkInfosAndSkips):
        SegmentIndex[projection.PK, projection.SK] = {
    SegmentIndex(binPart,
                 segmentKey,
                 projection.partitionType.fromBytes(binPart),
                 projection.segmentType.fromBytes(segmentKey),
                 infosAndSkips)
  }

  // NOTE: this is more or less a single-threaded implementation.  Reads of chunks for multiple columns
  // happen in parallel, but we still block to wait for all of them to come back.
  def scanSegments(projection: RichProjection,
                   columns: Seq[Column],
                   version: Int,
                   method: ScanMethod): Future[Iterator[Segment]] = {
    implicit val ec = readEc
    val segmentGroupSize = 3

    for { segmentIndexIt <- scanIndices(projection, version, method) }
    yield
    (for { // Group by partition key first
          (part, partChunkMaps) <- segmentIndexIt
                                     .sortedGroupBy { case SegmentIndex(part, _, _, _, _) => part }
          // Subdivide chunk row maps in each partition so we don't read more than we can chew
          // TODO: make a custom grouping function based on # of rows accumulated
          groupIndices <- partChunkMaps.grouped(segmentGroupSize) }
    yield {
      val indices = groupIndices.toSeq
      stats.incrReadSegments(indices.length)
      val binKeyRange = BinaryKeyRange(part,
                                       indices.head.segmentId,
                                       indices.last.segmentId,
                                       endExclusive = false)
      val chunks = Await.result(readChunks(projection.datasetRef, columns.map(_.name).toSet,
                                           binKeyRange, version), 5.minutes)
      buildSegments(projection, indices, chunks, columns).toIterator
    }).flatten
  }

  import scala.util.control.Breaks._

  private def buildSegments[P, S](projection: RichProjection,
                                  indices: Seq[SegmentIndex[P, S]],
                                  chunks: Seq[ChunkedData],
                                  schema: Seq[Column]): Seq[Segment] = {
    val segments = indices.map { case SegmentIndex(_, _, partition, segStart, infosAndSkips) =>
      val segInfo = SegmentInfo(partition, segStart)
      val chunkInfos = ChunkSetInfo.collectSkips(infosAndSkips)
      new RowReaderSegment(projection, segInfo, chunkInfos, schema)
    }
    chunks.foreach { case ChunkedData(columnName, chunkTriples) =>
      var segIndex = 0
      breakable {
        chunkTriples.foreach { case (segmentId, chunkId, chunkBytes) =>
          // Rely on the fact that chunks are sorted by segmentId, in the same order as the indices
          while (segmentId != indices(segIndex).segmentId) {
            segIndex += 1
            if (segIndex >= segments.length) {
              logger.warn(s"Chunks with segmentId=$segmentId (part ${indices.head.partition})" +
                           " with no index; corruption?")
              break
            }
          }
          segments(segIndex).addChunk(chunkId, columnName, chunkBytes)
        }
      }
    }
    segments
  }
}