package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import org.velvia.filo.RowReader
import org.velvia.filo.RowReader.TypedFieldExtractor
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}

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

  /**
   * Reads back all the chunks from multiple columns of a keyRange at once.  Note that no paging
   * is performed - so don't ask for too large of a range.  Recommendation is to read the ChunkRowMaps
   * first and use the info there to determine how much to read.
   * @param dataset the name of the dataset to read chunks from
   * @param columns the columns to read back chunks from
   * @param keyRange the binary range of segments to read from.  Note endExclusive flag.
   * @param version the version to read back
   * @return a sequence of ChunkedData, each triple is (segmentId, chunkId, bytes) for each chunk
   *          must be sorted in order of increasing segmentId
   */
  def readChunks(dataset: TableName,
                 columns: Set[ColumnId],
                 keyRange: BinaryKeyRange,
                 version: Int)(implicit ec: ExecutionContext): Future[Seq[ChunkedData]]

  type ChunkMapInfo = (BinaryPartition, SegmentId, BinaryChunkRowMap)

  /**
   * Reads back all the ChunkRowMaps from multiple segments in a keyRange.
   * @param keyRange the range of segments to read from, note [start, end) <-- end is exclusive!
   * @param version the version to read back
   * @return a sequence of (segmentId, ChunkRowMap)'s
   */
  def readChunkRowMaps(dataset: TableName,
                       keyRange: BinaryKeyRange,
                       version: Int)(implicit ec: ExecutionContext): Future[Iterator[ChunkMapInfo]]

  /**
   * Designed to scan over many many ChunkRowMaps from multiple partitions.  Intended for fast scanning
   * of many segments, such as reading all the data for one node from a Spark worker.
   * TODO: how to make passing stuff such as token ranges nicer, but still generic?
   *
   * @return an iterator over ChunkRowMaps.  Must be sorted by partitionkey first, then segment Id.
   */
  def scanChunkRowMaps(dataset: TableName,
                       version: Int,
                       params: Map[String, String])
                      (implicit ec: ExecutionContext): Future[Iterator[ChunkMapInfo]]

  case class SegmentIndex[P, S](binPartition: BinaryPartition,
                                segmentId: SegmentId,
                                partition: P,
                                segment: S,
                                chunkMap: BinaryChunkRowMap)

  def toSegIndex(projection: RichProjection, chunkMapInfo: ChunkMapInfo):
        SegmentIndex[projection.PK, projection.SK] = {
    SegmentIndex(chunkMapInfo._1,
                 chunkMapInfo._2,
                 projection.partitionType.fromBytes(chunkMapInfo._1),
                 projection.segmentType.fromBytes(chunkMapInfo._2),
                 chunkMapInfo._3)
  }

  def readSegments(projection: RichProjection,
                   columns: Seq[Column],
                   version: Int)
                  (keyRange: KeyRange[projection.PK, projection.SK]): Future[Iterator[Segment]] = {
    implicit val ec = readEc
    val binKeyRange = projection.toBinaryKeyRange(keyRange)
    (for { rowMaps <- readChunkRowMaps(projection.datasetName, binKeyRange, version)
           chunks  <- readChunks(projection.datasetName,
                                 columns.map(_.name).toSet,
                                 binKeyRange,
                                 version) if rowMaps.nonEmpty }
    yield {
      val indexMaps = rowMaps.map(crm => toSegIndex(projection, crm))
      buildSegments(projection, indexMaps.toSeq, chunks, columns).toIterator
    }).recover {
      // No chunk maps found, so just return empty list of segments
      case e: java.util.NoSuchElementException => Iterator.empty
    }
  }

  // NOTE: this is more or less a single-threaded implementation.  Reads of chunks for multiple columns
  // happen in parallel, but we still block to wait for all of them to come back.
  def scanSegments(projection: RichProjection,
                   columns: Seq[Column],
                   version: Int,
                   params: Map[String, String] = Map.empty)
                  (partitionFilter: (projection.PK => Boolean) = (x: projection.PK) => true):
                     Future[Iterator[Segment]] = {
    implicit val ec = readEc
    val segmentGroupSize = params.getOrElse("segment_group_size", "3").toInt

    for { chunkmapsIt <- scanChunkRowMaps(projection.datasetName, version, params) }
    yield
    (for { // Group by partition key first
          (part, partChunkMaps) <- chunkmapsIt.map(crm => toSegIndex(projection, crm))
                                     .filter { case SegmentIndex(_, _, part, _, _) => partitionFilter(part) }
                                     .sortedGroupBy { case SegmentIndex(part, _, _, _, _) => part }
          // Subdivide chunk row maps in each partition so we don't read more than we can chew
          // TODO: make a custom grouping function based on # of rows accumulated
          groupChunkMaps <- partChunkMaps.grouped(segmentGroupSize) }
    yield {
      val chunkMaps = groupChunkMaps.toSeq
      val binKeyRange = BinaryKeyRange(part,
                                       chunkMaps.head.segmentId,
                                       chunkMaps.last.segmentId,
                                       endExclusive = false)
      val chunks = Await.result(readChunks(projection.datasetName, columns.map(_.name).toSet,
                                           binKeyRange, version), 5.minutes)
      buildSegments(projection, chunkMaps, chunks, columns).toIterator
    }).flatten
  }

  import scala.util.control.Breaks._

  private def buildSegments[P, S](projection: RichProjection,
                                  rowMaps: Seq[SegmentIndex[P, S]],
                                  chunks: Seq[ChunkedData],
                                  schema: Seq[Column]): Seq[Segment] = {
    val segments = rowMaps.map { case SegmentIndex(_, _, partition, segStart, rowMap) =>
      val segInfo = SegmentInfo(partition, segStart)
      new RowReaderSegment(projection, segInfo, rowMap, schema)
    }
    chunks.foreach { case ChunkedData(columnName, chunkTriples) =>
      var segIndex = 0
      breakable {
        chunkTriples.foreach { case (segmentId, chunkId, chunkBytes) =>
          // Rely on the fact that chunks are sorted by segmentId, in the same order as the rowMaps
          while (segmentId != rowMaps(segIndex).segmentId) {
            segIndex += 1
            if (segIndex >= segments.length) {
              logger.warn(s"Chunks with segmentId=$segmentId (part ${rowMaps.head.partition})" +
                           " with no rowmap; corruption?")
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