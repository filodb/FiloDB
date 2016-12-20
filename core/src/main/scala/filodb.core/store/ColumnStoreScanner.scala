package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import monix.reactive.Observable
import scala.collection.mutable.{HashMap, ArrayBuffer}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.{Column, Projection, RichProjection}
import filodb.core.query.{ChunkSetReader, PartitionChunkIndex, RowkeyPartitionChunkIndex}

case class SingleChunkInfo(info: ChunkSetInfo, skips: Array[Int], colNo: Int, bytes: ByteBuffer)

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
   * Reads back chunks from a partition as an observable of SingleChunkInfos
   * Must not feed extraneous chunks (say from a range scan).
   * Think carefully about concurrency here - do work on other threads to be nonblocking
   * @param dataset the DatasetRef of the dataset to read chunks from
   * @param version the version to read back
   * @param columns the columns to read back chunks from
   * @param partitionIndex the PartitionChunkIndex containing chunk metadata for the partition
   * @param chunkMethod the method or filter for scanning the partition
   */
  def readPartitionChunks(dataset: DatasetRef,
                          version: Int,
                          columns: Seq[Column],
                          partitionIndex: PartitionChunkIndex,
                          chunkMethod: ChunkScanMethod): Observable[SingleChunkInfo]

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
   * @return an Observable over PartitionChunkIndex
   */
  def scanPartitions(projection: RichProjection,
                     version: Int,
                     partMethod: PartitionScanMethod): Observable[PartitionChunkIndex]

  def readChunks(projection: RichProjection,
                 columns: Seq[Column],
                 version: Int,
                 partMethod: PartitionScanMethod,
                 chunkMethod: ChunkScanMethod = AllChunkScan): Observable[ChunkSetReader] = {
    val readers = new HashMap[ChunkID, ChunkSetReader]
    val clazzes = columns.map(_.columnType.clazz).toArray

    scanPartitions(projection, version, partMethod)
      // Partitions to pipeline of single chunks
      .flatMap { partIndex =>
        stats.incrReadPartitions(1)
        readPartitionChunks(projection.datasetRef, version, columns, partIndex, chunkMethod)
      // Collate single chunks to ChunkSetReaders
      }.flatMap { case SingleChunkInfo(info, skips, colNo, buf) =>
        // TODO: use an Operator if this turns out to be a bottleneck
        val reader = readers.getOrElseUpdate(info.id, new ChunkSetReader(info, skips, clazzes))
        reader.addChunk(colNo, buf)
        if (reader.isFull) {
          readers.remove(info.id)
          Observable.now(reader)
        } else { Observable.empty }
      }
  }
}
