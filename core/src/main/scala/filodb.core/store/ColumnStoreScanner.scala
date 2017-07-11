package filodb.core.store

import com.typesafe.scalalogging.StrictLogging
import java.nio.ByteBuffer
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.{Column, Projection, RichProjection}
import filodb.core.query.{ChunkSetReader, PartitionChunkIndex}
import ChunkSetReader._


trait ChunkPipeItem
case class ChunkPipeInfos(infosAndSkips: ChunkSetInfo.ChunkInfosAndSkips) extends ChunkPipeItem
case class SingleChunkInfo(id: Types.ChunkID, colNo: Int, bytes: ByteBuffer) extends ChunkPipeItem

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
   * Before the SingleChunkInfos for a given ID, a ChunkPipeInfos must be sent to set up the readers,
   * otherwise the SingleChunkInfos will be discarded.
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
                          chunkMethod: ChunkScanMethod): Observable[ChunkPipeItem]

  /**
   * Reads back a subset of filters for ingestion row replacement / skip detection
   * @param dataset the DatasetRef of the dataset to read filters from
   * @param version the version to read back
   * @param chunkRange the inclusive start and end ChunkID to read from
   */
  def readFilters(dataset: DatasetRef,
                  version: Int,
                  partition: Types.PartitionKey,
                  chunkRange: (Types.ChunkID, Types.ChunkID))
                 (implicit ec: ExecutionContext): Future[Iterator[SegmentState.IDAndFilter]]

  def readFilters(projection: RichProjection,
                  version: Int,
                  chunkRange: (Types.ChunkID, Types.ChunkID))
                 (segInfo: SegmentInfo[projection.PK, projection.SK])
                 (implicit ec: ExecutionContext): Future[Iterator[SegmentState.IDAndFilter]] = {
    readFilters(projection.datasetRef, version, segInfo.partition, chunkRange)
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
                 chunkMethod: ChunkScanMethod = AllChunkScan,
                 colToMaker: ColumnToMaker = defaultColumnToMaker): Observable[ChunkSetReader] = {
    scanPartitions(projection, version, partMethod)
      // Partitions to pipeline of single chunks
      .flatMap { partIndex =>
        stats.incrReadPartitions(1)
        readPartitionChunks(projection.datasetRef, version, columns, partIndex, chunkMethod)
      // Collate single chunks to ChunkSetReaders
      }.scan(new ChunkSetReaderAggregator(columns, stats, colToMaker)) { _ add _ }
      .collect { case agg: ChunkSetReaderAggregator if agg.canEmit => agg.emit() }
  }
}

private[store] class ChunkSetReaderAggregator(schema: Seq[Column],
                                              stats: ColumnStoreStats,
                                              colToMaker: ColumnToMaker = defaultColumnToMaker) {
  val readers = new NonBlockingHashMapLong[ChunkSetReader](32, false)
  val makers = schema.map(colToMaker).toArray
  var emitReader: Option[ChunkSetReader] = None

  def canEmit: Boolean = emitReader.isDefined

  def emit(): ChunkSetReader = {
    val reader = emitReader.get
    emitReader = None
    reader
  }

  def add(pipeItem: ChunkPipeItem): ChunkSetReaderAggregator = {
    pipeItem match {
      case SingleChunkInfo(id, colNo, buf) =>
        readers.get(id) match {
          //scalastyle:off
          case null =>
          //scalastyle:on
            stats.incrChunkWithNoInfo()
          case reader: ChunkSetReader =>
            reader.addChunk(colNo, buf)
            if (reader.isFull) {
              readers.remove(id)
              stats.incrReadChunksets()
              emitReader = Some(reader)
            }
        }
      case ChunkPipeInfos(infos) =>
        infos.foreach { case (info, skips) =>
          readers.put(info.id, new ChunkSetReader(info, skips, makers))
        }
    }
    this
  }
}

