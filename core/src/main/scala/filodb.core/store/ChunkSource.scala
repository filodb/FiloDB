package filodb.core.store

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.reactive.Observable

import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.Types.PartitionKey
import filodb.core.metadata.{Column, Projection, RichProjection, InvalidFunctionSpec}
import filodb.core.query.{Aggregate, ChunkSetReader, MutableChunkSetReader, PartitionChunkIndex}
import ChunkSetReader._

/**
 * ChunkSource is the base trait for a source of chunks given a `PartitionScanMethod` and a
 * `ChunkScanMethod`.  It is the basis for querying and reading out of raw chunks.
 */
trait ChunkSource {
  def stats: ChunkSourceStats

  /**
   * Determines how to split the scanning of a dataset across a columnstore.
   * Used only for something like Spark that has to distribute scans across a cluster.
   * @param dataset the name of the dataset to determine splits for
   * @param splitsPerNode the number of splits to target per node.  May not actually be possible.
   * @return a Seq[ScanSplit]
   */
  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit]

  /**
   * Scans and returns partitions according to the method.  Note that the Partition returned may not
   * contain any data; an additional streamReaders method must be invoked to actually return chunks.
   * This allows data to be returned lazily for systems like Cassandra.
   * @return an Observable over FiloPartition
   */
  def scanPartitions(projection: RichProjection,
                     version: Int,
                     partMethod: PartitionScanMethod,
                     colToMaker: ColumnToMaker = defaultColumnToMaker): Observable[FiloPartition]

  /**
   * Reads chunks from a dataset and returns an Observable of chunk readers.
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
                 colToMaker: ColumnToMaker = defaultColumnToMaker): Observable[ChunkSetReader] = {
    val positions = projection.getPositions(columns)
    scanPartitions(projection, version, partMethod, colToMaker)
      .flatMap { partition =>
        stats.incrReadPartitions(1)
        partition.streamReaders(chunkMethod, positions)
      }
  }
}

/**
 * Statistics for a ChunkSource.  Some of this is used by unit tests.
 */
class ChunkSourceStats {
  private val readPartitionsCtr  = Kamon.metrics.counter("read-partitions")
  private val readChunksetsCtr   = Kamon.metrics.counter("read-chunksets")
  private val chunkNoInfoCtr     = Kamon.metrics.counter("read-chunks-with-no-info")
  var readChunkSets: Int = 0
  var readPartitions: Int = 0

  def incrReadPartitions(numPartitions: Int): Unit = {
    readPartitionsCtr.increment(numPartitions)
    readPartitions += numPartitions
  }

  def incrReadChunksets(): Unit = {
    readChunksetsCtr.increment
    readChunkSets += 1
  }

  def incrChunkWithNoInfo(): Unit = { chunkNoInfoCtr.increment }
}

import java.nio.ByteBuffer
case class SingleChunkInfo(id: Types.ChunkID, colNo: Int, bytes: ByteBuffer)
